"""面向一组日期的原子暂存、发布与回查。

一个发布组把多个自然日的候选快照绑定为整体：暂存与发布都在单个事务内完成，
任一日期的 head 已变化或任一候选未通过校验时整组回滚，不会部分生效。
"""
from __future__ import annotations

import sqlite3
import uuid

from exhibit_lake.catalog.registry import iso_day, now, require_id
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import InvalidInput, NotFound, StateError
from exhibit_lake.publish.snapshots import Snapshots


class PublishGroups:
    def __init__(self, store: Store, snapshots: Snapshots):
        self.store = store
        self.snapshots = snapshots

    @staticmethod
    def _days(days: list[str]) -> list[str]:
        if not isinstance(days, list) or len(days) < 2:
            raise InvalidInput("a publish group needs at least two days")
        ordered = sorted({iso_day(day, "local_day") for day in days})
        if len(ordered) != len(days):
            raise InvalidInput("publish group days must be distinct")
        return ordered

    @staticmethod
    def _expected(expected_heads: dict[str, str | None] | None, day: str) -> str | None:
        if expected_heads is None:
            return None
        if not isinstance(expected_heads, dict):
            raise InvalidInput("expected_heads must map local_day to head id")
        return expected_heads.get(day)

    @staticmethod
    def _check_expected_keys(expected_heads: dict[str, str | None] | None, days: list[str]) -> None:
        if expected_heads is None:
            return
        if not isinstance(expected_heads, dict):
            raise InvalidInput("expected_heads must map local_day to head id")
        unknown = sorted(set(expected_heads) - set(days))
        if unknown:
            raise InvalidInput("expected_heads refer to days outside the group", details={"days": unknown})

    def stage(self, venue_id: str, days: list[str],
              expected_heads: dict[str, str | None] | None = None) -> dict:
        ordered = self._days(days)
        self._check_expected_keys(expected_heads, ordered)
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            group_id = uuid.uuid4().hex
            members = []
            for day in ordered:
                expected = self._expected(expected_heads, day)
                snapshot_id = self.snapshots._stage(conn, venue_id, day, expected)
                members.append((day, snapshot_id, expected))
            conn.execute("INSERT INTO publish_groups VALUES(?,?,?,?,NULL)",
                         (group_id, venue_id, "staged", now()))
            conn.executemany("INSERT INTO publish_group_members VALUES(?,?,?,?)",
                             [(group_id, day, snapshot_id, expected) for day, snapshot_id, expected in members])
            Store.audit(conn, "group.staged", group_id,
                        {"venue_id": venue_id, "days": ordered}, now())
            return self._describe(conn, group_id)

    def publish(self, group_id: str, expected_heads: dict[str, str | None] | None = None) -> dict:
        members = self._members_for_publish(group_id, expected_heads)
        for snapshot_id, _ in members.values():
            self.snapshots.verify(snapshot_id)
        with self.store.transaction() as conn:
            group = self._group(conn, group_id)
            if group["status"] == "published":
                return self._describe(conn, group_id)
            if group["status"] != "staged":
                raise StateError("only staged publish groups can be published")
            stamp = now()
            for day in sorted(members):
                snapshot_id, expected = members[day]
                snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
                part = self.snapshots._partition(conn, snapshot["venue_id"], day)
                self.snapshots._apply_publish(conn, snapshot, part, expected, stamp)
            conn.execute("UPDATE publish_groups SET status='published',published_at=? WHERE group_id=?",
                         (stamp, group_id))
            Store.audit(conn, "group.published", group_id,
                        {"venue_id": group["venue_id"], "days": sorted(members)}, stamp)
            return self._describe(conn, group_id)

    def _members_for_publish(self, group_id: str,
                             expected_heads: dict[str, str | None] | None) -> dict[str, tuple[str, str | None]]:
        require_id(group_id, "group_id")
        if expected_heads is not None and not isinstance(expected_heads, dict):
            raise InvalidInput("expected_heads must map local_day to head id")
        with self.store.transaction(write=False) as conn:
            group = self._group(conn, group_id)
            if group["status"] == "published":
                return {}
            if group["status"] != "staged":
                raise StateError("only staged publish groups can be published")
            rows = Store.many(conn, "SELECT local_day,snapshot_id,expected_head FROM publish_group_members "
                              "WHERE group_id=? ORDER BY local_day", (group_id,))
            self._check_expected_keys(expected_heads, [row["local_day"] for row in rows])
            return {row["local_day"]: (row["snapshot_id"],
                                       expected_heads.get(row["local_day"], row["expected_head"])
                                       if expected_heads is not None else row["expected_head"])
                    for row in rows}

    def abandon(self, group_id: str) -> dict:
        require_id(group_id, "group_id")
        with self.store.transaction() as conn:
            group = self._group(conn, group_id)
            if group["status"] != "staged":
                raise StateError("only staged publish groups can be abandoned")
            rows = Store.many(conn, "SELECT snapshot_id FROM publish_group_members WHERE group_id=?", (group_id,))
            stamp = now()
            for row in rows:
                conn.execute("UPDATE snapshots SET status='abandoned' WHERE snapshot_id=? AND status='staged'",
                             (row["snapshot_id"],))
                Store.audit(conn, "snapshot.abandoned", row["snapshot_id"], {"group_id": group_id}, stamp)
            conn.execute("UPDATE publish_groups SET status='abandoned' WHERE group_id=?", (group_id,))
            Store.audit(conn, "group.abandoned", group_id, {"venue_id": group["venue_id"]}, stamp)
            return self._describe(conn, group_id)

    def _group(self, conn: sqlite3.Connection, group_id: str) -> dict:
        group = Store.one(conn, "SELECT * FROM publish_groups WHERE group_id=?", (group_id,))
        if not group:
            raise NotFound("publish group not found")
        return group

    def _describe(self, conn: sqlite3.Connection, group_id: str) -> dict:
        group = self._group(conn, group_id)
        rows = Store.many(conn, "SELECT m.local_day,m.snapshot_id,m.expected_head,s.generation,s.parent_id,s.status,s.digest "
                          "FROM publish_group_members m JOIN snapshots s ON s.snapshot_id=m.snapshot_id "
                          "WHERE m.group_id=? ORDER BY m.local_day", (group_id,))
        return {"group_id": group_id, "venue_id": group["venue_id"], "status": group["status"],
                "created_at": group["created_at"], "published_at": group["published_at"],
                "days": [row["local_day"] for row in rows],
                "members": [{"local_day": row["local_day"], "snapshot_id": row["snapshot_id"],
                             "expected_head": row["expected_head"], "generation": row["generation"],
                             "parent_id": row["parent_id"], "status": row["status"], "digest": row["digest"]}
                            for row in rows]}

    def describe(self, group_id: str) -> dict:
        require_id(group_id, "group_id")
        with self.store.transaction(write=False) as conn:
            return self._describe(conn, group_id)

    def list(self, venue_id: str, local_day: str | None = None) -> list[dict]:
        require_id(venue_id, "venue_id")
        if local_day is not None:
            iso_day(local_day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            if local_day is None:
                ids = [row["group_id"] for row in Store.many(
                    conn, "SELECT group_id FROM publish_groups WHERE venue_id=? ORDER BY created_at,group_id", (venue_id,))]
            else:
                ids = [row["group_id"] for row in Store.many(
                    conn, "SELECT g.group_id FROM publish_groups g JOIN publish_group_members m ON m.group_id=g.group_id "
                    "WHERE g.venue_id=? AND m.local_day=? ORDER BY g.created_at,g.group_id", (venue_id, local_day))]
            return [self._describe(conn, group_id) for group_id in ids]
