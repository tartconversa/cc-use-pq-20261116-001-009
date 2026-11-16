"""检查待发布候选并安全清理临时状态。"""
from __future__ import annotations

from exhibit_lake.catalog.registry import iso_day, now
from exhibit_lake.catalog.store import Store
from exhibit_lake.errors import NotFound, StateError
from exhibit_lake.publish.snapshots import Snapshots


class Recovery:
    def __init__(self, store: Store, snapshots: Snapshots):
        self.store = store
        self.snapshots = snapshots

    def inspect(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction(write=False) as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            staged = Store.many(conn, "SELECT s.snapshot_id,s.parent_id,s.generation,s.digest,s.created_at,"
                                "g.group_id AS group_id FROM snapshots s "
                                "LEFT JOIN publish_group_members m ON m.snapshot_id=s.snapshot_id "
                                "LEFT JOIN publish_groups g ON g.group_id=m.group_id AND g.status='staged' "
                                "WHERE s.venue_id=? AND s.local_day=? AND s.status='staged' ORDER BY s.created_at", (venue_id, day))
            abandoned = Store.many(conn, "SELECT snapshot_id,parent_id,generation,created_at FROM snapshots "
                                   "WHERE venue_id=? AND local_day=? AND status='abandoned' ORDER BY created_at", (venue_id, day))
            head = part["head_id"] if part else None
            return {"venue_id": venue_id, "local_day": day, "head_id": head,
                    "generation": part["generation"] if part else 0,
                    "candidates": [{**item, "publishable": item["group_id"] is None and
                                    item["parent_id"] == head and
                                    item["generation"] == (part["generation"] if part else 0) + 1}
                                   for item in staged], "abandoned": abandoned}

    def abandon_stale(self, venue_id: str, day: str) -> dict:
        iso_day(day, "local_day")
        with self.store.transaction() as conn:
            if not Store.one(conn, "SELECT venue_id FROM venues WHERE venue_id=?", (venue_id,)):
                raise NotFound("venue not found")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?", (venue_id, day))
            head = part["head_id"] if part else None
            generation = part["generation"] if part else 0
            staged = Store.many(conn, "SELECT s.snapshot_id,s.parent_id,s.generation,g.group_id AS group_id FROM snapshots s "
                                "LEFT JOIN publish_group_members m ON m.snapshot_id=s.snapshot_id "
                                "LEFT JOIN publish_groups g ON g.group_id=m.group_id AND g.status='staged' "
                                "WHERE s.venue_id=? AND s.local_day=? AND s.status='staged'", (venue_id, day))
            stale = [row["snapshot_id"] for row in staged if row["group_id"] is None and
                     (row["parent_id"] != head or row["generation"] != generation + 1)]
            skipped = sorted(row["snapshot_id"] for row in staged if row["group_id"] is not None)
            for snapshot_id in stale:
                conn.execute("UPDATE snapshots SET status='abandoned' WHERE snapshot_id=?", (snapshot_id,))
                Store.audit(conn, "snapshot.abandoned_stale", snapshot_id,
                            {"head_id": head, "generation": generation}, now())
            return {"venue_id": venue_id, "local_day": day, "abandoned_ids": sorted(stale),
                    "group_bound_ids": skipped, "head_id": head, "generation": generation}

    def candidate_diagnostics(self, snapshot_id: str) -> dict:
        with self.store.transaction(write=False) as conn:
            snapshot = Store.one(conn, "SELECT * FROM snapshots WHERE snapshot_id=?", (snapshot_id,))
            if not snapshot:
                raise NotFound("snapshot not found")
            part = Store.one(conn, "SELECT * FROM partitions WHERE venue_id=? AND local_day=?",
                             (snapshot["venue_id"], snapshot["local_day"]))
            current_head = part["head_id"] if part else None
            current_generation = part["generation"] if part else 0
            membership = Store.one(conn, "SELECT COUNT(*) AS n FROM snapshot_members WHERE snapshot_id=?",
                                   (snapshot_id,))["n"]
            bound = Store.one(conn, "SELECT g.group_id FROM publish_group_members m JOIN publish_groups g "
                              "ON g.group_id=m.group_id WHERE m.snapshot_id=? AND g.status='staged'", (snapshot_id,))
            group_id = bound["group_id"] if bound else None
            return {"snapshot_id": snapshot_id, "status": snapshot["status"],
                    "parent_id": snapshot["parent_id"], "current_head": current_head,
                    "generation": snapshot["generation"], "current_generation": current_generation,
                    "members": membership, "group_id": group_id,
                    "head_matches": snapshot["parent_id"] == current_head,
                    "generation_matches": snapshot["generation"] == current_generation + 1,
                    "publishable": snapshot["status"] == "staged" and group_id is None and
                    snapshot["parent_id"] == current_head and snapshot["generation"] == current_generation + 1}
