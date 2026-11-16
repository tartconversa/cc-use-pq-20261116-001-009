import tempfile
import unittest
from pathlib import Path

from fastapi.testclient import TestClient

from exhibit_lake.api import create_app
from exhibit_lake.errors import Conflict, IntegrityFailure, InvalidInput, NotFound, StateError
from exhibit_lake.service import ExhibitLake

DAY1 = "2026-02-28"
DAY2 = "2026-03-01"


class PublishGroupTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.lake = ExhibitLake(Path(self.folder.name) / "lake.db")
        self.lake.register_venue("venue", "Test Venue", "Asia/Shanghai")
        self.lake.register_exhibit("robot", "venue", "Robot", "Maker", "2026-01-01")
        self.lake.register_source("counter", "venue", "visit")
        self.lake.register_source("kiosk", "venue", "feedback")
        self.lake.register_contract("counter-v1", "counter",
                                    ["event_id", "exhibit_id", "event_at", "count"],
                                    "2026-01-01T00:00:00+00:00")
        self.lake.register_contract("kiosk-v1", "kiosk",
                                    ["event_id", "exhibit_id", "event_at", "rating", "visitor_id"],
                                    "2026-01-01T00:00:00+00:00")

    def add_visits(self, key, day, count=5):
        return self.lake.ingest("counter", "counter-v1", key, [
            {"event_id": key, "exhibit_id": "robot", "event_at": f"{day}T12:00:00+08:00", "count": count}])

    def add_feedback(self, key, day, rating=4):
        return self.lake.ingest("kiosk", "kiosk-v1", key, [
            {"event_id": key, "exhibit_id": "robot", "event_at": f"{day}T13:00:00+08:00",
             "rating": rating, "visitor_id": "person"}])

    def stage_group(self, days=(DAY1, DAY2), expected=None):
        return self.lake.groups.stage("venue", list(days), expected)

    def publish_group(self, days=(DAY1, DAY2), expected=None):
        return self.lake.groups.publish(self.stage_group(days, expected)["group_id"])

    def head(self, day):
        return self.lake.snapshots.head("venue", day)["head_id"]

    def test_group_publish_is_atomic_across_days(self):
        self.add_visits("v1", DAY1, 5)
        self.add_visits("v2", DAY2, 7)
        self.add_feedback("f2", DAY2)
        group = self.publish_group()
        self.assertEqual(group["status"], "published")
        self.assertEqual(group["days"], [DAY1, DAY2])
        self.assertEqual({m["local_day"] for m in group["members"]}, {DAY1, DAY2})
        self.assertTrue(all(m["status"] == "published" for m in group["members"]))
        self.assertEqual({self.head(DAY1), self.head(DAY2)},
                         {m["snapshot_id"] for m in group["members"]})
        report = self.lake.analytics.group_report(group["group_id"])
        self.assertEqual(report["group_id"], group["group_id"])
        self.assertEqual([d["local_day"] for d in report["days"]], [DAY1, DAY2])
        self.assertEqual(report["total_visitors"], 12)
        self.assertEqual(report["total_feedback"], 1)
        self.assertEqual(report["event_count"], 3)
        self.assertEqual(report["snapshot_ids"], [m["snapshot_id"] for m in group["members"]])

    def test_group_report_is_stable_when_single_day_republishes(self):
        self.add_visits("v1", DAY1, 5)
        self.add_visits("v2", DAY2, 7)
        group = self.publish_group()
        members = {m["local_day"]: m["snapshot_id"] for m in group["members"]}
        self.add_visits("v1b", DAY1, 3)
        self.lake.publish_day("venue", DAY1, members[DAY1])
        report = self.lake.analytics.group_report(group["group_id"])
        self.assertEqual(report["snapshot_ids"], [members[DAY1], members[DAY2]])
        self.assertEqual(report["days"][0]["generation"], 1)
        self.assertEqual(report["total_visitors"], 12)
        trend = self.lake.analytics.trend("venue", DAY1, DAY2)
        self.assertEqual(trend["days"][0]["generation"], 2)
        self.assertEqual(trend["days"][1]["generation"], 1)

    def test_group_publish_fails_when_any_candidate_invalid(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        group = self.stage_group()
        victim = [m for m in group["members"] if m["local_day"] == DAY2][0]
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE snapshot_members SET payload_hash='bad' WHERE snapshot_id=?",
                         (victim["snapshot_id"],))
        with self.assertRaises(IntegrityFailure):
            self.lake.groups.publish(group["group_id"])
        self.assertEqual(self.lake.groups.describe(group["group_id"])["status"], "staged")
        self.assertIsNone(self.head(DAY1))
        self.assertIsNone(self.head(DAY2))
        for member in group["members"]:
            self.assertEqual(self.lake.snapshots.describe(member["snapshot_id"])["status"], "staged")

    def test_group_publish_rolls_back_when_a_head_moves(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        group = self.stage_group()
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE partitions SET head_id='ghost',generation=7 WHERE venue_id='venue' AND local_day=?",
                         (DAY2,))
        with self.assertRaises(Conflict):
            self.lake.groups.publish(group["group_id"])
        self.assertEqual(self.lake.groups.describe(group["group_id"])["status"], "staged")
        self.assertIsNone(self.head(DAY1))
        self.assertEqual(self.head(DAY2), "ghost")
        self.assertEqual(self.lake.snapshots.head("venue", DAY2)["generation"], 7)
        for member in group["members"]:
            self.assertEqual(self.lake.snapshots.describe(member["snapshot_id"])["status"], "staged")

    def test_group_bound_snapshot_cannot_publish_or_abandon_alone(self):
        self.add_visits("v1", DAY1)
        group = self.stage_group()
        member = group["members"][0]["snapshot_id"]
        with self.assertRaises(StateError) as published:
            self.lake.snapshots.publish(member, None)
        self.assertEqual(published.exception.details["group_id"], group["group_id"])
        with self.assertRaises(StateError):
            self.lake.snapshots.abandon(member)
        self.assertEqual(self.lake.snapshots.describe(member)["status"], "staged")

    def test_abandon_group_releases_days_for_single_publish(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        group = self.stage_group()
        abandoned = self.lake.groups.abandon(group["group_id"])
        self.assertEqual(abandoned["status"], "abandoned")
        for member in abandoned["members"]:
            self.assertEqual(member["status"], "abandoned")
        with self.assertRaises(StateError):
            self.lake.groups.publish(group["group_id"])
        published = self.lake.publish_day("venue", DAY1)
        self.assertEqual(self.head(DAY1), published["snapshot_id"])
        self.assertIsNone(self.head(DAY2))

    def test_group_publish_is_idempotent(self):
        self.add_visits("v1", DAY1)
        group = self.publish_group()
        again = self.lake.groups.publish(group["group_id"])
        self.assertEqual(again["group_id"], group["group_id"])
        self.assertEqual(again["status"], "published")
        self.assertEqual(again["published_at"], group["published_at"])

    def test_stage_group_validation(self):
        with self.assertRaises(InvalidInput):
            self.stage_group([DAY1])
        with self.assertRaises(InvalidInput):
            self.stage_group([DAY1, DAY1])
        with self.assertRaises(InvalidInput):
            self.stage_group([DAY1, "2026-13-01"])
        with self.assertRaises(InvalidInput):
            self.stage_group([DAY1, DAY2], {DAY1: None, "2026-04-01": None})
        with self.assertRaises(NotFound):
            self.lake.groups.stage("missing", [DAY1, DAY2])

    def test_stage_group_head_mismatch_stages_nothing(self):
        self.add_visits("v1", DAY1)
        published = self.lake.publish_day("venue", DAY1)
        with self.assertRaises(Conflict):
            self.stage_group([DAY1, DAY2], {DAY1: None})
        self.assertEqual(self.lake.snapshots.list("venue", DAY2, include_staged=True), [])
        self.assertEqual(self.lake.groups.list("venue"), [])
        group = self.stage_group([DAY1, DAY2], {DAY1: published["snapshot_id"]})
        self.assertEqual(group["status"], "staged")

    def test_single_candidate_blocks_group_staging_until_resolved(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        single = self.lake.snapshots.stage("venue", DAY1)
        with self.assertRaises(Conflict):
            self.stage_group()
        self.assertEqual(self.lake.snapshots.list("venue", DAY2, include_staged=True), [])
        self.lake.snapshots.publish(single["snapshot_id"], None)
        group = self.stage_group(expected={DAY1: single["snapshot_id"]})
        published = self.lake.groups.publish(group["group_id"])
        day1 = [m for m in published["members"] if m["local_day"] == DAY1][0]
        self.assertEqual(day1["parent_id"], single["snapshot_id"])
        self.assertEqual(day1["generation"], 2)
        self.assertEqual(self.head(DAY1), day1["snapshot_id"])
        self.assertIsNotNone(self.head(DAY2))

    def test_lookback_from_snapshot_group_and_recovery(self):
        self.add_visits("v1", DAY1)
        group = self.stage_group()
        member = group["members"][0]
        self.assertEqual(self.lake.snapshots.describe(member["snapshot_id"])["group_id"], group["group_id"])
        inspected = self.lake.recovery.inspect("venue", DAY1)["candidates"][0]
        self.assertEqual(inspected["group_id"], group["group_id"])
        self.assertFalse(inspected["publishable"])
        diagnostics = self.lake.recovery.candidate_diagnostics(member["snapshot_id"])
        self.assertEqual(diagnostics["group_id"], group["group_id"])
        self.assertFalse(diagnostics["publishable"])
        self.lake.groups.publish(group["group_id"])
        self.assertEqual(self.lake.groups.list("venue")[0]["group_id"], group["group_id"])
        self.assertEqual(self.lake.groups.list("venue", local_day=DAY2)[0]["group_id"], group["group_id"])
        self.assertEqual(self.lake.groups.list("venue", local_day="2026-04-01"), [])
        actions = [x["action"] for x in self.lake.audit.trail(limit=100)["audits"]]
        self.assertIn("group.staged", actions)
        self.assertIn("group.published", actions)

    def test_recovery_abandon_stale_skips_group_bound_candidates(self):
        self.add_visits("v1", DAY1)
        group = self.stage_group()
        member = [m for m in group["members"] if m["local_day"] == DAY1][0]
        with self.lake.store.transaction() as conn:
            conn.execute("UPDATE partitions SET head_id='ghost',generation=3 WHERE venue_id='venue' AND local_day=?",
                         (DAY1,))
        result = self.lake.recovery.abandon_stale("venue", DAY1)
        self.assertEqual(result["abandoned_ids"], [])
        self.assertEqual(result["group_bound_ids"], [member["snapshot_id"]])
        self.assertEqual(self.lake.snapshots.describe(member["snapshot_id"])["status"], "staged")

    def test_group_report_requires_published_group(self):
        self.add_visits("v1", DAY1)
        group = self.stage_group()
        with self.assertRaises(StateError):
            self.lake.analytics.group_report(group["group_id"])
        with self.assertRaises(NotFound):
            self.lake.analytics.group_report("missing")
        with self.assertRaises(NotFound):
            self.lake.groups.describe("missing")

    def test_group_publish_marks_events_published(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        self.publish_group()
        for day in (DAY1, DAY2):
            self.assertEqual(self.lake.quality.partition("venue", day)["pending_events"], 0)
            self.assertEqual(self.lake.snapshots.inventory("venue", day)["unpublished_events"], 0)

    def test_publish_days_convenience(self):
        self.add_visits("v1", DAY1)
        self.add_visits("v2", DAY2)
        group = self.lake.publish_days("venue", [DAY2, DAY1])
        self.assertEqual(group["status"], "published")
        self.assertEqual(group["days"], [DAY1, DAY2])


class PublishGroupApiTests(unittest.TestCase):
    def setUp(self):
        self.folder = tempfile.TemporaryDirectory()
        self.addCleanup(self.folder.cleanup)
        self.client = TestClient(create_app(str(Path(self.folder.name) / "api.db")))
        self.assertEqual(self.client.post("/venues", json={"venue_id": "venue", "name": "Venue",
                                                    "timezone": "Asia/Shanghai"}).status_code, 201)
        self.assertEqual(self.client.post("/exhibits", json={"exhibit_id": "robot", "venue_id": "venue",
                                                      "name": "Robot", "maker": "Maker",
                                                      "launched_on": "2026-01-01"}).status_code, 201)
        self.assertEqual(self.client.post("/sources", json={"source_id": "counter", "venue_id": "venue",
                                                     "kind": "visit"}).status_code, 201)
        self.assertEqual(self.client.post("/contracts", json={"contract_id": "v1", "source_id": "counter",
                                                       "fields": ["event_id", "exhibit_id", "event_at", "count"],
                                                       "effective_from": "2026-01-01T00:00:00+00:00"}).status_code, 201)

    def ingest(self, key, day, count=5):
        result = self.client.post("/batches", json={"source_id": "counter", "contract_id": "v1",
                                                    "request_key": key,
                                                    "events": [{"event_id": key, "exhibit_id": "robot",
                                                                "event_at": f"{day}T10:00:00+08:00",
                                                                "count": count}]})
        self.assertEqual(result.status_code, 201, result.text)

    def test_group_flow_over_api(self):
        self.ingest("v1", DAY1, 5)
        self.ingest("v2", DAY2, 9)
        staged = self.client.post("/publish-groups/stage",
                                  json={"venue_id": "venue", "days": [DAY1, DAY2]})
        self.assertEqual(staged.status_code, 201, staged.text)
        group_id = staged.json()["group_id"]
        member = staged.json()["members"][0]["snapshot_id"]
        blocked = self.client.post(f"/snapshots/{member}/publish", json={"expected_head": None})
        self.assertEqual(blocked.status_code, 409)
        self.assertEqual(blocked.json()["error"]["code"], "invalid_state")
        unpublished = self.client.get(f"/publish-groups/{group_id}/report")
        self.assertEqual(unpublished.status_code, 409)
        published = self.client.post(f"/publish-groups/{group_id}/publish", json={})
        self.assertEqual(published.status_code, 200, published.text)
        self.assertEqual(published.json()["status"], "published")
        report = self.client.get(f"/publish-groups/{group_id}/report")
        self.assertEqual(report.status_code, 200, report.text)
        self.assertEqual(report.json()["total_visitors"], 14)
        self.assertEqual([d["local_day"] for d in report.json()["days"]], [DAY1, DAY2])
        described = self.client.get(f"/publish-groups/{group_id}")
        self.assertEqual(described.status_code, 200)
        self.assertEqual(described.json()["status"], "published")
        listed = self.client.get(f"/venues/venue/publish-groups?local_day={DAY2}")
        self.assertEqual([g["group_id"] for g in listed.json()], [group_id])
        snapshot = self.client.get(f"/snapshots/{member}")
        self.assertEqual(snapshot.json()["group_id"], group_id)

    def test_group_api_validation_errors(self):
        too_small = self.client.post("/publish-groups/stage",
                                     json={"venue_id": "venue", "days": [DAY1]})
        self.assertEqual(too_small.status_code, 422)
        duplicated = self.client.post("/publish-groups/stage",
                                      json={"venue_id": "venue", "days": [DAY1, DAY1]})
        self.assertEqual(duplicated.status_code, 400)
        self.assertEqual(duplicated.json()["error"]["code"], "invalid_input")
        missing = self.client.get("/publish-groups/unknown")
        self.assertEqual(missing.status_code, 404)
        self.assertEqual(missing.json()["error"]["code"], "not_found")
