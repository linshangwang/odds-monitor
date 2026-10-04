import gzip
import json
import unittest
from unittest.mock import Mock, patch

import sync_prematch


class ReadOnlyPrematchSyncTests(unittest.TestCase):
    def packet(self, league="UEFA Nations League"):
        return {"schema_version": "shadow_prematch_packet_v1", "league": league, "match": {"match_id": "m-1", "kickoff_utc": "2026-10-05T20:00:00+00:00"}, "timeline": []}

    def test_decodes_gzip_and_filters_league(self):
        raw = gzip.compress(json.dumps({"packets": [self.packet(), self.packet("Other")]}, ensure_ascii=False).encode())
        selected = sync_prematch.select_packets(sync_prematch.decode_bundle(raw), "UEFA Nations League")
        self.assertEqual(len(selected), 1)
        self.assertEqual(selected[0]["match"]["match_id"], "m-1")

    def test_decode_bundle_rejects_gzip_expansion_over_limit(self):
        raw = gzip.compress(json.dumps({"packets": [self.packet()]}).encode())
        with self.assertRaisesRegex(ValueError, "bundle_decompressed_size_limit_exceeded"):
            sync_prematch.decode_bundle(raw, max_decoded_bytes=10)

    def test_decode_bundle_rejects_large_plain_payload(self):
        with self.assertRaisesRegex(ValueError, "bundle_decompressed_size_limit_exceeded"):
            sync_prematch.decode_bundle(b"{}" + b" " * 20, max_decoded_bytes=10)

    def test_selects_only_fixtures_inside_prematch_window(self):
        now_ts = 1_800_000_000

        def packet(fixture, kickoff):
            item = self.packet()
            item["match"] = {"match_id": fixture, "kickoff_utc": kickoff}
            return item

        iso = lambda ts: sync_prematch.datetime.fromtimestamp(ts, sync_prematch.timezone.utc).isoformat()
        packets = [
            packet("past", iso(now_ts - 60)),
            packet("inside", iso(now_ts + 24 * 3600)),
            packet("far", iso(now_ts + 80 * 3600)),
            packet("invalid", "not-a-date"),
        ]
        selected, excluded = sync_prematch.select_prematch_window(packets, 72, now_ts=now_ts)
        self.assertEqual([item["match"]["match_id"] for item in selected], ["inside"])
        self.assertEqual(
            {item["fixture"]: item["reason"] for item in excluded},
            {"past": "fixture_not_prematch", "far": "outside_prematch_window", "invalid": "missing_or_invalid_kickoff"},
        )

    def test_prematch_window_rejects_unbounded_values(self):
        with self.assertRaisesRegex(ValueError, "prematch_window_hours_must_be_1_to_168"):
            sync_prematch.select_prematch_window([], 0)
        with self.assertRaisesRegex(ValueError, "prematch_window_hours_must_be_1_to_168"):
            sync_prematch.select_prematch_window([], 169)

    def test_empty_current_window_is_successful_noop_without_upload(self):
        old_packet = self.packet()
        old_packet["match"]["kickoff_utc"] = "2020-01-01T00:00:00+00:00"
        raw = json.dumps({"packets": [old_packet]}).encode()
        argv = ["sync_prematch.py", "--input", "bundle.json", "--prematch-window-hours", "72", "--require-prematch"]
        with patch.object(sync_prematch.sys, "argv", argv), patch("sync_prematch.read_local", return_value=raw), patch("sync_prematch.upload_packets") as upload, patch("builtins.print") as output:
            self.assertEqual(sync_prematch.main(), 0)
        upload.assert_not_called()
        report = json.loads(output.call_args.args[0])
        self.assertEqual(report["status"], "no_current_prematch_packets")
        self.assertEqual(report["action"], "safe_noop")
        self.assertEqual(report["selection"]["excluded_reason_counts"], {"fixture_not_prematch": 1})

    def test_exclusion_report_is_bounded(self):
        packets = []
        for index in range(sync_prematch.EXCLUDED_REPORT_LIMIT + 2):
            item = self.packet()
            item["match"] = {"match_id": f"old-{index}", "kickoff_utc": "2020-01-01T00:00:00+00:00"}
            packets.append(item)
        raw = json.dumps({"packets": packets}).encode()
        argv = ["sync_prematch.py", "--input", "bundle.json", "--prematch-window-hours", "72"]
        with patch.object(sync_prematch.sys, "argv", argv), patch("sync_prematch.read_local", return_value=raw), patch("builtins.print") as output:
            self.assertEqual(sync_prematch.main(), 0)
        selection = json.loads(output.call_args.args[0])["selection"]
        self.assertEqual(len(selection["excluded_sample"]), sync_prematch.EXCLUDED_REPORT_LIMIT)
        self.assertTrue(selection["excluded_truncated"])
        self.assertEqual(selection["excluded_count"], sync_prematch.EXCLUDED_REPORT_LIMIT + 2)

    def test_remote_reader_is_restricted_to_existing_absolute_file(self):
        with self.assertRaises(ValueError):
            sync_prematch.read_pang_file("pang", "/tmp/data.gz;touch /tmp/x")
        with self.assertRaises(ValueError):
            sync_prematch.read_pang_file("pang", "/tmp/../secret")
        completed = Mock(returncode=0, stdout=b"{}", stderr=b"")
        with patch("sync_prematch.subprocess.run", return_value=completed) as run:
            self.assertEqual(sync_prematch.read_pang_file("pang", "/data/export.json"), b"{}")
        command = run.call_args.args[0]
        self.assertEqual(command[-2:], ["cat --", "/data/export.json"])
        self.assertNotIn("scp", command)

    def test_upload_uses_header_token_gzip_and_incremental_mode(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        with patch("sync_prematch.requests.post", return_value=response) as post:
            result = sync_prematch.upload_packets("https://example.test", "secret", [self.packet()])
        self.assertTrue(result["ok"])
        kwargs = post.call_args.kwargs
        self.assertEqual(kwargs["headers"]["Authorization"], "Bearer secret")
        self.assertEqual(kwargs["headers"]["Content-Encoding"], "gzip")
        self.assertEqual(kwargs["headers"]["X-Sync-Mode"], "incremental")
        self.assertNotIn("secret", post.call_args.args[0])
        self.assertEqual(json.loads(gzip.decompress(kwargs["data"]))["packets"][0]["match"]["match_id"], "m-1")

    def test_preflight_rejects_historical_bundle_for_current_date(self):
        report = sync_prematch.packet_preflight([self.packet()], expected_date="2026-10-06")
        self.assertEqual(report["status"], "rejected")
        self.assertIn("kickoff_date_mismatch", report["rows"][0]["reasons"])

    def test_preflight_rejects_finished_fixture_when_prematch_required(self):
        report = sync_prematch.packet_preflight([self.packet()], require_prematch=True, now_ts=1791230401)
        self.assertEqual(report["status"], "rejected")
        self.assertIn("fixture_not_prematch", report["rows"][0]["reasons"])

    def test_preflight_accepts_matching_future_packet(self):
        report = sync_prematch.packet_preflight([self.packet()], expected_date="2026-10-05", require_prematch=True, now_ts=1791220000)
        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["ready_count"], 1)

    def test_upload_forwards_strict_preflight_headers(self):
        response = Mock()
        response.raise_for_status.return_value = None
        response.json.return_value = {"ok": True}
        with patch("sync_prematch.requests.post", return_value=response) as post:
            sync_prematch.upload_packets("https://example.test", "secret", [self.packet()], expected_date="2026-10-05", require_prematch=True)
        headers = post.call_args.kwargs["headers"]
        self.assertEqual(headers["X-Expected-Match-Date"], "2026-10-05")
        self.assertEqual(headers["X-Require-Prematch"], "true")


if __name__ == "__main__":
    unittest.main()
