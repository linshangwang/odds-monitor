import os
import copy
import gzip
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import Mock, patch

import main


def api_football_odds(bookmakers):
    return {"data": {"response": [{"update": "2026-09-29T00:00:00Z", "bookmakers": bookmakers}]}}


class ShadowV4UpgradeTests(unittest.TestCase):
    def setUp(self):
        self.old_store = main.SNAPSHOT_STORE_PATH
        self.tmp = tempfile.TemporaryDirectory()
        main.SNAPSHOT_STORE_PATH = os.path.join(self.tmp.name, "store.json")

    def tearDown(self):
        main.SNAPSHOT_STORE_PATH = self.old_store
        self.tmp.cleanup()

    def test_bounded_gzip_decompression_rejects_expansion_over_limit(self):
        encoded = gzip.compress(b"x" * 100)
        with self.assertRaisesRegex(ValueError, "decompressed_size_limit_exceeded"):
            main.bounded_gzip_decompress(encoded, 20)
        self.assertEqual(main.bounded_gzip_decompress(encoded, 100), b"x" * 100)

    def test_target_fixture_discovery_reports_missing_source_without_nami_id_fallback(self):
        original_api, original_user, original_secret = main.API_FOOTBALL_KEY, main.NAMI_API_USER, main.NAMI_API_SECRET
        main.API_FOOTBALL_KEY, main.NAMI_API_USER, main.NAMI_API_SECRET = "", "configured-user", "configured-secret"
        try:
            result = main.target_fixtures_for_date("2026-10-04")
        finally:
            main.API_FOOTBALL_KEY, main.NAMI_API_USER, main.NAMI_API_SECRET = original_api, original_user, original_secret
        self.assertFalse(result["ok"])
        self.assertEqual(result["data_status"], "data_missing")
        self.assertEqual(result["source_audit"]["blocker"], "api_football_not_configured")
        self.assertFalse(result["source_audit"]["nami"]["fallback_used"])
        self.assertFalse(result["source_audit"]["nami"]["fixture_id_namespace_compatible"])

    def test_target_fixture_discovery_distinguishes_empty_date_from_missing_config(self):
        original_api = main.API_FOOTBALL_KEY
        main.API_FOOTBALL_KEY = "configured"
        try:
            with patch.object(main, "call_api_football", return_value={"ok": True, "status_code": 200, "data": {"response": []}}):
                result = main.target_fixtures_for_date("2026-10-04")
        finally:
            main.API_FOOTBALL_KEY = original_api
        self.assertTrue(result["ok"])
        self.assertEqual(result["data_status"], "available_empty")
        self.assertFalse(result["data_missing"])
        self.assertEqual(result["source_audit"]["blocker"], "no_fixtures_returned_for_date")

    def test_provider_fixture_identity_matches_without_reusing_provider_id(self):
        pang = main.fixture_identity_from_match({
            "match_id": "pang-uuid", "kickoff_utc": "2026-10-04T12:00:00Z",
            "home_team_name": "Paris Saint-Germain", "away_team_name": "Marseille",
        }, "France Ligue 1", "pang")
        nami = main.fixture_identity_from_match({
            "id": 9988, "match_time": int(main.datetime(2026, 10, 4, 12, 5, tzinfo=main.timezone.utc).timestamp()),
            "home": "Paris Saint Germain", "away": "Marseille", "league_name": "France Ligue 1",
        }, source="nami")
        result = main.reconcile_fixture_identity(nami, [pang])
        self.assertEqual(result["status"], "matched")
        self.assertTrue(result["decision_eligible"])
        self.assertEqual(result["match"]["source_fixture_id"], "pang-uuid")
        self.assertEqual(nami["source_fixture_id"], "9988")

    def test_provider_fixture_identity_ambiguous_match_forces_pass(self):
        incoming = main.fixture_identity_from_match({"id": 1, "match_time": 1000, "home": "A", "away": "B"}, "League", "nami")
        first = main.fixture_identity_from_match({"match_id": "x", "kickoff_utc": 1000, "home": "A", "away": "B"}, "League", "pang")
        second = main.fixture_identity_from_match({"match_id": "y", "kickoff_utc": 1100, "home": "A", "away": "B"}, "League", "api_football")
        result = main.reconcile_fixture_identity(incoming, [first, second])
        self.assertEqual(result["status"], "ambiguous")
        self.assertFalse(result["decision_eligible"])
        self.assertEqual(result["reason"], "multiple_provider_candidates")

    def test_imported_packet_persists_provider_identity_namespace(self):
        packet = self.prematch_packet()
        packet["match"]["kickoff_utc"] = "2026-10-04T12:00:00Z"
        main.import_prematch_packet(packet)
        metadata = main.load_snapshot_store()["external_prematch"]["uuid-1"]
        self.assertEqual(metadata["provider_fixture_ids"], {"pang": "uuid-1"})
        self.assertEqual(metadata["fixture_identity"]["status"], "complete")

    def test_nami_schedule_is_parsed_as_non_decision_supplement(self):
        payload = {"code": 0, "results": {
            "competition": [{"id": 5, "name_en": "UEFA Nations League"}],
            "team": [{"id": 10, "name_en": "France"}, {"id": 11, "name_en": "Italy"}],
            "match": [{"id": 99, "competition_id": 5, "home_team_id": 10, "away_team_id": 11, "match_time": 1791115200, "status_id": 1}],
        }}
        parsed = main.parse_nami_schedule({"ok": True, "status_code": 200, "data": payload})
        self.assertEqual(parsed["fixture_count"], 1)
        self.assertEqual(parsed["target_candidate_count"], 1)
        fixture = parsed["fixtures"][0]
        self.assertEqual(fixture["provider_fixture_id"], "99")
        self.assertFalse(fixture["decision_eligible"])
        self.assertEqual(fixture["fixture_identity"]["source"], "nami")

    def test_target_fixture_probe_keeps_nami_out_of_primary_queue(self):
        nami = {"ok": True, "data_status": "available", "fixture_count": 1, "target_candidate_count": 1, "fixtures": [{"provider": "nami", "provider_fixture_id": "99", "decision_eligible": False}], "error": None, "status_code": 200}
        with patch.object(main, "call_api_football", return_value={"ok": False, "error": "Missing API_FOOTBALL_KEY"}), patch.object(main, "nami_fixtures_for_date", return_value=nami), patch.object(main, "API_FOOTBALL_KEY", ""):
            result = main.target_fixtures_for_date("2026-10-04", include_supplemental=True)
        self.assertEqual(result["target_count"], 0)
        self.assertEqual(result["supplemental_target_candidate_count"], 1)
        self.assertFalse(result["supplemental_fixtures"][0]["decision_eligible"])
        self.assertFalse(result["source_audit"]["nami"]["fallback_used"])

    def test_nami_provider_competition_id_maps_chinese_nations_league_name(self):
        payload = {"code": 0, "results": {
            "competition": [{"id": 2906, "name_zh": "欧洲国家联赛"}],
            "team": [{"id": 1, "name_zh": "法国"}, {"id": 2, "name_zh": "意大利"}],
            "match": [{"id": 7, "competition_id": 2906, "home_team_id": 1, "away_team_id": 2, "match_time": 1791115200, "status_id": 1}],
        }}
        fixture = main.parse_nami_schedule({"ok": True, "data": payload})["fixtures"][0]
        self.assertTrue(fixture["target_candidate"])
        self.assertEqual(fixture["canonical_competition"], "UEFA Nations League")
        self.assertEqual(fixture["fixture_identity"]["normalized"]["league"], "uefanationsleague")

    def test_provider_reconciliation_report_matches_persisted_pang_identity_read_only(self):
        pang_match = {"match_id": "pang-1", "kickoff_utc": "2026-10-05T12:00:00Z", "home_team_name": "France", "away_team_name": "Italy"}
        identity = main.fixture_identity_from_match(pang_match, "UEFA Nations League", "pang")
        store = {"external_prematch": {"pang-1": {"league": "UEFA Nations League", "match": pang_match, "fixture_identity": identity}}}
        nami_identity = main.fixture_identity_from_match({"id": 88, "match_time": int(main.datetime(2026, 10, 5, 12, 4, tzinfo=main.timezone.utc).timestamp()), "home": "France", "away": "Italy"}, "UEFA Nations League", "nami")
        schedule = {"ok": True, "fixtures": [{"provider_fixture_id": "88", "target_candidate": True, "canonical_competition": "UEFA Nations League", "home": "France", "away": "Italy", "kickoff_at": nami_identity["kickoff_at"], "fixture_identity": nami_identity}]}
        before = main._content_hash(store)
        report = main.provider_reconciliation_report("2026-10-05", store_override=store, nami_schedule_override=schedule)
        self.assertEqual(report["counts"]["matched"], 1)
        self.assertEqual(report["rows"][0]["matched_pang_fixture_id"], "pang-1")
        self.assertTrue(report["rows"][0]["decision_eligible"])
        self.assertEqual(main._content_hash(store), before)

    def test_provider_reconciliation_unmatched_candidate_remains_blocked(self):
        identity = main.fixture_identity_from_match({"id": 88, "match_time": 1000, "home": "A", "away": "B"}, "UEFA Nations League", "nami")
        schedule = {"ok": True, "fixtures": [{"provider_fixture_id": "88", "target_candidate": True, "home": "A", "away": "B", "kickoff_at": 1000, "fixture_identity": identity}]}
        report = main.provider_reconciliation_report("2026-10-05", store_override={"external_prematch": {}}, nami_schedule_override=schedule)
        self.assertEqual(report["counts"]["no_match"], 1)
        self.assertFalse(report["rows"][0]["decision_eligible"])

    def test_apply_provider_reconciliation_is_idempotent_and_local_only(self):
        pang_match = {"match_id": "pang-1", "kickoff_utc": 1000, "home": "A", "away": "B"}
        pang_identity = main.fixture_identity_from_match(pang_match, "UEFA Nations League", "pang")
        store = {"external_prematch": {"pang-1": {"league": "UEFA Nations League", "match": pang_match, "fixture_identity": pang_identity}}}
        nami_identity = main.fixture_identity_from_match({"id": 88, "match_time": 1000, "home": "A", "away": "B"}, "UEFA Nations League", "nami")
        schedule = {"ok": True, "fixtures": [{"provider_fixture_id": "88", "target_candidate": True, "home": "A", "away": "B", "kickoff_at": 1000, "fixture_identity": nami_identity}]}
        first = main.apply_provider_reconciliation("2026-10-05", store_override=store, nami_schedule_override=schedule, persist=False)
        second = main.apply_provider_reconciliation("2026-10-05", store_override=store, nami_schedule_override=schedule, persist=False)
        self.assertEqual(first["audit"]["applied_count"], 1)
        self.assertEqual(second["audit"]["unchanged_count"], 1)
        self.assertEqual(store["external_prematch"]["pang-1"]["provider_fixture_ids"]["nami"], "88")
        self.assertEqual(store["external_prematch"]["pang-1"]["provider_reconciliation"]["method"], "strict_team_league_kickoff_unique_match")

    def test_apply_provider_reconciliation_never_overwrites_conflicting_nami_id(self):
        pang_match = {"match_id": "pang-1", "kickoff_utc": 1000, "home": "A", "away": "B"}
        pang_identity = main.fixture_identity_from_match(pang_match, "UEFA Nations League", "pang")
        store = {"external_prematch": {"pang-1": {"league": "UEFA Nations League", "match": pang_match, "fixture_identity": pang_identity, "provider_fixture_ids": {"pang": "pang-1", "nami": "old"}}}}
        nami_identity = main.fixture_identity_from_match({"id": 88, "match_time": 1000, "home": "A", "away": "B"}, "UEFA Nations League", "nami")
        schedule = {"ok": True, "fixtures": [{"provider_fixture_id": "88", "target_candidate": True, "fixture_identity": nami_identity}]}
        result = main.apply_provider_reconciliation("2026-10-05", store_override=store, nami_schedule_override=schedule, persist=False)
        self.assertEqual(result["rejected"][0]["reason"], "existing_nami_id_conflict")
        self.assertEqual(store["external_prematch"]["pang-1"]["provider_fixture_ids"]["nami"], "old")

    def test_auto_provider_reconciliation_skips_without_pang_data(self):
        with patch.object(main, "load_snapshot_store", return_value={"external_prematch": {}}), patch.object(main, "apply_provider_reconciliation") as apply:
            result = main.auto_provider_reconciliation_cycle(main.datetime(2026, 10, 5, tzinfo=main.timezone.utc))
        self.assertEqual(result["reason"], "no_pang_fixtures_imported")
        apply.assert_not_called()

    def test_auto_provider_reconciliation_runs_once_per_local_date(self):
        now = main.datetime(2026, 10, 5, tzinfo=main.timezone.utc)
        already = {"date": "2026-10-05", "applied_count": 1, "matcher_schema_version": main.PROVIDER_RECONCILIATION_SCHEMA_VERSION}
        store = {"external_prematch": {"pang-1": {}}, "provider_reconciliation_audit": [already]}
        with patch.object(main, "load_snapshot_store", return_value=store), patch.object(main, "apply_provider_reconciliation") as apply:
            result = main.auto_provider_reconciliation_cycle(now)
        self.assertEqual(result["reason"], "already_completed_for_date")
        apply.assert_not_called()

    def test_auto_provider_reconciliation_reruns_after_matcher_schema_upgrade(self):
        now = main.datetime(2026, 10, 5, tzinfo=main.timezone.utc)
        store = {"external_prematch": {"pang-1": {}}, "provider_reconciliation_audit": [{"date": "2026-10-05", "matcher_schema_version": 1}]}
        applied = {"ok": True, "audit": {"applied_count": 0, "unchanged_count": 0, "rejected_count": 1}}
        with patch.object(main, "load_snapshot_store", return_value=store), patch.object(main, "apply_provider_reconciliation", return_value=applied) as apply:
            result = main.auto_provider_reconciliation_cycle(now)
        apply.assert_called_once_with("2026-10-05")
        self.assertEqual(result["status"], "completed")

    def test_auto_provider_reconciliation_applies_when_due(self):
        now = main.datetime(2026, 10, 5, tzinfo=main.timezone.utc)
        store = {"external_prematch": {"pang-1": {}}, "provider_reconciliation_audit": []}
        applied = {"ok": True, "audit": {"applied_count": 2, "unchanged_count": 1, "rejected_count": 3}}
        with patch.object(main, "load_snapshot_store", return_value=store), patch.object(main, "apply_provider_reconciliation", return_value=applied) as apply:
            result = main.auto_provider_reconciliation_cycle(now)
        apply.assert_called_once_with("2026-10-05")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["applied_count"], 2)

    def test_health_exposes_only_aggregate_auto_reconciliation_status(self):
        summary = {"status": "completed", "date": "2026-10-05", "applied_count": 2, "rejected_count": 3}
        with patch.object(main, "AUTO_RECONCILIATION_LAST_RESULT", summary):
            result = main.health()["auto_provider_reconciliation"]
        self.assertEqual(result, summary)
        self.assertNotIn("rows", result)
        self.assertNotIn("nami_fixture_id", result)

    def test_server_import_preflight_rejects_wrong_date_and_finished_match(self):
        packet = self.prematch_packet()
        packet["match"]["kickoff_utc"] = "2026-09-30T12:00:00Z"
        report = main.server_import_preflight([packet], expected_date="2026-10-05", require_prematch=True, now_ts=1791130000)
        self.assertEqual(report["status"], "rejected")
        self.assertIn("kickoff_date_mismatch", report["rows"][0]["reasons"])
        self.assertIn("fixture_not_prematch", report["rows"][0]["reasons"])

    def test_server_import_preflight_rejects_duplicate_fixture_ids(self):
        packet = self.prematch_packet()
        packet["match"]["kickoff_utc"] = "2026-10-05T20:00:00Z"
        report = main.server_import_preflight([packet, packet], expected_date="2026-10-05", now_ts=1)
        self.assertEqual(report["status"], "rejected")
        self.assertIn("duplicate_match_id", report["rows"][1]["reasons"])

    def test_multilingual_nami_team_aliases_match_english_pang_names(self):
        pang = main.fixture_identity_from_match({"match_id": "p1", "kickoff_utc": 1000, "home": "France", "away": "Italy"}, "UEFA Nations League", "pang")
        nami = main.fixture_identity_from_match({
            "id": 9, "match_time": 1000, "home": "法国", "away": "意大利",
            "home_aliases": ["法国", "France"], "away_aliases": ["意大利", "Italy"],
        }, "UEFA Nations League", "nami")
        result = main.reconcile_fixture_identity(nami, [pang])
        self.assertEqual(result["status"], "matched")
        self.assertEqual(result["match"]["source_fixture_id"], "p1")

    def test_nami_parser_preserves_multilingual_team_aliases(self):
        payload = {"results": {
            "competition": [{"id": 2906, "name_zh": "欧洲国家联赛", "name_en": "UEFA Nations League"}],
            "team": [{"id": 1, "name_zh": "法国", "name_en": "France"}, {"id": 2, "name_zh": "意大利", "name_en": "Italy"}],
            "match": [{"id": 3, "competition_id": 2906, "home_team_id": 1, "away_team_id": 2, "match_time": 1000}],
        }}
        fixture = main.parse_nami_schedule({"ok": True, "data": payload})["fixtures"][0]
        aliases = fixture["fixture_identity"]["normalized"]
        self.assertIn("france", aliases["home_aliases"])
        self.assertIn("法国", aliases["home_aliases"])
        self.assertIn("italy", aliases["away_aliases"])

    def test_complete_timeline_contains_opening(self):
        self.assertEqual(main.PREMATCH_STAGE_ORDER, ["Opening", "T-24h", "T-12h", "T-6h", "T-3h", "T-1h", "T-30m", "Closing"])

    def test_legacy_t15_record_does_not_backfill_required_t30_stage(self):
        legacy = {"stage": "T-15m", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "stage_timing_audit": {"status": "valid"}, "sequence_timing_audit": {"status": "valid"}}
        timeline = main.complete_prematch_timeline([legacy])
        t30 = next(row for row in timeline if row["stage"] == "T-30m")
        self.assertEqual(t30["timeline_status"], "data_missing")
        self.assertTrue(t30["synthetic_placeholder"])
        self.assertNotIn("T-15m", [row["stage"] for row in timeline])

    def test_opening_is_never_synthesized_from_scheduled_current_odds(self):
        kickoff = main.datetime.now(main.timezone.utc) + main.timedelta(hours=48)
        opening = next(stage for stage in main.TRACKING_STAGES if stage["key"] == "Opening")
        self.assertFalse(main.auto_snapshot_stage_due(main.datetime.now(main.timezone.utc), kickoff, opening))
        plan = main.tracking_plan_for_fixture({"fixture_id": 1, "home": "H", "away": "A", "date": kickoff.isoformat(), "status": "NS"})
        opening_plan = next(row for row in plan["tracking"] if row["stage"] == "Opening")
        self.assertEqual(opening_plan["status"], "requires_verified_opening_source")
        self.assertFalse(opening_plan["automatic_collection"])
        self.assertIsNone(opening_plan["action_url"])
        with patch.object(main, "SHADOW_ACCESS_TOKEN", ""), self.assertRaises(main.HTTPException) as rejected:
            main.shadow_snapshot(1, "Opening", token=None, authorization=None, x_shadow_token=None)
        self.assertEqual(rejected.exception.status_code, 400)
        self.assertEqual(rejected.exception.detail["error"], "verified_opening_source_required")

    def test_late_stage_team_news_snapshot_persists_confirmed_xi(self):
        data = {"generated_at": 1000, "structured_inputs": {"injuries": {"available": True}, "lineups": {"available": True, "confirmed": True, "teams": [{"team_name": "Home", "starter_count": 11, "starting_xi": [{"name": "Player"}]}]}}}
        snapshot = main.team_news_snapshot(data, "T-1h")
        self.assertEqual(snapshot["captured_at"], 1000)
        self.assertTrue(snapshot["lineups"]["confirmed"])
        self.assertEqual(snapshot["lineups"]["teams"][0]["starting_xi"][0]["name"], "Player")
        self.assertIsNone(main.team_news_snapshot(data, "T-3h"))

    def test_rotation_quality_requires_confirmed_xi_and_never_claims_unscored_available(self):
        partial = main.pure_fundamental_script({"structured_inputs": {"lineups_available": True, "lineups": {"available": True, "confirmed": False}}})
        self.assertEqual(partial["chain"]["rotation_quality"]["status"], "data_missing")
        confirmed = main.pure_fundamental_script({"structured_inputs": {"lineups_available": True, "lineups_confirmed": True, "lineups": {"available": True, "confirmed": True}}})
        rotation = confirmed["chain"]["rotation_quality"]
        self.assertEqual(rotation["status"], "partial")
        self.assertIsNone(rotation["starting_xi_strength"])
        self.assertIn("not_scored", rotation["reason"])

    def test_empty_stat_shells_do_not_inflate_fundamental_coverage(self):
        empty_shells = main.pure_fundamental_script({"structured_inputs": {
            "season_stats": {"home": {"available": False}, "away": {"available": False}},
            "recent_form_last_10": {"home": {"available": False}, "away": {"available": False}},
        }})
        self.assertEqual(empty_shells["chain"]["execution_ability"]["status"], "data_missing")
        self.assertEqual(empty_shells["chain"]["goal_conversion"]["status"], "data_missing")

        real_stats = main.pure_fundamental_script({"structured_inputs": {
            "season_stats": {"home": {"available": True, "goals_for_avg": 1.4}, "away": {"available": False}},
            "recent_form_last_10": {"home": {"available": True}, "away": {"available": False}},
        }})
        self.assertEqual(real_stats["chain"]["execution_ability"]["status"], "partial")
        self.assertEqual(real_stats["chain"]["goal_conversion"]["status"], "partial")
        self.assertEqual(real_stats["chain"]["goal_conversion"]["usable_stats_sides"], ["home"])

    def test_pure_fundamental_script_exposes_extended_structural_dimensions(self):
        script = main.pure_fundamental_script({"structured_inputs": {}})
        chain = script["chain"]
        self.assertIn("current_athletic_level", chain["rotation_quality"])
        self.assertIn("structural_replacement", chain["rotation_quality"])
        self.assertIn("absolute_attack_quality", chain["execution_ability"])
        self.assertIn("two_way", chain["open_game_beneficiary"])
        self.assertIn("late_game_resistance", chain["time_segment_strength"])
        self.assertIsNone(chain["open_game_beneficiary"]["two_way"]["home_defensive_exposure"])

    def test_failed_upstream_response_body_does_not_count_as_coverage(self):
        failed = main.coverage_summary({"ok": False, "status_code": 403, "data": {"results": 1, "response": [{"error": "forbidden"}]}})
        self.assertFalse(failed["has_data"])
        self.assertTrue(failed["response_present_but_unusable"])
        quality = main.data_quality({
            "standings": failed, "home_recent_10": failed, "away_recent_10": failed,
            "home_team_season_stats": failed, "away_team_season_stats": failed,
        })
        self.assertEqual(quality["level"], "low")

        successful = main.coverage_summary({"ok": True, "status_code": 200, "data": {"results": 1, "response": [{"id": 1}]}})
        self.assertTrue(successful["has_data"])
        self.assertFalse(successful["response_present_but_unusable"])

    def test_api_football_http_200_business_error_is_not_success(self):
        original_key, original_cooldown = main.API_FOOTBALL_KEY, main.API_FOOTBALL_RATE_LIMIT_UNTIL
        main.API_FOOTBALL_KEY, main.API_FOOTBALL_RATE_LIMIT_UNTIL = "configured", 0
        response = unittest.mock.Mock()
        response.ok, response.status_code, response.url = True, 200, "https://example.test/fixtures"
        response.json.return_value = {"errors": {"requests": "Daily request limit reached"}, "response": [{"id": 1}]}
        try:
            with patch("main.requests.get", return_value=response), patch("main.time.time", return_value=1000):
                result = main.call_api_football("/fixtures")
            self.assertFalse(result["ok"])
            self.assertEqual(result["error"], "api_football_business_error")
            self.assertEqual(main.API_FOOTBALL_RATE_LIMIT_UNTIL, 4600)
            self.assertFalse(main.coverage_summary(result)["has_data"])
        finally:
            main.API_FOOTBALL_KEY, main.API_FOOTBALL_RATE_LIMIT_UNTIL = original_key, original_cooldown

    def test_other_providers_reject_explicit_http_200_business_errors(self):
        original_stats, original_odds = main.THESTATS_API_KEY, main.THE_ODDS_API_KEY
        main.THESTATS_API_KEY, main.THE_ODDS_API_KEY = "stats-key", "odds-key"
        response = unittest.mock.Mock()
        response.ok, response.status_code, response.url, response.headers = True, 200, "https://example.test", {}
        try:
            response.json.return_value = {"success": False, "error": "subscription required"}
            with patch("main.requests.get", return_value=response):
                stats = main.call_thestats("/events")
            self.assertFalse(stats["ok"])
            self.assertEqual(stats["error"], "thestats_business_error")

            response.json.return_value = {"status": "error", "message": "invalid market"}
            with patch("main.requests.get", return_value=response):
                odds = main.call_the_odds_api("/sports")
            self.assertFalse(odds["ok"])
            self.assertEqual(odds["error"], "the_odds_api_business_error")

            response.json.return_value = [{"key": "soccer"}]
            with patch("main.requests.get", return_value=response):
                valid = main.call_the_odds_api("/sports")
            self.assertTrue(valid["ok"])
        finally:
            main.THESTATS_API_KEY, main.THE_ODDS_API_KEY = original_stats, original_odds

    def test_injury_fetch_failure_is_not_treated_as_zero_injuries(self):
        failed = main.injuries_summary({"ok": False, "status_code": 503}, 1, 2)
        self.assertFalse(failed["available"])
        self.assertEqual(failed["status"], "fetch_failed")
        self.assertIsNone(failed["home_count"])
        self.assertEqual(failed["error"], "http_503")

        confirmed_empty = main.injuries_summary({"ok": True, "data": {"response": []}}, 1, 2)
        self.assertTrue(confirmed_empty["available"])
        self.assertEqual(confirmed_empty["status"], "confirmed_empty")
        self.assertEqual(confirmed_empty["home_count"], 0)

    def test_bootstrap_refuses_missing_odds_and_persists_timing_when_available(self):
        kickoff = main.datetime.now(main.timezone.utc) + main.timedelta(hours=1)
        fixture = {"fixture_id": 77, "status": "NS", "date": kickoff.isoformat(), "home": "H", "away": "A"}
        missing = {"ok": True, "coverage": {"odds_prematch": {"ok": False, "has_data": False}}, "structured_inputs": {"odds_market_snapshot": main.empty_market_snapshot()}}
        with patch.object(main, "choose_bootstrap_candidate", return_value=fixture), patch.object(main, "get_fixture_snapshots", return_value=[]), patch.object(main, "collect_stage_snapshot_data", return_value=missing), patch.object(main, "save_snapshot") as save:
            result = main.bootstrap_current_snapshot_once()
        self.assertEqual(result["reason"], "odds_data_missing")
        save.assert_not_called()

        market = main.empty_market_snapshot()
        market["available"] = True
        available = {"ok": True, "coverage": {"odds_prematch": {"ok": True, "has_data": True}}, "structured_inputs": {"odds_market_snapshot": market, "injuries": {"available": True}, "lineups": {"available": True, "confirmed": True, "teams": []}}, "data_quality": {}, "shadow_summary": {}}
        with patch.object(main, "choose_bootstrap_candidate", return_value=fixture), patch.object(main, "get_fixture_snapshots", return_value=[]), patch.object(main, "collect_stage_snapshot_data", return_value=available), patch.object(main, "save_snapshot", side_effect=lambda row: row) as save:
            result = main.bootstrap_current_snapshot_once()
        saved = save.call_args.args[0]
        self.assertEqual(result["status"], "saved")
        self.assertEqual(saved["stage_timing_audit"]["status"], "valid")
        self.assertIsNotNone(saved["team_news_snapshot"])

    def test_invalid_repeat_snapshot_cannot_overwrite_valid_stage(self):
        valid_market = main.empty_market_snapshot()
        valid_market["available"] = True
        valid = {"fixture": 88, "stage": "T-1h", "snapshot_at": 100, "market_snapshot": valid_market, "stage_timing_audit": {"status": "valid"}}
        first = main.save_snapshot(valid)
        self.assertTrue(first["saved"])

        invalid = {"fixture": 88, "stage": "T-1h", "snapshot_at": 200, "market_snapshot": main.empty_market_snapshot(), "stage_timing_audit": {"status": "invalid"}}
        rejected = main.save_snapshot(invalid)
        self.assertFalse(rejected["saved"])
        self.assertTrue(rejected["preserved_existing"])
        self.assertEqual(rejected["reason"], "snapshot_quality_regression_rejected")
        stored = main.get_fixture_snapshots(88)
        self.assertEqual(len(stored), 1)
        self.assertEqual(stored[0]["snapshot_at"], 100)
        self.assertTrue(stored[0]["market_snapshot"]["available"])

    def test_older_valid_repeat_snapshot_cannot_move_stage_backwards(self):
        market = main.empty_market_snapshot()
        market["available"] = True
        newer = {"fixture": 89, "stage": "T-3h", "snapshot_at": 200, "market_snapshot": market, "stage_timing_audit": {"status": "valid"}}
        older = {"fixture": 89, "stage": "T-3h", "snapshot_at": 100, "market_snapshot": market, "stage_timing_audit": {"status": "valid"}}
        self.assertTrue(main.save_snapshot(newer)["saved"])
        rejected = main.save_snapshot(older)
        self.assertFalse(rejected["saved"])
        self.assertEqual(rejected["reason"], "stale_snapshot_rejected")
        self.assertEqual(main.get_fixture_snapshots(89)[0]["snapshot_at"], 200)

    def test_same_timestamp_conflicting_snapshot_is_rejected(self):
        first_market = main.empty_market_snapshot()
        first_market["available"] = True
        first_market["bookmaker_count"] = 2
        changed_market = main.empty_market_snapshot()
        changed_market["available"] = True
        changed_market["bookmaker_count"] = 3
        first = {"fixture": 90, "stage": "T-6h", "snapshot_at": 100, "market_snapshot": first_market, "stage_timing_audit": {"status": "valid"}}
        conflict = {"fixture": 90, "stage": "T-6h", "snapshot_at": 100, "market_snapshot": changed_market, "stage_timing_audit": {"status": "valid"}}
        self.assertTrue(main.save_snapshot(first)["saved"])
        rejected = main.save_snapshot(conflict)
        self.assertEqual(rejected["reason"], "snapshot_timestamp_conflict_rejected")
        self.assertEqual(main.get_fixture_snapshots(90)[0]["market_snapshot"]["bookmaker_count"], 2)

    def test_fresh_market_does_not_erase_confirmed_same_stage_lineup(self):
        first_market = main.empty_market_snapshot()
        first_market["available"] = True
        later_market = main.empty_market_snapshot()
        later_market["available"] = True
        later_market["bookmaker_count"] = 4
        confirmed_news = {"captured_at": 100, "stage": "T-1h", "lineups": {"available": True, "confirmed": True, "teams": [{"team_name": "H", "starter_count": 11}]}, "injuries": {"available": True}}
        failed_news = {"captured_at": 200, "stage": "T-1h", "lineups": {"available": False, "confirmed": False, "status": "fetch_failed", "teams": []}, "injuries": {"available": False}}
        first = {"fixture": 91, "stage": "T-1h", "snapshot_at": 100, "market_snapshot": first_market, "team_news_snapshot": confirmed_news, "stage_timing_audit": {"status": "valid"}}
        later = {"fixture": 91, "stage": "T-1h", "snapshot_at": 200, "market_snapshot": later_market, "team_news_snapshot": failed_news, "stage_timing_audit": {"status": "valid"}}
        self.assertTrue(main.save_snapshot(first)["saved"])
        self.assertTrue(main.save_snapshot(later)["saved"])
        stored = main.get_fixture_snapshots(91)[0]
        self.assertEqual(stored["market_snapshot"]["bookmaker_count"], 4)
        self.assertTrue(stored["team_news_snapshot"]["lineups"]["confirmed"])
        self.assertTrue(stored["team_news_snapshot"]["preservation_audit"]["preserved"])
        self.assertIn("lineups", stored["team_news_snapshot"]["preservation_audit"]["components"])
        self.assertEqual(stored["team_news_snapshot"]["preservation_audit"]["source_snapshot_at"], 100)

    def test_team_news_components_preserve_injuries_independently(self):
        market = main.empty_market_snapshot()
        market["available"] = True
        old_news = {"captured_at": 100, "lineups": {"available": True, "confirmed": True, "teams": [{"version": "old"}]}, "injuries": {"available": True, "home_count": 1}}
        new_news = {"captured_at": 200, "lineups": {"available": True, "confirmed": True, "teams": [{"version": "new"}]}, "injuries": {"available": False, "status": "fetch_failed"}}
        first = {"fixture": 92, "stage": "T-30m", "snapshot_at": 100, "market_snapshot": market, "team_news_snapshot": old_news, "stage_timing_audit": {"status": "valid"}}
        second = {"fixture": 92, "stage": "T-30m", "snapshot_at": 200, "market_snapshot": market, "team_news_snapshot": new_news, "stage_timing_audit": {"status": "valid"}}
        main.save_snapshot(first)
        main.save_snapshot(second)
        news = main.get_fixture_snapshots(92)[0]["team_news_snapshot"]
        self.assertEqual(news["lineups"]["teams"][0]["version"], "new")
        self.assertEqual(news["injuries"]["home_count"], 1)
        self.assertEqual(news["preservation_audit"]["components"], ["injuries"])
        self.assertEqual(news["preservation_audit"]["source_team_news_captured_at"], 100)

    def test_shadow_token_resolution_prefers_headers_without_breaking_query_compatibility(self):
        self.assertEqual(main.resolve_shadow_token("query", "Bearer bearer", "header"), "header")
        self.assertEqual(main.resolve_shadow_token("query", "Bearer bearer", None), "bearer")
        self.assertEqual(main.resolve_shadow_token("query", None, None), "query")

    def test_shadow_token_validation_uses_constant_time_comparison(self):
        original = main.SHADOW_ACCESS_TOKEN
        main.SHADOW_ACCESS_TOKEN = "configured-token"
        try:
            with patch("main.hmac.compare_digest", wraps=main.hmac.compare_digest) as compared:
                main.require_shadow_token("configured-token")
                compared.assert_called_once_with("configured-token", "configured-token")
            with self.assertRaises(main.HTTPException) as rejected:
                main.require_shadow_token("wrong-token")
            self.assertEqual(rejected.exception.status_code, 401)
        finally:
            main.SHADOW_ACCESS_TOKEN = original

    def test_learning_endpoints_fail_closed_without_configured_token(self):
        with patch.object(main, "SHADOW_ACCESS_TOKEN", ""):
            with self.assertRaises(main.HTTPException) as unavailable:
                main.require_learning_token(None)
        self.assertEqual(unavailable.exception.status_code, 503)
        self.assertEqual(unavailable.exception.detail, "shadow_access_token_required_for_learning")
        with patch.object(main, "SHADOW_ACCESS_TOKEN", "configured-token"):
            with self.assertRaises(main.HTTPException) as rejected:
                main.require_learning_token("wrong-token")
            main.require_learning_token("configured-token")
        self.assertEqual(rejected.exception.status_code, 401)

    def test_paid_odds_endpoints_fail_closed_without_configured_token(self):
        with patch.object(main, "SHADOW_ACCESS_TOKEN", ""):
            with self.assertRaises(main.HTTPException) as unavailable:
                main.require_paid_odds_token(None)
        self.assertEqual(unavailable.exception.status_code, 503)
        self.assertEqual(unavailable.exception.detail, "shadow_access_token_required_for_paid_odds")
        with patch.object(main, "SHADOW_ACCESS_TOKEN", "configured-token"):
            with self.assertRaises(main.HTTPException) as rejected:
                main.require_paid_odds_token("wrong-token")
            main.require_paid_odds_token("configured-token")
        self.assertEqual(rejected.exception.status_code, 401)

    def test_all_shadow_endpoints_expose_header_authentication(self):
        schema = main.app.openapi()
        for path, operations in schema["paths"].items():
            if not path.startswith("/shadow/"):
                continue
            for operation in operations.values():
                if not isinstance(operation, dict):
                    continue
                headers = {item.get("name") for item in operation.get("parameters", []) if item.get("in") == "header"}
                self.assertIn("authorization", headers, path)
                self.assertIn("x-shadow-token", headers, path)

    def test_complete_timeline_marks_uncaptured_stages_without_backfill(self):
        captured = {"stage": "T-1h", "import_status": "available", "market_snapshot": {"available": True, "primary": {"1x2": {"home": 2.0}}}}
        timeline = main.complete_prematch_timeline([captured])
        self.assertEqual([row["stage"] for row in timeline], main.PREMATCH_STAGE_ORDER)
        self.assertEqual(len(timeline), 8)
        opening = timeline[0]
        self.assertEqual(opening["timeline_status"], "data_missing")
        self.assertTrue(opening["synthetic_placeholder"])
        self.assertFalse(opening["backfilled_from_current"])
        t_one = next(row for row in timeline if row["stage"] == "T-1h")
        self.assertEqual(t_one["timeline_status"], "available")
        self.assertFalse(t_one["synthetic_placeholder"])

    def test_line_movement_gate_requires_two_real_comparable_stages(self):
        opening = {"stage": "Opening", "snapshot_at": 1, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}}
        single = main.audit_line_movement_timeline([opening])
        self.assertFalse(single["decision_eligible"])
        later = {"stage": "T-12h", "snapshot_at": 2, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": -.05}}}}
        complete = main.audit_line_movement_timeline([opening, later])
        self.assertTrue(complete["decision_eligible"])
        self.assertEqual(complete["available_stage_count"], 2)
        self.assertFalse(complete["current_odds_used_as_history"])

    def test_latest_prematch_snapshot_prefers_later_stage_over_write_time(self):
        rows = [
            {"stage": "Opening", "snapshot_at": 9999},
            {"stage": "T-3h", "snapshot_at": 100},
            {"stage": "T-30m", "snapshot_at": 200},
        ]
        self.assertEqual(main.latest_prematch_snapshot(rows)["stage"], "T-30m")
        self.assertIsNone(main.latest_prematch_snapshot([{"stage": "FT", "snapshot_at": 10000}]))

    def test_stage_timing_rejects_mislabeled_tx_snapshot(self):
        kickoff = 100000
        valid = main.audit_stage_timing("T-1h", kickoff - 3600, kickoff)
        self.assertEqual(valid["status"], "valid")
        invalid = main.audit_stage_timing("T-1h", kickoff - 36000, kickoff)
        self.assertEqual(invalid["status"], "invalid")
        self.assertFalse(invalid["decision_eligible"])
        selected = main.latest_prematch_snapshot([
            {"stage": "T-3h", "snapshot_at": 100, "stage_timing_audit": {"status": "valid"}},
            {"stage": "T-1h", "snapshot_at": 200, "stage_timing_audit": {"status": "invalid"}},
        ])
        self.assertEqual(selected["stage"], "T-3h")

    def test_stage_timing_is_unverifiable_not_invented_without_kickoff(self):
        audit = main.audit_stage_timing("T-30m", 1000, None)
        self.assertEqual(audit["status"], "data_missing")
        self.assertTrue(audit["decision_eligible"])

    def test_timeline_sequence_rejects_non_monotonic_stage_times(self):
        rows = [
            {"stage": "Opening", "snapshot_at": 200, "import_status": "available", "stage_timing_audit": {"status": "valid"}},
            {"stage": "T-24h", "snapshot_at": 100, "import_status": "available", "stage_timing_audit": {"status": "valid"}},
            {"stage": "T-12h", "snapshot_at": 300, "import_status": "available", "stage_timing_audit": {"status": "valid"}},
        ]
        audit = main.audit_timeline_sequence(rows)
        self.assertEqual(audit["Opening"]["status"], "valid")
        self.assertEqual(audit["T-24h"]["status"], "invalid")
        self.assertEqual(audit["T-24h"]["reason"], "non_monotonic_stage_timestamp")
        self.assertEqual(audit["T-12h"]["status"], "valid")

    def test_consensus_uses_all_books_not_first_book_primary(self):
        result = api_football_odds([
            {"name": "Outlier", "bets": [{"name": "Match Winner", "values": [{"value": "Home", "odd": "9.0"}, {"value": "Draw", "odd": "9.0"}, {"value": "Away", "odd": "1.1"}]}, {"name": "Asian Handicap", "values": [{"value": "Home -1", "odd": "1.9"}, {"value": "Away -1", "odd": "1.9"}]}]},
            {"name": "Book B", "bets": [{"name": "Match Winner", "values": [{"value": "Home", "odd": "2.0"}, {"value": "Draw", "odd": "3.2"}, {"value": "Away", "odd": "3.8"}]}, {"name": "Asian Handicap", "values": [{"value": "Home -0.25", "odd": "1.95"}, {"value": "Away -0.25", "odd": "1.91"}]}]},
            {"name": "Book C", "bets": [{"name": "Match Winner", "values": [{"value": "Home", "odd": "2.1"}, {"value": "Draw", "odd": "3.1"}, {"value": "Away", "odd": "3.7"}]}, {"name": "Asian Handicap", "values": [{"value": "Home -0.25", "odd": "1.93"}, {"value": "Away -0.25", "odd": "1.93"}]}]},
        ])
        snapshot = main.extract_market_snapshot(result)
        self.assertEqual(snapshot["primary"], snapshot["consensus_main_line"])
        self.assertEqual(snapshot["primary"]["1x2"]["home"], 2.1)
        self.assertEqual(snapshot["primary"]["asian_handicap"]["line"], -0.25)
        self.assertEqual(snapshot["primary"]["asian_handicap"]["bookmaker_count"], 2)

    def test_optional_markets_and_missing_status(self):
        result = api_football_odds([{"name": "Book", "bets": [{"name": "Both Teams Score", "values": [{"value": "Yes", "odd": "1.8"}, {"value": "No", "odd": "2.0"}]}]}])
        snapshot = main.extract_market_snapshot(result)
        self.assertEqual(snapshot["data_status"]["btts"], "available")
        self.assertEqual(snapshot["data_status"]["home_team_total"], "data_missing")

    def test_revalidation_trigger_and_classification(self):
        previous = {"stage": "T-24h", "market_snapshot": {"primary": {"asian_handicap": {"line": -0.25}, "over_under": {"line": 2.5}, "1x2": {"home": 2.0, "away": 4.0}}}}
        current = main.empty_market_snapshot()
        current["primary"] = {"asian_handicap": {"line": -0.5}, "over_under": {"line": 2.75}, "1x2": {"home": 1.8, "away": 4.4}}
        dynamics = main.compare_market_snapshots([previous], current, "T-12h")
        self.assertTrue(dynamics["revalidation_trigger"]["triggered"])
        self.assertIn("significant_line_move", dynamics["revalidation_trigger"]["reasons"])

    def test_likely_information_driven_requires_explicit_evidence_reference(self):
        dynamics = {"revalidation_trigger": {"triggered": True}, "comparison_status": "compared"}
        without_evidence = main.classify_market_move_details(dynamics, None, None)
        self.assertEqual(without_evidence["classification"], "Market-Only Move")
        dynamics["information_search"] = {"status": "suspected_unconfirmed", "evidence_refs": ["coach_press_conference_pending_confirmation"]}
        with_evidence = main.classify_market_move_details(dynamics, None, None)
        self.assertEqual(with_evidence["classification"], "Likely Information-Driven")
        self.assertFalse(with_evidence["inferred_without_evidence"])

    def test_unreferenced_suspicion_is_not_labeled_information_driven(self):
        dynamics = {"revalidation_trigger": {"triggered": True}, "information_search": {"status": "suspected_unconfirmed", "evidence_refs": []}}
        result = main.classify_market_move_details(dynamics, None, None)
        self.assertEqual(result["classification"], "Market-Only Move")

    def test_information_search_rejects_future_or_unidentifiable_evidence(self):
        normalized = main.normalize_information_search({
            "status": "suspected_unconfirmed",
            "evidence_refs": [
                {"source": "press", "title": "report", "observed_at": "2030-01-01T00:00:00Z"},
                {"source": "", "title": "anonymous"},
            ],
        }, snapshot_at=1000)
        self.assertFalse(normalized["evidence_audit"]["decision_eligible"])
        result = main.classify_market_move_details({"revalidation_trigger": {"triggered": True}, "information_search": normalized}, None, None)
        self.assertEqual(result["classification"], "Market-Only Move")

    def test_information_search_accepts_bounded_structured_evidence(self):
        normalized = main.normalize_information_search({
            "status": "suspected_unconfirmed",
            "evidence_refs": [{"source": "club_press_conference", "id": "report-1", "observed_at": "1970-01-01T00:10:00Z"}],
        }, snapshot_at=1000)
        self.assertTrue(normalized["evidence_audit"]["decision_eligible"])
        result = main.classify_market_move_details({"revalidation_trigger": {"triggered": True}, "information_search": normalized}, None, None)
        self.assertEqual(result["classification"], "Likely Information-Driven")

    def test_market_move_classification_preserves_overlapping_signals(self):
        previous = {"script": {"content_hash": "old", "chain": {"result_utility": {"status": "data_missing"}}}}
        script = {"content_hash": "new", "chain": {"result_utility": {"status": "available", "evidence": "verified"}}}
        dynamics = {"cross_market_divergence": True, "revalidation_trigger": {"triggered": True}}
        with patch("main.audit_fundamental_chain", return_value={"decision_eligible": True, "status": "eligible"}):
            result = main.classify_market_move_details(dynamics, previous, script, model_market_divergence=True)
        self.assertEqual(result["classification"], "Fundamental Confirmed")
        self.assertEqual(result["matched_classifications"], ["Fundamental Confirmed", "Model-Market Divergence", "Cross-Market Divergence"])
        self.assertIn("Cross-Market Divergence", result["classification_bases"])
        self.assertEqual(result["fundamental_change_audit"]["changed_sections"], ["result_utility"])

    def test_model_hash_change_alone_is_not_fundamental_confirmation(self):
        chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        previous = {"script": {"content_hash": "old", "chain": chain, "model": {"model_hash": "a"}}}
        script = {"content_hash": "new", "chain": chain, "model": {"model_hash": "b"}}
        result = main.classify_market_move_details({"revalidation_trigger": {"triggered": True}}, previous, script)
        self.assertEqual(result["classification"], "Market-Only Move")
        self.assertEqual(result["fundamental_change_audit"]["changed_sections"], [])
        self.assertFalse(result["fundamental_change_audit"]["evidence_eligible"])

    def test_unverified_chain_change_is_not_fundamental_confirmation(self):
        previous = {"script": {"content_hash": "old", "chain": {"result_utility": {"status": "data_missing"}}}}
        script = {"content_hash": "new", "chain": {"result_utility": {"status": "available", "evidence": "claim only"}}}
        with patch("main.audit_fundamental_chain", return_value={"decision_eligible": False, "status": "insufficient"}):
            result = main.classify_market_move_details({"revalidation_trigger": {"triggered": True}}, previous, script)
        self.assertEqual(result["classification"], "Market-Only Move")
        self.assertEqual(result["fundamental_change_audit"]["changed_sections"], ["result_utility"])
        self.assertFalse(result["fundamental_change_audit"]["evidence_eligible"])

    def test_evidence_metadata_refresh_is_not_fundamental_confirmation(self):
        old_section = {"status": "available", "home": {"win": 1, "draw": 0, "loss": -1}, "source": "feed-a", "observed_at": 100}
        new_section = {**old_section, "source": "feed-b", "observed_at": 200, "evidence_refs": ["new-report"]}
        previous = {"script": {"content_hash": "old", "chain": {"result_utility": old_section}}}
        script = {"content_hash": "new", "chain": {"result_utility": new_section}}
        result = main.classify_market_move_details({"revalidation_trigger": {"triggered": True}}, previous, script)
        self.assertEqual(result["classification"], "Market-Only Move")
        self.assertEqual(result["fundamental_change_audit"]["changed_sections"], [])
        self.assertEqual(result["fundamental_change_audit"]["evidence_metadata_only_sections"], ["result_utility"])

    def test_nested_evidence_metadata_refresh_is_not_substantive(self):
        old_section = {"status": "available", "home": {"score": .7, "source": "feed-a", "observed_at": 100}, "away": {"score": .6}}
        new_section = {"status": "available", "home": {"score": .7, "source": "feed-b", "observed_at": 200, "evidence_refs": ["report-2"]}, "away": {"score": .6}}
        substantive, metadata = main.fundamental_chain_change_sets({"execution_ability": old_section}, {"execution_ability": new_section})
        self.assertEqual(substantive, [])
        self.assertEqual(metadata, ["execution_ability"])

    def test_nested_numeric_change_remains_substantive(self):
        old_section = {"status": "available", "home": {"score": .7, "source": "feed-a"}}
        new_section = {"status": "available", "home": {"score": .8, "source": "feed-b"}}
        substantive, metadata = main.fundamental_chain_change_sets({"execution_ability": old_section}, {"execution_ability": new_section})
        self.assertEqual(substantive, ["execution_ability"])
        self.assertEqual(metadata, [])

    def test_status_or_value_change_remains_substantive(self):
        old_section = {"status": "partial", "home": {"score": .5}, "source": "feed", "observed_at": 100}
        new_section = {"status": "available", "home": {"score": .7}, "source": "feed", "observed_at": 200}
        previous = {"script": {"chain": {"execution_ability": old_section}}}
        script = {"chain": {"execution_ability": new_section}}
        with patch("main.audit_fundamental_chain", return_value={"decision_eligible": True, "status": "eligible"}):
            result = main.classify_market_move_details({}, previous, script)
        self.assertEqual(result["classification"], "Fundamental Confirmed")
        self.assertEqual(result["fundamental_change_audit"]["changed_sections"], ["execution_ability"])
        self.assertEqual(result["fundamental_change_audit"]["evidence_metadata_only_sections"], [])

    def test_optional_markets_participate_in_movement_and_divergence(self):
        previous = {"stage": "T-3h", "market_snapshot": {"primary": {
            "over_under": {"line": 2.5, "over": 1.9, "under": 1.9},
            "btts": {"yes": 1.8, "no": 2.0},
            "home_team_total": {"line": 1.5, "over": 1.9, "under": 1.9},
        }}}
        current = main.empty_market_snapshot()
        current["primary"].update({
            "over_under": {"line": 2.75, "over": 1.75, "under": 2.1},
            "btts": {"yes": 1.95, "no": 1.85},
            "home_team_total": {"line": 1.75, "over": 1.75, "under": 2.05},
        })
        dynamics = main.compare_market_snapshots([previous], current, "T-1h")
        self.assertEqual(dynamics["market_movements"]["btts"]["yes"], .15)
        self.assertEqual(dynamics["market_movements"]["home_team_total"]["line"], .25)
        self.assertTrue(dynamics["cross_market_divergence_pairs"]["over_under_vs_btts"])
        self.assertIn("cross_market_divergence", dynamics["revalidation_trigger"]["reasons"])

    def test_price_move_uses_no_vig_probability_and_rejects_line_mismatch(self):
        previous = {"stage": "T-6h", "market_snapshot": {"primary": {
            "1x2": {"home": 2.0, "draw": 3.5, "away": 4.0},
            "over_under": {"line": 2.5, "over": 1.9, "under": 1.9},
        }}}
        current = main.empty_market_snapshot()
        current["primary"].update({
            "1x2": {"home": 1.85, "draw": 3.7, "away": 4.2},
            "over_under": {"line": 2.75, "over": 1.9, "under": 1.9},
        })
        dynamics = main.compare_market_snapshots([previous], current, "T-3h")
        probability = dynamics["no_vig_probability_movements"]
        self.assertEqual(probability["1x2"]["status"], "compared")
        self.assertGreater(probability["1x2"]["deltas"]["home"], .03)
        self.assertEqual(probability["over_under"]["status"], "line_changed")
        self.assertIsNone(probability["over_under"]["deltas"])
        self.assertIn("abnormal_price_move", dynamics["revalidation_trigger"]["reasons"])

    def test_snapshot_comparison_requires_a_shared_market(self):
        previous = {"stage": "T-6h", "market_snapshot": {"primary": {"1x2": {"home": 2.0, "draw": 3.4, "away": 3.8}}}}
        current = main.empty_market_snapshot()
        current["primary"]["over_under"] = {"line": 2.5, "over": 1.9, "under": 1.9}
        dynamics = main.compare_market_snapshots([previous], current, "T-3h")
        self.assertEqual(dynamics["comparison_status"], "data_missing")
        self.assertEqual(dynamics["comparison_audit"]["reason"], "no_shared_comparable_market")
        self.assertEqual(dynamics["comparison_audit"]["shared_comparable_market_count"], 0)
        self.assertFalse(dynamics["revalidation_trigger"]["triggered"])

    def test_snapshot_comparison_audits_shared_unchanged_market(self):
        market = {"1x2": {"home": 2.0, "draw": 3.4, "away": 3.8}}
        previous = {"stage": "T-6h", "market_snapshot": {"primary": market}}
        current = main.empty_market_snapshot()
        current["primary"] = market
        dynamics = main.compare_market_snapshots([previous], current, "T-3h")
        self.assertEqual(dynamics["comparison_status"], "compared")
        self.assertEqual(dynamics["comparison_audit"]["shared_comparable_markets"], ["1x2"])
        self.assertFalse(dynamics["revalidation_trigger"]["triggered"])

    def test_decision_layer_passes_when_inputs_missing(self):
        result = main.decision_layer(main.empty_market_snapshot())
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("model_probability", result["pass_reasons"])

    def test_line_movement_gate_forces_pass_with_one_real_node(self):
        candidate = {"market": "1x2", "selection": "home"}
        decision = {"decision": "home", "best_market": candidate, "edge": .05, "ev": .08, "pass_reasons": [], "recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": candidate, "high_variance_single": candidate}}
        history = [{"stage": "T-3h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}}]
        gated = main.apply_line_movement_gate(decision, history)
        self.assertEqual(gated["decision"], "PASS")
        self.assertIsNone(gated["best_market"])
        self.assertIsNone(gated["edge"])
        self.assertIsNone(gated["ev"])
        self.assertTrue(all(gated["recommendation_tiers"][key] is None for key in ("first_choice_high_consistency", "second_choice_higher_return", "high_variance_single")))
        self.assertIn("line_movement_requires_two_real_comparable_stages", gated["pass_reasons"])

    def test_force_pass_decision_is_idempotent_and_clears_all_recommendations(self):
        candidate = {"market": "btts", "selection": "yes"}
        decision = {"decision": "btts:yes", "best_market": candidate, "edge": .1, "ev": .12, "pass_reasons": ["stale"], "recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": candidate, "high_variance_single": candidate, "ranking_rule": "test"}}
        main.force_pass_decision(decision, ["stale", "fundamental_chain_insufficient"])
        self.assertEqual(decision["pass_reasons"], ["stale", "fundamental_chain_insufficient"])
        self.assertIsNone(decision["best_market"])
        self.assertIsNone(decision["edge"])
        self.assertIsNone(decision["ev"])
        self.assertTrue(all(value is None for key, value in decision["recommendation_tiers"].items() if key != "ranking_rule"))

    def test_line_movement_gate_accepts_two_comparable_real_nodes(self):
        decision = {"decision": "home", "best_market": {"market": "1x2"}, "edge": .05, "ev": .08, "pass_reasons": []}
        history = [
            {"stage": "T-24h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": -.1}}}},
        ]
        gated = main.apply_line_movement_gate(decision, history)
        self.assertEqual(gated["decision"], "home")
        self.assertTrue(gated["line_movement_audit"]["decision_eligible"])

    def test_market_language_separates_pressure_proxy_from_line_response(self):
        audit = {"decision_eligible": True}
        base = {
            "comparison_status": "compared",
            "no_vig_probability_movements": {"1x2": {"status": "compared", "deltas": {"home": .04, "away": -.03}}},
            "market_movements": {"asian_handicap": {"line": -.25}},
        }
        accepted = main.market_language_for_candidate({"market": "asian_handicap", "selection": "home"}, base, audit)
        self.assertEqual(accepted["capital_pressure"]["label"], "Capital Pressure Proxy")
        self.assertEqual(accepted["capital_pressure"]["evidence_grade"], "B")
        self.assertFalse(accepted["capital_pressure"]["is_real_money"])
        self.assertIsNone(accepted["capital_pressure"]["money_percent"])
        self.assertEqual(accepted["line_response"]["response"], "upgrade")
        self.assertEqual(accepted["market_acceptance"], "Accepted")
        self.assertEqual(accepted["diagnostic"], "Accepted Repricing")

        resistance = main.market_language_for_candidate(
            {"market": "asian_handicap", "selection": "home"},
            {**base, "market_movements": {"asian_handicap": {"line": 0.0}}}, audit,
        )
        self.assertEqual(resistance["line_response"]["response"], "static")
        self.assertEqual(resistance["market_acceptance"], "Resistance")

        rejected = main.market_language_for_candidate(
            {"market": "asian_handicap", "selection": "home"},
            {**base, "market_movements": {"asian_handicap": {"line": .25}}}, audit,
        )
        self.assertEqual(rejected["line_response"]["response"], "downgrade")
        self.assertEqual(rejected["market_acceptance"], "Rejected")
        self.assertEqual(rejected["diagnostic"], "Strong Resistance / Divergence")

    def test_market_language_missing_timeline_never_invents_funds(self):
        result = main.market_language_for_candidate(
            {"market": "over_under", "selection": "over"},
            {"comparison_status": "data_missing"}, {"decision_eligible": False},
        )
        self.assertEqual(result["capital_pressure"]["status"], "data_missing")
        self.assertFalse(result["capital_pressure"]["is_real_money"])
        self.assertEqual(result["market_acceptance"], "data_missing")

    def test_real_money_schema_requires_bound_fresh_a_grade_evidence(self):
        packet = {
            "schema": "real_money_v1", "fixture": "fixture-1", "observed_at": 900,
            "source": {
                "name": "Verified Exchange Feed", "type": "betting_exchange",
                "evidence_ref": "https://exchange.example/markets/fixture-1",
                "authority_verified": True, "methodology_verified": True,
                "methodology": "Matched exchange stakes aggregated by selection before kickoff.",
            },
            "markets": {
                "asian_handicap": {
                    "line": -.5, "money_percent": {"home": 70, "away": 30},
                    "bet_percent": {"home": 60, "away": 40}, "turnover": 125000, "currency": "USD",
                },
                "over_under": {"line": 2.5, "money_percent": {"over": 65, "under": 35}},
            },
        }
        registry = {"exchange.example": {"registry_id": "exchange-1", "allowed_types": ["betting_exchange"]}}
        with patch.object(main, "REAL_MONEY_SOURCE_REGISTRY", registry):
            audited = main.audit_real_money_data(packet, expected_fixture="fixture-1", data_cutoff_at=1000, kickoff_at=2000)
        self.assertEqual(audited["status"], "available")
        self.assertTrue(audited["decision_eligible"])
        self.assertTrue(audited["is_real_money"])
        self.assertEqual(audited["evidence_grade"], "A")
        self.assertEqual(audited["source"]["authority_domain"], "exchange.example")
        self.assertEqual(audited["markets"]["asian_handicap"]["direction"], "home")
        self.assertEqual(audited["markets"]["asian_handicap"]["concentration_gap"], .4)
        self.assertTrue(audited["evidence_hash"])

    def test_real_money_schema_rejects_mismatch_stale_and_nonclosing_percentages(self):
        packet = {
            "schema": "real_money_v1", "fixture": "wrong-fixture", "observed_at": 1,
            "source": {
                "name": "Claimed Feed", "type": "verified_money_vendor",
                "evidence_ref": "https://money.example/item/1",
                "authority_verified": True, "methodology_verified": True,
                "methodology": "Verified stake and ticket distribution captured before kickoff.",
            },
            "markets": {"over_under": {"line": 2.5, "money_percent": {"over": 75, "under": 15}}},
        }
        audited = main.audit_real_money_data(packet, expected_fixture="fixture-1", data_cutoff_at=30000, kickoff_at=40000)
        self.assertEqual(audited["status"], "rejected")
        self.assertFalse(audited["decision_eligible"])
        self.assertFalse(audited["is_real_money"])
        self.assertIsNone(audited["evidence_hash"])
        self.assertIn("real_money_fixture_mismatch", audited["reasons"])
        self.assertIn("real_money_observation_stale", audited["reasons"])
        self.assertIn("over_under_money_percent_percentages_must_sum_to_100", audited["reasons"])

    def test_unverified_or_unlocatable_money_source_never_becomes_a_grade(self):
        packet = {
            "schema": "real_money_v1", "fixture": "fixture-1", "observed_at": 900,
            "source": {
                "name": "Anonymous", "type": "social_media",
                "evidence_ref": "rumor", "authority_verified": False,
                "methodology_verified": False, "methodology": "unknown",
            },
            "markets": {"asian_handicap": {"line": -.5, "money_percent": {"home": 55, "away": 45}}},
        }
        audited = main.audit_real_money_data(packet, expected_fixture="fixture-1", data_cutoff_at=1000, kickoff_at=2000)
        self.assertEqual(audited["status"], "rejected")
        self.assertIn("real_money_source_type_not_a_grade", audited["reasons"])
        self.assertIn("real_money_locatable_http_evidence_required", audited["reasons"])
        self.assertIn("real_money_source_not_in_verified_registry", audited["reasons"])
        self.assertIn("real_money_source_authority_verification_required", audited["reasons"])

    def test_a_grade_real_money_is_distinct_from_proxy_and_does_not_replace_line_response(self):
        packet = {
            "schema": "real_money_v1", "fixture": "fixture-1", "observed_at": 900,
            "source": {
                "name": "Official Stakes", "type": "bookmaker_official",
                "evidence_ref": "https://book.example/fixture-1/stakes",
                "authority_verified": True, "methodology_verified": True,
                "methodology": "Official prematch stake distribution across the quoted handicap line.",
            },
            "markets": {"asian_handicap": {"line": -.5, "money_percent": {"home": 70, "away": 30}, "bet_percent": {"home": 55, "away": 45}}},
        }
        registry = {"book.example": {"registry_id": "book-1", "allowed_types": ["bookmaker_official"]}}
        with patch.object(main, "REAL_MONEY_SOURCE_REGISTRY", registry):
            real = main.audit_real_money_data(packet, expected_fixture="fixture-1", data_cutoff_at=1000, kickoff_at=2000)
        dynamics = {
            "comparison_status": "compared",
            "no_vig_probability_movements": {"1x2": {"status": "compared", "deltas": {"home": -.02, "away": .02}}},
            "market_movements": {"asian_handicap": {"line": 0.0}},
        }
        language = main.market_language_for_candidate(
            {"market": "asian_handicap", "selection": "home", "line": -.5},
            dynamics, {"decision_eligible": True}, real,
        )
        self.assertEqual(language["capital_pressure"]["label"], "Real Funds")
        self.assertEqual(language["capital_pressure"]["evidence_grade"], "A")
        self.assertTrue(language["capital_pressure"]["is_real_money"])
        self.assertEqual(language["capital_pressure"]["money_percent"]["home"], 70)
        self.assertEqual(language["line_response"]["response"], "static")
        self.assertEqual(language["market_acceptance"], "Resistance")
        candidate = {
            "market": "asian_handicap", "selection": "home", "line": -.5, "price": 1.91,
            "model_probability": .57, "market_no_vig_probability": .52, "edge": .05, "ev": .089,
            "script_coverage": .82, "market_coverage_eligible": True,
            "consensus_source_eligible": True, "dispersion_eligible": True,
        }
        decision = {
            "decision": "asian_handicap:home", "best_market": candidate, "candidates": [candidate],
            "edge": .05, "ev": .089, "pass_reasons": [], "line_movement_audit": {"decision_eligible": True},
            "recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": None, "high_variance_single": None},
        }
        optimized = main.apply_market_language_and_expression_optimizer(
            decision, [{"stage": "T-1h", "snapshot_at": 1000, "market_dynamics": dynamics}], real,
        )
        self.assertFalse(optimized["market_language"]["capital_pressure_is_proxy_only"])
        self.assertEqual(optimized["market_language"]["real_money_data"]["status"], "available")
        self.assertEqual(optimized["market_language"]["real_money_data"]["evidence_hash"], real["evidence_hash"])

    def test_expression_optimizer_switches_same_script_not_direction(self):
        over = {
            "market": "over_under", "selection": "over", "line": 2.5, "price": 1.9,
            "model_probability": .57, "market_no_vig_probability": .52, "edge": .05, "ev": .083,
            "script_coverage": .82, "market_coverage_eligible": True,
            "consensus_source_eligible": True, "dispersion_eligible": True,
        }
        btts = {
            "market": "btts", "selection": "yes", "line": None, "price": 1.85,
            "model_probability": .58, "market_no_vig_probability": .53, "edge": .05, "ev": .073,
            "script_coverage": .78, "market_coverage_eligible": True,
            "consensus_source_eligible": True, "dispersion_eligible": True,
        }
        dynamics = {
            "comparison_status": "compared",
            "no_vig_probability_movements": {
                "over_under": {"status": "compared", "deltas": {"over": .04, "under": -.04}},
                "btts": {"status": "compared", "deltas": {"yes": .03, "no": -.03}},
            },
            "market_movements": {"over_under": {"line": 0.0}, "btts": {"yes": -.08, "no": .08}},
        }
        decision = {
            "decision": "over_under:over", "best_market": over, "candidates": [over, btts],
            "edge": over["edge"], "ev": over["ev"], "pass_reasons": [],
            "line_movement_audit": {"decision_eligible": True},
            "recommendation_tiers": {"first_choice_high_consistency": over, "second_choice_higher_return": btts, "high_variance_single": None},
        }
        history = [{"stage": "T-1h", "snapshot_at": 200, "market_dynamics": dynamics}]
        optimized = main.apply_market_language_and_expression_optimizer(decision, history)
        self.assertEqual(optimized["execution_action"], "BET")
        self.assertEqual(optimized["best_market"]["market"], "btts")
        self.assertEqual(optimized["best_market"]["selection"], "yes")
        self.assertEqual(optimized["expression_optimizer"]["switch_type"], "cross_market_same_script")
        self.assertFalse(optimized["expression_optimizer"]["automatic_direction_reversal"])
        self.assertEqual(optimized["market_language"]["selected_acceptance"], "Partial")
        self.assertEqual(set(optimized["market_language"]["axes"]), {"Home", "Away", "Over", "Under"})
        self.assertEqual(optimized["market_language"]["real_money_data"]["status"], "data_missing")

    def test_expression_optimizer_waits_when_pressure_meets_line_retreat(self):
        home = {
            "market": "asian_handicap", "selection": "home", "line": -.5, "price": 1.91,
            "model_probability": .57, "market_no_vig_probability": .52, "edge": .05, "ev": .089,
            "script_coverage": .82, "market_coverage_eligible": True,
            "consensus_source_eligible": True, "dispersion_eligible": True,
        }
        dynamics = {
            "comparison_status": "compared",
            "no_vig_probability_movements": {"1x2": {"status": "compared", "deltas": {"home": .04, "draw": -.01, "away": -.03}}},
            "market_movements": {"asian_handicap": {"line": .25}},
        }
        decision = {
            "decision": "asian_handicap:home", "best_market": home, "candidates": [home],
            "edge": home["edge"], "ev": home["ev"], "pass_reasons": [],
            "line_movement_audit": {"decision_eligible": True},
            "recommendation_tiers": {"first_choice_high_consistency": home, "second_choice_higher_return": None, "high_variance_single": None},
        }
        optimized = main.apply_market_language_and_expression_optimizer(decision, [{"stage": "T-1h", "snapshot_at": 200, "market_dynamics": dynamics}])
        self.assertEqual(optimized["execution_action"], "WAIT")
        self.assertEqual(optimized["market_language"]["selected_acceptance"], "Rejected")
        self.assertEqual(optimized["market_language"]["selected_diagnostic"], "Strong Resistance / Divergence")
        self.assertIsNone(optimized["recommendation_tiers"]["first_choice_high_consistency"])
        self.assertEqual(optimized["best_market"]["selection"], "home")
        self.assertFalse(optimized["expression_optimizer"]["automatic_direction_reversal"])

    def test_portfolio_excludes_wait_expression_even_when_candidate_exists(self):
        candidate = {
            "market": "1x2", "selection": "home", "line": None, "price": 1.8,
            "model_probability": .62, "market_no_vig_probability": .56,
            "edge": .06, "ev": .116, "script_coverage": .8,
        }
        rows = []
        for fixture, action in (("wait-leg", "WAIT"), ("bet-leg-1", "BET"), ("bet-leg-2", "BET")):
            rows.append({
                "fixture": fixture, "correlation_group": fixture,
                "evaluation": {"decision_layer": {
                    "execution_action": action, "lineup_confidence": .9, "crowding": .3,
                    "line_movement": {"comparison_status": "compared"}, "death_path": [],
                    "recommendation_tiers": {"first_choice_high_consistency": candidate},
                }},
            })
        combination = main._combination_from_rows(rows, "first_choice_high_consistency", 3)
        self.assertEqual(combination["decision"], "COMBINE")
        self.assertEqual({leg["fixture"] for leg in combination["legs"]}, {"bet-leg-1", "bet-leg-2"})
        excluded = {row["fixture"]: row for row in combination["selection_audit"]["excluded"]}
        self.assertEqual(excluded["wait-leg"]["reason"], "execution_action_not_bet")

    def test_wait_expression_is_not_retroactively_settled_as_a_bet(self):
        freeze = {"decision": {
            "decision": "asian_handicap:home", "execution_action": "WAIT",
            "best_market": {"market": "asian_handicap", "selection": "home", "line": -.5, "price": 1.91},
        }}
        outcome = main._derive_frozen_selection_outcome(freeze, {"result": {"home_goals": 3, "away_goals": 0}})
        self.assertEqual(outcome["status"], "not_executed")
        self.assertIsNone(outcome["outcome"])
        self.assertEqual(outcome["execution_action"], "WAIT")

    def test_line_movement_gate_rejects_duplicate_observation_times(self):
        history = [
            {"stage": "T-24h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": -.1}}}},
        ]
        audit = main.audit_line_movement_timeline(history)
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("fewer_than_two_distinct_observation_times", audit["blockers"])
        self.assertIn("observation_times_not_strictly_in_stage_order", audit["blockers"])

    def test_line_movement_gate_rejects_reverse_chronology(self):
        history = [
            {"stage": "T-24h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": -.1}}}},
        ]
        audit = main.audit_line_movement_timeline(history)
        self.assertFalse(audit["decision_eligible"])
        self.assertFalse(audit["observation_times_strictly_chronological"])
        self.assertIn("observation_times_not_strictly_in_stage_order", audit["blockers"])

    def test_line_movement_gate_rejects_missing_observation_time(self):
        history = [
            {"stage": "T-24h", "snapshot_at": None, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": -.1}}}},
        ]
        audit = main.audit_line_movement_timeline(history)
        self.assertFalse(audit["decision_eligible"])
        self.assertFalse(audit["all_observation_times_valid"])
        self.assertIn("observation_times_not_strictly_in_stage_order", audit["blockers"])

    def test_line_movement_gate_rejects_empty_compared_marker(self):
        history = [
            {"stage": "T-24h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared", "market_movements": {}, "no_vig_probability_movements": {}}},
        ]
        audit = main.audit_line_movement_timeline(history)
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("latest_stage_has_no_shared_comparable_market", audit["blockers"])

    def test_shadow_ai_packet_ignores_ft_and_invalid_prematch_nodes(self):
        snapshots = [
            {"stage": "T-24h", "snapshot_at": 100, "market_snapshot": {"available": True, "primary": {"asian_handicap": {"line": -.25}}}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"stage": "T-3h", "snapshot_at": 200, "stage_timing_audit": {"status": "invalid"}, "market_snapshot": {"available": True, "primary": {"asian_handicap": {"line": -1.0}}}, "market_dynamics": {"comparison_status": "compared"}},
            {"stage": "FT", "snapshot_at": 300, "market_snapshot": {"available": True, "primary": {"asian_handicap": {"line": -2.0}}}, "market_dynamics": {"comparison_status": "compared"}},
        ]
        data = {"ok": True, "fixture": {"home": "H", "away": "A"}, "structured_inputs": {"odds_market_snapshot": main.empty_market_snapshot()}, "coverage": {}, "data_quality": {}, "shadow_summary": {}}
        with patch.object(main, "collect_prematch_data", return_value=data), patch.object(main, "get_fixture_snapshots", return_value=snapshots), patch.object(main, "get_fundamental_versions", return_value=[]):
            packet = main.build_shadow_ai_packet(1)
        self.assertEqual(packet["market"]["available_prematch_stage_count"], 1)
        self.assertIn("T-3h", packet["market"]["missing_stages"])
        self.assertEqual(packet["market"]["total_asian_handicap_move"], 0.0)
        self.assertEqual(packet["decision_layer"]["decision"], "PASS")
        self.assertIn("line_movement_requires_two_real_comparable_stages", packet["decision_layer"]["pass_reasons"])

    def standings_model_fixture(self, observed_at=900):
        def row(team_id, home_played, home_for, home_against, away_played, away_for, away_against):
            return {
                "team": {"id": team_id, "name": f"Team {team_id}"},
                "home": {"played": home_played, "goals": {"for": home_for, "against": home_against}},
                "away": {"played": away_played, "goals": {"for": away_for, "against": away_against}},
            }
        response = {
            "ok": True,
            "data": {"response": [{"league": {"standings": [[
                row(1, 10, 18, 8, 10, 11, 13),
                row(2, 10, 14, 12, 10, 15, 14),
                row(3, 10, 16, 10, 10, 9, 16),
            ]]}}]},
        }
        return main.independent_model_inputs_from_standings(
            response, 1, 2, observed_at=observed_at, lineup_confidence=.9,
        )

    def test_standings_probability_replay_is_odds_independent_and_recomputable(self):
        bundle = self.standings_model_fixture()
        self.assertEqual(bundle["status"], "ready")
        self.assertFalse(bundle["uses_market_odds"])
        self.assertAlmostEqual(bundle["inputs"]["home_attack_rate"], 1.8)
        self.assertAlmostEqual(bundle["inputs"]["away_attack_rate"], 1.5)
        replay = main.build_learning_probability_replay("1", bundle, generated_at=950)
        self.assertEqual(replay["status"], "ready")
        self.assertTrue(replay["decision_eligible"])
        audit = main.audit_learning_probability_replay(replay, "1", 950, 2000)
        self.assertEqual(audit["status"], "ready")
        self.assertTrue(audit["decision_eligible"])

        forged = copy.deepcopy(replay)
        forged["model"]["probabilities"]["1x2"]["home"] += .01
        rejected = main.audit_learning_probability_replay(forged, "1", 950, 2000)
        self.assertEqual(rejected["status"], "invalid")
        self.assertIn("probability_output_mismatch", rejected["issues"])
        self.assertIn("replay_hash_mismatch", rejected["issues"])

        contaminated = copy.deepcopy(replay)
        contaminated["inputs"]["market_odds"] = {"home": 2.0}
        contaminated["input_hash"] = main._content_hash(contaminated["inputs"])
        contaminated["replay_hash"] = main._content_hash({key: value for key, value in contaminated.items() if key != "replay_hash"})
        rejected = main.audit_learning_probability_replay(contaminated, "1", 950, 2000)
        self.assertEqual(rejected["status"], "invalid")
        self.assertTrue(any(issue.startswith("unexpected_model_input_fields") for issue in rejected["issues"]))
        self.assertIn("market_derived_model_input_field_forbidden", rejected["issues"])

    def test_shadow_ai_packet_binds_probability_but_not_unpersisted_current_odds(self):
        current_market = main.empty_market_snapshot()
        current_market["available"] = True
        current_market["consensus_main_line"]["1x2"] = {
            "home": 2.0, "draw": 3.5, "away": 4.0,
            "source": "complete_company_array", "bookmaker_count": 4,
        }
        data = {
            "ok": True, "generated_at": 950,
            "fixture": {"fixture_id": 1, "home": "H", "away": "A", "date": 2000},
            "structured_inputs": {
                "odds_market_snapshot": current_market,
                "independent_model_inputs": self.standings_model_fixture(),
                "lineups_available": True, "lineups_confirmed": True,
            },
            "coverage": {}, "data_quality": {}, "shadow_summary": {},
        }
        with patch.object(main, "collect_prematch_data", return_value=data), patch.object(main, "get_fixture_snapshots", return_value=[]), patch.object(main, "get_fundamental_versions", return_value=[]):
            packet = main.build_shadow_ai_packet(1)
        self.assertEqual(packet["probability_replay"]["status"], "ready")
        self.assertEqual(packet["decision_layer"]["model_probability"], packet["probability_replay"]["model"]["probabilities"])
        self.assertFalse(packet["decision_layer"]["market_evidence_binding"]["current_unpersisted_quote_used"])
        self.assertEqual(packet["decision_layer"]["candidates"], [])
        self.assertEqual(packet["decision_layer"]["decision"], "PASS")

        candidate = {
            "fixture_id": 1, "timestamp": 2000, "status": "NS",
            "target_analysis_node": "T-12h", "scope": {"competition_id": 39},
        }
        frozen = main.build_learning_freeze_payload(candidate, packet, now_ts=950)
        self.assertTrue(frozen["analysis"]["probability_replay_audit"]["decision_eligible"])
        forged_action = copy.deepcopy(packet)
        forged_action["decision_layer"].update({
            "decision": "home", "execution_action": "BET",
            "best_market": {"market": "1x2", "selection": "home"},
        })
        with self.assertRaises(main.HTTPException) as unbound_market:
            main.build_learning_freeze_payload(candidate, forged_action, now_ts=950)
        self.assertEqual(unbound_market.exception.detail, "actionable_decision_market_snapshot_not_hash_bound")
        tampered = copy.deepcopy(packet)
        tampered["probability_replay"]["inputs"]["home_attack_rate"] = 4.0
        with self.assertRaises(main.HTTPException) as rejected:
            main.build_learning_freeze_payload(candidate, tampered, now_ts=950)
        self.assertEqual(rejected.exception.detail["error"], "learning_probability_replay_invalid")

    def test_decision_layer_calculates_no_vig_edge_ev(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0, "source": "complete_company_array"}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "home")
        self.assertAlmostEqual(result["ev"], .10, places=6)

    def test_decision_layer_passes_when_consensus_has_only_one_bookmaker(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0, "bookmaker_count": 1}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_bookmaker_coverage_below_minimum", result["pass_reasons"])
        self.assertFalse(result["candidates"][0]["market_coverage_eligible"])

    def test_decision_layer_does_not_bet_upstream_consensus_fallback(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0, "bookmaker_count": 4, "source": "upstream_consensus_fallback"}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_not_recalculated_from_company_array", result["pass_reasons"])
        self.assertFalse(result["candidates"][0]["consensus_source_eligible"])

    def test_decision_layer_does_not_treat_unknown_consensus_source_as_company_array(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0, "bookmaker_count": 4}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_not_recalculated_from_company_array", result["pass_reasons"])
        self.assertFalse(result["candidates"][0]["consensus_source_eligible"])

    def test_native_consensus_functions_mark_company_array_source(self):
        one_x_two = main._consensus_1x2([
            {"bookmaker": "A", "home": 2.0, "draw": 3.5, "away": 4.0},
            {"bookmaker": "B", "home": 2.1, "draw": 3.4, "away": 3.9},
        ])
        line = main._consensus_line([
            {"bookmaker": "A", "lines": [{"line": 2.5, "over": 1.9, "under": 1.95}]},
            {"bookmaker": "B", "lines": [{"line": 2.5, "over": 1.92, "under": 1.93}]},
        ], ("over", "under"))
        self.assertEqual(one_x_two["source"], "complete_company_array")
        self.assertEqual(line["source"], "complete_company_array")

    def test_consensus_dispersion_blocks_conflicting_bookmaker_prices(self):
        consensus = main._consensus_1x2([
            {"bookmaker": "A", "home": 1.7, "draw": 3.4, "away": 4.2},
            {"bookmaker": "B", "home": 2.1, "draw": 3.45, "away": 4.1},
        ])
        self.assertFalse(consensus["dispersion_eligible"])
        self.assertAlmostEqual(consensus["maximum_price_spread"], .4)
        self.assertGreater(consensus["maximum_no_vig_probability_spread"], .05)
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = consensus
        result = main.decision_layer(snapshot, {"home": .58, "draw": .23, "away": .19}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_price_dispersion_above_maximum", result["pass_reasons"])

    def test_probability_dispersion_does_not_over_penalize_long_odds_price_gap(self):
        consensus = main._consensus_1x2([
            {"bookmaker": "A", "home": 1.2, "draw": 6.0, "away": 15.0},
            {"bookmaker": "B", "home": 1.2, "draw": 6.0, "away": 15.4},
        ])
        self.assertGreater(consensus["maximum_price_spread"], .25)
        self.assertFalse(consensus["price_spread_within_reference"])
        self.assertTrue(consensus["dispersion_eligible"])
        self.assertLess(consensus["maximum_no_vig_probability_spread"], .05)

    def test_decision_uses_bookmaker_level_no_vig_consensus(self):
        consensus = main._consensus_1x2([
            {"bookmaker": "A", "home": 2.0, "draw": 3.0, "away": 4.0},
            {"bookmaker": "B", "home": 1.8, "draw": 4.0, "away": 5.0},
        ])
        consensus["dispersion_eligible"] = True
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = consensus
        result = main.decision_layer(snapshot, {"home": .60, "draw": .23, "away": .17}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        market = result["market_no_vig_probability"]["1x2"]
        self.assertEqual(market["method"], "bookmaker_level_no_vig_consensus")
        self.assertEqual(market["probabilities"], consensus["consensus_no_vig_probabilities"])
        self.assertAlmostEqual(sum(market["probabilities"].values()), 1.0, places=5)

    def test_timeline_movement_uses_same_bookmaker_level_no_vig_consensus(self):
        previous_market = {"home": 2.0, "draw": 3.5, "away": 4.0, "consensus_no_vig_probabilities": {"home": .50, "draw": .28, "away": .22}}
        current_market = {"home": 1.9, "draw": 3.6, "away": 4.2, "consensus_no_vig_probabilities": {"home": .54, "draw": .27, "away": .19}}
        previous = {"stage": "T-24h", "market_snapshot": {"primary": {"1x2": previous_market}}}
        current = main.empty_market_snapshot()
        current["primary"]["1x2"] = current_market
        dynamics = main.compare_market_snapshots([previous], current, "T-12h")
        movement = dynamics["no_vig_probability_movements"]["1x2"]
        self.assertEqual(movement["current_method"], "bookmaker_level_no_vig_consensus")
        self.assertEqual(movement["previous_method"], "bookmaker_level_no_vig_consensus")
        self.assertAlmostEqual(movement["deltas"]["home"], .04)

    def test_high_odds_price_change_does_not_override_small_probability_move(self):
        previous_market = {"home": 1.2, "draw": 6.0, "away": 15.0, "consensus_no_vig_probabilities": {"home": .78, "draw": .16, "away": .06}}
        current_market = {"home": 1.2, "draw": 6.0, "away": 15.4, "consensus_no_vig_probabilities": {"home": .781, "draw": .159, "away": .06}}
        previous = {"stage": "T-24h", "market_snapshot": {"primary": {"1x2": previous_market}}}
        current = main.empty_market_snapshot()
        current["primary"]["1x2"] = current_market
        dynamics = main.compare_market_snapshots([previous], current, "T-12h")
        self.assertNotIn("abnormal_price_move", dynamics["revalidation_trigger"]["reasons"])
        self.assertFalse(dynamics["revalidation_trigger"]["signal_audit"]["decimal_fallback_signal"])

    def test_decimal_price_fallback_remains_when_probability_is_unavailable(self):
        previous = {"stage": "T-24h", "market_snapshot": {"primary": {"1x2": {"home": 2.0}}}}
        current = main.empty_market_snapshot()
        current["primary"]["1x2"] = {"home": 1.85}
        dynamics = main.compare_market_snapshots([previous], current, "T-12h")
        self.assertIn("abnormal_price_move", dynamics["revalidation_trigger"]["reasons"])
        self.assertTrue(dynamics["revalidation_trigger"]["signal_audit"]["decimal_fallback_signal"])

    def test_cross_market_divergence_prefers_probability_direction(self):
        previous = {"stage": "T-24h", "market_snapshot": {"primary": {
            "over_under": {"line": 2.5, "over": 1.9, "under": 1.9, "consensus_no_vig_probabilities": {"over": .50, "under": .50}},
            "btts": {"yes": 1.9, "no": 1.9, "consensus_no_vig_probabilities": {"yes": .50, "no": .50}},
        }}}
        current = main.empty_market_snapshot()
        current["primary"].update({
            "over_under": {"line": 2.5, "over": 1.85, "under": 1.95, "consensus_no_vig_probabilities": {"over": .54, "under": .46}},
            "btts": {"yes": 1.85, "no": 1.95, "consensus_no_vig_probabilities": {"yes": .47, "no": .53}},
        })
        dynamics = main.compare_market_snapshots([previous], current, "T-12h")
        self.assertTrue(dynamics["cross_market_divergence_pairs"]["over_under_vs_btts"])
        self.assertEqual(dynamics["cross_market_directional_signals"]["total_over_strength"], .04)
        self.assertEqual(dynamics["cross_market_directional_signals"]["btts_yes_strength"], -.03)

    def test_market_comparison_never_uses_future_stage_as_previous(self):
        history = [
            {"stage": "T-24h", "snapshot_at": 100, "market_snapshot": {"primary": {"1x2": {"home": 2.0, "draw": 3.5, "away": 4.0}}}},
            {"stage": "T-1h", "snapshot_at": 300, "market_snapshot": {"primary": {"1x2": {"home": 1.7, "draw": 3.8, "away": 5.0}}}},
            {"stage": "Closing", "snapshot_at": 400, "market_snapshot": {"primary": {"1x2": {"home": 1.6, "draw": 4.0, "away": 5.5}}}},
        ]
        current = main.empty_market_snapshot()
        current["primary"]["1x2"] = {"home": 1.9, "draw": 3.6, "away": 4.2}
        dynamics = main.compare_market_snapshots(history, current, "T-6h")
        self.assertEqual(dynamics["previous_stage"], "T-24h")
        self.assertAlmostEqual(dynamics["market_movements"]["1x2"]["home"], -.1)

    def test_opening_never_uses_later_stage_as_history(self):
        history = [{"stage": "T-24h", "snapshot_at": 200, "market_snapshot": {"primary": {"1x2": {"home": 2.0}}}}]
        current = main.empty_market_snapshot()
        current["primary"]["1x2"] = {"home": 2.1}
        dynamics = main.compare_market_snapshots(history, current, "Opening")
        self.assertEqual(dynamics["comparison_status"], "data_missing")
        self.assertIsNone(dynamics["previous_stage"])

    def test_native_timeline_sequence_is_audited_without_import_status(self):
        rows = [
            {"stage": "T-24h", "snapshot_at": 100, "market_snapshot": {"available": True}, "stage_timing_audit": {"status": "valid"}},
            {"stage": "T-12h", "snapshot_at": 200, "market_snapshot": {"available": True}, "stage_timing_audit": {"status": "valid"}},
        ]
        audit = main.audit_timeline_sequence(rows)
        self.assertEqual(audit["T-24h"]["status"], "valid")
        self.assertEqual(audit["T-12h"]["status"], "valid")

    def test_auto_snapshot_never_catches_up_missed_historical_stage(self):
        kickoff = main.datetime(2026, 10, 10, 20, 0, tzinfo=main.timezone.utc)
        stage = next(row for row in main.TRACKING_STAGES if row["key"] == "T-24h")
        due = kickoff + stage["offset"]
        self.assertTrue(main.auto_snapshot_stage_due(due + main.timedelta(minutes=5), kickoff, stage))
        self.assertFalse(main.auto_snapshot_stage_due(due + main.timedelta(hours=7), kickoff, stage))
        self.assertFalse(main.auto_snapshot_stage_due(due - main.timedelta(hours=7), kickoff, stage))

    def test_auto_snapshot_stage_due_rejects_late_but_tolerance_valid_quote(self):
        kickoff = main.datetime(2026, 1, 2, 12, tzinfo=main.timezone.utc)
        stage = next(item for item in main.TRACKING_STAGES if item["key"] == "T-24h")
        due = kickoff + stage["offset"]
        self.assertFalse(main.auto_snapshot_stage_due(due + main.timedelta(hours=1), kickoff, stage))

    def test_invalid_or_empty_saved_stage_remains_retryable(self):
        history = [
            {"stage": "T-24h", "market_snapshot": {"available": True}, "stage_timing_audit": {"status": "invalid"}},
            {"stage": "T-12h", "market_snapshot": {"available": False}, "stage_timing_audit": {"status": "valid"}},
            {"stage": "T-6h", "market_snapshot": {"available": True}, "stage_timing_audit": {"status": "valid"}, "sequence_timing_audit": {"status": "valid"}},
        ]
        completed = main.completed_auto_snapshot_stages(history)
        self.assertNotIn("T-24h", completed)
        self.assertNotIn("T-12h", completed)
        self.assertIn("T-6h", completed)

    def test_consensus_deduplicates_bookmaker_name_variants(self):
        consensus = main._consensus_1x2([
            {"bookmaker": "Book A", "home": 2.0, "draw": 3.4, "away": 4.0},
            {"bookmaker": "  book   a ", "home": 2.2, "draw": 3.2, "away": 3.8},
        ])
        self.assertEqual(consensus["bookmaker_count"], 1)
        self.assertEqual(consensus["home"], 2.1)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["raw_quote_count"], 2)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["unique_bookmaker_count"], 1)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["duplicate_quote_count"], 1)
        self.assertFalse(consensus["bookmaker_coverage_audit"]["duplicate_quotes_count_as_additional_bookmakers"])
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = consensus
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_bookmaker_coverage_below_minimum", result["pass_reasons"])

    def test_consensus_missing_bookmaker_names_share_one_identity(self):
        consensus = main._consensus_1x2([
            {"bookmaker": None, "home": 2.0, "draw": 3.4, "away": 3.8},
            {"bookmaker": "", "home": 2.1, "draw": 3.3, "away": 3.7},
        ])
        self.assertEqual(consensus["bookmaker_count"], 1)
        audit = consensus["bookmaker_coverage_audit"]
        self.assertEqual(audit["missing_bookmaker_name_quote_count"], 2)
        self.assertEqual(audit["unique_bookmaker_count"], 1)

    def test_consensus_rejects_synthetic_completion_from_fragments(self):
        rows = [
            {"bookmaker": "Book A", "home": 2.0, "draw": 3.4, "away": None},
            {"bookmaker": "book a", "home": None, "draw": None, "away": 3.8},
        ]
        complete, deduped, fragmented = main._complete_deduped_bookmaker_rows(rows, ("home", "draw", "away"))
        self.assertEqual(complete, [])
        self.assertEqual(len(deduped), 1)
        self.assertEqual(fragmented, ["book a"])
        self.assertIsNone(main._consensus_1x2(rows))

    def test_consensus_accepts_duplicate_rows_when_one_is_complete(self):
        rows = [
            {"bookmaker": "Book A", "home": 2.0, "draw": 3.4, "away": 3.8},
            {"bookmaker": "book a", "home": 2.2, "draw": None, "away": None},
        ]
        consensus = main._consensus_1x2(rows)
        self.assertEqual(consensus["bookmaker_count"], 1)
        self.assertEqual(consensus["home"], 2.0)
        self.assertEqual(consensus["draw"], 3.4)
        self.assertEqual(consensus["away"], 3.8)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["eligible_unique_bookmaker_count"], 1)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["fragmented_synthetic_complete_identities_rejected"], [])

    def test_incomplete_duplicate_cannot_distort_complete_quote(self):
        rows = [
            {"bookmaker": "Book A", "yes": 1.8, "no": 2.0},
            {"bookmaker": "book a", "yes": 9.0, "no": None},
        ]
        complete, deduped, fragmented = main._complete_deduped_bookmaker_rows(rows, ("yes", "no"))
        self.assertEqual(len(complete), 1)
        self.assertEqual(complete[0]["yes"], 1.8)
        self.assertEqual(complete[0]["no"], 2.0)
        self.assertEqual(len(deduped), 1)
        self.assertEqual(fragmented, [])

    def test_duplicate_selection_inside_quote_is_rejected(self):
        rows = [{"bookmaker": "Book A", "home": 2.0, "draw": 3.4, "away": 3.8, "ambiguous_duplicate_selection": True}]
        self.assertIsNone(main._consensus_1x2(rows))

    def test_line_parser_marks_repeated_side_at_same_line_ambiguous(self):
        lines = main._line_market([
            {"value": "Over 2.5", "odd": "1.90"},
            {"value": "Over 2.5", "odd": "1.95"},
            {"value": "Under 2.5", "odd": "1.90"},
        ], ("Over", "Under"), ("over", "under"))
        self.assertTrue(lines[0]["ambiguous_duplicate_selection"])
        self.assertEqual(lines[0]["selection_counts"]["over"], 2)
        self.assertIsNone(main._consensus_line([{"bookmaker": "A", "lines": lines}], ("over", "under")))

    def test_two_way_parser_marks_repeated_selection_ambiguous(self):
        entry = main._two_way_market([
            {"value": "Yes", "odd": "1.8"}, {"value": "Yes", "odd": "1.9"}, {"value": "No", "odd": "2.0"},
        ], ("yes", "no"))
        self.assertTrue(entry["ambiguous_duplicate_selection"])
        self.assertEqual(entry["selection_counts"], {"yes": 2, "no": 1})

    def test_line_consensus_exposes_selected_line_bookmaker_coverage(self):
        consensus = main._consensus_line([
            {"bookmaker": "Book A", "lines": [{"line": 2.5, "over": 1.9, "under": 1.9}, {"line": 2.5, "over": 1.92, "under": 1.88}]},
            {"bookmaker": "Book B", "lines": [{"line": 2.5, "over": 1.91, "under": 1.89}]},
        ], ("over", "under"))
        self.assertEqual(consensus["bookmaker_count"], 2)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["raw_quote_count"], 3)
        self.assertEqual(consensus["bookmaker_coverage_audit"]["duplicate_quote_count"], 1)

    def test_consensus_line_tie_uses_market_center_not_shallowest_line(self):
        consensus = main._consensus_line([{
            "bookmaker": "A",
            "lines": [
                {"line": 2.0, "over": 1.9, "under": 1.9},
                {"line": 2.5, "over": 1.9, "under": 1.9},
                {"line": 3.0, "over": 1.9, "under": 1.9},
            ],
        }], ("over", "under"))
        self.assertEqual(consensus["line"], 2.5)
        self.assertEqual(consensus["tie_break_reference_line"], 2.5)

    def test_model_market_divergence_uses_probability_gap(self):
        decision = {"lineup_confidence": .9, "candidates": [
            {"market": "1x2", "selection": "home", "edge": .035},
            {"market": "btts", "selection": "yes", "edge": -.12},
        ]}
        result = main.detect_model_market_divergence(decision)
        self.assertTrue(result["triggered"])
        self.assertEqual(result["market"], "btts")
        self.assertEqual(result["direction"], "model_below_market")
        self.assertEqual(result["maximum_absolute_probability_gap"], .12)

    def test_model_market_divergence_requires_comparable_probabilities(self):
        result = main.detect_model_market_divergence({"lineup_confidence": .9, "candidates": []})
        self.assertFalse(result["triggered"])
        self.assertEqual(result["comparison_status"], "data_missing")

    def test_model_market_divergence_is_gated_by_model_fundamentals_and_lineup(self):
        decision = {"lineup_confidence": .4, "candidates": [{"market": "1x2", "selection": "home", "edge": .2}]}
        result = main.detect_model_market_divergence(decision, model_ready=False, fundamental_eligible=False)
        self.assertFalse(result["triggered"])
        self.assertFalse(result["classification_eligible"])
        self.assertIn("model_not_ready", result["eligibility_reasons"])
        self.assertIn("fundamental_chain_insufficient", result["eligibility_reasons"])
        self.assertIn("lineup_confidence_insufficient", result["eligibility_reasons"])
        self.assertEqual(result["maximum_absolute_probability_gap"], .2)

    def test_model_market_divergence_rejects_stale_or_invalid_market_data(self):
        decision = {"lineup_confidence": .9, "candidates": [{"market": "1x2", "selection": "home", "edge": .2}]}
        result = main.detect_model_market_divergence(decision, market_data_eligible=False)
        self.assertFalse(result["triggered"])
        self.assertFalse(result["classification_eligible"])
        self.assertIn("market_data_not_fresh_or_valid", result["eligibility_reasons"])
        self.assertEqual(result["maximum_absolute_probability_gap"], .2)

    def test_model_market_divergence_excludes_ineligible_consensus_candidates(self):
        decision = {"lineup_confidence": .9, "candidates": [
            {"market": "1x2", "selection": "home", "edge": .2, "market_coverage_eligible": False, "consensus_source_eligible": True, "dispersion_eligible": True},
            {"market": "btts", "selection": "yes", "edge": -.15, "market_coverage_eligible": True, "consensus_source_eligible": False, "dispersion_eligible": True},
        ]}
        result = main.detect_model_market_divergence(decision)
        self.assertFalse(result["triggered"])
        self.assertFalse(result["classification_eligible"])
        self.assertIn("market_consensus_ineligible", result["eligibility_reasons"])
        self.assertEqual(result["candidate_audit"]["eligible_comparable_count"], 0)
        self.assertEqual(result["candidate_audit"]["excluded_ineligible_consensus_count"], 2)

    def test_model_market_divergence_uses_only_eligible_candidate_when_mixed(self):
        decision = {"lineup_confidence": .9, "candidates": [
            {"market": "1x2", "selection": "home", "edge": .3, "market_coverage_eligible": False},
            {"market": "over_under", "selection": "over", "edge": .09, "market_coverage_eligible": True, "consensus_source_eligible": True, "dispersion_eligible": True},
        ]}
        result = main.detect_model_market_divergence(decision)
        self.assertTrue(result["triggered"])
        self.assertEqual(result["market"], "over_under")
        self.assertEqual(result["maximum_absolute_probability_gap"], .09)

    def test_no_vig_rejects_implausible_market_overround(self):
        self.assertIsNone(main.no_vig_probabilities({"home": 1000, "draw": 1000, "away": 1000}, ["home", "draw", "away"]))
        self.assertIsNone(main.no_vig_probabilities({"yes": 1.01, "no": 1.01}, ["yes", "no"]))
        valid = main.no_vig_probabilities({"home": 2.0, "draw": 3.5, "away": 4.0}, ["home", "draw", "away"])
        self.assertAlmostEqual(sum(valid.values()), 1.0, places=5)

    def test_market_no_vig_rejects_zero_probability_and_reports_invalid_fallback(self):
        probabilities, method = main.market_no_vig_probabilities({"consensus_no_vig_probabilities": {"yes": 1.0, "no": 0.0}}, ["yes", "no"])
        self.assertIsNone(probabilities)
        self.assertEqual(method, "invalid_or_missing_market_prices")

    def test_decision_layer_handles_embedded_probabilities_without_prices(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {
            "consensus_no_vig_probabilities": {"home": .5, "draw": .3, "away": .2},
            "bookmaker_count": 3, "source": "complete_company_array",
        }
        result = main.decision_layer(snapshot, {"home": .6, "draw": .25, "away": .15}, {"home": .8, "draw": .4, "away": .3}, .2, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("market_price_for_ev_missing_or_invalid", result["pass_reasons"])
        self.assertEqual(result["candidate_generation_audit"]["generated_candidate_count"], 0)
        self.assertEqual(result["candidate_generation_audit"]["invalid_market_price_count"], 3)

    def test_invalid_price_in_one_market_does_not_block_valid_other_market(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"].update({
            "1x2": {"consensus_no_vig_probabilities": {"home": .5, "draw": .3, "away": .2}, "bookmaker_count": 3, "source": "complete_company_array"},
            "btts": {"yes": 2.2, "no": 1.8, "bookmaker_count": 3, "source": "complete_company_array"},
        })
        model = {"1x2": {"home": .6, "draw": .25, "away": .15}, "btts": {"yes": .55, "no": .45}}
        coverage = {"1x2": {"home": .8}, "btts": {"yes": .8, "no": .3}}
        result = main.decision_layer(snapshot, model, coverage, .2, .9, [])
        self.assertEqual(result["best_market"]["market"], "btts")
        self.assertNotIn("market_price_for_ev_missing_or_invalid", result["pass_reasons"])
        self.assertEqual(result["candidate_generation_audit"]["invalid_market_price_count"], 3)

    def test_embedded_probabilities_must_match_offered_price_shape(self):
        mismatched = {"home": 4.0, "draw": 3.5, "away": 2.0, "consensus_no_vig_probabilities": {"home": .5, "draw": .3, "away": .2}}
        probabilities, method = main.market_no_vig_probabilities(mismatched, ["home", "draw", "away"])
        self.assertIsNone(probabilities)
        self.assertEqual(method, "embedded_probability_price_mismatch")
        aligned = {"home": 2.0, "draw": 3.5, "away": 4.0, "consensus_no_vig_probabilities": {"home": .48, "draw": .28, "away": .24}}
        probabilities, method = main.market_no_vig_probabilities(aligned, ["home", "draw", "away"])
        self.assertIsNotNone(probabilities)
        self.assertEqual(method, "bookmaker_level_no_vig_consensus")

    def test_embedded_probabilities_reject_implausible_complete_price_set(self):
        market = {"yes": 1.01, "no": 1.01, "consensus_no_vig_probabilities": {"yes": .5, "no": .5}}
        probabilities, method = main.market_no_vig_probabilities(market, ["yes", "no"])
        self.assertIsNone(probabilities)
        self.assertEqual(method, "embedded_probability_prices_invalid")

    def test_decision_exposes_per_market_probability_failure_reason(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {
            "home": 4.0, "draw": 3.5, "away": 2.0,
            "consensus_no_vig_probabilities": {"home": .5, "draw": .3, "away": .2},
            "bookmaker_count": 3, "source": "complete_company_array",
        }
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .2}, {"home": .8}, .2, .9, [])
        audit = result["market_probability_audit"]["1x2"]
        self.assertEqual(audit["status"], "data_missing")
        self.assertEqual(audit["method"], "embedded_probability_price_mismatch")
        self.assertIn("market_no_vig_probability", result["pass_reasons"])

    def test_valid_settlement_model_is_not_blamed_for_invalid_market_prices(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["over_under"] = {"line": 2.75, "over": 1.01, "under": 1.01, "bookmaker_count": 3, "source": "complete_company_array"}
        model = {"settlement_distributions": {"total_goals": {0: .05, 1: .15, 2: .25, 3: .25, 4: .2, 5: .1}}}
        result = main.decision_layer(snapshot, model, {"over_under": {"over": .8, "under": .8}}, .2, .9, [])
        self.assertEqual(result["model_probability_audit"]["over_under"]["status"], "available")
        self.assertEqual(result["model_probability_audit"]["over_under"]["method"], "settlement_distribution")
        self.assertNotIn("model_probability_invalid_or_not_normalized", result["pass_reasons"])
        self.assertIn("market_no_vig_probability", result["pass_reasons"])

    def test_model_probability_audit_marks_unsupported_market_line(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["over_under"] = {"line": 2.6, "over": 1.9, "under": 1.9, "source": "complete_company_array"}
        model = {"settlement_distributions": {"total_goals": {0: .1, 1: .2, 2: .3, 3: .25, 4: .15}}}
        result = main.decision_layer(snapshot, model, {"over_under": {"over": .8, "under": .8}}, .2, .9, [])
        audit = result["model_probability_audit"]["over_under"]
        self.assertEqual(audit["status"], "data_missing")
        self.assertEqual(audit["reason"], "model_probability_not_available_for_market_line")
        self.assertIn("model_probability_invalid_or_not_normalized", result["pass_reasons"])

    def test_decision_layer_enforces_minimums_and_probability_validation(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0}
        weak = main.decision_layer(snapshot, {"home": .50, "draw": .28, "away": .22}, {"home": .8, "draw": .4, "away": .3}, .4, .9, [])
        self.assertEqual(weak["decision"], "PASS")
        self.assertTrue(any(reason in weak["pass_reasons"] for reason in ("edge_below_minimum", "ev_below_minimum")))
        crowded = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .4, "away": .3}, .9, .9, [])
        self.assertIn("crowding_above_maximum", crowded["pass_reasons"])
        invalid = main.decision_layer(snapshot, {"home": .70, "draw": .40, "away": .20}, {"home": .8}, .2, .9, [])
        self.assertIn("model_probability_invalid_or_not_normalized", invalid["pass_reasons"])

    def test_decision_layer_rejects_invalid_risk_gate_ranges_and_death_path_shape(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0}
        model = {"home": .55, "draw": .25, "away": .20}
        invalid = main.decision_layer(snapshot, model, {"home": 1.2, "draw": .4, "away": .3}, -.1, 1.1, "not-a-list")
        self.assertEqual(invalid["decision"], "PASS")
        self.assertIn("script_coverage_out_of_range", invalid["pass_reasons"])
        self.assertIn("crowding_out_of_range", invalid["pass_reasons"])
        self.assertIn("lineup_confidence_out_of_range", invalid["pass_reasons"])
        self.assertIn("death_path_invalid", invalid["pass_reasons"])
        self.assertFalse(invalid["death_path_audit"]["valid"])
        self.assertIsNone(invalid["recommendation_tiers"]["high_variance_single"])

    def test_decision_layer_requires_explicit_death_path_evaluation(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .4, "away": .3}, .2, .9, None)
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("death_path", result["pass_reasons"])

    def test_imported_pipeline_does_not_invent_empty_death_path(self):
        main.import_prematch_packet(self.prematch_packet())
        latest = max(row.get("snapshot_at") or 0 for row in main.get_fixture_snapshots("uuid-1") if row.get("import_status") == "available")
        payload = {
            "fixture": "uuid-1", "league_home_rate": 1.5, "league_away_rate": 1.2,
            "home_attack_rate": 1.8, "home_defense_rate": 1.0, "away_attack_rate": 1.1, "away_defense_rate": 1.5,
            "home_sample_size": 10, "away_sample_size": 10, "league_sample_size": 100, "metric_type": "xg",
            "home_adjustment": 1.0, "away_adjustment": 1.0, "lineup_confidence": .85,
            "provenance": {"source": "verified_event_data", "uses_market_odds": False},
            "script_coverage": {"home": .8, "draw": .4, "away": .3}, "crowding": .3,
        }
        with patch("main.time.time", return_value=latest + 60):
            result = main.evaluate_imported_prematch(payload, persist_version=False)
        self.assertIn("death_path", result["decision_layer"]["pass_reasons"])
        self.assertEqual(result["decision_layer"]["death_path"], [])

    def test_recommendation_tiers_prioritize_consistency_then_return(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"].update({
            "1x2": {"home": 2.0, "draw": 3.5, "away": 4.0, "source": "complete_company_array"},
            "btts": {"yes": 2.5, "no": 1.6, "source": "complete_company_array"},
        })
        model = {"1x2": {"home": .55, "draw": .25, "away": .20}, "btts": {"yes": .50, "no": .50}}
        coverage = {"1x2": {"home": .90, "draw": .30, "away": .20}, "btts": {"yes": .65, "no": .20}}
        result = main.decision_layer(snapshot, model, coverage, .3, .9, [])
        tiers = result["recommendation_tiers"]
        self.assertEqual(tiers["first_choice_high_consistency"]["market"], "1x2")
        self.assertEqual(tiers["second_choice_higher_return"]["market"], "btts")
        self.assertGreater(tiers["second_choice_higher_return"]["ev"], tiers["first_choice_high_consistency"]["ev"])

    def test_high_variance_candidate_does_not_fill_main_tier(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = {"home": 2.0, "draw": 3.5, "away": 4.0, "source": "complete_company_array"}
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .50, "draw": .20, "away": .20}, .3, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIsNone(result["recommendation_tiers"]["first_choice_high_consistency"])
        self.assertEqual(result["recommendation_tiers"]["high_variance_single"]["selection"], "home")

    def test_portfolio_builds_two_tiers_without_correlated_duplicate_legs(self):
        def row(fixture, group, coverage, price, second_price=None):
            first = {"market": "1x2", "selection": "home", "line": None, "price": price, "script_coverage": coverage, "edge": .06, "ev": .08}
            second = {"market": "over_under", "selection": "over", "line": 2.5, "price": second_price, "script_coverage": .65, "edge": .05, "ev": .12} if second_price else None
            return {"fixture": fixture, "correlation_group": group, "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": first, "second_choice_higher_return": second, "high_variance_single": None}}}}
        rows = [row("a", "g1", .90, 1.5, 1.9), row("b", "g1", .80, 1.7), row("c", "g2", .85, 1.6, 2.0), row("d", "g3", .75, 1.8)]
        result = main.build_portfolio(rows, 3)
        first = result["first_choice_combination"]
        second = result["second_choice_combination"]
        self.assertEqual(first["decision"], "COMBINE")
        self.assertEqual(first["leg_count"], 3)
        self.assertEqual(len({leg["correlation_group"] for leg in first["legs"]}), 3)
        self.assertEqual(second["decision"], "COMBINE")
        self.assertTrue(any(leg["market"] == "over_under" for leg in second["legs"]))

    def test_portfolio_does_not_force_fill_single_leg(self):
        candidate = {"market": "1x2", "selection": "home", "price": 1.6, "script_coverage": .8, "edge": .05, "ev": .07}
        rows = [{"fixture": "only", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": None, "high_variance_single": None}}}}]
        result = main.build_portfolio(rows)
        self.assertEqual(result["portfolio_decision"], "PASS")
        self.assertEqual(result["first_choice_combination"]["decision"], "PASS")

    def test_portfolio_offers_more_than_three_legs_as_optional_choices(self):
        rows = []
        for index in range(5):
            candidate = {"market": "1x2", "selection": "home", "price": 1.5 + index * .1, "script_coverage": .9 - index * .03, "edge": .06, "ev": .08}
            rows.append({"fixture": f"f{index}", "correlation_group": f"g{index}", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": None, "high_variance_single": None}}}})
        combination = main.build_portfolio(rows, 5)["first_choice_combination"]
        self.assertEqual(combination["leg_count"], 3)
        self.assertEqual(combination["available_leg_count"], 5)
        self.assertEqual([option["leg_count"] for option in combination["suggested_options"]], [2, 3, 4, 5])
        self.assertEqual([row["rank"] for row in combination["priority_ranking"]], [1, 2, 3, 4, 5])
        self.assertEqual([row["role"] for row in combination["priority_ranking"]], ["core_top_three", "core_top_three", "core_top_three", "optional_extension", "optional_extension"])
        self.assertEqual(combination["core_priority_count"], 3)
        self.assertEqual(combination["optional_extension_count"], 2)
        self.assertEqual(combination["suggested_options"][-1]["risk_label"], "expanded_high_variance")

    def test_portfolio_priority_has_human_readable_match_and_selection(self):
        candidate = {"market": "over_under", "selection": "under", "line": 2.5, "price": 1.9, "script_coverage": .9, "edge": .06, "ev": .08}
        rows = [
            {"fixture": f"f{index}", "match": {"home_team_name": f"Home {index}", "away_team_name": f"Away {index}"}, "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate}}}}
            for index in range(2)
        ]
        ranking = main.build_portfolio(rows, 2)["first_choice_combination"]["priority_ranking"]
        self.assertEqual(ranking[0]["match_label"], "Home 0 vs Away 0")
        self.assertEqual(ranking[0]["selection_label"], "under 2.5")
        self.assertEqual(ranking[0]["display_text"], "Home 0 vs Away 0 · under 2.5")

    def test_portfolio_selection_labels_cover_btts_and_handicap(self):
        self.assertEqual(main.portfolio_selection_label({"market": "btts", "selection": "yes"}), "BTTS yes")
        self.assertEqual(main.portfolio_selection_label({"market": "asian_handicap", "selection": "home", "line": -.25}), "home -0.25")

    def test_portfolio_selects_recommended_option_by_risk_preference(self):
        rows = []
        for index in range(5):
            candidate = {"market": "1x2", "selection": "home", "price": 1.5, "script_coverage": .9, "edge": .06, "ev": .08}
            rows.append({"fixture": f"r{index}", "correlation_group": f"rg{index}", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate, "second_choice_higher_return": None, "high_variance_single": None}}}})
        conservative = main.build_portfolio(rows, 5, "conservative")["first_choice_combination"]
        balanced = main.build_portfolio(rows, 5, "balanced")["first_choice_combination"]
        aggressive = main.build_portfolio(rows, 5, "aggressive")["first_choice_combination"]
        self.assertEqual((conservative["leg_count"], balanced["leg_count"], aggressive["leg_count"]), (2, 3, 5))
        self.assertEqual(aggressive["recommended_option"]["risk_label"], "expanded_high_variance")

    def test_unknown_portfolio_risk_preference_falls_back_to_balanced(self):
        candidate = {"market": "1x2", "selection": "home", "price": 1.5, "script_coverage": .9, "edge": .06, "ev": .08}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate}}}} for index in range(3)]
        result = main.build_portfolio(rows, 6, "unknown")
        self.assertEqual(result["risk_preference"], "balanced")
        self.assertEqual(result["first_choice_combination"]["leg_count"], 3)

    def test_normalize_max_legs_is_safe_for_malformed_internal_values(self):
        self.assertEqual(main.normalize_max_legs("bad"), 6)
        self.assertEqual(main.normalize_max_legs(True), 6)
        self.assertEqual(main.normalize_max_legs(1), 2)
        self.assertEqual(main.normalize_max_legs(99), 10)

    def test_portfolio_audit_explains_correlation_and_leg_cap_exclusions(self):
        def row(fixture, group, coverage):
            candidate = {"market": "1x2", "selection": "home", "price": 1.6, "script_coverage": coverage, "edge": .06, "ev": .08}
            return {"fixture": fixture, "correlation_group": group, "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate}}}}
        rows = [row("a", "shared", .9), row("b", "shared", .8), row("c", "unique-c", .7), row("d", "unique-d", .6)]
        result = main.build_portfolio(rows, 2)["first_choice_combination"]
        reasons = {item["fixture"]: item["reason"] for item in result["selection_audit"]["excluded"]}
        self.assertEqual(reasons["b"], "correlation_group_already_selected")
        self.assertEqual(reasons["d"], "max_legs_reached")

    def test_second_choice_requires_an_actually_selected_upgrade(self):
        first = {"market": "1x2", "selection": "home", "price": 1.6, "script_coverage": .8, "edge": .06, "ev": .08}
        second = {"market": "over_under", "selection": "over", "price": 2.0, "script_coverage": .9, "edge": .05, "ev": .12}
        rows = [
            {"fixture": "fallback", "correlation_group": "same", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": first}}}},
            {"fixture": "upgrade", "correlation_group": "same", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": first, "second_choice_higher_return": second}}}},
            {"fixture": "other", "correlation_group": "other", "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": first}}}},
        ]
        result = main.build_portfolio(rows, 2)["second_choice_combination"]
        self.assertIn(result["decision"], {"COMBINE", "PASS"})
        if result["decision"] == "COMBINE":
            self.assertTrue(any(leg["source_tier"] == "second_choice_higher_return" for leg in result["legs"]))

    def test_portfolio_estimates_binary_combined_probability_and_ev(self):
        candidate = {"market": "1x2", "selection": "home", "price": 2.0, "model_probability": .55, "market_no_vig_probability": .50, "script_coverage": .9, "edge": .05, "ev": .10}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate}}}} for index in range(3)]
        option = main.build_portfolio(rows, 3)["first_choice_combination"]["recommended_option"]
        self.assertAlmostEqual(option["estimated_full_win_probability"], .55 ** 3, places=8)
        self.assertAlmostEqual(option["market_no_vig_combined_probability"], .5 ** 3, places=8)
        self.assertAlmostEqual(option["combined_probability_edge"], .55 ** 3 - .5 ** 3, places=8)
        self.assertAlmostEqual(option["estimated_combined_ev"], 1.1 ** 3 - 1, places=6)
        self.assertEqual(option["weakest_leg"]["script_coverage"], .9)

    def test_portfolio_does_not_multiply_conditional_asian_win_probability(self):
        asian = {"market": "asian_handicap", "selection": "home", "line": -.25, "price": 1.9, "model_probability": .55, "script_coverage": .9, "edge": .05, "ev": .08, "settlement_aware": True, "push_probability": .2}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": asian}}}} for index in range(2)]
        option = main.build_portfolio(rows, 2)["first_choice_combination"]["recommended_option"]
        self.assertEqual(option["estimated_full_win_probability"]["status"], "data_missing")
        self.assertEqual(option["market_no_vig_combined_probability"]["status"], "data_missing")
        self.assertIn("asian_settlement_can_include_push_half_win_or_half_loss", option["risk_warnings"])
        self.assertAlmostEqual(option["estimated_combined_ev"], 1.08 ** 2 - 1, places=6)

    def test_four_leg_option_has_explicit_variance_warning(self):
        candidate = {"market": "btts", "selection": "yes", "price": 2.0, "model_probability": .55, "market_no_vig_probability": .5, "script_coverage": .8, "edge": .05, "ev": .1}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": candidate}}}} for index in range(4)]
        option = main.build_portfolio(rows, 4, "aggressive")["first_choice_combination"]["recommended_option"]
        self.assertIn("four_or_more_legs_materially_increase_variance", option["risk_warnings"])

    def test_binary_portfolio_has_half_edge_stress_test_and_context(self):
        candidate = {"market": "1x2", "selection": "home", "price": 2.0, "model_probability": .55, "market_no_vig_probability": .5, "script_coverage": .9, "edge": .05, "ev": .1}
        rows = []
        for index, confidence in enumerate((.9, .8)):
            decision = {"lineup_confidence": confidence, "crowding": .3 + index * .1, "line_movement": {"status": "available"}, "death_path": [], "recommendation_tiers": {"first_choice_high_consistency": candidate}}
            rows.append({"fixture": str(index), "evaluation": {"decision_layer": decision}})
        option = main.build_portfolio(rows, 2)["first_choice_combination"]["recommended_option"]
        stress = option["half_edge_stress_test"]
        self.assertEqual(stress["status"], "available")
        self.assertAlmostEqual(stress["estimated_full_win_probability"], .525 ** 2, places=8)
        self.assertTrue(stress["remains_positive_ev"])
        self.assertEqual(option["portfolio_context"]["minimum_lineup_confidence"], .8)
        self.assertEqual(option["portfolio_context"]["maximum_crowding"], .4)

    def test_asian_portfolio_stress_probability_is_data_missing(self):
        asian = {"market": "asian_handicap", "selection": "home", "line": -.25, "price": 1.9, "model_probability": .55, "market_no_vig_probability": .5, "script_coverage": .9, "edge": .05, "ev": .08, "settlement_aware": True}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": asian}}}} for index in range(2)]
        option = main.build_portfolio(rows, 2)["first_choice_combination"]["recommended_option"]
        self.assertEqual(option["half_edge_stress_test"]["status"], "data_missing")

    def test_conservative_portfolio_passes_when_stress_test_is_not_positive(self):
        fragile = {"market": "1x2", "selection": "home", "price": 2.0, "model_probability": .52, "market_no_vig_probability": .45, "script_coverage": .9, "edge": .07, "ev": .04}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": fragile}}}} for index in range(2)]
        result = main.build_portfolio(rows, 2, "conservative")["risk_adjusted_recommendation"]
        self.assertEqual(result["decision"], "PASS")
        self.assertEqual(result["reason"], "no_combination_passed_half_edge_stress_test")

    def test_aggressive_portfolio_can_select_fragile_higher_return_with_warning(self):
        first = {"market": "1x2", "selection": "home", "price": 2.0, "model_probability": .55, "market_no_vig_probability": .5, "script_coverage": .9, "edge": .05, "ev": .1}
        second = {"market": "btts", "selection": "yes", "price": 2.0, "model_probability": .52, "market_no_vig_probability": .45, "script_coverage": .7, "edge": .07, "ev": .04}
        rows = [{"fixture": str(index), "evaluation": {"decision_layer": {"recommendation_tiers": {"first_choice_high_consistency": first, "second_choice_higher_return": second}}}} for index in range(2)]
        result = main.build_portfolio(rows, 2, "aggressive")["risk_adjusted_recommendation"]
        self.assertEqual(result["source"], "second_choice_higher_return")
        self.assertEqual(result["robustness"], "fragile")
        self.assertIsNotNone(result["warning"])

    def test_portfolio_run_history_records_recommendation_change(self):
        first_portfolio = {"risk_preference": "balanced", "portfolio_decision": "READY", "risk_adjusted_recommendation": {"decision": "COMBINE", "source": "first_choice_high_consistency", "robustness": "resilient", "reason": "test", "recommended_option": {"legs": [{"fixture": "a", "market": "1x2", "selection": "home", "line": None, "price": 2.0}]}}}
        first = main.save_portfolio_run("test-set", ["a", "b"], first_portfolio, 3, ["T-3h"])
        second = main.save_portfolio_run("test-set", ["a", "b"], first_portfolio, 3, ["T-1h"])
        changed_portfolio = {**first_portfolio, "risk_adjusted_recommendation": {"decision": "PASS", "reason": "no_combination_passed_half_edge_stress_test"}}
        third = main.save_portfolio_run("test-set", ["a", "b"], changed_portfolio, 3, ["T-30m"])
        self.assertTrue(first["recommendation_change"]["changed"])
        self.assertFalse(second["recommendation_change"]["changed"])
        self.assertTrue(third["recommendation_change"]["changed"])
        self.assertEqual(len(main.get_portfolio_runs("test-set")), 3)
        self.assertEqual(third["recommendation"]["decision"], "PASS")

    def test_portfolio_run_retention_keeps_monotonic_numbers(self):
        portfolio = {"risk_preference": "balanced", "portfolio_decision": "PASS", "risk_adjusted_recommendation": {"decision": "PASS", "reason": "test"}}
        with patch.object(main, "PORTFOLIO_RUN_RETENTION", 3):
            for _ in range(5):
                main.save_portfolio_run("retained", ["a"], portfolio, 3, [])
        self.assertEqual([row["version_number"] for row in main.get_portfolio_runs("retained")], [3, 4, 5])

    def test_portfolio_history_timeline_never_backfills_missing_stages(self):
        portfolio = {"risk_preference": "balanced", "portfolio_decision": "PASS", "risk_adjusted_recommendation": {"decision": "PASS", "reason": "test"}}
        opening = main.save_portfolio_run("timeline", ["a", "b"], portfolio, 3, [], "Opening")
        closing = main.save_portfolio_run("timeline", ["a", "b"], portfolio, 3, [], "Closing")
        timeline = main.portfolio_run_timeline([opening, closing])
        self.assertEqual([row["stage"] for row in timeline], main.PREMATCH_STAGE_ORDER)
        self.assertEqual(timeline[0]["status"], "available")
        self.assertEqual(timeline[1]["status"], "data_missing")
        self.assertEqual(timeline[-1]["status"], "available")
        self.assertIn("not backfilled", timeline[1]["reason"])

    def test_portfolio_transition_classifies_leg_change_and_pass_downgrade(self):
        before = {"decision": "COMBINE", "source": "first_choice_high_consistency", "robustness": "resilient", "legs": [{"fixture": "a", "market": "1x2", "selection": "home", "line": None}]}
        changed = {"decision": "COMBINE", "source": "first_choice_high_consistency", "robustness": "resilient", "legs": [{"fixture": "b", "market": "btts", "selection": "yes", "line": None}]}
        selection_transition = main._portfolio_transition(before, changed)
        self.assertEqual(selection_transition["classification"], "Selection Change")
        self.assertEqual(selection_transition["added_legs"][0]["fixture"], "b")
        downgraded = main._portfolio_transition(before, {"decision": "PASS", "source": None, "robustness": None, "legs": []})
        self.assertEqual(downgraded["classification"], "Risk Downgrade to PASS")

    def test_portfolio_transition_classifies_recovery_and_no_change(self):
        passed = {"decision": "PASS", "source": None, "robustness": None, "legs": []}
        active = {"decision": "COMBINE", "source": "first_choice_high_consistency", "robustness": "resilient", "legs": []}
        self.assertEqual(main._portfolio_transition(passed, active)["classification"], "Recovery from PASS")
        self.assertEqual(main._portfolio_transition(active, active)["classification"], "No Change")

    def test_portfolio_change_drivers_preserve_facts_without_market_inference(self):
        rows = [{
            "fixture": "fixture-a",
            "evaluation": {
                "fundamental_version": {"version_number": 3, "trigger": {"triggered": True, "reasons": ["significant_line_move"]}},
                "decision_layer": {"decision": "PASS", "pass_reasons": ["lineup_confidence_below_minimum"], "line_movement": {"classification": "Market-Only Move"}, "best_market": None},
            },
        }, {"fixture": "fixture-b", "evaluation": {"decision_layer": {"decision": "PASS"}}}]
        drivers = main._portfolio_change_drivers(rows)
        self.assertEqual(drivers[0]["market_move_classification"], "Market-Only Move")
        self.assertIn("significant_line_move", drivers[0]["reasons"])
        self.assertIn("lineup_confidence_below_minimum", drivers[0]["reasons"])
        self.assertEqual(drivers[0]["fundamental_version"], 3)
        self.assertEqual(drivers[1]["evidence_status"], "data_missing")
        self.assertEqual(drivers[1]["market_move_classification"], "data_missing")

    def test_independent_poisson_model_is_normalized_and_symmetric(self):
        model = main.poisson_probability_model(1.4, 1.4, 0.8, {"source": "verified_team_metrics", "uses_market_odds": False})
        self.assertTrue(model["ok"])
        self.assertEqual(model["status"], "ready")
        one_x_two = model["probabilities"]["1x2"]
        self.assertAlmostEqual(sum(one_x_two.values()), 1.0, places=5)
        self.assertAlmostEqual(one_x_two["home"], one_x_two["away"], places=6)
        self.assertFalse(model["uses_market_odds"])
        self.assertAlmostEqual(sum(model["probabilities"]["home_team_totals"][key] for key in ("over_1_5", "under_1_5")), 1.0, places=5)

    def test_cross_market_decision_can_prefer_total_over_1x2(self):
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"].update({
            "1x2": {"home": 1.7, "draw": 4.0, "away": 5.0, "source": "complete_company_array"},
            "over_under": {"line": 2.5, "over": 2.2, "under": 1.7, "source": "complete_company_array"},
            "btts": {"yes": 2.0, "no": 1.8, "source": "complete_company_array"},
        })
        model = main.poisson_probability_model(2.0, 1.2, 0.9, {"source": "verified_team_metrics", "uses_market_odds": False})
        result = main.decision_layer(snapshot, model["probabilities"], {
            "1x2": {"home": .7, "draw": .3, "away": .2},
            "over_under": {"over": .85, "under": .2}, "btts": {"yes": .7, "no": .2},
        }, .3, .9, [])
        self.assertEqual(result["decision"], "over_under:over")
        self.assertEqual(result["best_market"]["line"], 2.5)
        self.assertIn("over_under", result["market_no_vig_probability"])

    def test_asian_quarter_line_ev_includes_half_loss_and_push(self):
        metrics = main.asian_settlement_metrics({"1": .5, "0": .5}, -.25, "home", 2.0, "asian_handicap")
        self.assertAlmostEqual(metrics["win_equivalent"], .5, places=6)
        self.assertAlmostEqual(metrics["loss_equivalent"], .25, places=6)
        self.assertAlmostEqual(metrics["push_probability"], .25, places=6)
        self.assertAlmostEqual(metrics["ev"], .25, places=6)

    def test_integer_total_ev_includes_push(self):
        metrics = main.asian_settlement_metrics({"2": .5, "3": .5}, 2.0, "over", 2.0, "over_under")
        self.assertAlmostEqual(metrics["win_equivalent"], .5, places=6)
        self.assertAlmostEqual(metrics["push_probability"], .5, places=6)
        self.assertAlmostEqual(metrics["ev"], .5, places=6)

    def test_poisson_model_rejects_market_derived_or_low_confidence_inputs(self):
        invalid = main.poisson_probability_model(1.5, 1.0, 0.9, {"source": "odds", "uses_market_odds": True})
        self.assertFalse(invalid["ok"])
        self.assertIn("independent_provenance_required", invalid["errors"])
        low = main.poisson_probability_model(1.5, 1.0, 0.4, {"source": "limited_stats", "uses_market_odds": False})
        self.assertEqual(low["status"], "insufficient_confidence")

    def test_fundamental_expected_goals_is_auditable_and_market_independent(self):
        inputs = {
            "league_home_rate": 1.5, "league_away_rate": 1.2,
            "home_attack_rate": 1.8, "home_defense_rate": 1.0,
            "away_attack_rate": 1.1, "away_defense_rate": 1.5,
            "home_sample_size": 10, "away_sample_size": 10, "league_sample_size": 100,
            "metric_type": "xg", "home_adjustment": 1.0, "away_adjustment": 1.0,
            "lineup_confidence": 0.8,
            "provenance": {"source": "verified_event_data", "uses_market_odds": False},
        }
        result = main.fundamental_expected_goals(inputs)
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "ready")
        self.assertAlmostEqual(result["expected_goals"]["home"], 1.8, places=6)
        self.assertAlmostEqual(result["expected_goals"]["away"], 1.1 * 1.0 / 1.2, places=6)
        self.assertGreaterEqual(result["confidence"], 0.6)
        self.assertFalse(result["uses_market_odds"])

    def test_fundamental_expected_goals_rejects_extreme_adjustment(self):
        result = main.fundamental_expected_goals({"home_adjustment": 1.5})
        self.assertFalse(result["ok"])
        self.assertIn("home_adjustment_out_of_range", result["errors"])

    def test_complete_prematch_pipeline_binds_fresh_market_and_versions_fundamentals(self):
        main.import_prematch_packet(self.prematch_packet())
        latest = max(row.get("snapshot_at") or 0 for row in main.get_fixture_snapshots("uuid-1") if row.get("import_status") == "available")
        payload = {
            "fixture": "uuid-1", "league_home_rate": 1.5, "league_away_rate": 1.2,
            "home_attack_rate": 1.8, "home_defense_rate": 1.0,
            "away_attack_rate": 1.1, "away_defense_rate": 1.5,
            "home_sample_size": 10, "away_sample_size": 10, "league_sample_size": 100,
            "metric_type": "xg", "home_adjustment": 1.0, "away_adjustment": 1.0,
            "lineup_confidence": 0.85,
            "provenance": {"source": "verified_event_data", "uses_market_odds": False},
            "script_coverage": {"home": 0.8, "draw": 0.4, "away": 0.3},
            "crowding": 0.3, "death_path": [],
            "revalidation_trigger": {"triggered": True, "reasons": ["significant_line_move"]},
        }
        with patch("main.time.time", return_value=latest + 60):
            first = main.evaluate_imported_prematch(payload)
            second = main.evaluate_imported_prematch({**payload, "home_adjustment": 1.05})
        self.assertEqual(first["decision_layer"]["data_freshness"]["state"], "fresh")
        self.assertNotIn("market_no_vig_probability", first["decision_layer"]["pass_reasons"])
        self.assertEqual(first["fundamental_version"]["version_number"], 1)
        self.assertIn(first["decision_summary"]["status"], {"pass", "actionable"})
        self.assertEqual(first["decision_summary"]["decision"], first["decision_layer"]["decision"])
        self.assertEqual(second["fundamental_version"]["version_number"], 2)
        self.assertIsNotNone(second["fundamental_version"]["probability_change"]["delta"])
        self.assertIn("fundamental_estimator", second["fundamental_version"]["variable_changes"])
        self.assertEqual(second["fundamental_version"]["trigger"]["reasons"], ["significant_line_move"])
        self.assertTrue(second["fundamental_version"]["script"]["odds_independent"])

    def test_fundamental_version_persists_audit_fields(self):
        script = {"content_hash": "x", "chain": {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}}
        row = main.save_fundamental_version(1, script, {"stage": "Opening", "triggered": False})
        self.assertEqual(row["version_number"], 1)
        self.assertIsNone(row["previous_version_number"])
        self.assertIn("probability_change", row)
        self.assertIn("best_market_change", row)
        self.assertTrue(row["recalculation_audit"]["baseline_created"])
        self.assertFalse(row["recalculation_audit"]["comparison_available"])
        self.assertFalse(row["recalculation_audit"]["fundamental_changed"])
        self.assertIsNone(row["recalculation_audit"]["probability_changed"])
        self.assertIsNone(row["recalculation_audit"]["best_market_changed"])
        self.assertEqual(row["changed_information"], [])
        self.assertEqual(row["variable_changes"], {})
        self.assertEqual(row["recalculation_audit"]["stage"], "Opening")
        self.assertEqual(len(main.get_fundamental_versions(1)), 1)

    def test_fundamental_recalculation_audit_records_real_changes_and_normalizes_trigger(self):
        first_script = {"content_hash": "x", "chain": {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}}
        first = main.save_fundamental_version(2, first_script, {"triggered": False})
        second_script = {**first_script, "content_hash": "y", "chain": {**first_script["chain"], "goal_conversion": {"status": "available"}}}
        second = main.save_fundamental_version(
            2, second_script, {"stage": "t-1h", "triggered": True, "reasons": "significant_line_move"}, first,
            probability_change={"before": {"home": .4}, "after": {"home": .45}, "delta": {"home": .05}},
            best_market_change={"before": None, "after": {"market": "1x2", "selection": "home"}, "changed": True},
        )
        self.assertEqual(second["previous_version_number"], 1)
        self.assertEqual(second["trigger"]["reasons"], ["significant_line_move"])
        self.assertEqual(second["trigger"]["source"], "market_revalidation")
        self.assertTrue(second["recalculation_audit"]["comparison_available"])
        self.assertTrue(second["recalculation_audit"]["fundamental_changed"])
        self.assertTrue(second["recalculation_audit"]["probability_changed"])
        self.assertTrue(second["recalculation_audit"]["best_market_changed"])
        self.assertEqual(second["recalculation_audit"]["stage"], "T-1h")

    def test_version_audit_does_not_treat_evidence_refresh_as_fundamental_change(self):
        base_chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        base_chain["result_utility"] = {"status": "available", "home": {"win": 1, "draw": 0, "loss": -1}, "source": "feed-a", "observed_at": 100}
        first = main.save_fundamental_version(22, {"content_hash": "a", "chain": base_chain, "estimator": {"value": 1}}, {"triggered": False})
        refreshed_chain = {**base_chain, "result_utility": {**base_chain["result_utility"], "source": "feed-b", "observed_at": 200}}
        second = main.save_fundamental_version(22, {"content_hash": "b", "chain": refreshed_chain, "estimator": {"value": 2}}, {"triggered": True}, first)
        self.assertFalse(second["recalculation_audit"]["fundamental_changed"])
        self.assertTrue(second["recalculation_audit"]["estimator_changed"])
        self.assertTrue(second["recalculation_audit"]["evidence_metadata_refreshed"])
        self.assertEqual(second["changed_information"], [])
        self.assertEqual(second["evidence_metadata_changes"], ["result_utility"])
        self.assertIn("fundamental_estimator", second["variable_changes"])
        self.assertEqual(second["change_types"], ["evidence_metadata", "estimator"])
        self.assertEqual(second["primary_change_type"], "evidence_metadata")

    def test_version_change_types_distinguish_baseline_and_no_change(self):
        script = {"content_hash": "same", "chain": {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}}
        first = main.save_fundamental_version(24, script, {"triggered": False})
        second = main.save_fundamental_version(24, script, {"triggered": False}, first)
        self.assertEqual(first["change_types"], ["baseline"])
        self.assertEqual(first["primary_change_type"], "baseline")
        self.assertEqual(second["change_types"], ["no_change"])
        self.assertEqual(second["primary_change_type"], "no_change")

    def test_reason_and_warning_refresh_are_evidence_metadata(self):
        old = {"goal_conversion": {"status": "partial", "home": {"rate": .1}, "reason": "limited sample", "warning": "old"}}
        new = {"goal_conversion": {"status": "partial", "home": {"rate": .1}, "reason": "sample rechecked", "warning": "new"}}
        substantive, metadata = main.fundamental_chain_change_sets(old, new)
        self.assertEqual(substantive, [])
        self.assertEqual(metadata, ["goal_conversion"])

    def test_fundamental_version_retention_keeps_monotonic_numbers(self):
        script = {"content_hash": "x", "chain": {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}}
        with patch.object(main, "FUNDAMENTAL_VERSION_RETENTION", 3):
            for _ in range(5):
                main.save_fundamental_version(9, script, {"triggered": False})
        self.assertEqual([row["version_number"] for row in main.get_fundamental_versions(9)], [3, 4, 5])

    def test_fundamental_version_rebases_stale_concurrent_comparison(self):
        chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        first = main.save_fundamental_version(10, {"content_hash": "v1", "chain": chain, "model": {"probabilities": {"1x2": {"home": .4, "draw": .3, "away": .3}}}}, {"triggered": False})
        second = main.save_fundamental_version(10, {"content_hash": "v2", "chain": {**chain, "goal_conversion": {"status": "partial"}}, "model": {"probabilities": {"1x2": {"home": .45, "draw": .3, "away": .25}}}}, {"triggered": True}, first, best_market_change={"before": None, "after": {"market": "1x2", "selection": "home"}, "changed": True})
        third = main.save_fundamental_version(
            10, {"content_hash": "v3", "chain": {**chain, "goal_conversion": {"status": "available"}}, "model": {"probabilities": {"1x2": {"home": .5, "draw": .3, "away": .2}}}},
            {"triggered": True}, first,
            probability_change={"before": {"home": .4, "draw": .3, "away": .3}, "after": {"home": .5, "draw": .3, "away": .2}},
            best_market_change={"before": None, "after": {"market": "over_under", "selection": "over"}, "changed": True},
        )
        self.assertEqual(third["previous_version_number"], second["version_number"])
        self.assertTrue(third["recalculation_audit"]["comparison_rebased_to_latest"])
        self.assertEqual(third["recalculation_audit"]["requested_previous_version_number"], first["version_number"])
        self.assertEqual(third["probability_change"]["before"]["home"], .45)
        self.assertAlmostEqual(third["probability_change"]["delta"]["home"], .05)
        self.assertEqual(third["best_market_change"]["before"]["selection"], "home")

    def test_fundamental_chain_audit_requires_critical_sections_and_minimum_coverage(self):
        chain = {key: {"status": "available", "evidence": f"verified-{key}", "source": "trusted-feed", "observed_at": "2026-10-03T00:00:00Z"} for key in main.FUNDAMENTAL_CHAIN}
        chain["result_utility"].update({"home": {"win": 1, "draw": 0, "loss": -1}, "away": {"win": 1, "draw": 0, "loss": -1}})
        rotation_side = {"starting_xi_strength": .8, "creativity": .7, "finishing": .75, "chemistry": .8}
        chain["rotation_quality"].update({"home": rotation_side, "away": rotation_side})
        chain["execution_ability"].update({"home": {"score": .7}, "away": {"score": .6}})
        chain["goal_conversion"].update({"home": {"rate": .12}, "away": {"rate": .1}, "strength_edge": "home", "goal_edge": "home", "margin_edge": "home"})
        chain["game_state_elasticity"]["states"] = {"0_0_persists": "compact", "home_scores_first": "away_expands", "away_scores_first": "home_expands", "draw_at_60": "both_expand", "trailing_last_30": "trailing_team_expands"}
        chain["first_goal_state_transition"].update({"home_first": "lower_risk", "away_first": "higher_risk"})
        chain["open_game_beneficiary"]["team"] = "home"
        chain["time_segment_strength"]["segments"] = {key: "home" for key in ("0_15", "16_30", "31_45", "46_60", "61_75", "76_90")}
        eligible = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertTrue(eligible["decision_eligible"])
        chain["goal_conversion"] = {"status": "data_missing"}
        insufficient = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertFalse(insufficient["decision_eligible"])
        self.assertIn("goal_conversion", insufficient["critical_missing"])

    def test_extended_dimension_audit_is_backward_compatible_and_explicit(self):
        chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertFalse(audit["extended_dimensions_ready"])
        self.assertEqual(audit["extended_dimension_audit"]["two_way_open_game"]["status"], "data_missing")
        chain["open_game_beneficiary"]["two_way"] = {
            "home_attack_gain": .7, "home_defensive_exposure": .4,
            "away_attack_gain": .6, "away_defensive_exposure": .5,
        }
        updated = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertEqual(updated["extended_dimension_audit"]["two_way_open_game"]["status"], "available")

    def test_chain_structural_states_and_conversion_edges_are_required(self):
        now_ts = 1790989200
        chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        chain["game_state_elasticity"] = {"status": "available", "source": "analyst", "observed_at": now_ts, "states": {"0_0_persists": "compact"}}
        chain["first_goal_state_transition"] = {"status": "partial", "source": "analyst", "observed_at": now_ts, "home_first": "lower_risk"}
        chain["open_game_beneficiary"] = {"status": "available", "source": "analyst", "observed_at": now_ts, "team": "both"}
        chain["time_segment_strength"] = {"status": "available", "source": "events", "observed_at": now_ts, "segments": {"0_15": "home"}}
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=now_ts)
        self.assertIn("game_state_elasticity", audit["structural_issues"])
        self.assertIn("first_goal_state_transition", audit["structural_issues"])
        self.assertIn("open_game_beneficiary", audit["structural_issues"])
        self.assertIn("time_segment_strength", audit["structural_issues"])

    def test_fundamental_chain_rejects_market_contamination(self):
        chain = {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}
        chain["tactical_matchup"] = {"status": "partial", "source": "bookmaker market odds", "observed_at": 1790989200, "evidence": "price shortened"}
        chain["game_state_elasticity"] = {"status": "partial", "source": "analyst", "observed_at": 1790989200, "market_probability": .62}
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("tactical_matchup", audit["market_contaminated_sections"])
        self.assertIn("game_state_elasticity", audit["market_contaminated_sections"])
        self.assertIn("market_probability", " ".join(audit["market_contaminated_sections"]["game_state_elasticity"]))

    def test_fundamental_chain_rejects_empty_available_and_unknown_status(self):
        chain = {key: {"status": "available", "evidence": f"verified-{key}", "source": "trusted-feed"} for key in main.FUNDAMENTAL_CHAIN}
        chain["result_utility"] = {"status": "available"}
        chain["tactical_matchup"] = {"status": "certain", "evidence": "unsupported status"}
        audit = main.audit_fundamental_chain({"chain": chain})
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("result_utility", audit["unsubstantiated_sections"])
        self.assertIn("tactical_matchup", audit["invalid_status_sections"])
        self.assertIn("result_utility", audit["critical_missing"])

    def test_critical_fundamental_chain_requires_provenance_and_audits_timestamp(self):
        chain = {key: {"status": "available", "evidence": f"verified-{key}", "source": "trusted-feed", "observed_at": "2026-10-03T00:00:00Z"} for key in main.FUNDAMENTAL_CHAIN}
        chain["rotation_quality"].pop("source")
        chain["goal_conversion"].pop("observed_at")
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=1790989200)
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("rotation_quality", audit["critical_provenance_missing"])
        self.assertIn("goal_conversion", audit["timestamp_missing_sections"])
        self.assertNotIn("goal_conversion", audit["critical_provenance_missing"])

    def test_critical_fundamental_timestamps_use_type_specific_freshness(self):
        now_ts = 1790989200
        chain = {key: {"status": "available", "evidence": key, "source": "trusted-feed", "observed_at": now_ts - 3600} for key in main.FUNDAMENTAL_CHAIN}
        chain["rotation_quality"]["observed_at"] = now_ts - main.CRITICAL_FUNDAMENTAL_MAX_AGE_SECONDS["rotation_quality"] - 1
        chain["goal_conversion"]["observed_at"] = now_ts + 600
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=now_ts)
        self.assertFalse(audit["decision_eligible"])
        self.assertEqual(audit["critical_timestamp_issues"]["rotation_quality"]["issue"], "stale")
        self.assertEqual(audit["critical_timestamp_issues"]["goal_conversion"]["issue"], "future_timestamp")
        self.assertNotIn("execution_ability", audit["critical_timestamp_issues"])

    def test_critical_fundamental_semantics_require_both_teams(self):
        now_ts = 1790989200
        chain = {key: {"status": "available", "evidence": key, "source": "trusted-feed", "observed_at": now_ts} for key in main.FUNDAMENTAL_CHAIN}
        chain["result_utility"].update({"home": {"win": 1, "draw": 0, "loss": -1}, "away": {"win": 1, "draw": 0, "loss": -1}})
        rotation_side = {"starting_xi_strength": .8, "creativity": .7, "finishing": .75, "chemistry": .8}
        chain["rotation_quality"].update({"home": rotation_side, "away": rotation_side})
        chain["execution_ability"].update({"home": {"score": .7}, "away": {"score": .6}})
        chain["goal_conversion"].update({"home": {"rate": .12}, "away": {"rate": .1}, "strength_edge": "home", "goal_edge": "home", "margin_edge": "home"})
        chain["game_state_elasticity"]["states"] = {"0_0_persists": "compact", "home_scores_first": "away_expands", "away_scores_first": "home_expands", "draw_at_60": "both_expand", "trailing_last_30": "trailing_team_expands"}
        chain["first_goal_state_transition"].update({"home_first": "lower_risk", "away_first": "higher_risk"})
        chain["open_game_beneficiary"]["team"] = "home"
        chain["time_segment_strength"]["segments"] = {key: "home" for key in ("0_15", "16_30", "31_45", "46_60", "61_75", "76_90")}
        valid = main.audit_fundamental_chain({"chain": chain}, now_ts=now_ts)
        self.assertTrue(valid["decision_eligible"])
        del chain["result_utility"]["away"]["loss"]
        chain["goal_conversion"]["away"] = {"note": "no numeric metric"}
        invalid = main.audit_fundamental_chain({"chain": chain}, now_ts=now_ts)
        self.assertFalse(invalid["decision_eligible"])
        self.assertIn("loss", invalid["critical_semantic_issues"]["result_utility"]["away"]["missing_numeric_fields"])
        self.assertEqual(invalid["critical_semantic_issues"]["goal_conversion"]["away"]["reason"], "at_least_one_numeric_metric_required")

    def test_critical_fundamental_numeric_values_reject_nonfinite_order_and_range(self):
        now_ts = 1790989200
        chain = {key: {"status": "available", "evidence": key, "source": "trusted-feed", "observed_at": now_ts} for key in main.FUNDAMENTAL_CHAIN}
        chain["result_utility"].update({"home": {"win": 0, "draw": 1, "loss": -1}, "away": {"win": 1, "draw": 0, "loss": -1}})
        bad_rotation = {"starting_xi_strength": 1.2, "creativity": .7, "finishing": .75, "chemistry": .8}
        chain["rotation_quality"].update({"home": bad_rotation, "away": bad_rotation})
        chain["execution_ability"].update({"home": {"score": "NaN"}, "away": {"score": .6}})
        chain["goal_conversion"].update({"home": {"rate": -.1}, "away": {"rate": .1}})
        audit = main.audit_fundamental_chain({"chain": chain}, now_ts=now_ts)
        self.assertFalse(audit["decision_eligible"])
        self.assertIn("utility_must_satisfy", audit["critical_semantic_issues"]["result_utility"]["home"]["reason"])
        self.assertIn("starting_xi_strength", audit["critical_semantic_issues"]["rotation_quality"]["home"]["numeric_fields_outside_0_to_1"])
        self.assertEqual(audit["critical_semantic_issues"]["execution_ability"]["home"]["reason"], "at_least_one_numeric_metric_required")
        self.assertIn("rate", audit["critical_semantic_issues"]["goal_conversion"]["home"]["negative_numeric_fields"])

    def test_as_float_rejects_nonfinite_values(self):
        self.assertIsNone(main.as_float("NaN"))
        self.assertIsNone(main.as_float("Infinity"))
        self.assertEqual(main.as_float("1.25"), 1.25)

    def test_prematch_pipeline_passes_when_fundamental_chain_is_missing(self):
        main.import_prematch_packet(self.prematch_packet())
        latest = max(row.get("snapshot_at") or 0 for row in main.get_fixture_snapshots("uuid-1") if row.get("import_status") == "available")
        payload = {
            "fixture": "uuid-1", "league_home_rate": 1.5, "league_away_rate": 1.2,
            "home_attack_rate": 1.8, "home_defense_rate": 1.0, "away_attack_rate": 1.1, "away_defense_rate": 1.5,
            "home_sample_size": 10, "away_sample_size": 10, "league_sample_size": 100, "metric_type": "xg",
            "home_adjustment": 1.0, "away_adjustment": 1.0, "lineup_confidence": .85,
            "provenance": {"source": "verified_event_data", "uses_market_odds": False},
            "script_coverage": {"home": .8, "draw": .4, "away": .3}, "crowding": .3, "death_path": [],
        }
        with patch("main.time.time", return_value=latest + 60):
            result = main.evaluate_imported_prematch(payload)
        self.assertEqual(result["decision_layer"]["decision"], "PASS")
        self.assertEqual(result["decision_summary"]["status"], "pass")
        self.assertIsNone(result["decision_summary"]["recommended_market"])
        self.assertIn("fundamental_chain_insufficient", result["decision_layer"]["pass_reasons"])
        self.assertEqual(result["fundamental_chain_audit"]["status"], "insufficient")

    def test_nami_call_never_returns_credentials_in_endpoint(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "user-value", "secret-value"
        response = unittest.mock.Mock()
        response.ok, response.status_code = True, 200
        response.json.return_value = {"results": [{"id": 1}]}
        try:
            with patch("main.requests.get", return_value=response):
                result = main.call_nami("/api/v4/football/competition/list", {"limit": 1})
            self.assertTrue(result["ok"])
            self.assertEqual(result["endpoint"], "/api/v4/football/competition/list")
            self.assertNotIn("secret-value", str(result))
            self.assertNotIn("user-value", str(result))
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_nami_call_redacts_credentials_echoed_by_upstream(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "user-value", "secret-value"
        response = unittest.mock.Mock()
        response.ok, response.status_code = True, 200
        response.json.return_value = {
            "query": {"user": "user-value", "secret": "secret-value"},
            "nested": ["request for user-value used secret-value"],
        }
        try:
            with patch("main.requests.get", return_value=response):
                result = main.call_nami("/api/v5/football/match/schedule/diary")
            rendered = str(result)
            self.assertNotIn("user-value", rendered)
            self.assertNotIn("secret-value", rendered)
            self.assertIn("YOUR_SECRET", rendered)
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_safe_json_response_redacts_all_configured_secrets(self):
        original_key = main.API_FOOTBALL_KEY
        main.API_FOOTBALL_KEY = "api-key-value"
        response = unittest.mock.Mock()
        response.json.return_value = {"debug": {"authorization": "Bearer api-key-value"}}
        try:
            result = main.safe_json_response(response)
            self.assertNotIn("api-key-value", str(result))
            self.assertIn("YOUR_SECRET", str(result))
        finally:
            main.API_FOOTBALL_KEY = original_key

    def test_secret_masking_redacts_overlapping_credentials_longest_first(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "credential", "credential-with-private-suffix"
        try:
            masked = main.mask_secret("credential-with-private-suffix credential")
            self.assertEqual(masked, "YOUR_SECRET YOUR_SECRET")
            self.assertNotIn("private-suffix", masked)
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_external_api_exception_messages_do_not_leak_credentials(self):
        original_api_key, original_stats_key = main.API_FOOTBALL_KEY, main.THESTATS_API_KEY
        main.API_FOOTBALL_KEY, main.THESTATS_API_KEY = "football-private-key", "stats-private-key"
        try:
            with patch("main.requests.get", side_effect=main.requests.RequestException("failed football-private-key stats-private-key")):
                football = main.call_api_football("/fixtures")
                stats = main.call_thestats("/events")
            self.assertNotIn("football-private-key", str(football))
            self.assertNotIn("stats-private-key", str(football))
            self.assertNotIn("football-private-key", str(stats))
            self.assertNotIn("stats-private-key", str(stats))
        finally:
            main.API_FOOTBALL_KEY, main.THESTATS_API_KEY = original_api_key, original_stats_key

    def test_nami_failure_is_optional_and_degraded(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "user-value", "secret-value"
        try:
            with patch("main.requests.get", side_effect=RuntimeError("unexpected client failure")):
                result = main.call_nami("/api/v4/football/competition/list")
            self.assertFalse(result["ok"])
            self.assertTrue(result["degraded"])
            self.assertFalse(result["required"])
            self.assertEqual(result["fallback"], "continue_without_nami")
            self.assertTrue(main.health()["ok"])
            self.assertEqual(main.health()["nami_failure_policy"], "continue_without_nami")
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_nami_upstream_error_does_not_become_available_data(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "user-value", "secret-value"
        response = unittest.mock.Mock()
        response.ok, response.status_code = True, 200
        response.json.return_value = {"err": "ip未授权访问", "results": [{"id": 1}]}
        try:
            with patch("main.requests.get", return_value=response):
                result = main.call_nami("/api/v4/football/competition/list")
            self.assertFalse(result["ok"])
            self.assertFalse(result["available"])
            self.assertTrue(result["degraded"])
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_nami_nonzero_code_and_false_success_degrade_without_breaking_system(self):
        original_user, original_secret = main.NAMI_API_USER, main.NAMI_API_SECRET
        main.NAMI_API_USER, main.NAMI_API_SECRET = "user-value", "secret-value"
        response = unittest.mock.Mock()
        response.ok, response.status_code = True, 200
        try:
            for payload in (
                {"code": 1001, "message": "product not entitled", "results": {"match": [{"id": 1}]}},
                {"success": False, "msg": "temporary upstream failure"},
            ):
                response.json.return_value = payload
                with patch("main.requests.get", return_value=response):
                    result = main.call_nami("/api/v5/football/match/schedule/diary")
                self.assertFalse(result["ok"])
                self.assertFalse(result["available"])
                self.assertTrue(result["degraded"])
                self.assertEqual(result["fallback"], "continue_without_nami")
                self.assertTrue(main.health()["ok"])
        finally:
            main.NAMI_API_USER, main.NAMI_API_SECRET = original_user, original_secret

    def test_nami_capability_uses_entitled_v5_realtime_endpoint(self):
        payload = {
            "code": 0,
            "query": {"total": 2, "type": "diary"},
            "results": {
                "match": [{"id": 1}, {"id": 2}],
                "competition": [{"id": 3}],
                "team": [{"id": 4}, {"id": 5}],
            },
        }
        with patch("main.call_nami", return_value={
            "ok": True, "available": True, "degraded": False,
            "status_code": 200, "data": payload, "error": None,
        }) as mocked:
            result = main.nami_capability_check()
        endpoint, params = mocked.call_args.args
        self.assertEqual(endpoint, "/api/v5/football/match/schedule/diary")
        self.assertRegex(params["date"], r"^\d{8}$")
        self.assertEqual(result["api_version"], "v5")
        self.assertEqual(result["product"], "football_realtime")
        self.assertEqual(result["sample_count"], 2)
        self.assertEqual(result["competition_count"], 1)
        self.assertEqual(result["team_count"], 2)

    def test_nami_odds_capability_is_optional_and_probe_only(self):
        payload = {"code": 0, "results": {"asia": [{"id": 1}], "eu": [{"id": 2}, {"id": 3}]}}
        with patch("main.call_nami", return_value={
            "ok": True, "available": True, "degraded": False,
            "status_code": 200, "data": payload, "error": None,
        }) as mocked:
            result = main.nami_odds_capability_check()
        mocked.assert_called_once_with("/api/v5/football/odds/live")
        self.assertTrue(result["ok"])
        self.assertFalse(result["required"])
        self.assertFalse(result["decision_use"])
        self.assertEqual(result["entitlement"], "available")
        self.assertEqual(result["results_shape"], "object")
        self.assertEqual(result["sample_count"], 3)
        paths = {row["path"]: row for row in result["structure_fingerprint"]["paths"]}
        self.assertEqual(paths["results"]["type"], "object")
        self.assertEqual(paths["results.asia"]["length"], 1)
        self.assertEqual(paths["results.asia[].id"]["type"], "number")
        self.assertNotIn("value", str(result["structure_fingerprint"]))

    def test_nami_odds_missing_entitlement_does_not_break_system(self):
        with patch("main.call_nami", return_value={
            "ok": False, "available": False, "degraded": True,
            "status_code": 200, "data": {"code": 1001}, "error": "套餐未开通",
        }):
            result = main.nami_odds_capability_check()
        self.assertEqual(result["entitlement"], "not_entitled")
        self.assertEqual(result["error_category"], "product_not_entitled")
        self.assertEqual(result["fallback"], "continue_without_nami_odds")
        self.assertTrue(main.health()["ok"])

    def test_payload_structure_fingerprint_never_returns_values(self):
        secret_value = "sensitive-provider-value"
        result = main.payload_structure_fingerprint({"companies": [{"name": secret_value, "price": 1.91}]})
        rendered = str(result)
        self.assertNotIn(secret_value, rendered)
        self.assertNotIn("1.91", rendered)
        self.assertIn("results.companies[].name", rendered)
        self.assertIn("results.companies[].price", rendered)

    def test_nami_odds_startup_probe_caches_only_redacted_summary(self):
        original = main.NAMI_ODDS_STARTUP_PROBE
        try:
            with patch("main.load_snapshot_store", return_value={"fixtures": {}}), patch("main.write_snapshot_store", return_value=True), patch("main.nami_odds_capability_check", return_value={
                "configured": True, "available": True, "entitlement": "available",
                "error_category": None, "results_shape": "object", "sample_count": 2,
                "structure_fingerprint": {"paths": [{"path": "results.asia", "type": "array", "length": 2}]},
                "integration_status": "capability_probe_only", "raw_secret": "must-not-be-cached",
            }):
                main.run_nami_odds_startup_probe()
            rendered = str(main.NAMI_ODDS_STARTUP_PROBE)
            self.assertEqual(main.NAMI_ODDS_STARTUP_PROBE["status"], "completed")
            self.assertFalse(main.NAMI_ODDS_STARTUP_PROBE["decision_use"])
            self.assertNotIn("raw_secret", rendered)
            self.assertNotIn("must-not-be-cached", rendered)
        finally:
            main.NAMI_ODDS_STARTUP_PROBE = original

    def test_nami_odds_startup_probe_reuses_unexpired_persistent_cache(self):
        original = main.NAMI_ODDS_STARTUP_PROBE
        now_ts = 1_800_000_000
        cached = {
            "status": "completed", "checked_at": now_ts - 60, "configured": True,
            "available": False, "entitlement": "not_entitled", "decision_use": False,
        }
        try:
            with patch("main.time.time", return_value=now_ts), patch("main.load_snapshot_store", return_value={
                "provider_capability_cache": {"nami_football_odds": cached}
            }), patch("main.nami_odds_capability_check") as live_probe:
                main.run_nami_odds_startup_probe()
            live_probe.assert_not_called()
            self.assertEqual(main.NAMI_ODDS_STARTUP_PROBE["source"], "persistent_cache")
            self.assertEqual(main.NAMI_ODDS_STARTUP_PROBE["age_seconds"], 60)
            self.assertFalse(main.NAMI_ODDS_STARTUP_PROBE["decision_use"])
        finally:
            main.NAMI_ODDS_STARTUP_PROBE = original

    def prematch_packet(self):
        consensus = {
            "1x2": {"status": "available", "median_prices": {"home": 2.0, "draw": 3.4, "away": 3.8}},
            "asian_handicap": {"status": "available", "line": -0.25, "bookmaker_coverage": 2, "median_prices": {"home": 1.9, "away": 1.95}},
            "over_under": {"status": "available", "line": 2.5, "bookmaker_coverage": 2, "median_prices": {"over": 1.91, "under": 1.94}},
            "btts": {"status": "data_missing"}, "home_team_total": {"status": "available", "line": 0.5, "median_prices": {"over": 1.8, "under": 2.0}}, "away_team_total": {"status": "data_missing"},
        }
        row = {"stage": "Opening", "status": "available", "latest_observed_at": "2026-09-29T00:00:00+00:00", "bookmaker_count": 2, "quote_count": 9, "consensus_main_line": consensus, "company_market_array": [
            {"bookmaker_name": "A", "market": "1x2", "selection": "Home", "price": "2.0"},
            {"bookmaker_name": "A", "market": "1x2", "selection": "Draw", "price": "3.4"},
            {"bookmaker_name": "A", "market": "1x2", "selection": "Away", "price": "3.8"},
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Total - Home", "selection": "Over 2.5", "line": "2.5", "price": "1.9"},
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Total - Home", "selection": "Under 2.5", "line": "2.5", "price": "1.95"},
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Home Team Total Goals(1st Half)", "selection": "Over 0.5", "line": "0.5", "price": "1.8"},
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Home Team Total Goals(1st Half)", "selection": "Under 0.5", "line": "0.5", "price": "2.0"},
        ]}
        return {"schema_version": "shadow_prematch_packet_v1", "league": "UEFA Nations League", "match": {"match_id": "uuid-1", "home_team_name": "Home", "away_team_name": "Away"}, "required_timeline": main.PREMATCH_STAGE_ORDER, "timeline": [row, {"stage": "T-30m", "status": "data_missing", "reason": "not captured"}], "lineup_history": [{"observed_at": "2026-09-29T00:00:00+00:00", "status": "official"}], "data_quality": {"level": "partial"}}

    def test_imported_packet_preserves_uuid_arrays_and_missing_stage(self):
        result = main.import_prematch_packet(self.prematch_packet())
        self.assertEqual(result["fixture"], "uuid-1")
        rows = main.get_fixture_snapshots("uuid-1")
        self.assertEqual(len(rows), 2)
        opening = rows[0]
        self.assertTrue(opening["opening_source_audit"]["verified"])
        self.assertIn("1x2", opening["opening_source_audit"]["verified_markets"])
        self.assertEqual(opening["market_snapshot"]["primary"]["asian_handicap"]["line"], -0.25)
        self.assertEqual(opening["market_snapshot"]["primary"]["asian_handicap"]["source"], "upstream_consensus_fallback")
        self.assertEqual(opening["market_snapshot"]["primary"]["home_team_total"]["line"], 2.5)
        self.assertEqual(opening["market_snapshot"]["primary"]["1x2"]["source"], "complete_company_array")
        self.assertEqual(opening["market_snapshot"]["consensus_audit"]["1x2"], "recalculated_from_company_array")
        self.assertEqual(len(opening["market_snapshot"]["markets"]["home_team_total"][0]["lines"]), 1)
        self.assertEqual(opening["market_snapshot"]["markets"]["1x2"][0]["home"], 2.0)
        missing = [x for x in rows if x["stage"] == "T-30m"][0]
        self.assertEqual(missing["import_status"], "data_missing")
        packet = main.build_imported_ai_packet("uuid-1")
        self.assertIn("T-30m", packet["market"]["missing_stages"])
        self.assertNotIn("company_market_array", packet["market"]["timeline"][0])
        self.assertIsNone(packet["fundamentals"]["lineup_history"])
        self.assertNotIn("markets", packet["market"]["current"])
        expanded = main.build_imported_ai_packet("uuid-1", include_companies=True, include_lineups=True)
        self.assertIn("company_market_array", expanded["market"]["timeline"][0])
        self.assertEqual(len(expanded["fundamentals"]["lineup_history"]), 1)
        self.assertEqual(packet["decision_layer"]["decision"], "PASS")

    def test_unverified_imported_opening_is_downgraded_to_data_missing(self):
        packet = self.prematch_packet()
        packet["timeline"][0].pop("company_market_array")
        result = main.import_prematch_packet(packet)
        opening_result = next(row for row in result["stages"] if row["stage"] == "Opening")
        self.assertEqual(opening_result["status"], "data_missing")
        opening = next(row for row in main.get_fixture_snapshots("uuid-1") if row["stage"] == "Opening")
        self.assertEqual(opening["missing_reason"], "opening_source_unverified")
        self.assertFalse(opening["opening_source_audit"]["verified"])
        self.assertFalse(opening["market_snapshot"]["available"])

    def test_incomplete_company_rows_do_not_verify_opening(self):
        packet = self.prematch_packet()
        packet["timeline"][0]["company_market_array"] = [
            {"bookmaker_name": "A", "market": "1x2", "selection": "Home", "price": "2.0"}
        ]
        main.import_prematch_packet(packet)
        opening = next(row for row in main.get_fixture_snapshots("uuid-1") if row["stage"] == "Opening")
        self.assertEqual(opening["import_status"], "data_missing")
        self.assertEqual(opening["opening_source_audit"]["verified_markets"], [])

    def test_fixture_readiness_distinguishes_shadow_from_decision_ready(self):
        consensus = {
            "1x2": {"home": 2.0, "draw": 3.4, "away": 3.8, "bookmaker_count": 2, "source": "complete_company_array"},
            "asian_handicap": {"line": -0.25, "home": 1.9, "away": 1.95, "bookmaker_count": 2, "source": "complete_company_array"},
            "over_under": {"line": 2.5, "over": 1.91, "under": 1.94, "bookmaker_count": 2, "source": "complete_company_array"},
        }
        rows = [
            {"fixture": "ready-1", "stage": "T-24h", "snapshot_at": 100, "import_status": "available", "market_snapshot": {"available": True, "consensus_main_line": consensus}, "stage_timing_audit": {"status": "valid"}, "market_dynamics": {"comparison_status": "data_missing"}},
            {"fixture": "ready-1", "stage": "T-12h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True, "consensus_main_line": consensus}, "stage_timing_audit": {"status": "valid"}, "market_dynamics": {"comparison_status": "compared", "market_movements": {"1x2": {"home": 0.0, "draw": 0.0, "away": 0.0}}}},
        ]
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {"ready-1": rows}, "external_prematch": {"ready-1": {"match": {"kickoff_utc": "1970-01-01T00:16:40+00:00"}, "lineup_history": [{"observed_at": 190, "status": "official"}]}}})
        report = main.fixture_readiness_report("ready-1", now_ts=250)
        self.assertEqual(report["status"], "shadow_ready")
        self.assertTrue(report["shadow_ready"])
        self.assertFalse(report["decision_ready"])
        self.assertEqual(report["market_blockers"], [])
        self.assertIn("fundamental_chain_incomplete", report["decision_blockers"])
        self.assertEqual(report["latest_stage"], "T-12h")

        weak = dict(consensus)
        weak["1x2"] = {**consensus["1x2"], "bookmaker_count": 1}
        store = main.load_snapshot_store()
        store["fixtures"]["ready-1"][1]["market_snapshot"]["consensus_main_line"] = weak
        main.write_snapshot_store(store)
        blocked = main.fixture_readiness_report("ready-1", now_ts=250)
        self.assertEqual(blocked["status"], "not_ready")
        self.assertIn("1x2:insufficient_bookmakers", blocked["market_blockers"])

    def test_calibration_lock_settle_and_report_are_immutable(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "external_prematch": {"cal-1": {"match": {"kickoff_utc": "1970-01-01T00:16:40+00:00"}}}})
        locked = main.lock_calibration_prediction("cal-1", {"home": .6, "draw": .25, "away": .15}, {"decision": "home", "price": 2.0}, captured_at=900)
        self.assertEqual(locked["action"], "locked")
        self.assertEqual(main.lock_calibration_prediction("cal-1", {"home": .6, "draw": .25, "away": .15}, {"decision": "home", "price": 2.0}, captured_at=950)["action"], "unchanged")
        with self.assertRaises(main.HTTPException) as changed:
            main.lock_calibration_prediction("cal-1", {"home": .5, "draw": .3, "away": .2}, captured_at=950)
        self.assertEqual(changed.exception.status_code, 409)

        settlement = main.settle_calibration_prediction("cal-1", 2, 1, settled_at=1100)
        self.assertEqual(settlement["outcome_1x2"], "home")
        self.assertAlmostEqual(settlement["brier_score"], .245, places=8)
        self.assertAlmostEqual(settlement["log_loss"], -main.math.log(.6), places=8)
        self.assertEqual(settlement["unit_return"], 1.0)
        self.assertEqual(main.settle_calibration_prediction("cal-1", 2, 1)["action"], "unchanged")
        with self.assertRaises(main.HTTPException):
            main.settle_calibration_prediction("cal-1", 1, 2)
        report = main.calibration_report()
        self.assertEqual(report["settled_count"], 1)
        self.assertEqual(report["bet_count"], 1)
        self.assertEqual(report["roi"], 1.0)

    def test_calibration_pass_counts_probability_not_betting_roi(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}})
        main.lock_calibration_prediction("cal-pass", {"home": .4, "draw": .3, "away": .3}, {"decision": "PASS"}, captured_at=100)
        settled = main.settle_calibration_prediction("cal-pass", 0, 0, settled_at=200)
        self.assertFalse(settled["bet_placed"])
        self.assertIsNone(settled["unit_return"])
        report = main.calibration_report()
        self.assertEqual(report["settled_count"], 1)
        self.assertEqual(report["bet_count"], 0)
        self.assertIsNone(report["roi"])

    def test_operations_status_separates_info_warning_and_critical(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {}})
        with patch.object(main, "AUTO_SNAPSHOT_ENABLED", False), patch.object(main, "CALIBRATION_MIN_SAMPLE", 3):
            healthy = main.operations_status_report(now_ts=4000)
        self.assertEqual(healthy["status"], "healthy")
        self.assertFalse(healthy["calibration"]["sample_ready"])
        self.assertIn("calibration_sample_collecting", [alert["code"] for alert in healthy["alerts"]])

        store = main.load_snapshot_store()
        store["fundamental_revalidation_queue"] = {"task": {"task_id": "task", "fixture": "1", "status": "pending", "stage": "T-1h", "created_at": 100, "reasons": ["significant_line_move"]}}
        main.write_snapshot_store(store)
        with patch.object(main, "AUTO_SNAPSHOT_ENABLED", False):
            degraded = main.operations_status_report(now_ts=4000)
        self.assertEqual(degraded["status"], "degraded")
        self.assertEqual(degraded["revalidation_queue"]["overdue_count"], 1)

        with patch.object(main, "AUTO_SNAPSHOT_ENABLED", True), patch.object(main, "AUTO_SNAPSHOT_THREAD", None):
            blocked = main.operations_status_report(now_ts=4000)
        self.assertEqual(blocked["status"], "blocked")
        self.assertIn("auto_snapshot_worker_not_running", [alert["code"] for alert in blocked["alerts"]])

        alive = unittest.mock.Mock()
        alive.is_alive.return_value = True
        with patch.object(main, "AUTO_SNAPSHOT_ENABLED", True), patch.object(main, "AUTO_SNAPSHOT_THREAD", alive), patch.object(main, "AUTO_SNAPSHOT_LAST_CYCLE_AT", 1000), patch.object(main, "AUTO_SNAPSHOT_LAST_ERROR", "TimeoutError"):
            stale = main.operations_status_report(now_ts=4000)
        codes = [alert["code"] for alert in stale["alerts"]]
        self.assertIn("auto_snapshot_cycle_stale", codes)
        self.assertIn("auto_snapshot_last_cycle_failed", codes)

    def test_v130_release_acceptance_authorizes_shadow_not_real_money_use(self):
        operations = {
            "status": "healthy",
            "store": {"operational": True, "recovery_ready": True},
            "calibration": {"settled_count": 12, "minimum_sample": 30, "sample_ready": False},
        }
        fixture_acceptance = {
            "status": "awaiting_real_fixture", "fixture_count": 0,
            "status_counts": {"not_ready": 0, "shadow_ready": 0, "decision_ready": 0},
            "shadow_path_validated": False, "decision_path_validated": False,
            "fixtures": [], "fixtures_truncated": False,
        }
        with patch.object(main, "operations_status_report", return_value=operations), patch.object(main, "fixture_acceptance_summary", return_value=fixture_acceptance), patch.object(main, "SHADOW_ACCESS_TOKEN", "configured"):
            release = main.release_acceptance_report(now_ts=1000)
        self.assertEqual(release["version"], main.VERSION)
        self.assertEqual(release["status"], "shadow_usable")
        self.assertTrue(release["shadow_use_authorized"])
        self.assertFalse(release["real_money_use_authorized"])
        self.assertFalse(release["controlled_decision_candidate"])
        self.assertIn("calibration_minimum_sample_not_reached", release["warnings"])
        self.assertIn("real_fixture_shadow_path_not_yet_validated", release["warnings"])
        self.assertFalse(release["fixture_acceptance"]["shadow_path_validated"])
        self.assertTrue(release["usable_scope"]["system_usable"])
        self.assertEqual(release["usable_scope"]["operating_mode"], "manual_or_api_import_shadow")
        self.assertTrue(release["usable_scope"]["safe_pass_without_fresh_data"])
        self.assertIn("real_money_betting", release["usable_scope"]["not_authorized"])
        self.assertEqual(release["blockers"], [])

        blocked_operations = {**operations, "status": "blocked", "store": {"operational": False, "recovery_ready": False}}
        with patch.object(main, "operations_status_report", return_value=blocked_operations), patch.object(main, "fixture_acceptance_summary", return_value=fixture_acceptance), patch.object(main, "SHADOW_ACCESS_TOKEN", ""):
            blocked = main.release_acceptance_report(now_ts=1000)
        self.assertEqual(blocked["status"], "not_ready")
        self.assertFalse(blocked["shadow_use_authorized"])
        self.assertIn("persistent_store_operational", blocked["blockers"])
        self.assertIn("protected_api_configured", blocked["blockers"])

    def test_fixture_acceptance_requires_a_real_ready_fixture(self):
        reports = {
            "a": {"fixture": "a", "status": "not_ready", "latest_stage": "Opening", "market_blockers": ["line_movement:data_missing"], "decision_blockers": []},
            "b": {"fixture": "b", "status": "shadow_ready", "latest_stage": "T-12h", "market_blockers": [], "decision_blockers": ["fundamental_chain_incomplete"]},
        }
        with patch.object(main, "fixture_readiness_report", side_effect=lambda fixture, now_ts=None: reports[fixture]):
            summary = main.fixture_acceptance_summary(["a", "b"], now_ts=1000)
        self.assertEqual(summary["status"], "shadow_path_validated")
        self.assertTrue(summary["shadow_path_validated"])
        self.assertFalse(summary["decision_path_validated"])
        self.assertEqual(summary["status_counts"]["shadow_ready"], 1)

        empty = main.fixture_acceptance_summary([], now_ts=1000)
        self.assertEqual(empty["status"], "awaiting_real_fixture")
        self.assertFalse(empty["shadow_path_validated"])

    def test_final_decision_output_contract_accepts_explained_pass(self):
        decision = main.decision_layer(main.empty_market_snapshot())
        audit = main.audit_decision_output(decision)
        self.assertEqual(audit["status"], "complete")
        self.assertTrue(audit["decision_eligible"])
        self.assertEqual(audit["mode"], "pass")

    def test_final_decision_output_contract_rejects_inconsistent_outputs(self):
        incomplete_pass = {"decision": "PASS", "best_market": {"market": "1x2"}, "pass_reasons": []}
        pass_audit = main.audit_decision_output(incomplete_pass)
        self.assertFalse(pass_audit["decision_eligible"])
        self.assertIn("pass_must_not_have_best_market", pass_audit["consistency_issues"])
        self.assertTrue(pass_audit["missing_fields"])

        actionable = {field: None for field in (
            "model_probability", "market_no_vig_probability", "edge", "ev", "script_coverage",
            "crowding", "line_movement", "lineup_confidence", "death_path",
        )}
        actionable.update({"decision": "home", "best_market": {}, "pass_reasons": []})
        action_audit = main.audit_decision_output(actionable)
        self.assertFalse(action_audit["decision_eligible"])
        self.assertIn("actionable_decision_requires_best_market", action_audit["consistency_issues"])

    def test_release_candidate_self_test_covers_pass_and_actionable_paths(self):
        result = main.release_candidate_self_test()
        self.assertEqual(result["status"], "passed")
        self.assertTrue(result["passed"])
        self.assertTrue(all(result["checks"].values()))
        self.assertNotEqual(result["actionable_selection"], "PASS")

    def test_imported_ai_packet_excludes_invalid_latest_node(self):
        packet = self.prematch_packet()
        invalid_late = dict(packet["timeline"][0])
        invalid_late["stage"] = "T-30m"
        invalid_late["latest_observed_at"] = "2030-01-01T00:00:00+00:00"
        packet["timeline"] = [packet["timeline"][0], invalid_late]
        main.import_prematch_packet(packet)
        store = main.load_snapshot_store()
        late = next(row for row in store["fixtures"]["uuid-1"] if row["stage"] == "T-30m")
        late["stage_timing_audit"] = {"status": "invalid", "decision_eligible": False}
        main.write_snapshot_store(store)
        result = main.build_imported_ai_packet("uuid-1")
        self.assertEqual(result["market"]["available_prematch_stage_count"], 1)
        self.assertIn("T-30m", result["market"]["missing_stages"])
        self.assertEqual(result["market"]["current"]["consensus_main_line"]["1x2"]["home"], 2.0)
        self.assertIn("line_movement_requires_two_real_comparable_stages", result["decision_layer"]["pass_reasons"])

    def test_imported_missing_stage_cannot_erase_valid_pang_history(self):
        main.import_prematch_packet(self.prematch_packet())
        degraded = self.prematch_packet()
        degraded["timeline"][0] = {
            "stage": "Opening", "status": "data_missing",
            "latest_observed_at": "2026-09-29T01:00:00+00:00", "reason": "temporary upstream gap",
        }
        result = main.import_prematch_packet(degraded)
        opening_result = next(row for row in result["stages"] if row["stage"] == "Opening")
        self.assertEqual(opening_result["action"], "quality_regression_skipped")
        self.assertEqual(result["counts"]["quality_regression_skipped"], 1)
        opening = next(row for row in main.get_fixture_snapshots("uuid-1") if row["stage"] == "Opening")
        self.assertEqual(opening["import_status"], "available")
        self.assertTrue(opening["market_snapshot"]["available"])

    def test_imported_same_timestamp_conflict_is_not_silently_overwritten(self):
        main.import_prematch_packet(self.prematch_packet())
        conflict = self.prematch_packet()
        conflict["timeline"][0]["company_market_array"][0]["price"] = "1.7"
        result = main.import_prematch_packet(conflict)
        opening_result = next(row for row in result["stages"] if row["stage"] == "Opening")
        self.assertEqual(opening_result["action"], "timestamp_conflict_skipped")
        self.assertEqual(result["counts"]["timestamp_conflict_skipped"], 1)
        opening = next(row for row in main.get_fixture_snapshots("uuid-1") if row["stage"] == "Opening")
        self.assertEqual(opening["market_snapshot"]["markets"]["1x2"][0]["home"], 2.0)

    def test_imported_consensus_ignores_conflicting_upstream_main_line_when_array_complete(self):
        packet = self.prematch_packet()
        packet["timeline"][0]["consensus_main_line"]["1x2"]["median_prices"] = {"home": 9.0, "draw": 9.0, "away": 9.0}
        main.import_prematch_packet(packet)
        snapshot = main.get_fixture_snapshots("uuid-1")[0]["market_snapshot"]
        self.assertEqual(snapshot["consensus_main_line"]["1x2"]["home"], 2.0)
        self.assertEqual(snapshot["consensus_main_line"]["1x2"]["method"], "median_all_complete_bookmakers")

    def test_imported_duplicate_selection_is_rejected_instead_of_last_value_winning(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["consensus_main_line"]["1x2"] = {"status": "data_missing"}
        stage["company_market_array"].insert(1, {
            "bookmaker_name": " a ", "market": "1x2", "selection": "Home", "price": "1.6",
        })
        snapshot = main.imported_market_snapshot(stage)
        self.assertIsNone(snapshot["consensus_main_line"]["1x2"])
        self.assertTrue(snapshot["markets"]["1x2"][0]["ambiguous_duplicate_selection"])
        audit = snapshot["company_array_quality_audit"]
        self.assertEqual(audit["ambiguous_duplicate_selection_group_count"], 1)
        self.assertEqual(audit["ambiguous_duplicate_selection_group_count_by_market"]["1x2"], 1)
        self.assertEqual(audit["ambiguous_duplicate_selection_groups"][0]["duplicate_selections"], ["home"])

    def test_imported_ambiguous_company_array_cannot_fall_back_to_upstream_consensus(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["company_market_array"].insert(1, {
            "bookmaker_name": "A", "market": "1x2", "selection": "Home", "price": "1.6",
        })
        snapshot = main.imported_market_snapshot(stage)
        self.assertIsNone(snapshot["consensus_main_line"]["1x2"])
        self.assertEqual(snapshot["data_status"]["1x2"], "data_missing")
        self.assertEqual(
            snapshot["consensus_audit"]["1x2"],
            "data_missing_company_array_ambiguous_duplicate_selection",
        )

    def test_imported_incomplete_company_array_cannot_fall_back_to_upstream_consensus(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["company_market_array"] = [
            {"bookmaker_name": "A", "market": "1x2", "selection": "Home", "price": "2.0"},
            {"bookmaker_name": "A", "market": "1x2", "selection": "Draw", "price": "3.4"},
        ]
        snapshot = main.imported_market_snapshot(stage)
        self.assertIsNone(snapshot["consensus_main_line"]["1x2"])
        self.assertEqual(snapshot["data_status"]["1x2"], "data_missing")
        self.assertEqual(
            snapshot["consensus_audit"]["1x2"],
            "data_missing_company_array_no_complete_quote",
        )
        self.assertEqual(snapshot["consensus_audit"]["asian_handicap"], "upstream_fallback_company_array_unavailable")
        quality = snapshot["company_array_quality_audit"]
        self.assertEqual(quality["quote_group_count_by_market"]["1x2"], 1)
        self.assertEqual(quality["eligible_complete_quote_group_count_by_market"]["1x2"], 0)
        self.assertEqual(quality["incomplete_or_invalid_quote_group_count_by_market"]["1x2"], 1)
        self.assertEqual(
            quality["rejection_reason_count_by_market"]["1x2"],
            {"missing_or_invalid_selection_price": 1},
        )
        self.assertEqual(quality["quote_group_count_by_market"]["asian_handicap"], 0)

    def test_imported_quality_audit_distinguishes_missing_line_from_duplicate_selection(self):
        markets = main._import_company_markets([
            {"bookmaker_name": "A", "market": "over_under", "selection": "Over", "price": "1.9"},
            {"bookmaker_name": "A", "market": "over_under", "selection": "Under", "price": "1.9"},
            {"bookmaker_name": "B", "market": "btts", "selection": "Yes", "price": "1.8"},
            {"bookmaker_name": "B", "market": "btts", "selection": "Yes", "price": "1.9"},
            {"bookmaker_name": "B", "market": "btts", "selection": "No", "price": "2.0"},
        ])
        audit = main._import_company_array_quality_audit(markets)
        self.assertEqual(
            audit["rejection_reason_count_by_market"]["over_under"],
            {"missing_or_invalid_line": 1},
        )
        self.assertEqual(
            audit["rejection_reason_count_by_market"]["btts"],
            {"ambiguous_duplicate_selection": 1},
        )

    def test_imported_raw_quotes_drop_unneeded_large_fields_with_audit(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["company_market_array"][0]["unused_payload"] = "x" * 10000
        snapshot = main.imported_market_snapshot(stage)
        raw_quote = snapshot["markets"]["1x2"][0]["raw_values"][0]
        self.assertNotIn("unused_payload", raw_quote)
        self.assertEqual(raw_quote["selection"], "Home")
        audit = snapshot["company_array_compaction_audit"]
        self.assertEqual(audit["quote_count"], len(stage["company_market_array"]))
        self.assertEqual(audit["dropped_unneeded_field_count"], 1)

    def test_imported_company_array_rejects_non_array_before_persistence(self):
        packet = self.prematch_packet()
        packet["timeline"][0]["company_market_array"] = {"selection": "Home"}
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "company_market_array_must_be_an_array")
        self.assertEqual(main.get_fixture_snapshots("uuid-1"), [])

    def test_imported_company_array_rejects_non_object_rows_with_bounded_audit(self):
        packet = self.prematch_packet()
        packet["timeline"][0]["company_market_array"] = [None, "bad-row"]
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail["error"], "company_market_array_rows_must_be_objects")
        self.assertEqual(raised.exception.detail["invalid_row_indexes"], [0, 1])
        self.assertEqual(main.get_fixture_snapshots("uuid-1"), [])

    def test_imported_consensus_rejects_non_object_before_persistence(self):
        packet = self.prematch_packet()
        packet["timeline"][0]["consensus_main_line"] = ["invalid"]
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "consensus_main_line_must_be_an_object")

    def test_imported_packet_rejects_non_object_match(self):
        packet = self.prematch_packet()
        packet["match"] = "invalid"
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "match_must_be_an_object")

    def test_imported_packet_rejects_non_object_timeline_rows_before_persistence(self):
        packet = self.prematch_packet()
        packet["timeline"] = [packet["timeline"][0], None, "invalid"]
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail["error"], "timeline_rows_must_be_objects")
        self.assertEqual(raised.exception.detail["invalid_row_indexes"], [1, 2])
        self.assertEqual(main.get_fixture_snapshots("uuid-1"), [])

    def test_imported_packet_rejects_non_array_lineup_history(self):
        packet = self.prematch_packet()
        packet["lineup_history"] = {"status": "official"}
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail, "lineup_history_must_be_an_array")

    def test_imported_packet_rejects_non_object_lineup_rows_before_persistence(self):
        packet = self.prematch_packet()
        packet["lineup_history"] = [packet["lineup_history"][0], None]
        with self.assertRaises(main.HTTPException) as raised:
            main.import_prematch_packet(packet)
        self.assertEqual(raised.exception.status_code, 400)
        self.assertEqual(raised.exception.detail["error"], "lineup_history_rows_must_be_objects")
        self.assertEqual(raised.exception.detail["invalid_row_indexes"], [1])
        self.assertEqual(main.get_fixture_snapshots("uuid-1"), [])

    def test_imported_duplicate_line_selection_only_rejects_affected_line(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["consensus_main_line"]["home_team_total"] = {"status": "data_missing"}
        stage["company_market_array"].append({
            "bookmaker_name": "A", "market": "home_team_total", "market_name": "Total - Home",
            "selection": "Over 2.5", "line": "2.5", "price": "1.7",
        })
        stage["company_market_array"].extend([
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Total - Home", "selection": "Over 1.5", "line": "1.5", "price": "1.8"},
            {"bookmaker_name": "A", "market": "home_team_total", "market_name": "Total - Home", "selection": "Under 1.5", "line": "1.5", "price": "2.0"},
        ])
        snapshot = main.imported_market_snapshot(stage)
        consensus = snapshot["consensus_main_line"]["home_team_total"]
        self.assertEqual(consensus["line"], 1.5)
        self.assertEqual(consensus["bookmaker_count"], 1)
        self.assertEqual(snapshot["company_array_quality_audit"]["ambiguous_duplicate_selection_group_count"], 1)

    def test_imported_same_selection_across_bookmakers_is_not_ambiguous(self):
        stage = self.prematch_packet()["timeline"][0]
        stage["company_market_array"].extend([
            {"bookmaker_name": "B", "market": "1x2", "selection": "Home", "price": "2.2"},
            {"bookmaker_name": "B", "market": "1x2", "selection": "Draw", "price": "3.2"},
            {"bookmaker_name": "B", "market": "1x2", "selection": "Away", "price": "3.6"},
        ])
        snapshot = main.imported_market_snapshot(stage)
        self.assertEqual(snapshot["consensus_main_line"]["1x2"]["bookmaker_count"], 2)
        self.assertEqual(snapshot["company_array_quality_audit"]["ambiguous_duplicate_selection_group_count"], 0)

    def test_snapshot_store_is_gzip_and_reads_plain_legacy_json(self):
        main.write_snapshot_store({"version": "test", "fixtures": {}})
        with open(main.SNAPSHOT_STORE_PATH, "rb") as handle:
            self.assertEqual(handle.read(2), b"\x1f\x8b")
        self.assertEqual(main.load_snapshot_store()["version"], "test")
        with open(main.SNAPSHOT_STORE_PATH, "wb") as handle:
            handle.write(b'{"version":"legacy","fixtures":{}}')
        self.assertEqual(main.load_snapshot_store()["version"], "legacy")

    def test_snapshot_store_write_failure_is_never_reported_as_success(self):
        with patch("pathlib.Path.write_bytes", side_effect=OSError("disk full")):
            with self.assertRaises(main.SnapshotStoreWriteError) as raised:
                main.write_snapshot_store({"version": main.VERSION, "fixtures": {}})
        self.assertEqual(str(raised.exception), "snapshot_store_write_failed")

    def test_corrupt_existing_snapshot_store_is_not_treated_as_empty(self):
        with open(main.SNAPSHOT_STORE_PATH, "wb") as handle:
            handle.write(b"not-json-and-not-gzip")
        with self.assertRaises(main.SnapshotStoreReadError) as raised:
            main.load_snapshot_store()
        self.assertIn("existing_file_was_not_overwritten", str(raised.exception))
        with open(main.SNAPSHOT_STORE_PATH, "rb") as handle:
            self.assertEqual(handle.read(), b"not-json-and-not-gzip")

    def test_non_object_snapshot_store_is_rejected(self):
        with open(main.SNAPSHOT_STORE_PATH, "wb") as handle:
            handle.write(b"[]")
        with self.assertRaises(main.SnapshotStoreReadError):
            main.load_snapshot_store()

    def test_snapshot_store_keeps_readable_rolling_backup(self):
        main.write_snapshot_store({"version": "backup-test", "fixtures": {"a": []}})
        self.assertTrue(main.snapshot_backup_path().exists())
        self.assertEqual(main.load_snapshot_store()["version"], "backup-test")
        with open(main.SNAPSHOT_STORE_PATH, "wb") as handle:
            handle.write(b"corrupt-primary")
        with self.assertRaises(main.SnapshotStoreReadError):
            main.load_snapshot_store()
        self.assertIsInstance(main.load_snapshot_backup_store()["fixtures"], dict)

    def test_snapshot_store_backup_is_previous_committed_version(self):
        main.write_snapshot_store({"version": "first", "fixtures": {"a": []}})
        main.write_snapshot_store({"version": "second", "fixtures": {"b": []}})
        self.assertEqual(main.load_snapshot_store()["version"], "second")
        self.assertEqual(main.load_snapshot_backup_store()["version"], "first")

    def test_store_integrity_reports_primary_backup_without_data_payload(self):
        main.write_snapshot_store({"version": "integrity-test", "fixtures": {"a": [{"secret": "not-returned"}]}, "portfolio_runs": {"p": []}})
        integrity = main.snapshot_store_integrity()
        self.assertTrue(integrity["operational"])
        self.assertTrue(integrity["recovery_ready"])
        self.assertEqual(integrity["primary"]["fixture_count"], 1)
        self.assertEqual(integrity["primary"]["snapshot_count"], 1)
        self.assertGreater(integrity["primary"]["raw_size_bytes"], 0)
        self.assertGreater(integrity["primary"]["compression_ratio"], 0)
        self.assertEqual(integrity["primary"]["capacity_state"], "normal")
        self.assertTrue(integrity["capacity_ok"])
        self.assertIn("portfolio_count", integrity["backup"])
        self.assertNotIn("fixtures", integrity["primary"])
        self.assertNotIn("secret", str(integrity))
        self.assertFalse(integrity["automatic_restore"])

    def test_store_integrity_detects_corrupt_primary_with_readable_backup(self):
        main.write_snapshot_store({"version": "good", "fixtures": {}})
        with open(main.SNAPSHOT_STORE_PATH, "wb") as handle:
            handle.write(b"corrupt")
        integrity = main.snapshot_store_integrity()
        self.assertFalse(integrity["operational"])
        self.assertEqual(integrity["primary"]["status"], "corrupt")
        self.assertTrue(integrity["recovery_ready"])

    def test_shadow_token_supports_header_and_bearer(self):
        self.assertEqual(main.resolve_shadow_token("query", None, "header"), "header")
        self.assertEqual(main.resolve_shadow_token("query", "Bearer bearer-value", None), "bearer-value")
        self.assertEqual(main.resolve_shadow_token("query", None, None), "query")

    def test_import_is_idempotent_per_stage(self):
        packet = self.prematch_packet()
        first = main.import_prematch_packet(packet)
        second = main.import_prematch_packet(packet)
        self.assertEqual(len(main.get_fixture_snapshots("uuid-1")), 2)
        self.assertEqual(first["counts"]["inserted"], 2)
        self.assertEqual(second["counts"]["unchanged"], 2)
        self.assertFalse(second["changed"])

    def test_batch_import_merges_in_memory_without_partial_persistence(self):
        first = self.prematch_packet()
        second = self.prematch_packet()
        second["match"] = {**second["match"], "match_id": "uuid-2"}
        with patch.object(main, "write_snapshot_store") as writer:
            results, store = main.import_prematch_packet_batch([first, second])
        self.assertEqual(writer.call_count, 0)
        self.assertEqual(len(results), 2)
        self.assertEqual(set(store["fixtures"]), {"uuid-1", "uuid-2"})

    def test_batch_import_failure_does_not_persist_earlier_packets(self):
        valid = self.prematch_packet()
        invalid = {**self.prematch_packet(), "schema_version": "unsupported"}
        with patch.object(main, "write_snapshot_store") as writer:
            with self.assertRaises(main.HTTPException):
                main.import_prematch_packet_batch([valid, invalid])
        self.assertEqual(writer.call_count, 0)

    def test_imported_market_move_creates_deduplicated_revalidation_task(self):
        packet = self.prematch_packet()
        moved = dict(packet["timeline"][0])
        moved["stage"] = "T-12h"
        moved["latest_observed_at"] = "2026-09-29T12:00:00+00:00"
        moved["consensus_main_line"] = {key: dict(value) for key, value in moved["consensus_main_line"].items()}
        moved["consensus_main_line"]["asian_handicap"] = dict(moved["consensus_main_line"]["asian_handicap"])
        moved["consensus_main_line"]["asian_handicap"]["line"] = -0.5
        packet["timeline"] = [packet["timeline"][0], moved]
        first = main.import_prematch_packet(packet)
        second = main.import_prematch_packet(packet)
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertEqual(first["revalidation_tasks_created"], 1)
        self.assertEqual(second["revalidation_tasks_created"], 0)
        self.assertEqual(len(queue), 1)
        task = next(iter(queue.values()))
        self.assertEqual(task["status"], "pending")
        self.assertIn("significant_line_move", task["reasons"])
        self.assertIn("market_move_alone_must_not_modify_fundamentals", task["policy"])

    def test_revalidation_dedupes_metadata_only_change_and_supersedes_new_signal(self):
        store = {"fundamental_revalidation_queue": {}}
        row = {"stage": "T-3h", "snapshot_at": 100, "source_content_hash": "metadata-a", "market_snapshot": {"consensus_main_line": {"1x2": {"home": 2.0}}}, "market_dynamics": {"classification": "Market-Only Move", "revalidation_trigger": {"triggered": True, "reasons": ["abnormal_price_move"]}}}
        first = main._enqueue_revalidation(store, "fixture", row)
        metadata_only = {**row, "source_content_hash": "metadata-b"}
        self.assertIsNone(main._enqueue_revalidation(store, "fixture", metadata_only))
        self.assertEqual(len(store["fundamental_revalidation_queue"]), 1)
        changed_signal = {**row, "market_snapshot": {"consensus_main_line": {"1x2": {"home": 1.8}}}}
        second = main._enqueue_revalidation(store, "fixture", changed_signal)
        self.assertIsNotNone(second)
        self.assertEqual(len(store["fundamental_revalidation_queue"]), 2)
        old = store["fundamental_revalidation_queue"][first["task_id"]]
        self.assertEqual(old["status"], "superseded")
        self.assertEqual(old["superseded_by"], second["task_id"])

    def test_native_snapshot_trigger_enters_revalidation_queue(self):
        record = {
            "fixture": 123, "stage": "T-3h", "snapshot_at": 100,
            "market_snapshot": {"consensus_main_line": {"1x2": {"home": 1.8}}},
            "market_dynamics": {
                "classification": "Market-Only Move",
                "revalidation_trigger": {"triggered": True, "reasons": ["abnormal_price_move"]},
            },
        }
        saved = main.save_snapshot(record)
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertTrue(saved["revalidation_task_created"])
        self.assertIn(saved["revalidation_task_id"], queue)
        self.assertEqual(queue[saved["revalidation_task_id"]]["fixture"], "123")

    def test_native_snapshot_without_trigger_does_not_enter_queue(self):
        record = {
            "fixture": 124, "stage": "Opening", "snapshot_at": 100,
            "market_snapshot": {"consensus_main_line": {"1x2": {"home": 2.0}}},
            "market_dynamics": {"classification": "Market-Only Move", "revalidation_trigger": {"triggered": False, "reasons": []}},
        }
        saved = main.save_snapshot(record)
        self.assertFalse(saved["revalidation_task_created"])
        self.assertEqual(main.load_snapshot_store().get("fundamental_revalidation_queue", {}), {})

    def test_corrected_earlier_snapshot_recalculates_later_dynamics(self):
        initial = {
            "version": main.VERSION,
            "fixtures": {"77": [
                {"fixture": 77, "stage": "T-24h", "snapshot_at": 100, "market_snapshot": {"primary": {"1x2": {"home": 2.0, "draw": 3.5, "away": 4.0}}}, "market_dynamics": {}},
                {"fixture": 77, "stage": "T-3h", "snapshot_at": 200, "market_snapshot": {"primary": {"1x2": {"home": 1.9, "draw": 3.6, "away": 4.2}}}, "market_dynamics": {"market_movements": {"1x2": {"home": -.1}}}},
            ]},
        }
        main.write_snapshot_store(initial)
        corrected = {"fixture": 77, "stage": "T-24h", "snapshot_at": 110, "market_snapshot": {"primary": {"1x2": {"home": 2.2, "draw": 3.3, "away": 3.8}}}, "market_dynamics": {"revalidation_trigger": {"triggered": False, "reasons": []}}}
        main.save_snapshot(corrected)
        later = next(row for row in main.get_fixture_snapshots(77) if row["stage"] == "T-3h")
        self.assertEqual(later["market_dynamics"]["previous_stage"], "T-24h")
        self.assertAlmostEqual(later["market_dynamics"]["market_movements"]["1x2"]["home"], -.3)

    def test_downstream_recalculation_skips_invalid_timing_node(self):
        initial = {"version": main.VERSION, "fixtures": {"78": [
            {"fixture": 78, "stage": "T-24h", "snapshot_at": 100, "market_snapshot": {"primary": {"1x2": {"home": 2.0}}}, "market_dynamics": {}},
            {"fixture": 78, "stage": "T-3h", "snapshot_at": 50, "stage_timing_audit": {"status": "invalid"}, "market_snapshot": {"primary": {"1x2": {"home": 1.5}}}, "market_dynamics": {"revalidation_trigger": {"triggered": True, "reasons": ["abnormal_price_move"]}}},
        ]}}
        main.write_snapshot_store(initial)
        corrected = {"fixture": 78, "stage": "T-24h", "snapshot_at": 110, "market_snapshot": {"primary": {"1x2": {"home": 2.1}}}, "market_dynamics": {"revalidation_trigger": {"triggered": False, "reasons": []}}}
        saved = main.save_snapshot(corrected)
        later = next(row for row in main.get_fixture_snapshots(78) if row["stage"] == "T-3h")
        self.assertEqual(later["market_dynamics"]["comparison_status"], "data_missing")
        self.assertFalse(later["market_dynamics"]["revalidation_trigger"]["triggered"])
        self.assertEqual(saved["downstream_revalidation_tasks_created"], 0)

    def test_revalidation_retention_never_evicts_pending_tasks(self):
        queue = {f"p-{index}": {"task_id": f"p-{index}", "status": "pending", "created_at": index} for index in range(501)}
        queue.update({f"done-{index}": {"task_id": f"done-{index}", "status": "revalidated", "created_at": index} for index in range(20)})
        audit = main._trim_revalidation_queue(queue, limit=500)
        self.assertEqual(sum(task["status"] == "pending" for task in queue.values()), 501)
        self.assertEqual(len(queue), 501)
        self.assertTrue(audit["over_capacity"])
        self.assertEqual(audit["terminal_removed"], 20)
        self.assertEqual(audit["policy"], "pending_tasks_are_never_silently_evicted")

    def test_revalidation_retention_keeps_newest_terminal_history(self):
        queue = {"pending": {"task_id": "pending", "status": "pending", "created_at": 99}}
        queue.update({f"done-{index}": {"task_id": f"done-{index}", "status": "revalidated", "created_at": index} for index in range(5)})
        main._trim_revalidation_queue(queue, limit=3)
        self.assertEqual(set(queue), {"pending", "done-4", "done-3"})

    def test_revalidation_queue_prioritizes_late_and_overdue_moves(self):
        tasks = [
            {"task_id": "early", "status": "pending", "stage": "T-24h", "created_at": 9000, "reasons": ["abnormal_price_move"]},
            {"task_id": "late", "status": "pending", "stage": "T-30m", "created_at": 7000, "reasons": ["cross_market_divergence"]},
        ]
        viewed = main.revalidation_queue_view(tasks, now_ts=10000)
        self.assertEqual(viewed[0]["task_id"], "late")
        self.assertEqual(viewed[0]["priority"], "critical")
        self.assertTrue(viewed[0]["overdue"])
        self.assertFalse(viewed[1]["overdue"])

    def test_revalidation_queue_distinguishes_unattempted_and_awaiting_evidence(self):
        tasks = [
            {"task_id": "new", "status": "pending", "stage": "T-3h", "created_at": 9900},
            {"task_id": "attempted", "status": "pending", "stage": "T-3h", "created_at": 9000, "attempt_count": 1, "required_evidence": ["result_utility"]},
        ]
        viewed = {task["task_id"]: task for task in main.revalidation_queue_view(tasks, now_ts=10000)}
        self.assertEqual(viewed["new"]["workflow_state"], "queued")
        self.assertEqual(viewed["new"]["next_action"], "run_fundamental_revalidation")
        self.assertEqual(viewed["attempted"]["workflow_state"], "awaiting_evidence")
        self.assertEqual(viewed["attempted"]["next_action"], "collect_required_evidence")

    def test_revalidation_resolution_records_evidence_and_actual_change(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "stage": "T-3h", "status": "pending", "reasons": ["significant_line_move"]}
        }})
        version = {
            "version_number": 2, "changed_information": ["rotation_quality"],
            "fundamental_chain_audit": {"decision_eligible": True},
            "probability_change": {"delta": {"home": -.03, "draw": .01, "away": .02}},
            "best_market_change": {"changed": True, "before": "home", "after": "away"},
        }
        self.assertEqual(main.resolve_revalidation_tasks("fixture-1", version), 1)
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["resolution_classification"], "Fundamental Confirmed")
        self.assertEqual(task["evidence_status"], "verified_fundamental_chain")
        self.assertTrue(task["fundamental_changed"])
        self.assertEqual(task["changed_information"], ["rotation_quality"])

    def test_revalidation_resolution_can_record_model_market_divergence(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "status": "pending"}
        }})
        main.resolve_revalidation_tasks("fixture-1", {"version_number": 2, "changed_information": [], "fundamental_chain_audit": {"decision_eligible": True}}, model_market_divergence=True)
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["resolution_classification"], "Model-Market Divergence")

    def test_revalidation_resolution_does_not_close_later_stage_tasks(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "early": {"task_id": "early", "fixture": "fixture-1", "stage": "T-6h", "status": "pending"},
            "current": {"task_id": "current", "fixture": "fixture-1", "stage": "T-3h", "status": "pending"},
            "later": {"task_id": "later", "fixture": "fixture-1", "stage": "T-30m", "status": "pending"},
        }})
        resolved = main.resolve_revalidation_tasks("fixture-1", {"version_number": 2, "changed_information": [], "fundamental_chain_audit": {"decision_eligible": True}}, resolved_through_stage="T-3h")
        self.assertEqual(resolved, 2)
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertEqual(queue["early"]["status"], "revalidated")
        self.assertEqual(queue["current"]["status"], "revalidated")
        self.assertEqual(queue["later"]["status"], "pending")

    def test_incomplete_revalidation_attempt_stays_pending_with_missing_evidence(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "stage": "T-3h", "status": "pending", "created_at": 1},
            "later": {"task_id": "later", "fixture": "fixture-1", "stage": "T-30m", "status": "pending", "created_at": 1},
        }})
        audit = {"critical_missing": ["result_utility"], "critical_provenance_missing": ["rotation_quality"], "critical_semantic_issues": {}, "critical_structural_issues": {}}
        recorded = main.record_incomplete_revalidation_attempt("fixture-1", audit, "T-3h")
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertEqual(recorded, 1)
        self.assertEqual(queue["task"]["status"], "pending")
        self.assertEqual(queue["task"]["attempt_count"], 1)
        self.assertEqual(queue["task"]["required_evidence"], ["result_utility", "rotation_quality"])
        self.assertNotIn("last_attempt_at", queue["later"])

    def test_revalidation_required_evidence_includes_all_hard_blocker_types(self):
        audit = {
            "critical_missing": ["result_utility"],
            "critical_timestamp_issues": {"rotation_quality": {"issue": "stale"}},
            "structural_issues": {"goal_conversion_edges": {"missing_fields": ["margin_edge"]}},
            "market_contaminated_sections": {"execution_ability": ["market_odds"]},
        }
        self.assertEqual(main.revalidation_required_evidence(audit), ["execution_ability", "goal_conversion_edges", "result_utility", "rotation_quality"])

    def test_first_revalidation_version_does_not_claim_fundamental_change(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "status": "pending"}
        }})
        main.resolve_revalidation_tasks("fixture-1", {"version_number": 1, "changed_information": main.FUNDAMENTAL_CHAIN, "fundamental_chain_audit": {"decision_eligible": True}})
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["resolution_classification"], "Market-Only Move")
        self.assertFalse(task["fundamental_changed"])

    def test_unverified_version_cannot_close_revalidation_task(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "status": "pending"}
        }})
        version = {"version_number": 2, "changed_information": ["rotation_quality"], "fundamental_chain_audit": {"decision_eligible": False}}
        self.assertEqual(main.resolve_revalidation_tasks("fixture-1", version), 0)
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["status"], "pending")
        self.assertEqual(task["attempt_count"], 1)
        self.assertEqual(task["last_attempt_outcome"], "insufficient_verified_fundamental_evidence")
        self.assertEqual(task["required_evidence"], ["fundamental_chain_completeness"])

    def test_saved_version_contains_bounded_chain_evidence_audit(self):
        script = {"content_hash": "x", "chain": {key: {"status": "data_missing"} for key in main.FUNDAMENTAL_CHAIN}}
        row = main.save_fundamental_version(23, script, {"triggered": False})
        self.assertFalse(row["fundamental_chain_audit"]["decision_eligible"])
        self.assertFalse(row["recalculation_audit"]["fundamental_evidence_eligible"])
        self.assertIn("result_utility", row["fundamental_chain_audit"]["critical_missing"])

    def test_incremental_import_skips_stale_stage(self):
        packet = self.prematch_packet()
        main.import_prematch_packet(packet)
        packet["timeline"][0]["latest_observed_at"] = "2026-09-28T00:00:00+00:00"
        packet["timeline"][0]["consensus_main_line"]["1x2"]["median_prices"]["home"] = 9.0
        result = main.import_prematch_packet(packet)
        self.assertEqual(result["counts"]["stale_skipped"], 1)
        opening = [x for x in main.get_fixture_snapshots("uuid-1") if x["stage"] == "Opening"][0]
        self.assertEqual(opening["market_snapshot"]["primary"]["1x2"]["home"], 2.0)

    def test_metadata_and_kickoff_corrections_are_not_ignored(self):
        packet = self.prematch_packet()
        packet["match"]["kickoff_utc"] = "2026-09-30T00:00:00+00:00"
        first = main.import_prematch_packet(packet)
        self.assertTrue(first["metadata_changed"])
        corrected = self.prematch_packet()
        corrected["match"]["kickoff_utc"] = "2026-09-29T23:00:00+00:00"
        corrected["lineup_history"].append({"observed_at": "2026-09-29T22:00:00+00:00", "status": "official"})
        second = main.import_prematch_packet(corrected)
        self.assertTrue(second["changed"])
        self.assertTrue(second["metadata_changed"])
        self.assertEqual(second["counts"]["updated"], 2)
        metadata = main.load_snapshot_store()["external_prematch"]["uuid-1"]
        self.assertEqual(metadata["match"]["kickoff_utc"], "2026-09-29T23:00:00+00:00")
        self.assertEqual(len(metadata["lineup_history"]), 2)

    def test_imported_fixture_freshness_gates_decisions(self):
        metadata = {"match": {"kickoff_utc": "2026-10-02T12:00:00+00:00"}}
        history = [{"stage": "Opening", "import_status": "available", "snapshot_at": 1000}]
        fresh = main.imported_fixture_freshness(metadata, history, now_ts=1100)
        self.assertEqual(fresh["state"], "fresh")
        self.assertTrue(fresh["decision_eligible"])
        stale = main.imported_fixture_freshness(metadata, history, now_ts=1000 + main.EXTERNAL_DATA_STALE_SECONDS + 1)
        self.assertEqual(stale["state"], "stale")
        self.assertFalse(stale["decision_eligible"])
        historical = main.imported_fixture_freshness({"match": {"kickoff_utc": "1970-01-01T00:16:00+00:00"}}, history, now_ts=1100)
        self.assertEqual(historical["state"], "historical")
        missing = main.imported_fixture_freshness(metadata, [], now_ts=1100)
        self.assertEqual(missing["state"], "data_missing")

    def test_market_data_route_does_not_treat_configured_key_as_decision_data(self):
        store = {
            "fixtures": {}, "external_prematch": {},
            "provider_capability_cache": {"nami_football_odds": {"entitlement": "not_entitled", "available": False}},
        }
        with patch.object(main, "API_FOOTBALL_KEY", "configured-key"):
            report = main.market_data_route_report(store_override=store, now_ts=1000)
        self.assertEqual(report["status"], "awaiting_verified_snapshot")
        self.assertFalse(report["decision_eligible"])
        self.assertIsNone(report["decision_route"])
        self.assertEqual(report["missing_action"], "PASS")
        self.assertIn("api_football_configured_unverified_for_current_fixture", report["collector_candidates"])

    def test_market_data_route_accepts_only_fresh_persisted_snapshot(self):
        store = {
            "fixtures": {"fixture-1": [{"stage": "T-3h", "snapshot_at": 990, "import_status": "available", "market_snapshot": {"available": True}}]},
            "external_prematch": {"fixture-1": {"match": {"kickoff_utc": "1970-01-01T00:33:20+00:00"}}},
        }
        report = main.market_data_route_report(store_override=store, now_ts=1000)
        self.assertEqual(report["status"], "decision_data_available")
        self.assertTrue(report["decision_eligible"])
        self.assertEqual(report["decision_route"], "pang_persisted_snapshot")
        self.assertEqual(report["pang"]["fresh_fixture_count"], 1)
        self.assertIsNone(report["missing_action"])

    def test_public_health_exposes_only_compact_market_route_summary(self):
        route = {
            "status": "awaiting_verified_snapshot", "decision_eligible": False,
            "decision_route": None, "collector_candidates": ["api_football_configured_unverified_for_current_fixture"],
            "pang": {"fresh_fixture_count": 0, "fixtures": [{"secret": "must-not-leak"}]},
            "missing_action": "PASS", "rule": "internal-rule-detail",
        }
        with patch("main.market_data_route_report", return_value=route):
            result = main.health()
        public = result["market_data_route"]
        self.assertEqual(public["status"], "awaiting_verified_snapshot")
        self.assertEqual(public["missing_action"], "PASS")
        self.assertEqual(public["fresh_fixture_count"], 0)
        self.assertNotIn("pang", public)
        self.assertNotIn("rule", public)
        self.assertNotIn("must-not-leak", str(public))

    def test_freshness_uses_latest_stage_and_rejects_future_timestamp(self):
        metadata = {"match": {"kickoff_utc": "2030-01-01T00:00:00+00:00"}}
        history = [
            {"stage": "Opening", "import_status": "available", "snapshot_at": 9900},
            {"stage": "T-1h", "import_status": "available", "snapshot_at": 1000},
        ]
        stale = main.imported_fixture_freshness(metadata, history, now_ts=10000)
        self.assertEqual(stale["latest_stage"], "T-1h")
        self.assertEqual(stale["state"], "stale")
        future = main.imported_fixture_freshness(metadata, [{"stage": "T-30m", "import_status": "available", "snapshot_at": 10401}], now_ts=10000)
        self.assertEqual(future["state"], "invalid_timestamp")
        self.assertFalse(future["decision_eligible"])

    def test_lineup_confidence_is_capped_by_verified_evidence(self):
        now_ts = 1790989200
        official = main.audit_lineup_confidence([{"observed_at": now_ts - 600, "status": "official"}], .99, now_ts=now_ts)
        self.assertEqual(official["evidence_status"], "official_fresh")
        self.assertEqual(official["effective_confidence"], .95)
        predicted = main.audit_lineup_confidence([{"observed_at": now_ts - 3600, "type": "predicted"}], .95, now_ts=now_ts)
        self.assertEqual(predicted["effective_confidence"], .8)
        missing = main.audit_lineup_confidence([], .9, now_ts=now_ts)
        self.assertEqual(missing["evidence_status"], "data_missing")
        self.assertEqual(missing["effective_confidence"], .4)

    def test_future_or_invalid_lineup_observations_are_not_trusted(self):
        now_ts = 1790989200
        audit = main.audit_lineup_confidence([{"observed_at": now_ts + 301, "is_official": True}, {"observed_at": "invalid"}], .9, now_ts=now_ts)
        self.assertEqual(audit["valid_observation_count"], 0)
        self.assertEqual(audit["effective_confidence"], .4)

    def test_import_rejects_unknown_schema(self):
        packet = self.prematch_packet()
        packet["schema_version"] = "unknown"
        with self.assertRaises(main.HTTPException):
            main.import_prematch_packet(packet)

    def odds_collection_payload(self, **overrides):
        payload = {
            "fixture": "odds-incremental-1", "sport_key": "soccer_finland_veikkausliiga",
            "league": "Finland Veikkausliiga", "home_team": "IF Gnistan",
            "away_team": "Inter Turku", "kickoff_utc": "2026-10-07T16:00:00Z",
            "persist": False,
        }
        payload.update(overrides)
        return payload

    def test_the_odds_api_incremental_collection_noops_when_nodes_exist(self):
        store = {
            "fixtures": {"odds-incremental-1": [{"stage": "Opening"}, {"stage": "T-24h"}]},
            "external_prematch": {},
        }
        with patch.object(main, "THE_ODDS_API_KEY", "configured"), \
                patch.object(main, "load_snapshot_store", return_value=store), \
                patch.object(main, "collect_historical_timeline") as collector:
            result = main.collect_the_odds_api_timeline(self.odds_collection_payload(requested_stages=["Opening", "T-24h"]))
        self.assertTrue(result["incremental_noop"])
        self.assertEqual(result["request_count"], 0)
        collector.assert_not_called()

    def test_the_odds_api_incremental_collection_passes_only_missing_nodes_and_known_event(self):
        store = {
            "fixtures": {"odds-incremental-1": [{"stage": "Opening"}]},
            "external_prematch": {
                "odds-incremental-1": {"provider_fixture_ids": {"the_odds_api": "known-event-1"}}
            },
        }
        provider_result = {
            "ok": True, "request_count": 1, "packet": {"timeline": []},
            "requested_stages": ["T-6h"], "request_audit": [],
        }
        with patch.object(main, "THE_ODDS_API_KEY", "configured"), \
                patch.object(main, "load_snapshot_store", return_value=store), \
                patch.object(main, "collect_historical_timeline", return_value=provider_result) as collector:
            result = main.collect_the_odds_api_timeline(self.odds_collection_payload(requested_stages=["Opening", "T-6h"]))
        self.assertEqual(result["stages_to_fetch"], ["T-6h"])
        self.assertEqual(result["skipped_existing_stages"], ["Opening"])
        self.assertEqual(collector.call_args.kwargs["requested_stages"], ["T-6h"])
        self.assertEqual(collector.call_args.kwargs["known_event_id"], "known-event-1")

    def test_the_odds_api_incremental_collection_requires_explicit_retry_for_recorded_missing_node(self):
        store = {
            "fixtures": {"odds-incremental-1": [{"stage": "T-3h", "import_status": "data_missing"}]},
            "external_prematch": {},
        }
        with patch.object(main, "THE_ODDS_API_KEY", "configured"), \
                patch.object(main, "load_snapshot_store", return_value=store), \
                patch.object(main, "collect_historical_timeline") as collector:
            no_retry = main.collect_the_odds_api_timeline(self.odds_collection_payload(requested_stages=["T-3h"]))
        self.assertTrue(no_retry["incremental_noop"])
        collector.assert_not_called()

        provider_result = {"ok": True, "request_count": 2, "packet": {"timeline": []}, "request_audit": []}
        with patch.object(main, "THE_ODDS_API_KEY", "configured"), \
                patch.object(main, "load_snapshot_store", return_value=store), \
                patch.object(main, "collect_historical_timeline", return_value=provider_result) as collector:
            retry = main.collect_the_odds_api_timeline(self.odds_collection_payload(requested_stages=["T-3h"], retry_data_missing=True))
        self.assertEqual(retry["stages_to_fetch"], ["T-3h"])
        collector.assert_called_once()

    def learning_payload(self, fixture="learn-1", captured_at=900, kickoff_at=1000, analysis=None):
        return {
            "fixture": fixture,
            "captured_at": captured_at,
            "data_cutoff_at": captured_at,
            "kickoff_at": kickoff_at,
            "scope": {
                "competition_id": 39, "competition_name": "England Premier League", "country": "England",
                "competition_type": "domestic_league",
                "tier": 1, "gender": "men", "professional": True,
                "team_level": "first_team", "season": "2026", "phase": "regular_season",
            },
            "versions": {"rules": "2026-10-08", "model": "champion-test", "league_dna": "data_missing"},
            "analysis": analysis or {"fundamental_chain": {"status": "complete"}},
            "decision": {"decision": "PASS", "match_rating": "B"},
            "source_refs": ["source:test"],
        }

    def settle_learning_fixture(self, fixture, captured_at, kickoff_at):
        frozen = main.freeze_learning_sample(self.learning_payload(fixture, captured_at, kickoff_at), now_ts=captured_at)
        facts = self.collect_verified_learning_facts(frozen, 1, 1, kickoff_at + 7200)
        settled = main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 1},
            "PROCESS_CORRECT_RESULT_LOSS", {"status": "clean", "red_cards": 0}, kickoff_at + 7200,
            self.learning_postmatch_review(), facts["fact_hash"],
        )
        return frozen, settled

    def learning_ablation_plan(self, modules=("MSCB",)):
        return {
            "primary_module": modules[0],
            "required_modules": list(modules),
            "module_interventions": {
                module: {
                    "intervention": main.LEARNING_ABLATION_INTERVENTIONS[module],
                    "minimum_brier_gain": 0.0,
                }
                for module in modules
            },
            "minimum_challenger_brier_gain_over_champion": 0.0,
        }

    def learning_validation_plan(self, minimum_samples=2, modules=("MSCB",), markets=("1x2",)):
        return {
            "minimum_samples": minimum_samples,
            "structured_scope": {"competition_ids": [39], "markets": list(markets)},
            "ablation_plan": self.learning_ablation_plan(modules),
        }

    def module_ablation_outputs(self, computed_at, modules=("MSCB",), probabilities=None):
        probabilities = probabilities or {"home": 0.33, "draw": 0.34, "away": 0.33}
        return {
            module: {
                "probabilities": probabilities,
                "output_reference": f"test:ablation:{module}:{computed_at}",
                "computed_at": computed_at,
            }
            for module in modules
        }

    def learning_shadow_runner(self, champion=None, challenger=None, ablation=None, market="1x2"):
        champion = champion or {"home": 0.45, "draw": 0.25, "away": 0.30}
        challenger = challenger or {"home": 0.30, "draw": 0.45, "away": 0.25}
        ablation = ablation or {"home": 0.33, "draw": 0.34, "away": 0.33}

        def runner(request):
            generated_at = int(request["freeze"]["captured_at"]) + 5
            modules = request["hypothesis"]["ablation_plan"]["required_modules"]
            content = {
                "schema": "learning_shadow_model_run_v1",
                "runner_id": "test-pit-runner", "runner_version": "1.0",
                "generated_at": generated_at, "input_hash": request["input_hash"],
                "freeze_hash": request["freeze"]["content_hash"],
                "hypothesis_hash": request["hypothesis"]["hypothesis_hash"],
                "champion_probabilities": champion,
                "challenger_probabilities": challenger,
                "module_ablations": {module: {"probabilities": ablation} for module in modules},
                "selected_expression": {
                    "market": market, "selection": "draw" if market == "1x2" else "under",
                    "line": None if market == "1x2" else 2.75,
                    "entry_decimal_price": 3.0 if market == "1x2" else 1.91,
                    "entry_price_evidence_ref": f"test:internal-runner:{market}",
                },
                "risk": {"champion_tail_risk": 0.10, "challenger_tail_risk": 0.10},
            }
            return {**content, "run_hash": main._content_hash(content)}

        return runner

    def settle_hypothesis_validation_fixture(self, hypothesis_id, fixture, captured_at, kickoff_at, modules=("MSCB",)):
        frozen = main.freeze_learning_sample(self.learning_payload(fixture, captured_at, kickoff_at), now_ts=captured_at)
        shadow_lock = main.generate_internal_shadow_lock(
            hypothesis_id, frozen["freeze_id"], model_runner=self.learning_shadow_runner(), now_ts=captured_at + 10,
        )
        facts = self.collect_verified_learning_facts(frozen, 1, 1, kickoff_at + 7200)
        main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 1},
            "PROCESS_CORRECT_RESULT_LOSS", {"status": "clean"}, kickoff_at + 7200,
            self.learning_postmatch_review(), facts["fact_hash"],
        )
        evidence = main.record_hypothesis_validation(hypothesis_id, {
            "freeze_id": frozen["freeze_id"], "outcome": "support",
            "evidence_summary": f"Independent locked validation for {fixture}",
            "pit_audit": {"status": "passed"}, "event_pollution_audit": {"status": "passed"},
            "shadow_lock_hash": shadow_lock["lock_hash"], "closing_decimal_price": 2.8,
            "closing_price_evidence_ref": f"test:closing:{fixture}",
        })
        return frozen, evidence

    def learning_postmatch_review(self, **disposition_overrides):
        disposition = {
            "result_backfit_used": False, "champion_change_requested": False,
            "new_theory_status": "none", "existing_rule_implementation_gap": False,
        }
        disposition.update(disposition_overrides)
        return {
            "match_selection_quality": {"status": "inconclusive"},
            "fundamental_chain_audit": {"status": "passed"},
            "state_tree_coverage": {"status": "inconclusive"},
            "market_language_audit": {"status": "passed"},
            "expression_audit": {"status": "inconclusive"},
            "price_execution_audit": {"status": "inconclusive"},
            "process_reasoning": "Review compares the frozen prematch process with verified events, independent of the final score.",
            "review_mode": "manual",
            "outcome_not_used_for_process_grade": True,
            "learning_disposition": disposition,
        }

    def test_learning_freeze_requires_verified_top_flight_scope_and_prematch_time(self):
        payload = self.learning_payload()
        payload["scope"]["tier"] = 2
        with self.assertRaises(main.HTTPException) as wrong_tier:
            main.freeze_learning_sample(payload, now_ts=900)
        self.assertEqual(wrong_tier.exception.status_code, 422)
        self.assertIn("tier_one_required", wrong_tier.exception.detail["reasons"])

        late = self.learning_payload(captured_at=1000, kickoff_at=1000)
        with self.assertRaises(main.HTTPException) as after_kickoff:
            main.freeze_learning_sample(late, now_ts=1000)
        self.assertEqual(after_kickoff.exception.status_code, 409)

    def test_learning_scope_requires_registry_match_or_external_evidence(self):
        unverified = self.learning_payload()["scope"]
        unverified["competition_id"] = 999999
        unverified["competition_name"] = "Unregistered Premier"
        audit = main.audit_learning_scope(unverified)
        self.assertFalse(audit["eligible"])
        self.assertIn("top_flight_verification_required", audit["reasons"])

        unverified["verification_status"] = "verified"
        unverified["verification_refs"] = [{"source": "league_organizer", "url": "https://example.test/competition"}]
        evidenced = main.audit_learning_scope(unverified)
        self.assertTrue(evidenced["eligible"])
        self.assertEqual(evidenced["normalized"]["verification_method"], "explicit_external_evidence")

        mismatch = self.learning_payload()["scope"]
        mismatch["competition_name"] = "UEFA Champions League"
        registry_audit = main.audit_learning_scope(mismatch)
        self.assertFalse(registry_audit["eligible"])
        self.assertIn("competition_name_registry_mismatch", registry_audit["reasons"])

    def learning_fixture_row(self, fixture_id, league_id, kickoff_at, status="NS"):
        return {
            "fixture": {"id": fixture_id, "timestamp": kickoff_at, "date": main.datetime.fromtimestamp(kickoff_at, tz=main.timezone.utc).isoformat(), "status": {"short": status}},
            "league": {"id": league_id, "name": "League", "country": "Country", "season": 2026, "round": "Regular Season - 1"},
            "teams": {"home": {"id": fixture_id * 10, "name": "Home"}, "away": {"id": fixture_id * 10 + 1, "name": "Away"}},
            "goals": {"home": None, "away": None},
        }

    def test_learning_discovery_excludes_continental_live_and_outside_horizon(self):
        now_ts = 100000
        rows = [
            self.learning_fixture_row(1, 39, now_ts + 3600),
            self.learning_fixture_row(2, 2, now_ts + 3600),
            self.learning_fixture_row(3, 39, now_ts + 3600, status="1H"),
            self.learning_fixture_row(4, 39, now_ts + 25 * 3600),
        ]
        discovery = main.discover_learning_fixtures(now_ts=now_ts, fixture_rows=rows)
        self.assertEqual(discovery["candidate_count"], 1)
        self.assertEqual(discovery["candidates"][0]["fixture_id"], 1)
        self.assertTrue(discovery["candidates"][0]["learning_scope_verified"])
        self.assertEqual(discovery["candidates"][0]["scope"]["competition_type"], "domestic_league")
        self.assertEqual(discovery["excluded_counts"]["competition_not_in_top_flight_registry"], 1)
        self.assertEqual(discovery["excluded_counts"]["fixture_not_not_started"], 1)
        self.assertEqual(discovery["excluded_counts"]["outside_learning_discovery_horizon"], 1)

    def test_learning_discovery_accepts_canonical_fixture_summaries_from_snapshot_worker(self):
        now_ts = 100000
        raw = self.learning_fixture_row(6, 39, now_ts + 3600)
        summary = main.fixture_summary(raw)
        discovery = main.discover_learning_fixtures(now_ts=now_ts, fixture_rows=[summary])
        self.assertEqual(discovery["candidate_count"], 1)
        self.assertEqual(discovery["candidates"][0]["fixture_id"], 6)
        self.assertEqual(discovery["candidates"][0]["scope"]["tier"], 1)

    def test_learning_cycle_plan_prioritizes_latest_unsettled_freeze_without_mutating(self):
        main.freeze_learning_sample(self.learning_payload("due", captured_at=900, kickoff_at=1000), now_ts=900)
        main.freeze_learning_sample(self.learning_payload("due", captured_at=950, kickoff_at=1000, analysis={"node": "T-30m"}), now_ts=950)
        rows = [self.learning_fixture_row(5, 39, 11800 + 3600)]
        before = main.load_snapshot_store()
        plan = main.learning_cycle_plan(now_ts=11800, fixture_rows=rows)
        after = main.load_snapshot_store()
        self.assertEqual(plan["settlement_due_count"], 1)
        self.assertEqual(plan["settlement_due"][0]["freeze_id"], "due:v2")
        self.assertIsNone(plan["remaining_freeze_capacity"])
        self.assertIsNone(plan["match_limit"])
        self.assertIsNone(plan["full_historical_odds_sample_limit"])
        self.assertEqual(plan["learning_prematch_stages"], ["Opening", "T-12h", "T-6h", "T-1h"])
        self.assertEqual(plan["reanalysis_today_count"], 1)
        self.assertEqual(plan["discovery"]["candidate_count"], 1)
        self.assertEqual(before, after)
        self.assertFalse(plan["automatic_champion_promotion"])

    def test_learning_freeze_is_immutable_and_changed_updates_create_new_version(self):
        payload = self.learning_payload()
        first = main.freeze_learning_sample(payload, now_ts=900)
        self.assertEqual(first["freeze_id"], "learn-1:v1")
        self.assertTrue(first["immutable"])
        duplicate = main.freeze_learning_sample(payload, now_ts=900)
        self.assertEqual(duplicate["action"], "unchanged")

        updated = self.learning_payload(captured_at=950, analysis={"fundamental_chain": {"status": "complete"}, "new_stage": "T-1h"})
        second = main.freeze_learning_sample(updated, now_ts=950)
        self.assertEqual(second["freeze_id"], "learn-1:v2")
        rows = main.load_snapshot_store()["learning_frozen"]["learn-1"]
        self.assertEqual(len(rows), 2)
        self.assertNotEqual(rows[0]["content_hash"], rows[1]["content_hash"])

        stale_update = self.learning_payload(captured_at=925, analysis={"changed": True})
        with self.assertRaises(main.HTTPException) as non_monotonic:
            main.freeze_learning_sample(stale_update, now_ts=925)
        self.assertEqual(non_monotonic.exception.status_code, 409)

    def test_learning_postmatch_requires_and_immutably_binds_frozen_version(self):
        with self.assertRaises(main.HTTPException) as missing:
            main.settle_learning_sample("missing:v1", {"status": "FT", "home_goals": 0, "away_goals": 0}, "PROCESS_CORRECT_RESULT_WIN", settled_at=2000)
        self.assertEqual(missing.exception.status_code, 404)

        frozen, settled = self.settle_learning_fixture("settled-1", 900, 1000)
        self.assertEqual(settled["freeze_hash"], frozen["content_hash"])
        self.assertFalse(settled["champion_effect"])
        unchanged = main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 1},
            "PROCESS_CORRECT_RESULT_LOSS", {"status": "clean", "red_cards": 0}, 8200,
            self.learning_postmatch_review(), settled["fact_hash"],
        )
        self.assertEqual(unchanged["action"], "unchanged")
        with self.assertRaises(main.HTTPException) as changed:
            main.settle_learning_sample(frozen["freeze_id"], {"status": "FT", "home_goals": 2, "away_goals": 1}, "PROCESS_ERROR_RESULT_WIN", settled_at=8300, review=self.learning_postmatch_review(), fact_hash=settled["fact_hash"])
        self.assertEqual(changed.exception.status_code, 409)

    def test_learning_postmatch_review_rejects_backfit_and_single_match_champion_change(self):
        frozen = main.freeze_learning_sample(self.learning_payload("review-gate", 900, 1000), now_ts=900)
        with self.assertRaises(main.HTTPException) as missing:
            main.settle_learning_sample(
                frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 0},
                "PROCESS_CORRECT_RESULT_WIN", settled_at=8200,
            )
        self.assertEqual(missing.exception.status_code, 422)
        self.assertIn("result_backfit_must_be_explicitly_false", missing.exception.detail["reasons"])

        forbidden = self.learning_postmatch_review(result_backfit_used=True, champion_change_requested=True, new_theory_status="LEAGUE_TAG_CANDIDATE")
        with self.assertRaises(main.HTTPException) as rejected:
            main.settle_learning_sample(
                frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 0},
                "PROCESS_CORRECT_RESULT_WIN", settled_at=8200, review=forbidden,
            )
        self.assertIn("result_backfit_must_be_explicitly_false", rejected.exception.detail["reasons"])
        self.assertIn("single_match_champion_change_forbidden", rejected.exception.detail["reasons"])

    def test_learning_implementation_gap_requires_rule_reference_and_regression_test(self):
        invalid = self.learning_postmatch_review(existing_rule_implementation_gap=True)
        audit = main.audit_learning_postmatch_review(invalid)
        self.assertFalse(audit["eligible"])
        self.assertIn("implementation_gap_requires_existing_rule_reference", audit["reasons"])
        self.assertIn("implementation_gap_requires_regression_test", audit["reasons"])
        valid = self.learning_postmatch_review(
            existing_rule_implementation_gap=True, existing_rule_ref="MODEL_RULES.md#market-resistance",
            regression_test_required=True,
        )
        self.assertTrue(main.audit_learning_postmatch_review(valid)["eligible"])

    def test_single_match_hypothesis_cannot_promote_and_discovery_sample_cannot_validate(self):
        discovery, _ = self.settle_learning_fixture("discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            hypothesis = main.register_learning_hypothesis({
                "hypothesis_id": "hyp-one", "type": "HYPOTHESIS_ONLY", "title": "Candidate only",
                "definition": "A candidate relationship", "applicable_scope": "men tier-one leagues",
                "expected_direction": "positive", "failure_conditions": "effect disappears",
                "falsification_criteria": "independent counterexample", "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(minimum_samples=30),
            })
        self.assertFalse(hypothesis["champion_effect"])
        with self.assertRaises(main.HTTPException) as reuse:
            main.record_hypothesis_validation("hyp-one", {"freeze_id": discovery["freeze_id"], "outcome": "support", "evidence_summary": "same sample"})
        self.assertEqual(reuse.exception.status_code, 409)
        with self.assertRaises(main.HTTPException) as premature:
            main.create_promotion_candidate("hyp-one", {})
        self.assertEqual(premature.exception.status_code, 409)
        self.assertIn("independent_support_sample_minimum_not_reached", premature.exception.detail["blockers"])
        fake_gates = {gate: {"status": "passed", "evidence_refs": ["caller:self-attestation"]} for gate in main.LEARNING_PROMOTION_REQUIRED_GATES}
        with self.assertRaises(main.HTTPException) as forged:
            main.create_promotion_candidate("hyp-one", fake_gates)
        self.assertEqual(forged.exception.status_code, 400)
        self.assertEqual(forged.exception.detail, "caller_supplied_promotion_gate_audit_forbidden")

    def test_validated_hypothesis_only_creates_candidate_waiting_for_user(self):
        discovery, _ = self.settle_learning_fixture("discovery-ready", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "hyp-ready", "type": "LEAGUE_TAG_CANDIDATE", "title": "League candidate",
                "definition": "A registered league prior", "applicable_scope": "one league-season-phase",
                "expected_direction": "positive", "failure_conditions": "unstable out of sample",
                "falsification_criteria": "any unresolved counterexample", "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        for index in range(2):
            self.settle_hypothesis_validation_fixture("hyp-ready", f"validation-{index}", 8400 + index, 9000 + index)
        with patch.object(main, "LEARNING_MIN_VALIDATION_SAMPLES", 2):
            evidence_report = main.promotion_evidence_report("hyp-ready")
            candidate = main.create_promotion_candidate("hyp-ready", None)
        self.assertTrue(evidence_report["promotion_ready"])
        self.assertTrue(all(row["status"] == "passed" for row in evidence_report["gates"].values()))
        self.assertFalse(evidence_report["caller_supplied_gate_status_used"])
        self.assertFalse(evidence_report["result_outcome_used_as_optimization_target"])
        self.assertEqual(candidate["promotion_evidence_report_hash"], evidence_report["report_hash"])
        self.assertEqual(candidate["status"], "AWAITING_EXPLICIT_USER_CONFIRMATION")
        self.assertFalse(candidate["champion_effect"])
        self.assertFalse(candidate["automatic_promotion"])
        status = main.learning_status_report()
        self.assertEqual(status["promotion_candidate_count"], 1)
        self.assertFalse(status["automatic_champion_promotion"])

    def test_shadow_validation_requires_forward_locked_sample_and_is_immutable(self):
        discovery, _ = self.settle_learning_fixture("shadow-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "shadow-lock-hyp", "type": "HYPOTHESIS_ONLY", "title": "Locked challenger",
                "definition": "Forward locked challenger output", "applicable_scope": "men top flights",
                "expected_direction": "lower brier", "failure_conditions": "no OOS improvement",
                "falsification_criteria": "challenger underperforms", "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        frozen = main.freeze_learning_sample(self.learning_payload("shadow-validation", 8400, 9000), now_ts=8400)
        lock_payload = {
            "freeze_id": frozen["freeze_id"],
            "champion_probabilities": {"home": 0.45, "draw": 0.25, "away": 0.30},
            "challenger_probabilities": {"home": 0.30, "draw": 0.45, "away": 0.25},
            "module_ablation_outputs": self.module_ablation_outputs(8405),
            "selected_expression": {"market": "1x2", "selection": "draw", "entry_decimal_price": 3.0, "entry_price_evidence_ref": "test:entry:shadow-validation"},
            "risk": {"champion_tail_risk": 0.10, "challenger_tail_risk": 0.10},
        }
        with self.assertRaises(main.HTTPException) as late:
            main.lock_hypothesis_shadow_prediction("shadow-lock-hyp", lock_payload, now_ts=9000)
        self.assertEqual(late.exception.detail, "shadow_prediction_must_be_locked_before_kickoff")
        locked = main.lock_hypothesis_shadow_prediction("shadow-lock-hyp", lock_payload, now_ts=8410)
        changed = {**lock_payload, "challenger_probabilities": {"home": 0.20, "draw": 0.55, "away": 0.25}}
        with self.assertRaises(main.HTTPException) as overwrite:
            main.lock_hypothesis_shadow_prediction("shadow-lock-hyp", changed, now_ts=8420)
        self.assertEqual(overwrite.exception.detail, "shadow_prediction_already_locked")
        self.assertTrue(locked["immutable"])
        self.assertFalse(locked["champion_effect"])

    def test_validation_evidence_cannot_be_added_without_pre_kickoff_lock(self):
        discovery, _ = self.settle_learning_fixture("no-lock-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "no-lock-hyp", "type": "HYPOTHESIS_ONLY", "title": "No lock rejection",
                "definition": "Must lock before validation", "applicable_scope": "men top flights",
                "expected_direction": "positive", "failure_conditions": "not locked",
                "falsification_criteria": "missing lock", "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        frozen, _ = self.settle_learning_fixture("no-lock-validation", 8400, 9000)
        with self.assertRaises(main.HTTPException) as rejected:
            main.record_hypothesis_validation("no-lock-hyp", {
                "freeze_id": frozen["freeze_id"], "outcome": "support", "evidence_summary": "Retrospective claim",
                "closing_decimal_price": 2.8, "closing_price_evidence_ref": "test:closing",
            })
        self.assertEqual(rejected.exception.detail, "pre_kickoff_shadow_lock_required")

    def test_hypothesis_requires_exact_preregistered_module_ablation_plan(self):
        discovery, _ = self.settle_learning_fixture("ablation-plan-discovery", 900, 1000)
        base = {
            "hypothesis_id": "ablation-plan-hyp", "type": "HYPOTHESIS_ONLY",
            "title": "Module ablation required", "definition": "Test one isolated module change.",
            "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
            "failure_conditions": "no isolated gain", "falsification_criteria": "module removal does not degrade output",
            "discovery_freeze_ids": [discovery["freeze_id"]],
        }
        with self.assertRaises(main.HTTPException) as missing:
            main.register_learning_hypothesis({
                **base,
                "validation_plan": {
                    "minimum_samples": 2,
                    "structured_scope": {"competition_ids": [39], "markets": ["1x2"]},
                },
            })
        self.assertEqual(missing.exception.detail, "ablation_plan_required_modules_required")

        invalid_plan = self.learning_ablation_plan()
        invalid_plan["required_modules"] = ["UNKNOWN"]
        invalid_plan["primary_module"] = "UNKNOWN"
        with self.assertRaises(main.HTTPException) as unsupported:
            main.register_learning_hypothesis({
                **base,
                "validation_plan": {
                    "minimum_samples": 2,
                    "structured_scope": {"competition_ids": [39], "markets": ["1x2"]},
                    "ablation_plan": invalid_plan,
                },
            })
        self.assertEqual(unsupported.exception.detail["error"], "unsupported_ablation_module")

        invalid_scope = self.learning_validation_plan()
        invalid_scope["structured_scope"]["markets"] = ["invented_market"]
        with self.assertRaises(main.HTTPException) as unsupported_market:
            main.register_learning_hypothesis({**base, "validation_plan": invalid_scope})
        self.assertEqual(unsupported_market.exception.detail["error"], "unsupported_structured_scope_market")

    def test_shadow_lock_requires_exact_modules_and_pit_ablation_timestamp(self):
        discovery, _ = self.settle_learning_fixture("exact-ablation-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "exact-ablation-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Exact module lock", "definition": "Lock MSCB and State Tree removals.",
                "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
                "failure_conditions": "either module has no gain", "falsification_criteria": "any module counterexample",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(modules=("MSCB", "STATE_TREE")),
            })
        frozen = main.freeze_learning_sample(self.learning_payload("exact-ablation-validation", 8400, 9000), now_ts=8400)
        base_lock = {
            "freeze_id": frozen["freeze_id"],
            "champion_probabilities": {"home": 0.45, "draw": 0.25, "away": 0.30},
            "challenger_probabilities": {"home": 0.30, "draw": 0.45, "away": 0.25},
            "selected_expression": {"market": "1x2", "selection": "draw", "entry_decimal_price": 3.0, "entry_price_evidence_ref": "test:entry:exact"},
            "risk": {"champion_tail_risk": 0.10, "challenger_tail_risk": 0.10},
        }
        with self.assertRaises(main.HTTPException) as wrong_market:
            main.lock_hypothesis_shadow_prediction("exact-ablation-hyp", {
                **base_lock,
                "selected_expression": {
                    **base_lock["selected_expression"], "market": "over_under",
                },
                "module_ablation_outputs": self.module_ablation_outputs(8405, modules=("MSCB", "STATE_TREE")),
            }, now_ts=8410)
        self.assertEqual(wrong_market.exception.detail, "validation_market_outside_preregistered_scope")

        with self.assertRaises(main.HTTPException) as partial:
            main.lock_hypothesis_shadow_prediction("exact-ablation-hyp", {
                **base_lock, "module_ablation_outputs": self.module_ablation_outputs(8405),
            }, now_ts=8410)
        self.assertEqual(partial.exception.detail["error"], "exact_preregistered_module_ablation_outputs_required")

        with self.assertRaises(main.HTTPException) as post_lock_time:
            main.lock_hypothesis_shadow_prediction("exact-ablation-hyp", {
                **base_lock,
                "module_ablation_outputs": self.module_ablation_outputs(8420, modules=("MSCB", "STATE_TREE")),
            }, now_ts=8410)
        self.assertEqual(post_lock_time.exception.detail["error"], "module_ablation_computed_at_must_be_pit")

        locked = main.lock_hypothesis_shadow_prediction("exact-ablation-hyp", {
            **base_lock,
            "module_ablation_outputs": self.module_ablation_outputs(8405, modules=("MSCB", "STATE_TREE")),
        }, now_ts=8410)
        self.assertEqual(set(locked["module_ablations"]), {"MSCB", "STATE_TREE"})
        self.assertTrue(all(row["freeze_hash"] == frozen["content_hash"] for row in locked["module_ablations"].values()))

    def test_validation_outcome_is_metric_derived_and_caller_support_is_ignored(self):
        discovery, _ = self.settle_learning_fixture("derived-outcome-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "derived-outcome-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Derived validation outcome", "definition": "Caller cannot self-attest support.",
                "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
                "failure_conditions": "challenger is worse", "falsification_criteria": "negative locked metric gain",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        frozen = main.freeze_learning_sample(self.learning_payload("derived-outcome-validation", 8400, 9000), now_ts=8400)
        lock = main.lock_hypothesis_shadow_prediction("derived-outcome-hyp", {
            "freeze_id": frozen["freeze_id"],
            "champion_probabilities": {"home": 0.25, "draw": 0.50, "away": 0.25},
            "challenger_probabilities": {"home": 0.45, "draw": 0.10, "away": 0.45},
            "module_ablation_outputs": self.module_ablation_outputs(8405),
            "selected_expression": {"market": "1x2", "selection": "draw", "entry_decimal_price": 3.0, "entry_price_evidence_ref": "test:entry:derived"},
            "risk": {"champion_tail_risk": 0.10, "challenger_tail_risk": 0.10},
        }, now_ts=8410)
        facts = self.collect_verified_learning_facts(frozen, 1, 1, 16200)
        main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 1, "away_goals": 1},
            "PROCESS_CORRECT_RESULT_LOSS", {"status": "clean"}, 16200,
            self.learning_postmatch_review(), facts["fact_hash"],
        )
        evidence = main.record_hypothesis_validation("derived-outcome-hyp", {
            "freeze_id": frozen["freeze_id"], "outcome": "support",
            "evidence_summary": "Caller attempts to claim support despite worse locked metrics.",
            "shadow_lock_hash": lock["lock_hash"], "closing_decimal_price": 2.8,
            "closing_price_evidence_ref": "test:closing:derived",
        })
        self.assertEqual(evidence["outcome"], "counterexample")
        self.assertEqual(evidence["outcome_derivation"]["caller_supplied_outcome"], "support")
        self.assertFalse(evidence["outcome_derivation"]["caller_supplied_outcome_used"])
        report = main.promotion_evidence_report("derived-outcome-hyp")
        self.assertEqual(report["support_count"], 0)
        self.assertEqual(report["counterexample_count"], 1)
        self.assertFalse(report["caller_supplied_validation_outcome_used"])

    def test_multimodule_ablation_gate_reports_each_preregistered_module(self):
        discovery, _ = self.settle_learning_fixture("multimodule-discovery", 900, 1000)
        modules = ("MSCB", "STATE_TREE")
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "multimodule-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Two-module ablation", "definition": "Validate two isolated module contributions.",
                "applicable_scope": "men top flights", "expected_direction": "positive isolated gains",
                "failure_conditions": "either module fails", "falsification_criteria": "any forward module counterexample",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(modules=modules),
            })
        for index in range(2):
            self.settle_hypothesis_validation_fixture(
                "multimodule-hyp", f"multimodule-validation-{index}", 8400 + index, 9000 + index, modules=modules,
            )
        with patch.object(main, "LEARNING_MIN_VALIDATION_SAMPLES", 2):
            report = main.promotion_evidence_report("multimodule-hyp")
        gate = report["gates"]["ablation"]
        self.assertEqual(gate["status"], "passed")
        self.assertEqual(set(gate["required_modules"]), set(modules))
        self.assertEqual(set(gate["module_metrics"]), set(modules))
        self.assertTrue(all(row["sample_count"] == 2 for row in gate["module_metrics"].values()))

    def test_internal_shadow_runner_hash_and_identity_are_fail_closed(self):
        discovery, _ = self.settle_learning_fixture("runner-hash-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "runner-hash-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Runner hash", "definition": "Only verified internal model runs may lock.",
                "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
                "failure_conditions": "runner identity fails", "falsification_criteria": "invalid run hash",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        payload = self.learning_payload("runner-hash-validation", 8400, 9000)
        payload["decision"] = {
            "decision": "BET", "selected_expression": {"market": "1x2", "selection": "draw", "price": 3.0},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=8400)

        valid_runner = self.learning_shadow_runner()
        def tampered_runner(request):
            result = valid_runner(request)
            return {**result, "run_hash": "0" * 64}

        with self.assertRaises(main.HTTPException) as tampered:
            main.generate_internal_shadow_lock(
                "runner-hash-hyp", frozen["freeze_id"], model_runner=tampered_runner, now_ts=8410,
            )
        self.assertEqual(tampered.exception.detail, "internal_shadow_model_run_hash_mismatch")
        self.assertNotIn(frozen["freeze_id"], main.load_snapshot_store().get("learning_shadow_locks", {}).get("runner-hash-hyp", {}))

    def test_learning_cycle_automatically_locks_forward_sample_with_internal_runner(self):
        discovery, _ = self.settle_learning_fixture("cycle-runner-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "cycle-runner-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Automatic runner", "definition": "Cycle invokes the trusted PIT runner.",
                "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
                "failure_conditions": "no runnable calculator", "falsification_criteria": "invalid forward output",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        payload = self.learning_payload("cycle-runner-validation", 8400, 10000)
        payload["decision"] = {
            "decision": "BET", "selected_expression": {"market": "1x2", "selection": "draw", "price": 3.0},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=8400)
        result = main.run_learning_cycle(
            {"apply": True, "run_id": "internal-runner-cycle", "auto_prepare_prematch": False},
            now_ts=8500, fixture_rows=[], shadow_model_runner=self.learning_shadow_runner(),
        )
        self.assertEqual(result["shadow_lock_created_count"], 1)
        self.assertEqual(result["shadow_lock_results"][0]["action"], "locked")
        self.assertEqual(result["forward_validation_queue"]["queue_count"], 0)
        lock = main.load_snapshot_store()["learning_shadow_locks"]["cycle-runner-hyp"][frozen["freeze_id"]]
        self.assertEqual(lock["calculator_provenance"]["origin"], "internal_shadow_runner")
        self.assertTrue(lock["calculator_provenance"]["promotion_eligible"])

    def test_learning_cycle_reports_runner_blocker_without_accepting_external_outputs(self):
        discovery, _ = self.settle_learning_fixture("cycle-no-runner-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": "cycle-no-runner-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Missing runner", "definition": "Missing calculator remains blocked.",
                "applicable_scope": "men top flights", "expected_direction": "lower forward Brier",
                "failure_conditions": "calculator unavailable", "falsification_criteria": "no internal run",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        payload = self.learning_payload("cycle-no-runner-validation", 8400, 10000)
        payload["decision"] = {
            "decision": "BET", "selected_expression": {"market": "1x2", "selection": "draw", "price": 3.0},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=8400)
        result = main.run_learning_cycle(
            {"apply": True, "run_id": "missing-runner-cycle", "auto_prepare_prematch": False},
            now_ts=8500, fixture_rows=[],
        )
        self.assertEqual(result["shadow_lock_created_count"], 0)
        self.assertEqual(result["shadow_lock_results"][0]["action"], "blocked")
        self.assertEqual(result["shadow_lock_results"][0]["reason"], "internal_shadow_model_runner_not_configured")
        self.assertNotIn(frozen["freeze_id"], main.load_snapshot_store().get("learning_shadow_locks", {}).get("cycle-no-runner-hyp", {}))

    def register_league_dna_fixture(self, hypothesis_id="league-dna-hyp", tag_id="eng-goal-environment"):
        discovery, _ = self.settle_learning_fixture(f"{tag_id}-discovery", 900, 1000)
        with patch.object(main.time, "time", return_value=8300):
            main.register_learning_hypothesis({
                "hypothesis_id": hypothesis_id, "type": "LEAGUE_TAG_CANDIDATE",
                "title": "League goal environment candidate",
                "definition": "A preregistered league-level goal environment offset",
                "applicable_scope": "England Premier League 2026 regular season",
                "expected_direction": "positive", "failure_conditions": "effect decays out of sample",
                "falsification_criteria": "unresolved independent counterexample",
                "discovery_freeze_ids": [discovery["freeze_id"]],
                "validation_plan": self.learning_validation_plan(),
            })
        candidate = main.register_league_dna_candidate({
            "tag_id": tag_id, "hypothesis_id": hypothesis_id,
            "scope": self.learning_payload()["scope"],
            "category": "goal_environment", "market": "over_under",
            "label": "Goal environment above global comparable baseline",
            "magnitude_score": 1.25,
            "metric_definition": "PIT opening total distribution and event-level goal environment",
            "baseline_definition": "Comparable professional domestic tier-one leagues",
            "expected_model_effect": "Bounded prior offset before team residuals",
            "anti_double_counting_rule": "Team inputs must use residuals relative to this league prior",
            "sample_window": {
                "training_start": "2024-01-01", "training_end": "2025-12-31",
                "validation_start": "2026-01-01", "validation_end": "2026-12-31",
                "minimum_independent_samples": 30,
            },
        })
        return discovery, candidate

    def test_league_dna_candidate_is_stored_but_never_enters_prematch_prior(self):
        _, candidate = self.register_league_dna_fixture()
        self.assertEqual(candidate["status"], "LEAGUE_TAG_CANDIDATE")
        self.assertEqual(candidate["evidence_confidence"], 0)
        self.assertFalse(candidate["champion_effect"])
        view = main.league_dna_model_view(self.learning_payload()["scope"])
        self.assertEqual(view["status"], "candidate_only")
        self.assertEqual(view["active_tag_count"], 0)
        self.assertFalse(view["champion_effect"])

        payload = self.learning_payload("dna-freeze", 1200, 1300)
        payload["versions"]["league_dna"] = "fabricated-active-version"
        with self.assertRaises(main.HTTPException) as rejected:
            main.freeze_learning_sample(payload, now_ts=1200)
        self.assertEqual(rejected.exception.status_code, 409)
        self.assertEqual(rejected.exception.detail, "unverified_league_dna_cannot_enter_prematch_freeze")

        payload["versions"]["league_dna"] = "candidate_only"
        frozen = main.freeze_learning_sample(payload, now_ts=1200)
        self.assertEqual(frozen["versions"]["league_dna"], "candidate_only")
        self.assertFalse(frozen["league_dna_audit"]["champion_effect"])

    def test_league_dna_activation_candidate_still_waits_for_user_confirmation(self):
        _, candidate = self.register_league_dna_fixture("dna-ready-hyp", "dna-ready-tag")
        with self.assertRaises(main.HTTPException) as premature:
            main.create_league_dna_activation_candidate(candidate["tag_id"])
        self.assertEqual(premature.exception.status_code, 409)

        for index in range(2):
            self.settle_hypothesis_validation_fixture("dna-ready-hyp", f"dna-validation-{index}", 8400 + index, 9000 + index)
        with patch.object(main, "LEARNING_MIN_VALIDATION_SAMPLES", 2):
            main.create_promotion_candidate("dna-ready-hyp", None)
            activation = main.create_league_dna_activation_candidate("dna-ready-tag")
        self.assertEqual(activation["status"], "AWAITING_EXPLICIT_USER_CONFIRMATION")
        self.assertEqual(activation["evidence_confidence"], 99)
        self.assertFalse(activation["champion_effect"])
        self.assertFalse(activation["automatic_activation"])
        report = main.league_dna_status_report(39)
        self.assertEqual(report["activation_candidate_count"], 1)
        self.assertEqual(report["verified_active_count"], 0)
        self.assertFalse(report["automatic_activation"])
        self.assertEqual(main.league_dna_model_view(self.learning_payload()["scope"])["status"], "candidate_only")

    def test_league_dna_activation_requires_hash_bound_explicit_user_confirmation(self):
        _, candidate = self.register_league_dna_fixture("dna-confirm-hyp", "dna-confirm-tag")
        for index in range(2):
            self.settle_hypothesis_validation_fixture("dna-confirm-hyp", f"dna-confirm-validation-{index}", 8500 + index, 9100 + index)
        with patch.object(main, "LEARNING_MIN_VALIDATION_SAMPLES", 2):
            main.create_promotion_candidate("dna-confirm-hyp", None)
            activation = main.create_league_dna_activation_candidate(candidate["tag_id"])

        base = {
            "confirmed": True,
            "activation_hash": activation["activation_hash"],
            "confirmed_by": "project-owner",
            "confirmation_reference": "thread:test-turn-1",
            "confirmation_statement": f"CONFIRM LEAGUE_DNA {candidate['tag_id']} {activation['activation_hash']}",
        }
        with self.assertRaises(main.HTTPException) as missing_true:
            main.confirm_league_dna_activation(candidate["tag_id"], {**base, "confirmed": False}, now_ts=9200)
        self.assertEqual(missing_true.exception.detail, "explicit_confirmation_true_required")

        wrong_hash = "0" * 64
        with self.assertRaises(main.HTTPException) as stale_hash:
            main.confirm_league_dna_activation(candidate["tag_id"], {
                **base,
                "activation_hash": wrong_hash,
                "confirmation_statement": f"CONFIRM LEAGUE_DNA {candidate['tag_id']} {wrong_hash}",
            }, now_ts=9200)
        self.assertEqual(stale_hash.exception.detail, "latest_activation_candidate_hash_required")

        with self.assertRaises(main.HTTPException) as wrong_statement:
            main.confirm_league_dna_activation(candidate["tag_id"], {**base, "confirmation_statement": "yes"}, now_ts=9200)
        self.assertEqual(wrong_statement.exception.status_code, 422)

        active = main.confirm_league_dna_activation(candidate["tag_id"], base, now_ts=9200)
        self.assertEqual(active["status"], "VERIFIED_ACTIVE")
        self.assertEqual(active["evidence_confidence"], 100)
        self.assertTrue(active["champion_effect"])
        self.assertFalse(active["automatic_activation"])
        self.assertTrue(active["immutable"])

        repeated = main.confirm_league_dna_activation(candidate["tag_id"], base, now_ts=9300)
        self.assertEqual(repeated["action"], "unchanged")
        self.assertEqual(repeated["active_hash"], active["active_hash"])
        with self.assertRaises(main.HTTPException) as overwrite:
            main.confirm_league_dna_activation(candidate["tag_id"], {**base, "confirmation_reference": "thread:different-turn"}, now_ts=9400)
        self.assertEqual(overwrite.exception.detail, "league_dna_tag_already_active")

        view = main.league_dna_model_view(self.learning_payload()["scope"])
        self.assertEqual(view["status"], "VERIFIED_ACTIVE")
        self.assertEqual(view["active_tag_count"], 1)
        self.assertTrue(view["champion_effect"])
        report = main.league_dna_status_report(39)
        self.assertEqual(report["verified_active_count"], 1)

    def learning_prematch_packet(self, fixture_id, generated_at):
        return {
            "ok": True, "version": main.VERSION, "generated_at": generated_at,
            "source": "test_pit_packet", "fixture": {"fixture_id": fixture_id},
            "data_quality": {"status": "partial"}, "coverage": {"status": "partial"},
            "pure_fundamental_script": {
                "status": "partial",
                "chain": {"game_state_elasticity": {"status": "data_missing"}},
            },
            "market": {"timeline": [], "latest_dynamics": {"status": "data_missing"}},
            "analysis_rules": {"prematch_only": True},
            "decision_layer": {"decision": "PASS", "pass_reasons": ["fundamental_chain_insufficient"]},
        }

    def test_learning_cycle_dry_run_previews_freeze_without_mutation(self):
        now_ts = 100000
        fixture_row = self.learning_fixture_row(71, 39, now_ts + 3600)
        preview = main.run_learning_cycle({
            "apply": False,
            "prematch_packets": {"71": self.learning_prematch_packet(71, now_ts)},
        }, now_ts=now_ts, fixture_rows=[fixture_row])
        self.assertEqual(preview["action"], "previewed")
        self.assertEqual(preview["freeze_results"][0]["action"], "would_freeze")
        self.assertFalse(preview["automatic_hypothesis_registration"])
        self.assertFalse(preview["automatic_champion_change"])
        self.assertFalse(os.path.exists(main.SNAPSHOT_STORE_PATH))

    def test_learning_cycle_apply_auto_prepares_and_is_run_idempotent(self):
        now_ts = 100000
        fixture_row = self.learning_fixture_row(72, 39, now_ts + 3600)
        builder_calls = []

        def builder(fixture_id):
            builder_calls.append(fixture_id)
            return self.learning_prematch_packet(fixture_id, now_ts)

        payload = {"apply": True, "run_id": "cycle-20261008-a", "auto_prepare_prematch": True}
        first = main.run_learning_cycle(payload, now_ts=now_ts, fixture_rows=[fixture_row], prematch_packet_builder=builder)
        self.assertEqual(first["frozen_count"], 1)
        self.assertEqual(first["probability_replay_ready_count"], 0)
        self.assertEqual(first["probability_replay_missing_count"], 1)
        self.assertEqual(first["freeze_results"][0]["decision"], "PASS")
        self.assertEqual(first["freeze_results"][0]["probability_replay_status"], "data_missing")
        self.assertEqual(builder_calls, [72])
        store = main.load_snapshot_store()
        self.assertIn("72", store["learning_frozen"])
        self.assertIn("cycle-20261008-a", store["learning_runs"])
        self.assertNotIn("learning_hypotheses", store)
        self.assertNotIn("league_dna_active", store)

        second = main.run_learning_cycle(payload, now_ts=now_ts, fixture_rows=[fixture_row], prematch_packet_builder=builder)
        self.assertEqual(second["action"], "unchanged")
        self.assertEqual(builder_calls, [72])
        self.assertEqual(len(main.load_snapshot_store()["learning_frozen"]["72"]), 1)

    def test_learning_cycle_creates_one_version_per_due_clock_node(self):
        kickoff_at = 200000
        t12_now = kickoff_at - 10 * 3600
        t6_now = kickoff_at - 5 * 3600
        fixture_row = self.learning_fixture_row(720, 39, kickoff_at)
        builder_times = []

        def builder(fixture_id):
            generated_at = builder_times[-1]
            return self.learning_prematch_packet(fixture_id, generated_at)

        builder_times.append(t12_now)
        first = main.run_learning_cycle(
            {"apply": True, "run_id": "node-t12"}, now_ts=t12_now,
            fixture_rows=[fixture_row], prematch_packet_builder=builder,
        )
        self.assertEqual(first["frozen_count"], 1)
        self.assertEqual(first["freeze_results"][0]["analysis_node"], "T-12h")

        builder_times.append(t6_now)
        second = main.run_learning_cycle(
            {"apply": True, "run_id": "node-t6"}, now_ts=t6_now,
            fixture_rows=[fixture_row], prematch_packet_builder=builder,
        )
        self.assertEqual(second["frozen_count"], 1)
        self.assertEqual(second["freeze_results"][0]["analysis_node"], "T-6h")
        self.assertEqual(second["freeze_results"][0]["reason"], "analysis_node_advanced")

        calls_before = len(builder_times)
        same_node = main.run_learning_cycle(
            {"apply": True, "run_id": "node-t6-repeat"}, now_ts=t6_now + 60,
            fixture_rows=[fixture_row],
            prematch_packet_builder=Mock(side_effect=AssertionError("same node must not rebuild")),
        )
        self.assertEqual(same_node["frozen_count"], 0)
        self.assertEqual(same_node["freeze_results"][0]["reason"], "no_new_node_or_material_evidence")
        self.assertEqual(len(main.load_snapshot_store()["learning_frozen"]["720"]), 2)
        self.assertEqual(len(builder_times), calls_before)

    def test_learning_cycle_reanalyzes_same_node_only_for_new_snapshot_evidence(self):
        kickoff_at = 300000
        now_ts = kickoff_at - 10 * 3600
        fixture_row = self.learning_fixture_row(721, 39, kickoff_at)
        first = main.run_learning_cycle(
            {"apply": True, "run_id": "same-node-initial"}, now_ts=now_ts,
            fixture_rows=[fixture_row],
            prematch_packet_builder=lambda fixture_id: self.learning_prematch_packet(fixture_id, now_ts),
        )
        self.assertEqual(first["freeze_results"][0]["analysis_node"], "T-12h")

        snapshot_at = now_ts + 60
        market = main.empty_market_snapshot()
        market["available"] = True
        market["primary"] = {"1x2": {"home": 2.0, "draw": 3.4, "away": 4.0}}
        store = main.load_snapshot_store()
        store.setdefault("fixtures", {})["721"] = [{
            "fixture": 721, "stage": "T-12h", "snapshot_at": snapshot_at,
            "import_status": "available", "market_snapshot": market,
            "market_dynamics": {"comparison_status": "data_missing"},
            "stage_timing_audit": main.audit_stage_timing("T-12h", snapshot_at, kickoff_at),
            "sequence_timing_audit": {"status": "valid"},
        }]
        main.write_snapshot_store(store)
        marker = main._learning_snapshot_marker(main.load_snapshot_store(), "721", snapshot_at)

        packet = self.learning_prematch_packet(721, snapshot_at)
        packet["market"]["timeline"] = [{
            "stage": "T-12h", "status": "available", "snapshot_at": snapshot_at,
            "source_content_hash": marker["evidence_hash"],
        }]
        second = main.run_learning_cycle(
            {"apply": True, "run_id": "same-node-new-evidence", "prematch_packets": {"721": packet}},
            now_ts=snapshot_at, fixture_rows=[fixture_row],
        )
        self.assertEqual(second["frozen_count"], 1)
        self.assertEqual(second["freeze_results"][0]["reason"], "new_snapshot_evidence_at_current_node")
        self.assertEqual(second["freeze_results"][0]["analysis_node"], "T-12h")

        repeat = main.learning_cycle_plan(now_ts=snapshot_at + 60, fixture_rows=[fixture_row])
        candidate = repeat["discovery"]["candidates"][0]
        self.assertFalse(candidate["analysis_due"])
        self.assertEqual(candidate["analysis_due_reason"], "no_new_node_or_material_evidence")

    def test_learning_plan_ignores_non_learning_closing_snapshot(self):
        kickoff_at = 400000
        now_ts = kickoff_at - 5 * 60
        fixture_row = self.learning_fixture_row(722, 39, kickoff_at)
        without_closing = main.learning_cycle_plan(now_ts=now_ts, fixture_rows=[fixture_row])
        self.assertEqual(without_closing["discovery"]["candidates"][0]["target_analysis_node"], "T-1h")

        market = main.empty_market_snapshot()
        market["available"] = True
        store = main.load_snapshot_store()
        store.setdefault("fixtures", {})["722"] = [{
            "fixture": 722, "stage": "Closing", "snapshot_at": now_ts,
            "import_status": "available", "market_snapshot": market,
            "stage_timing_audit": main.audit_stage_timing("Closing", now_ts, kickoff_at),
            "sequence_timing_audit": {"status": "valid"},
        }]
        main.write_snapshot_store(store)
        with_closing = main.learning_cycle_plan(now_ts=now_ts, fixture_rows=[fixture_row])
        self.assertEqual(with_closing["discovery"]["candidates"][0]["target_analysis_node"], "T-1h")

    def test_learning_freeze_payload_reaudits_caller_supplied_stage_time(self):
        generated_at = 500000
        kickoff_at = generated_at + 20 * 3600
        candidate = main.discover_learning_fixtures(
            now_ts=generated_at,
            fixture_rows=[self.learning_fixture_row(723, 39, kickoff_at)],
        )["candidates"][0]
        candidate["target_analysis_node"] = "Opening"
        packet = self.learning_prematch_packet(723, generated_at)
        packet["market"]["timeline"] = [{
            "stage": "Closing", "status": "available", "snapshot_at": generated_at,
            "source_content_hash": "caller-forged-future-closing",
        }]
        with self.assertRaises(main.HTTPException) as rejected:
            main.build_learning_freeze_payload(candidate, packet, now_ts=generated_at)
        self.assertEqual(rejected.exception.status_code, 409)
        self.assertEqual(rejected.exception.detail, "verified_opening_snapshot_required_for_learning_node")

    def test_auto_learning_cycle_runs_only_in_daily_1430_window(self):
        before_due = datetime(2026, 10, 8, 6, 29, tzinfo=timezone.utc)
        due = datetime(2026, 10, 8, 6, 30, tzinfo=timezone.utc)
        cycle_result = {
            "frozen_count": 0, "settled_count": 0, "rejected_count": 0,
            "review_draft_count": 0,
            "execution_order": ["past_36h_postmatch", "future_24h_prematch"],
            "postmatch_fact_results": [{"action": "facts_collected"}],
            "postmatch_review_draft_results": [],
        }
        with patch.object(main, "run_learning_cycle", return_value=cycle_result) as runner:
            idle = main.auto_learning_daily_cycle(before_due, [])
            result = main.auto_learning_daily_cycle(due, [])
        self.assertEqual(idle["status"], "not_due")
        self.assertEqual(result["status"], "completed")
        self.assertEqual(result["run_id"], "daily-20261008-1430")
        self.assertEqual(result["execution_order"], ["past_36h_postmatch", "future_24h_prematch"])
        self.assertEqual(result["postmatch_fact_results"][0]["action"], "facts_collected")
        self.assertFalse(result["automatic_hypothesis_registration"])
        self.assertFalse(result["automatic_champion_change"])
        runner.assert_called_once()

    def test_snapshot_worker_enters_daily_learning_before_future_fixture_collection(self):
        due = datetime(2026, 10, 8, 6, 30, tzinfo=timezone.utc)
        calls = []

        def daily(now, fixture_rows=None):
            calls.append("learning")
            self.assertIsNone(fixture_rows)
            return {"status": "completed", "execution_order": ["past_36h_postmatch", "future_24h_prematch"]}

        def targets(date_str, timezone_name):
            calls.append("raw_snapshot_fixture_discovery")
            return {"fixtures": []}

        with patch.object(main, "auto_learning_daily_cycle", side_effect=daily), \
             patch.object(main, "target_fixtures_for_date", side_effect=targets):
            result = main.auto_snapshot_cycle(due)
        self.assertEqual(calls[0], "learning")
        self.assertEqual(result["execution_order"], ["past_36h_postmatch", "future_24h_prematch"])

    def test_learning_cycle_only_builds_packets_for_top_flight_not_started_candidates(self):
        now_ts = 100000
        rows = [
            self.learning_fixture_row(73, 39, now_ts + 3600),
            self.learning_fixture_row(74, 2, now_ts + 3600),
            self.learning_fixture_row(75, 39, now_ts + 3600, status="1H"),
        ]
        built = []

        def builder(fixture_id):
            built.append(fixture_id)
            return self.learning_prematch_packet(fixture_id, now_ts)

        result = main.run_learning_cycle(
            {"apply": True, "run_id": "cycle-scope-gate", "auto_prepare_prematch": True},
            now_ts=now_ts, fixture_rows=rows, prematch_packet_builder=builder,
        )
        self.assertEqual(result["frozen_count"], 1)
        self.assertEqual(built, [73])
        self.assertEqual(set(main.load_snapshot_store()["learning_frozen"]), {"73"})

    def test_learning_cycle_settles_only_due_freeze_using_explicit_process_review(self):
        frozen = main.freeze_learning_sample(self.learning_payload("cycle-due", 900, 1000), now_ts=900)
        facts = self.collect_verified_learning_facts(frozen, 2, 0, 9000)
        settlement = {
            "freeze_id": frozen["freeze_id"],
            "fact_hash": facts["fact_hash"],
            "result": {"status": "FT", "home_goals": 2, "away_goals": 0},
            "process_classification": "PROCESS_ERROR_RESULT_WIN",
            "event_audit": {"status": "clean", "source_count": 2},
            "settled_at": 9000,
            "review": self.learning_postmatch_review(),
        }
        result = main.run_learning_cycle({
            "apply": True, "run_id": "cycle-settle-due", "auto_prepare_prematch": False,
            "settlement_packets": [settlement],
        }, now_ts=9000, fixture_rows=[])
        self.assertEqual(result["settled_count"], 1)
        saved = main.load_snapshot_store()["learning_postmatch"][frozen["freeze_id"]]
        self.assertEqual(saved["process_classification"], "PROCESS_ERROR_RESULT_WIN")
        self.assertFalse(saved["review"]["learning_disposition"]["result_backfit_used"])
        self.assertNotIn("learning_hypotheses", main.load_snapshot_store())
        self.assertFalse(result["automatic_champion_change"])

    def learning_postmatch_facts(self, sources=("api_football",), home_goals=2, away_goals=1):
        return {
            "ok": True,
            "result": {"status": "FT", "home_goals": home_goals, "away_goals": away_goals},
            "events": [{"elapsed": 10, "type": "Goal", "detail": "Normal Goal"}],
            "statistics": [{"team": "Home", "statistics": {"Total Shots": 12}}],
            "source_audit": [
                {"source": source, "component": "result", "ok": True, "evidence_ref": f"test:{source}:fixture",
                 "home_goals": home_goals, "away_goals": away_goals}
                for source in sources
            ],
        }

    def collect_verified_learning_facts(self, frozen, home_goals, away_goals, collected_at=9000):
        return main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=collected_at,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(
                ("api_football", "official_league"), home_goals, away_goals,
            ),
        )

    def test_learning_cycle_collects_single_source_facts_but_never_classifies_or_settles(self):
        frozen = main.freeze_learning_sample(self.learning_payload("801", 900, 1000), now_ts=900)
        calls = []

        def fetcher(fixture_id):
            calls.append(fixture_id)
            return self.learning_postmatch_facts()

        result = main.run_learning_cycle({
            "apply": True, "run_id": "cycle-facts-only", "auto_prepare_prematch": False,
            "auto_collect_postmatch_facts": True,
        }, now_ts=9000, fixture_rows=[], postmatch_fact_fetcher=fetcher)
        self.assertEqual(calls, ["801"])
        self.assertEqual(result["postmatch_fact_results"][0]["action"], "facts_collected")
        self.assertFalse(result["postmatch_fact_results"][0]["verification"]["settlement_eligible"])
        store = main.load_snapshot_store()
        facts = store["learning_postmatch_facts"][frozen["freeze_id"]][0]
        self.assertEqual(facts["verification"]["status"], "single_source_pending")
        self.assertIsNone(facts["process_classification"])
        self.assertFalse(facts["result_backfit_used"])
        self.assertNotIn(frozen["freeze_id"], store.get("learning_postmatch", {}))
        self.assertNotIn("learning_hypotheses", store)

    def test_two_source_fact_packet_becomes_review_eligible_but_does_not_auto_settle(self):
        frozen = main.freeze_learning_sample(self.learning_payload("802", 900, 1000), now_ts=900)
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(("api_football", "official_league")),
        )
        self.assertTrue(facts["verification"]["settlement_eligible"])
        self.assertEqual(facts["verification"]["independent_source_count"], 2)
        self.assertIsNone(facts["process_classification"])
        self.assertNotIn(frozen["freeze_id"], main.load_snapshot_store().get("learning_postmatch", {}))

    def test_learning_cycle_reuses_fact_queue_without_repeating_provider_calls(self):
        frozen = main.freeze_learning_sample(self.learning_payload("803", 900, 1000), now_ts=900)
        main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(),
        )
        fetcher = Mock(side_effect=AssertionError("provider must not be called twice"))
        result = main.run_learning_cycle({
            "apply": True, "run_id": "cycle-facts-reuse", "auto_prepare_prematch": False,
        }, now_ts=9100, fixture_rows=[], postmatch_fact_fetcher=fetcher)
        self.assertEqual(result["postmatch_fact_results"][0]["action"], "awaiting_independent_verification")
        fetcher.assert_not_called()

    def test_learning_cycle_can_version_fact_queue_with_supplied_second_source(self):
        frozen = main.freeze_learning_sample(self.learning_payload("804", 900, 1000), now_ts=900)
        main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(),
        )
        supplied = self.learning_postmatch_facts(("api_football", "official_league"))
        result = main.run_learning_cycle({
            "apply": True, "run_id": "cycle-facts-verified", "auto_prepare_prematch": False,
            "postmatch_fact_packets": {frozen["freeze_id"]: supplied},
        }, now_ts=9100, fixture_rows=[])
        verification = result["postmatch_fact_results"][0]["verification"]
        self.assertTrue(verification["settlement_eligible"])
        rows = main.load_snapshot_store()["learning_postmatch_facts"][frozen["freeze_id"]]
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[-1]["verification"]["independent_source_count"], 2)
        self.assertNotIn(frozen["freeze_id"], main.load_snapshot_store().get("learning_postmatch", {}))

    def test_fact_verification_counts_only_matching_result_evidence(self):
        frozen = main.freeze_learning_sample(self.learning_payload("805", 900, 1000), now_ts=900)
        supplied = self.learning_postmatch_facts(("api_football", "official_league"))
        supplied["source_audit"][1]["away_goals"] = 0
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000, fact_fetcher=lambda fixture_id: supplied,
        )
        self.assertEqual(facts["verification"]["independent_source_count"], 1)
        self.assertFalse(facts["verification"]["settlement_eligible"])

    def test_fact_verification_rejects_duplicate_refs_and_same_domain_aliases(self):
        frozen = main.freeze_learning_sample(self.learning_payload("805-alias", 900, 1000), now_ts=900)
        supplied = self.learning_postmatch_facts(("source_a", "source_b"))
        supplied["source_audit"][0]["evidence_ref"] = "https://scores.example.test/match/805"
        supplied["source_audit"][1]["evidence_ref"] = "https://scores.example.test/match/805?mirror=1"
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000, fact_fetcher=lambda fixture_id: supplied,
        )
        self.assertEqual(facts["verification"]["independent_source_count"], 1)
        self.assertEqual(facts["verification"]["verified_source_authorities"], ["scores.example.test"])
        self.assertFalse(facts["verification"]["settlement_eligible"])

        frozen_duplicate = main.freeze_learning_sample(self.learning_payload("805-duplicate", 901, 1001), now_ts=901)
        duplicate = self.learning_postmatch_facts(("source_a", "source_b"))
        duplicate["source_audit"][0]["evidence_ref"] = "opaque:same-result-record"
        duplicate["source_audit"][1]["evidence_ref"] = "opaque:same-result-record"
        duplicate_facts = main.collect_learning_postmatch_facts(
            frozen_duplicate["freeze_id"], now_ts=9000, fact_fetcher=lambda fixture_id: duplicate,
        )
        self.assertEqual(duplicate_facts["verification"]["unique_evidence_ref_count"], 1)
        self.assertEqual(duplicate_facts["verification"]["independent_source_count"], 1)
        self.assertFalse(duplicate_facts["verification"]["settlement_eligible"])

    def test_review_queue_prioritizes_verified_facts_and_preserves_frozen_context(self):
        waiting = main.freeze_learning_sample(self.learning_payload("806", 900, 1000), now_ts=900)
        ready = main.freeze_learning_sample(self.learning_payload("807", 901, 1001), now_ts=901)
        main.collect_learning_postmatch_facts(
            waiting["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(),
        )
        main.collect_learning_postmatch_facts(
            ready["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(("api_football", "official_league")),
        )
        queue = main.learning_review_queue()
        self.assertEqual(queue["queue_count"], 2)
        self.assertEqual(queue["review_ready_count"], 1)
        self.assertEqual(queue["items"][0]["freeze_id"], ready["freeze_id"])
        self.assertTrue(queue["items"][0]["review_ready"])
        self.assertEqual(queue["items"][0]["freeze_hash"], ready["content_hash"])
        self.assertEqual(queue["items"][0]["frozen_decision"]["decision"], "PASS")
        self.assertFalse(queue["automatic_process_classification"])
        self.assertFalse(queue["result_backfit_allowed"])
        self.assertFalse(queue["automatic_champion_change"])

    def test_postmatch_review_draft_is_immutable_and_never_grades_process_from_result(self):
        frozen = main.freeze_learning_sample(self.learning_payload("draft-1", 900, 1000), now_ts=900)
        facts = self.collect_verified_learning_facts(frozen, 4, 0, 9000)
        first = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        second = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9002)

        self.assertEqual(first["action"], "drafted")
        self.assertEqual(second["action"], "unchanged")
        self.assertEqual(first["freeze_hash"], frozen["content_hash"])
        self.assertEqual(first["fact_hash"], facts["fact_hash"])
        self.assertEqual(first["suggested_process_classification"], "DATA_INSUFFICIENT")
        self.assertTrue(first["manual_review_required"])
        self.assertFalse(first["automatic_settlement_eligible"])
        self.assertFalse(first["result_outcome_used_to_grade_process"])
        self.assertFalse(first["automatic_hypothesis_registration"])
        self.assertFalse(first["automatic_champion_change"])
        store = main.load_snapshot_store()
        self.assertNotIn(frozen["freeze_id"], store.get("learning_postmatch", {}))
        self.assertNotIn("learning_hypotheses", store)

    def test_postmatch_review_draft_requires_two_source_result_verification(self):
        frozen = main.freeze_learning_sample(self.learning_payload("draft-single", 900, 1000), now_ts=900)
        main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(),
        )
        with self.assertRaises(main.HTTPException) as rejected:
            main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        self.assertEqual(rejected.exception.status_code, 409)
        self.assertEqual(rejected.exception.detail, "independent_result_verification_required_for_review_draft")

    def test_postmatch_review_draft_flags_event_pollution_without_auto_classification(self):
        frozen = main.freeze_learning_sample(self.learning_payload("draft-red", 900, 1000), now_ts=900)
        packet = self.learning_postmatch_facts(("api_football", "official_league"), 2, 1)
        packet["events"].append({"elapsed": 55, "type": "Card", "detail": "Red Card", "team": "Away"})
        packet["source_audit"].extend([
            {"source": "api_football", "component": "events", "ok": True, "evidence_ref": "test:api:events"},
            {"source": "official_league", "component": "events", "ok": True, "evidence_ref": "test:official:events"},
        ])
        main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000, fact_fetcher=lambda fixture_id: packet,
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        self.assertEqual(draft["event_evidence"]["pollution_status"], "requires_human_review")
        self.assertEqual(draft["event_evidence"]["pollution_flags"], ["red_card"])
        self.assertEqual(draft["suggested_process_classification"], "DATA_INSUFFICIENT")
        self.assertTrue(draft["manual_review_required"])

    def test_event_verification_rejects_same_domain_aliases(self):
        frozen = main.freeze_learning_sample(self.learning_payload("draft-event-alias", 900, 1000), now_ts=900)
        packet = self.learning_postmatch_facts(("api_football", "official_league"), 2, 1)
        packet["source_audit"].extend([
            {"source": "event_feed_a", "component": "events", "ok": True, "evidence_ref": "https://events.example.test/match/1"},
            {"source": "event_feed_b", "component": "events", "ok": True, "evidence_ref": "https://events.example.test/match/1/timeline"},
        ])
        main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000, fact_fetcher=lambda fixture_id: packet,
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        self.assertEqual(draft["event_evidence"]["event_source_count"], 1)
        self.assertEqual(draft["event_evidence"]["event_verification"], "single_source")
        self.assertEqual(draft["event_evidence"]["event_source_authorities"], ["events.example.test"])

    def verified_event_fact_packet(self, home_goals=3, away_goals=2):
        packet = self.learning_postmatch_facts(("api_football", "official_league"), home_goals, away_goals)
        packet["events"] = [
            {"elapsed": 12, "type": "Goal", "detail": "Normal Goal", "team": "Home"},
            {"elapsed": 40, "type": "Goal", "detail": "Normal Goal", "team": "Away"},
        ]
        packet["source_audit"].extend([
            {"source": "api_football", "component": "events", "ok": True, "evidence_ref": "test:api:events"},
            {"source": "official_league", "component": "events", "ok": True, "evidence_ref": "test:official:events"},
        ])
        return packet

    def automatic_evidence_review(self, frozen, facts, expression_status="passed"):
        freeze_ref = f"freeze:{frozen['content_hash']}"
        fact_ref = f"fact:{facts['fact_hash']}"
        review = self.learning_postmatch_review()
        statuses = {
            "match_selection_quality": "passed",
            "fundamental_chain_audit": "passed",
            "state_tree_coverage": "passed",
            "market_language_audit": "passed",
            "expression_audit": expression_status,
            "price_execution_audit": "passed",
        }
        for section, status in statuses.items():
            review[section] = {
                "status": status,
                "reason": f"Hash-bound evidence supports the {section} process assessment.",
                "evidence_refs": [fact_ref if section == "state_tree_coverage" else freeze_ref],
            }
        review["review_mode"] = "scheduled_agent"
        review["outcome_not_used_for_process_grade"] = True
        review["process_reasoning"] = "The process grade is derived from frozen inputs and hash-bound event evidence before the separately calculated selection outcome."
        return review

    def test_evidence_review_derives_process_before_selection_outcome_and_ignores_caller_class(self):
        payload = self.learning_payload("auto-review-error", 900, 1000)
        payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=900)
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.verified_event_fact_packet(3, 2),
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        completed = main.complete_learning_postmatch_review({
            "freeze_id": frozen["freeze_id"], "draft_hash": draft["draft_hash"],
            "process_classification": "PROCESS_CORRECT_RESULT_WIN",
            "review": self.automatic_evidence_review(frozen, facts, expression_status="failed"),
            "event_audit": {"status": "clean", "evidence_refs": [f"fact:{facts['fact_hash']}"]},
            "settled_at": 9002,
        }, now_ts=9002)
        self.assertEqual(completed["process_classification"], "PROCESS_ERROR_RESULT_LOSS")
        derivation = completed["event_audit"]["automatic_review_derivation"]
        self.assertEqual(derivation["process_grade"], "error")
        self.assertTrue(derivation["process_grade_derived_before_outcome"])
        self.assertEqual(derivation["selection_outcome_audit"]["outcome"], "loss")
        self.assertFalse(derivation["caller_supplied_process_classification_used"])
        self.assertFalse(derivation["result_backfit_used"])

    def test_evidence_review_can_mark_process_correct_despite_selection_loss(self):
        payload = self.learning_payload("auto-review-correct-loss", 900, 1000)
        payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=900)
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.verified_event_fact_packet(3, 2),
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        completed = main.complete_learning_postmatch_review({
            "freeze_id": frozen["freeze_id"], "draft_hash": draft["draft_hash"],
            "review": self.automatic_evidence_review(frozen, facts, expression_status="passed"),
            "event_audit": {"status": "clean", "evidence_refs": [f"draft:{draft['draft_hash']}"]},
        }, now_ts=9002)
        self.assertEqual(completed["process_classification"], "PROCESS_CORRECT_RESULT_LOSS")

    def test_automatic_evidence_review_rejects_unbound_section_assertions(self):
        frozen = main.freeze_learning_sample(self.learning_payload("auto-review-unbound", 900, 1000), now_ts=900)
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.verified_event_fact_packet(1, 1),
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        review = self.automatic_evidence_review(frozen, facts)
        review["market_language_audit"]["evidence_refs"] = ["unbound:claim"]
        with self.assertRaises(main.HTTPException) as rejected:
            main.complete_learning_postmatch_review({
                "freeze_id": frozen["freeze_id"], "draft_hash": draft["draft_hash"],
                "review": review,
                "event_audit": {"status": "clean", "evidence_refs": [f"fact:{facts['fact_hash']}"]},
            }, now_ts=9002)
        self.assertEqual(rejected.exception.status_code, 422)
        self.assertIn("market_language_audit_hash_bound_evidence_required", rejected.exception.detail["reasons"])

    def test_learning_cycle_can_complete_hash_bound_evidence_review(self):
        payload = self.learning_payload("cycle-auto-review", 900, 1000)
        payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=900)
        facts = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.verified_event_fact_packet(3, 2),
        )
        draft = main.build_learning_postmatch_review_draft(frozen["freeze_id"], now_ts=9001)
        result = main.run_learning_cycle({
            "apply": True, "run_id": "cycle-complete-review",
            "auto_collect_postmatch_facts": False,
            "review_completion_packets": [{
                "freeze_id": frozen["freeze_id"], "draft_hash": draft["draft_hash"],
                "review": self.automatic_evidence_review(frozen, facts),
                "event_audit": {"status": "clean", "evidence_refs": [f"fact:{facts['fact_hash']}"]},
            }],
        }, now_ts=9002, fixture_rows=[])
        self.assertEqual(result["settled_count"], 1)
        self.assertEqual(result["review_completion_results"][0]["action"], "settled")
        self.assertEqual(result["review_completion_results"][0]["process_classification"], "PROCESS_CORRECT_RESULT_LOSS")
        self.assertFalse(result["review_completion_results"][0]["caller_supplied_process_classification_used"])

    def test_learning_cycle_finishes_postmatch_evidence_before_future_prematch(self):
        frozen = main.freeze_learning_sample(self.learning_payload("order-past", 900, 1000), now_ts=900)
        future = self.learning_fixture_row(809, 39, 10000)
        calls = []

        def fetcher(fixture_id):
            calls.append("postmatch")
            return self.learning_postmatch_facts(("api_football", "official_league"))

        def builder(fixture_id):
            calls.append("prematch")
            return self.learning_prematch_packet(fixture_id, 9000)

        result = main.run_learning_cycle(
            {"apply": True, "run_id": "ordered-cycle"}, now_ts=9000,
            fixture_rows=[future], prematch_packet_builder=builder,
            postmatch_fact_fetcher=fetcher,
        )
        self.assertEqual(calls, ["postmatch", "prematch"])
        self.assertEqual(result["execution_order"], ["past_36h_postmatch", "future_24h_prematch"])
        self.assertEqual(result["postmatch_review_draft_results"][0]["action"], "drafted")
        self.assertEqual(result["review_draft_count"], 1)
        self.assertEqual(result["frozen_count"], 1)
        self.assertNotIn(frozen["freeze_id"], main.load_snapshot_store().get("learning_postmatch", {}))

    def test_learning_plan_has_no_daily_match_or_historical_sample_limit(self):
        now_ts = 100000
        fixtures = [self.learning_fixture_row(900 + index, 39, now_ts + 3600) for index in range(12)]
        plan = main.learning_cycle_plan(now_ts=now_ts, fixture_rows=fixtures)
        self.assertEqual(plan["discovery"]["candidate_count"], 12)
        self.assertIsNone(plan["match_limit"])
        self.assertIsNone(plan["daily_freeze_cap"])
        self.assertIsNone(plan["full_historical_odds_sample_limit"])
        self.assertIsNone(plan["remaining_freeze_capacity"])

    def test_settlement_cannot_bypass_latest_verified_fact_packet(self):
        frozen = main.freeze_learning_sample(self.learning_payload("808", 900, 1000), now_ts=900)
        result = {"status": "FT", "home_goals": 2, "away_goals": 1}
        with self.assertRaises(main.HTTPException) as no_facts:
            main.settle_learning_sample(
                frozen["freeze_id"], result, "PROCESS_CORRECT_RESULT_WIN",
                {"status": "clean"}, 9000, self.learning_postmatch_review(), "missing-hash",
            )
        self.assertEqual(no_facts.exception.detail, "verified_postmatch_fact_packet_required")

        single = main.collect_learning_postmatch_facts(
            frozen["freeze_id"], now_ts=9000,
            fact_fetcher=lambda fixture_id: self.learning_postmatch_facts(),
        )
        with self.assertRaises(main.HTTPException) as one_source:
            main.settle_learning_sample(
                frozen["freeze_id"], result, "PROCESS_CORRECT_RESULT_WIN",
                {"status": "clean"}, 9000, self.learning_postmatch_review(), single["fact_hash"],
            )
        self.assertEqual(one_source.exception.detail, "independent_result_verification_required")

        verified = self.collect_verified_learning_facts(frozen, 2, 1, 9001)
        with self.assertRaises(main.HTTPException) as stale_hash:
            main.settle_learning_sample(
                frozen["freeze_id"], result, "PROCESS_CORRECT_RESULT_WIN",
                {"status": "clean"}, 9001, self.learning_postmatch_review(), single["fact_hash"],
            )
        self.assertEqual(stale_hash.exception.detail, "latest_verified_fact_hash_required")
        with self.assertRaises(main.HTTPException) as wrong_result:
            main.settle_learning_sample(
                frozen["freeze_id"], {"status": "FT", "home_goals": 3, "away_goals": 1},
                "PROCESS_CORRECT_RESULT_WIN", {"status": "clean"}, 9001,
                self.learning_postmatch_review(), verified["fact_hash"],
            )
        self.assertEqual(wrong_result.exception.detail, "submitted_result_does_not_match_verified_facts")

    def test_learning_cycle_rejects_apply_without_safe_run_id(self):
        with self.assertRaises(main.HTTPException) as missing:
            main.run_learning_cycle({"apply": True}, now_ts=100000, fixture_rows=[])
        self.assertEqual(missing.exception.status_code, 400)
        with self.assertRaises(main.HTTPException):
            main.run_learning_cycle({"apply": True, "run_id": "unsafe/id"}, now_ts=100000, fixture_rows=[])

    def settle_selection_quality_sample(self, fixture, process_class, expression_status="passed", market_rating=None, match_status="passed"):
        payload = self.learning_payload(fixture, 900, 1000)
        payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        if market_rating is not None:
            payload["decision"]["market_rating"] = market_rating
        frozen = main.freeze_learning_sample(payload, now_ts=900)
        facts = self.collect_verified_learning_facts(frozen, 3, 2, 8200)
        review = self.learning_postmatch_review()
        review["match_selection_quality"] = {"status": match_status}
        review["expression_audit"] = {"status": expression_status}
        review["price_execution_audit"] = {"status": "passed"}
        return main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 3, "away_goals": 2},
            process_class, {"status": "clean"}, 8200, review, facts["fact_hash"],
        )

    def test_selection_quality_cards_use_process_not_result_and_never_auto_optimize(self):
        self.settle_selection_quality_sample("quality-correct", "PROCESS_CORRECT_RESULT_LOSS", expression_status="failed")
        self.settle_selection_quality_sample("quality-error", "PROCESS_ERROR_RESULT_WIN", expression_status="passed")
        self.settle_selection_quality_sample("quality-event", "EVENT_CONTAMINATED", expression_status="failed")
        report = main.learning_selection_quality_report(minimum_samples=2)
        self.assertEqual(report["card_count"], 1)
        card = report["cards"][0]
        self.assertEqual(card["eligible_sample_count"], 2)
        self.assertEqual(card["process_correct_count"], 1)
        self.assertEqual(card["process_error_count"], 1)
        self.assertEqual(card["rates"]["process_accuracy"], 0.5)
        self.assertEqual(card["rates"]["expression_failure_rate"], 0.5)
        self.assertTrue(card["sample_ready"])
        self.assertEqual(card["research_signal"], "HYPOTHESIS_ONLY_REVIEW_ALLOWED")
        self.assertFalse(card["result_outcome_used_for_optimization"])
        self.assertFalse(card["automatic_weight_change"])
        self.assertFalse(card["champion_effect"])
        self.assertEqual(report["excluded_counts"]["event_contaminated"], 1)
        self.assertFalse(report["automatic_hypothesis_registration"])
        self.assertFalse(report["automatic_champion_change"])

    def test_selection_quality_counts_latest_version_per_fixture_only(self):
        self.settle_selection_quality_sample("quality-versioned", "PROCESS_CORRECT_RESULT_LOSS", expression_status="failed")
        payload = self.learning_payload(
            "quality-versioned", 950, 1000,
            analysis={"fundamental_chain": {"status": "complete"}, "revision": 2},
        )
        payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        latest = main.freeze_learning_sample(payload, now_ts=950)
        facts = self.collect_verified_learning_facts(latest, 3, 2, 8201)
        review = self.learning_postmatch_review()
        review["match_selection_quality"] = {"status": "passed"}
        review["expression_audit"] = {"status": "failed"}
        review["price_execution_audit"] = {"status": "passed"}
        main.settle_learning_sample(
            latest["freeze_id"], {"status": "FT", "home_goals": 3, "away_goals": 2},
            "PROCESS_CORRECT_RESULT_WIN", {"status": "clean"}, 8201, review, facts["fact_hash"],
        )

        report = main.learning_selection_quality_report(minimum_samples=2)
        self.assertEqual(report["cards"][0]["eligible_sample_count"], 1)
        self.assertEqual(report["cards"][0]["fixture_ids"], ["quality-versioned"])
        self.assertEqual(report["excluded_counts"]["superseded_freeze_version"], 1)

    def test_learning_quality_cards_are_hash_bound_immutable_and_result_independent(self):
        self.settle_selection_quality_sample(
            "card-result-win", "PROCESS_CORRECT_RESULT_WIN", expression_status="failed", market_rating="B+",
        )
        self.settle_selection_quality_sample(
            "card-result-loss", "PROCESS_CORRECT_RESULT_LOSS", expression_status="failed", market_rating="B+",
        )
        first = main.refresh_learning_quality_cards(now_ts=9000)
        second = main.refresh_learning_quality_cards(now_ts=9001)
        self.assertEqual(first["created_count"], 2)
        self.assertEqual(second["created_count"], 0)
        cards = main.load_snapshot_store()["learning_quality_cards"]
        win_card = cards["card-result-win:v1"]
        loss_card = cards["card-result-loss:v1"]
        self.assertEqual(win_card["priority_quality"]["label"], loss_card["priority_quality"]["label"])
        self.assertEqual(win_card["selection_quality"]["label"], loss_card["selection_quality"]["label"])
        self.assertEqual(win_card["selection_quality"]["label"], "failed")
        self.assertNotEqual(win_card["outcome_context"]["process_classification"], loss_card["outcome_context"]["process_classification"])
        self.assertFalse(win_card["outcome_context"]["final_score_copied_into_card"])
        self.assertEqual(win_card["freeze_hash"], main._learning_freeze_by_id(main.load_snapshot_store(), "card-result-win:v1")["content_hash"])
        self.assertFalse(first["result_outcome_used_for_labels"])
        self.assertFalse(first["automatic_champion_change"])

    def test_quality_calibration_uses_explicit_ratings_and_process_labels_only(self):
        self.settle_selection_quality_sample(
            "calibration-pass", "PROCESS_CORRECT_RESULT_LOSS", market_rating="A", match_status="passed",
        )
        self.settle_selection_quality_sample(
            "calibration-fail", "PROCESS_ERROR_RESULT_WIN", market_rating="A", match_status="failed",
        )
        self.settle_selection_quality_sample(
            "calibration-no-market-rating", "PROCESS_CORRECT_RESULT_WIN", match_status="passed",
        )
        main.refresh_learning_quality_cards(now_ts=9000)
        report = main.learning_quality_calibration_report(minimum_samples=2)
        priority = next(row for row in report["groups"] if row["dimension"] == "priority_quality")
        selection = next(row for row in report["groups"] if row["dimension"] == "selection_quality")
        self.assertEqual(priority["eligible_sample_count"], 3)
        self.assertEqual(priority["observed_process_pass_rate"], 0.666667)
        self.assertEqual(selection["eligible_sample_count"], 2)
        self.assertEqual(selection["observed_process_pass_rate"], 1.0)
        self.assertTrue(selection["sample_ready"])
        self.assertEqual(report["excluded_counts"]["selection_rating_missing"], 1)
        self.assertFalse(report["result_outcome_used_for_calibration"])
        self.assertFalse(report["automatic_weight_change"])

    def test_quality_calibration_counts_only_latest_freeze_version(self):
        self.settle_selection_quality_sample(
            "quality-card-versioned", "PROCESS_CORRECT_RESULT_LOSS", market_rating="B",
        )
        payload = self.learning_payload(
            "quality-card-versioned", 950, 1000,
            analysis={"fundamental_chain": {"status": "complete"}, "revision": 2},
        )
        payload["decision"] = {
            "decision": "BET", "match_rating": "B", "market_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        latest = main.freeze_learning_sample(payload, now_ts=950)
        facts = self.collect_verified_learning_facts(latest, 3, 2, 8201)
        review = self.learning_postmatch_review()
        review["match_selection_quality"] = {"status": "failed"}
        review["expression_audit"] = {"status": "failed"}
        review["price_execution_audit"] = {"status": "passed"}
        main.settle_learning_sample(
            latest["freeze_id"], {"status": "FT", "home_goals": 3, "away_goals": 2},
            "PROCESS_ERROR_RESULT_WIN", {"status": "clean"}, 8201, review, facts["fact_hash"],
        )
        main.refresh_learning_quality_cards(now_ts=9000)
        report = main.learning_quality_calibration_report(minimum_samples=2)
        self.assertEqual(report["stored_card_count"], 2)
        self.assertEqual(report["latest_fixture_card_count"], 1)
        self.assertEqual(report["excluded_counts"]["superseded_freeze_version"], 1)
        self.assertTrue(all(row["eligible_sample_count"] == 1 for row in report["groups"]))

    def test_quality_card_without_outcome_independence_attestation_is_audit_only(self):
        payload = self.learning_payload("quality-unattested", 900, 1000)
        payload["decision"] = {
            "decision": "BET", "match_rating": "A", "market_rating": "A",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        frozen = main.freeze_learning_sample(payload, now_ts=900)
        facts = self.collect_verified_learning_facts(frozen, 3, 2, 8200)
        review = self.learning_postmatch_review()
        review.pop("outcome_not_used_for_process_grade")
        review["match_selection_quality"] = {"status": "passed"}
        review["expression_audit"] = {"status": "passed"}
        review["price_execution_audit"] = {"status": "passed"}
        main.settle_learning_sample(
            frozen["freeze_id"], {"status": "FT", "home_goals": 3, "away_goals": 2},
            "PROCESS_CORRECT_RESULT_WIN", {"status": "clean"}, 8200, review, facts["fact_hash"],
        )
        main.refresh_learning_quality_cards(now_ts=9000)
        card = main.load_snapshot_store()["learning_quality_cards"][frozen["freeze_id"]]
        self.assertFalse(card["sample_eligibility"]["eligible"])
        self.assertEqual(card["sample_eligibility"]["reason"], "outcome_independence_attestation_missing")
        report = main.learning_quality_calibration_report(minimum_samples=2)
        self.assertEqual(report["calibration_group_count"], 0)
        self.assertEqual(report["excluded_counts"]["sample_ineligible"], 1)

    def test_single_match_failure_cannot_create_research_proposal(self):
        self.settle_selection_quality_sample("proposal-single", "PROCESS_CORRECT_RESULT_WIN", expression_status="failed")
        derived = main.learning_research_proposal_candidates(minimum_matches=3)
        self.assertEqual(derived["candidate_count"], 0)
        self.assertFalse(derived["automatic_hypothesis_registration"])
        self.assertNotIn("learning_research_proposals", main.load_snapshot_store())
        self.assertNotIn("learning_hypotheses", main.load_snapshot_store())

    def test_repeated_multi_match_failure_creates_immutable_proposal_only(self):
        for index in range(3):
            self.settle_selection_quality_sample(
                f"proposal-repeat-{index}", "PROCESS_CORRECT_RESULT_LOSS", expression_status="failed",
            )
        derived = main.learning_research_proposal_candidates(minimum_matches=3)
        self.assertEqual(derived["candidate_count"], 1)
        candidate = derived["candidates"][0]
        self.assertEqual(candidate["signal_dimension"], "expression")
        self.assertEqual(candidate["evidence"]["eligible_distinct_match_count"], 3)
        self.assertEqual(candidate["evidence"]["failure_count"], 3)
        self.assertFalse(candidate["evidence"]["result_outcome_used"])
        self.assertEqual(candidate["causal_claim_status"], "not_formulated")
        self.assertFalse(candidate["automatic_hypothesis_registration"])
        self.assertFalse(candidate["automatic_model_effect"])
        self.assertFalse(candidate["champion_effect"])

        first = main.refresh_learning_research_proposals(now_ts=9000, minimum_matches=3)
        second = main.refresh_learning_research_proposals(now_ts=9001, minimum_matches=3)
        self.assertEqual(first["new_proposal_version_count"], 1)
        self.assertEqual(first["results"][0]["action"], "proposed")
        self.assertEqual(second["new_proposal_version_count"], 0)
        self.assertEqual(second["results"][0]["action"], "unchanged")
        self.assertEqual(main.learning_research_proposal_report()["proposal_count"], 1)
        store = main.load_snapshot_store()
        self.assertNotIn("learning_hypotheses", store)
        self.assertNotIn("learning_promotions", store)

    def test_learning_cycle_refreshes_repeated_signal_before_future_discovery(self):
        for index in range(3):
            self.settle_selection_quality_sample(
                f"cycle-proposal-{index}", "PROCESS_CORRECT_RESULT_WIN", expression_status="failed",
            )
        with patch.object(main, "LEARNING_RESEARCH_PROPOSAL_MIN_MATCHES", 3):
            result = main.run_learning_cycle(
                {"apply": True, "run_id": "proposal-cycle"}, now_ts=9000, fixture_rows=[],
            )
        self.assertEqual(result["execution_order"], ["past_36h_postmatch", "future_24h_prematch"])
        self.assertEqual(result["research_proposal_version_count"], 1)
        self.assertEqual(result["research_proposal_results"][0]["action"], "proposed")
        self.assertFalse(result["automatic_hypothesis_registration"])
        self.assertNotIn("learning_hypotheses", main.load_snapshot_store())

    def create_expression_research_proposal(self):
        for index in range(3):
            self.settle_selection_quality_sample(
                f"proposal-source-{index}", "PROCESS_CORRECT_RESULT_LOSS", expression_status="failed",
            )
        refreshed = main.refresh_learning_research_proposals(now_ts=8400, minimum_matches=3)
        return refreshed["results"][0]

    def test_proposal_sourced_hypothesis_binds_latest_hash_and_all_discovery_samples(self):
        proposal = self.create_expression_research_proposal()
        base = {
            "hypothesis_id": "proposal-hyp", "type": "HYPOTHESIS_ONLY",
            "title": "Expression failure challenger",
            "definition": "A preregistered challenger will test a lower-condition expression gate.",
            "applicable_scope": "England Premier League over_under",
            "expected_direction": "lower expression audit failure without worse tail risk",
            "failure_conditions": "no out-of-sample process gain or worse tail risk",
            "falsification_criteria": "any unresolved counterexample or failed promotion gate",
            "discovery_freeze_ids": proposal["evidence"]["supporting_freeze_ids"],
            "source_proposal_id": proposal["proposal_id"],
            "source_proposal_hash": proposal["proposal_hash"],
            "validation_plan": {
                "minimum_samples": main.LEARNING_MIN_VALIDATION_SAMPLES,
                "structured_scope": {"competition_ids": [39], "markets": ["over_under"]},
                "ablation_plan": self.learning_ablation_plan(),
            },
        }
        stale = {**base, "source_proposal_hash": "stale"}
        with self.assertRaises(main.HTTPException) as stale_rejected:
            main.register_learning_hypothesis(stale)
        self.assertEqual(stale_rejected.exception.detail, "latest_source_research_proposal_hash_required")

        partial = {**base, "discovery_freeze_ids": base["discovery_freeze_ids"][:-1]}
        with self.assertRaises(main.HTTPException) as partial_rejected:
            main.register_learning_hypothesis(partial)
        self.assertEqual(partial_rejected.exception.detail, "hypothesis_must_bind_all_and_only_supporting_proposal_samples")

        with patch.object(main.time, "time", return_value=8500):
            hypothesis = main.register_learning_hypothesis(base)
        self.assertEqual(hypothesis["source_research_proposal"]["proposal_id"], proposal["proposal_id"])
        self.assertEqual(hypothesis["source_research_proposal"]["proposal_hash"], proposal["proposal_hash"])
        self.assertFalse(hypothesis["champion_effect"])

    def test_forward_validation_queue_requires_new_distinct_matching_frozen_match(self):
        proposal = self.create_expression_research_proposal()
        with patch.object(main.time, "time", return_value=8500):
            main.register_learning_hypothesis({
                "hypothesis_id": "forward-hyp", "type": "HYPOTHESIS_ONLY",
                "title": "Forward expression challenger",
                "definition": "Test a preregistered lower-condition expression gate.",
                "applicable_scope": "England Premier League over_under",
                "expected_direction": "lower expression failures",
                "failure_conditions": "no independent gain",
                "falsification_criteria": "challenger fails any derived gate",
                "discovery_freeze_ids": proposal["evidence"]["supporting_freeze_ids"],
                "source_proposal_id": proposal["proposal_id"],
                "source_proposal_hash": proposal["proposal_hash"],
                "validation_plan": {
                    "minimum_samples": main.LEARNING_MIN_VALIDATION_SAMPLES,
                    "structured_scope": {"competition_ids": [39], "markets": ["over_under"]},
                    "ablation_plan": self.learning_ablation_plan(),
                },
            })

        future_payload = self.learning_payload("forward-fixture", 8600, 10000)
        future_payload["decision"] = {
            "decision": "BET", "match_rating": "B",
            "selected_expression": {"market": "over_under", "selection": "under", "line": 2.75, "price": 1.91},
        }
        frozen = main.freeze_learning_sample(future_payload, now_ts=8600)
        queue = main.learning_forward_validation_queue(now_ts=8700)
        self.assertEqual(queue["queue_count"], 1)
        self.assertEqual(queue["items"][0]["freeze_id"], frozen["freeze_id"])
        self.assertFalse(queue["items"][0]["automatic_shadow_lock"])

        lock_payload = {
            "freeze_id": frozen["freeze_id"],
            "champion_probabilities": {"home": 0.45, "draw": 0.25, "away": 0.30},
            "challenger_probabilities": {"home": 0.30, "draw": 0.45, "away": 0.25},
            "module_ablation_outputs": self.module_ablation_outputs(8650),
            "selected_expression": {"market": "over_under", "selection": "under", "entry_decimal_price": 1.91, "entry_price_evidence_ref": "test:entry:forward"},
            "risk": {"champion_tail_risk": 0.10, "challenger_tail_risk": 0.10},
        }
        main.lock_hypothesis_shadow_prediction("forward-hyp", lock_payload, now_ts=8700)
        self.assertEqual(main.learning_forward_validation_queue(now_ts=8750)["queue_count"], 0)

        later_payload = self.learning_payload(
            "forward-fixture", 8800, 10000,
            analysis={"fundamental_chain": {"status": "complete"}, "revision": 2},
        )
        later_payload["decision"] = future_payload["decision"]
        later = main.freeze_learning_sample(later_payload, now_ts=8800)
        with self.assertRaises(main.HTTPException) as duplicate_fixture:
            main.lock_hypothesis_shadow_prediction(
                "forward-hyp", {
                    **lock_payload, "freeze_id": later["freeze_id"],
                    "module_ablation_outputs": self.module_ablation_outputs(8805),
                }, now_ts=8810,
            )
        self.assertEqual(duplicate_fixture.exception.detail, "validation_fixture_already_locked_for_hypothesis")


if __name__ == "__main__":
    unittest.main()
