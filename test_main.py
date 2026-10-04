import os
import tempfile
import unittest
from unittest.mock import patch

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

    def test_complete_timeline_contains_opening(self):
        self.assertEqual(main.PREMATCH_STAGE_ORDER, ["Opening", "T-24h", "T-12h", "T-6h", "T-3h", "T-1h", "T-15m", "Closing"])

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
        first = {"fixture": 92, "stage": "T-15m", "snapshot_at": 100, "market_snapshot": market, "team_news_snapshot": old_news, "stage_timing_audit": {"status": "valid"}}
        second = {"fixture": 92, "stage": "T-15m", "snapshot_at": 200, "market_snapshot": market, "team_news_snapshot": new_news, "stage_timing_audit": {"status": "valid"}}
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
        later = {"stage": "T-12h", "snapshot_at": 2, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared"}}
        complete = main.audit_line_movement_timeline([opening, later])
        self.assertTrue(complete["decision_eligible"])
        self.assertEqual(complete["available_stage_count"], 2)
        self.assertFalse(complete["current_odds_used_as_history"])

    def test_latest_prematch_snapshot_prefers_later_stage_over_write_time(self):
        rows = [
            {"stage": "Opening", "snapshot_at": 9999},
            {"stage": "T-3h", "snapshot_at": 100},
            {"stage": "T-15m", "snapshot_at": 200},
        ]
        self.assertEqual(main.latest_prematch_snapshot(rows)["stage"], "T-15m")
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
        audit = main.audit_stage_timing("T-15m", 1000, None)
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
        previous = {"script": {"content_hash": "old"}}
        script = {"content_hash": "new"}
        dynamics = {"cross_market_divergence": True, "revalidation_trigger": {"triggered": True}}
        result = main.classify_market_move_details(dynamics, previous, script, model_market_divergence=True)
        self.assertEqual(result["classification"], "Fundamental Confirmed")
        self.assertEqual(result["matched_classifications"], ["Fundamental Confirmed", "Model-Market Divergence", "Cross-Market Divergence"])
        self.assertIn("Cross-Market Divergence", result["classification_bases"])

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
            {"stage": "T-3h", "snapshot_at": 200, "import_status": "available", "market_snapshot": {"available": True}, "market_dynamics": {"comparison_status": "compared"}},
        ]
        gated = main.apply_line_movement_gate(decision, history)
        self.assertEqual(gated["decision"], "home")
        self.assertTrue(gated["line_movement_audit"]["decision_eligible"])

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
        snapshot = main.empty_market_snapshot()
        snapshot["consensus_main_line"]["1x2"] = consensus
        result = main.decision_layer(snapshot, {"home": .55, "draw": .25, "away": .20}, {"home": .8, "draw": .3, "away": .2}, .4, .9, [])
        self.assertEqual(result["decision"], "PASS")
        self.assertIn("consensus_bookmaker_coverage_below_minimum", result["pass_reasons"])

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
        third = main.save_portfolio_run("test-set", ["a", "b"], changed_portfolio, 3, ["T-15m"])
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
        return {"schema_version": "shadow_prematch_packet_v1", "league": "UEFA Nations League", "match": {"match_id": "uuid-1", "home_team_name": "Home", "away_team_name": "Away"}, "required_timeline": main.PREMATCH_STAGE_ORDER, "timeline": [row, {"stage": "T-15m", "status": "data_missing", "reason": "not captured"}], "lineup_history": [{"observed_at": "2026-09-29T00:00:00+00:00", "status": "official"}], "data_quality": {"level": "partial"}}

    def test_imported_packet_preserves_uuid_arrays_and_missing_stage(self):
        result = main.import_prematch_packet(self.prematch_packet())
        self.assertEqual(result["fixture"], "uuid-1")
        rows = main.get_fixture_snapshots("uuid-1")
        self.assertEqual(len(rows), 2)
        opening = rows[0]
        self.assertTrue(opening["opening_source_audit"]["verified"])
        self.assertEqual(opening["market_snapshot"]["primary"]["asian_handicap"]["line"], -0.25)
        self.assertEqual(opening["market_snapshot"]["primary"]["asian_handicap"]["source"], "upstream_consensus_fallback")
        self.assertEqual(opening["market_snapshot"]["primary"]["home_team_total"]["line"], 2.5)
        self.assertEqual(opening["market_snapshot"]["primary"]["1x2"]["source"], "complete_company_array")
        self.assertEqual(opening["market_snapshot"]["consensus_audit"]["1x2"], "recalculated_from_company_array")
        self.assertEqual(len(opening["market_snapshot"]["markets"]["home_team_total"][0]["lines"]), 1)
        self.assertEqual(opening["market_snapshot"]["markets"]["1x2"][0]["home"], 2.0)
        missing = [x for x in rows if x["stage"] == "T-15m"][0]
        self.assertEqual(missing["import_status"], "data_missing")
        packet = main.build_imported_ai_packet("uuid-1")
        self.assertIn("T-15m", packet["market"]["missing_stages"])
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

    def test_imported_ai_packet_excludes_invalid_latest_node(self):
        packet = self.prematch_packet()
        invalid_late = dict(packet["timeline"][0])
        invalid_late["stage"] = "T-15m"
        invalid_late["latest_observed_at"] = "2030-01-01T00:00:00+00:00"
        packet["timeline"] = [packet["timeline"][0], invalid_late]
        main.import_prematch_packet(packet)
        store = main.load_snapshot_store()
        late = next(row for row in store["fixtures"]["uuid-1"] if row["stage"] == "T-15m")
        late["stage_timing_audit"] = {"status": "invalid", "decision_eligible": False}
        main.write_snapshot_store(store)
        result = main.build_imported_ai_packet("uuid-1")
        self.assertEqual(result["market"]["available_prematch_stage_count"], 1)
        self.assertIn("T-15m", result["market"]["missing_stages"])
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
            {"task_id": "late", "status": "pending", "stage": "T-15m", "created_at": 7000, "reasons": ["cross_market_divergence"]},
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
        main.resolve_revalidation_tasks("fixture-1", {"version_number": 2, "changed_information": []}, model_market_divergence=True)
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["resolution_classification"], "Model-Market Divergence")

    def test_revalidation_resolution_does_not_close_later_stage_tasks(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "early": {"task_id": "early", "fixture": "fixture-1", "stage": "T-6h", "status": "pending"},
            "current": {"task_id": "current", "fixture": "fixture-1", "stage": "T-3h", "status": "pending"},
            "later": {"task_id": "later", "fixture": "fixture-1", "stage": "T-15m", "status": "pending"},
        }})
        resolved = main.resolve_revalidation_tasks("fixture-1", {"version_number": 2, "changed_information": []}, resolved_through_stage="T-3h")
        self.assertEqual(resolved, 2)
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertEqual(queue["early"]["status"], "revalidated")
        self.assertEqual(queue["current"]["status"], "revalidated")
        self.assertEqual(queue["later"]["status"], "pending")

    def test_incomplete_revalidation_attempt_stays_pending_with_missing_evidence(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "stage": "T-3h", "status": "pending", "created_at": 1},
            "later": {"task_id": "later", "fixture": "fixture-1", "stage": "T-15m", "status": "pending", "created_at": 1},
        }})
        audit = {"critical_missing": ["result_utility"], "critical_provenance_missing": ["rotation_quality"], "critical_semantic_issues": {}, "critical_structural_issues": {}}
        recorded = main.record_incomplete_revalidation_attempt("fixture-1", audit, "T-3h")
        queue = main.load_snapshot_store()["fundamental_revalidation_queue"]
        self.assertEqual(recorded, 1)
        self.assertEqual(queue["task"]["status"], "pending")
        self.assertEqual(queue["task"]["attempt_count"], 1)
        self.assertEqual(queue["task"]["required_evidence"], ["result_utility", "rotation_quality"])
        self.assertNotIn("last_attempt_at", queue["later"])

    def test_first_revalidation_version_does_not_claim_fundamental_change(self):
        main.write_snapshot_store({"version": main.VERSION, "fixtures": {}, "fundamental_revalidation_queue": {
            "task": {"task_id": "task", "fixture": "fixture-1", "status": "pending"}
        }})
        main.resolve_revalidation_tasks("fixture-1", {"version_number": 1, "changed_information": main.FUNDAMENTAL_CHAIN})
        task = main.load_snapshot_store()["fundamental_revalidation_queue"]["task"]
        self.assertEqual(task["resolution_classification"], "Market-Only Move")
        self.assertFalse(task["fundamental_changed"])

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

    def test_freshness_uses_latest_stage_and_rejects_future_timestamp(self):
        metadata = {"match": {"kickoff_utc": "2030-01-01T00:00:00+00:00"}}
        history = [
            {"stage": "Opening", "import_status": "available", "snapshot_at": 9900},
            {"stage": "T-1h", "import_status": "available", "snapshot_at": 1000},
        ]
        stale = main.imported_fixture_freshness(metadata, history, now_ts=10000)
        self.assertEqual(stale["latest_stage"], "T-1h")
        self.assertEqual(stale["state"], "stale")
        future = main.imported_fixture_freshness(metadata, [{"stage": "T-15m", "import_status": "available", "snapshot_at": 10401}], now_ts=10000)
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


if __name__ == "__main__":
    unittest.main()
