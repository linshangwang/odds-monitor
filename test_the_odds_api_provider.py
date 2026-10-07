import os
import tempfile
import unittest
from datetime import datetime, timezone

import main
import the_odds_api_provider as provider


KICKOFF = int(datetime(2026, 10, 7, 16, 0, tzinfo=timezone.utc).timestamp())
OPENING = KICKOFF - 36 * 3600


def event_payload(book_count=5):
    bookmakers = []
    for index in range(book_count):
        markets = [{
            "key": "h2h", "last_update": provider.iso_utc(KICKOFF - 3600),
            "outcomes": [
                {"name": "IF Gnistan", "price": 3.70 + index * 0.01},
                {"name": "Draw", "price": 3.60 + index * 0.01},
                {"name": "Inter Turku", "price": 1.90 + index * 0.01},
            ],
        }]
        if index < 3:
            markets.extend([
                {
                    "key": "spreads", "last_update": provider.iso_utc(KICKOFF - 3600),
                    "outcomes": [
                        {"name": "IF Gnistan", "price": 1.80 + index * 0.01, "point": 0.75},
                        {"name": "Inter Turku", "price": 2.00 + index * 0.01, "point": -0.75},
                    ],
                },
                {
                    "key": "totals", "last_update": provider.iso_utc(KICKOFF - 3600),
                    "outcomes": [
                        {"name": "Over", "price": 1.87 + index * 0.01, "point": 2.75},
                        {"name": "Under", "price": 1.92 + index * 0.01, "point": 2.75},
                    ],
                },
            ])
        bookmakers.append({"key": f"book_{index}", "title": f"Book {index}", "markets": markets})
    return {
        "id": "event-gnistan-inter", "sport_key": "soccer_finland_veikkausliiga",
        "commence_time": provider.iso_utc(KICKOFF), "home_team": "IF Gnistan", "away_team": "Inter Turku",
        "bookmakers": bookmakers,
    }


def historical_api(path, params):
    requested = provider.parse_timestamp(params["date"])
    if path.endswith("/events"):
        data = [event_payload()] if requested >= OPENING else []
    else:
        data = event_payload()
    return {
        "ok": True, "status_code": 200,
        "data": {"timestamp": provider.iso_utc(requested), "data": data},
        "quota_remaining": "999", "quota_used": "1", "quota_last": "1",
    }


class TheOddsApiProviderTests(unittest.TestCase):
    def test_company_quotes_preserve_handicap_orientation_and_coverage(self):
        normalized = provider.normalize_event_quotes(
            event_payload(), KICKOFF - 3600, KICKOFF, "IF Gnistan", "Inter Turku"
        )
        coverage = provider.company_coverage(normalized["quotes"])
        self.assertEqual(coverage, {"1x2": 5, "asian_handicap": 3, "over_under": 3})
        ah = [row for row in normalized["quotes"] if row["market"] == "asian_handicap"]
        self.assertTrue(ah)
        self.assertTrue(all(row["line"] == 0.75 for row in ah))
        self.assertEqual({row["selection"] for row in ah[:2]}, {"home", "away"})

    def test_collects_verified_eight_node_timeline(self):
        result = provider.collect_historical_timeline(
            historical_api,
            fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
            home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
            regions="fi,eu", opening_lookback_days=2, opening_scan_hours=12,
            minimums={"1x2": 5, "asian_handicap": 3, "over_under": 3},
        )
        self.assertTrue(result["ok"])
        self.assertTrue(result["opening_discovery"]["verified"])
        timeline = result["packet"]["timeline"]
        self.assertEqual([row["stage"] for row in timeline], list(provider.PREMATCH_STAGES))
        self.assertTrue(all(row["status"] == "available" for row in timeline))
        self.assertTrue(all(row["provider_audit"]["coverage"]["primary_reference_eligible"] for row in timeline))
        self.assertEqual(result["packet"]["source"], "the_odds_api")
        self.assertLessEqual(result["request_count"], 48)

    def test_opening_is_missing_when_first_seen_is_left_censored(self):
        result = provider.collect_historical_timeline(
            historical_api,
            fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
            home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
            regions="fi", opening_lookback_days=1, opening_scan_hours=12,
            minimums={"1x2": 5, "asian_handicap": 3, "over_under": 3},
        )
        opening = result["packet"]["timeline"][0]
        self.assertFalse(result["opening_discovery"]["verified"])
        self.assertEqual(opening["status"], "data_missing")
        self.assertIn("left_censored", opening["reason"])

    def test_provider_discovery_error_fails_fast_without_retries(self):
        calls = []

        def failed_api(path, params):
            calls.append((path, params))
            return {"ok": False, "status_code": 401, "error": "unauthorized"}

        result = provider.collect_historical_timeline(
            failed_api,
            fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
            home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
            opening_lookback_days=2,
        )
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "unauthorized")
        self.assertEqual(len(calls), 1)

    def test_incremental_known_event_fetches_only_requested_nodes(self):
        calls = []

        def audited_api(path, params):
            calls.append((path, params))
            return historical_api(path, params)

        result = provider.collect_historical_timeline(
            audited_api,
            fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
            home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
            known_event_id="event-gnistan-inter", requested_stages=["T-6h", "T-1h"],
            minimums={"1x2": 5, "asian_handicap": 3, "over_under": 3},
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["requested_stages"], ["T-6h", "T-1h"])
        self.assertEqual([row["stage"] for row in result["packet"]["timeline"]], ["T-6h", "T-1h"])
        self.assertEqual(result["request_count"], 2)
        self.assertTrue(all("/odds" in path for path, _ in calls))
        self.assertFalse(result["opening_discovery"]["attempted"])

    def test_incremental_unknown_event_uses_one_discovery_call(self):
        calls = []

        def audited_api(path, params):
            calls.append((path, params))
            return historical_api(path, params)

        result = provider.collect_historical_timeline(
            audited_api,
            fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
            home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
            requested_stages=["T-3h"],
        )
        self.assertTrue(result["ok"])
        self.assertEqual(result["request_count"], 2)
        self.assertTrue(calls[0][0].endswith("/events"))
        self.assertIn("/odds", calls[1][0])

    def test_incremental_rejects_invalid_stage(self):
        with self.assertRaisesRegex(ValueError, "invalid_requested_stage"):
            provider.collect_historical_timeline(
                historical_api,
                fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
                home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
                requested_stages=["T-15m"],
            )

    def test_import_preserves_provider_provenance_and_primary_gate(self):
        old_store = main.SNAPSHOT_STORE_PATH
        with tempfile.TemporaryDirectory() as directory:
            main.SNAPSHOT_STORE_PATH = os.path.join(directory, "store.json")
            try:
                result = provider.collect_historical_timeline(
                    historical_api,
                    fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
                    home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
                    regions="fi,eu", opening_lookback_days=2, opening_scan_hours=12,
                    minimums={"1x2": 5, "asian_handicap": 3, "over_under": 3},
                )
                imported = main.import_prematch_packet(result["packet"])
                self.assertTrue(imported["changed"])
                store = main.load_snapshot_store()
                metadata = store["external_prematch"]["fin-1"]
                self.assertEqual(metadata["source"], "the_odds_api")
                self.assertEqual(metadata["provider_fixture_ids"]["the_odds_api"], "event-gnistan-inter")
                opening = next(row for row in store["fixtures"]["fin-1"] if row["stage"] == "Opening")
                self.assertTrue(opening["opening_source_audit"]["verified"])
                self.assertEqual(opening["source"], "the_odds_api_import")

                incremental = provider.collect_historical_timeline(
                    historical_api,
                    fixture="fin-1", sport_key="soccer_finland_veikkausliiga", league="Finland Veikkausliiga",
                    home_team="IF Gnistan", away_team="Inter Turku", kickoff_utc=provider.iso_utc(KICKOFF),
                    known_event_id="event-gnistan-inter", requested_stages=["T-6h"],
                    minimums={"1x2": 5, "asian_handicap": 3, "over_under": 3},
                )
                main.import_prematch_packet(incremental["packet"])
                metadata = main.load_snapshot_store()["external_prematch"]["fin-1"]
                self.assertEqual(metadata["data_quality"]["primary_reference_eligible_stage_count"], 8)
                self.assertEqual(metadata["data_quality"]["last_requested_stages"], ["T-6h"])
            finally:
                main.SNAPSHOT_STORE_PATH = old_store


if __name__ == "__main__":
    unittest.main()
