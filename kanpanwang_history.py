"""Bounded, read-only historical market intake for Window 08.

This returns source snapshots for qualification, not a model packet, prediction,
or settlement. It deliberately does not change the existing 02 odds collector.
"""

from __future__ import annotations

import re
import time
from datetime import datetime
from typing import Any, Callable, Dict, List

from the_odds_api_provider import iso_utc, parse_timestamp, select_event, validate_sport_key


STAGE_OFFSETS = {"T-24h": 86400, "T-12h": 43200, "T-6h": 21600, "T-3h": 10800, "T-1h": 3600}
MARKETS = ("spreads", "totals", "btts", "team_totals")
MAX_SNAPSHOT_LAG_SECONDS = 600


def _wrapper(result: Dict[str, Any]) -> Dict[str, Any]:
    value = result.get("data")
    return value if isinstance(value, dict) else {}


def collect_08_history(call_api: Callable[[str, Dict[str, Any]], Dict[str, Any]], payload: Dict[str, Any], *, now: int | None = None) -> Dict[str, Any]:
    """At most one discovery plus five odds requests; never writes or retries."""
    if not isinstance(payload, dict):
        raise ValueError("request_body_must_be_object")
    sport_key = validate_sport_key(payload.get("sport_key"))
    if not sport_key.startswith("soccer_"):
        raise ValueError("soccer_sport_key_required")
    kickoff_text = payload.get("kickoff_utc")
    try:
        kickoff_datetime = datetime.fromisoformat(str(kickoff_text).replace("Z", "+00:00"))
    except ValueError:
        kickoff_datetime = None
    kickoff = parse_timestamp(kickoff_text) if kickoff_datetime is not None and kickoff_datetime.tzinfo is not None else None
    home, away = str(payload.get("home_team") or "").strip(), str(payload.get("away_team") or "").strip()
    if kickoff is None or not home or not away or len(home) > 120 or len(away) > 120:
        raise ValueError("invalid_fixture_identity")
    stages = payload.get("stages")
    if not isinstance(stages, list) or not stages or len(stages) > 5 or any(not isinstance(stage, str) or stage not in STAGE_OFFSETS for stage in stages) or len(set(stages)) != len(stages):
        raise ValueError("invalid_08_stages")
    now = int(time.time()) if now is None else int(now)
    if any(kickoff - STAGE_OFFSETS[stage] > now for stage in stages):
        raise ValueError("future_history_stage")
    books = payload.get("bookmakers")
    if not isinstance(books, list) or not 1 <= len(books) <= 10 or any(not isinstance(book, str) or not re.fullmatch(r"[a-z0-9_]{2,60}", book) for book in books) or len(set(books)) != len(books):
        raise ValueError("one_region_equivalent_bookmakers_required")
    aliases = {}
    for side in ("home", "away"):
        values = payload.get(f"{side}_aliases") or []
        if not isinstance(values, list) or len(values) > 10 or any(not isinstance(value, str) or not 1 <= len(value) <= 120 for value in values):
            raise ValueError(f"invalid_{side}_aliases")
        aliases[side] = values
    event_id = str(payload.get("event_id") or "").strip()
    if event_id and not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", event_id):
        raise ValueError("invalid_event_id")
    audit: List[Dict[str, Any]] = []

    def request(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        result = call_api(path, params)
        audit.append({"family": "historical_event_odds" if path.endswith("/odds") else "historical_events",
                      "status_code": result.get("status_code"), "ok": bool(result.get("ok")),
                      "quota_last": result.get("quota_last"), "quota_remaining": result.get("quota_remaining")})
        return result

    if not event_id:
        discovery_target = max(kickoff - STAGE_OFFSETS[stage] for stage in stages)
        result = request(f"/historical/sports/{sport_key}/events", {"date": iso_utc(discovery_target), "dateFormat": "iso"})
        wrapper = _wrapper(result)
        discovery_snapshot = parse_timestamp(wrapper.get("timestamp"))
        rows = wrapper.get("data")
        match = select_event(rows if isinstance(rows, list) else [], home, away, kickoff, aliases["home"], aliases["away"])
        if (not result.get("ok") or match.get("status") != "matched" or discovery_snapshot is None
                or not 0 <= discovery_target - discovery_snapshot <= MAX_SNAPSHOT_LAG_SECONDS):
            return {"ok": False, "reason": "event_discovery_failed_or_ambiguous", "request_audit": audit, "request_count": len(audit)}
        event_id = str(match["event"].get("id") or "")
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,100}", event_id):
            return {"ok": False, "reason": "event_id_missing", "request_audit": audit, "request_count": len(audit)}

    nodes = []
    for stage in stages:
        target = kickoff - STAGE_OFFSETS[stage]
        result = request(f"/historical/sports/{sport_key}/events/{event_id}/odds",
                         {"date": iso_utc(target), "dateFormat": "iso", "oddsFormat": "decimal",
                          "markets": ",".join(MARKETS), "bookmakers": ",".join(books)})
        wrapper = _wrapper(result)
        snapshot = parse_timestamp(wrapper.get("timestamp"))
        event = wrapper.get("data")
        match = select_event([event] if isinstance(event, dict) else [], home, away, kickoff, aliases["home"], aliases["away"])
        lag = target - snapshot if snapshot is not None else None
        if not result.get("ok") or match.get("status") != "matched" or not isinstance(event, dict) or str(event.get("id")) != event_id or lag is None or not 0 <= lag <= MAX_SNAPSHOT_LAG_SECONDS:
            nodes.append({"stage": stage, "target_at": iso_utc(target), "snapshot_at": iso_utc(snapshot) if snapshot is not None else None,
                          "status": "data_missing", "reason": "source_error_identity_or_snapshot_lag"})
            continue
        filtered = []
        rejected_stale = 0
        company_sets = {market: set() for market in MARKETS}
        for bookmaker in event.get("bookmakers") or []:
            if not isinstance(bookmaker, dict):
                continue
            bookmaker_key = bookmaker.get("key")
            if not isinstance(bookmaker_key, str) or bookmaker_key not in books:
                continue
            kept_markets = []
            for market in bookmaker.get("markets") or []:
                if not isinstance(market, dict) or market.get("key") not in MARKETS:
                    continue
                updated = parse_timestamp(market.get("last_update") or bookmaker.get("last_update"))
                if updated is None or updated > snapshot or updated >= kickoff:
                    rejected_stale += 1
                    continue
                if len([row for row in market.get("outcomes") or [] if isinstance(row, dict)]) < 2:
                    continue
                kept_markets.append(market)
                company_sets[market["key"]].add(bookmaker_key)
            if kept_markets:
                filtered.append({"key": bookmaker_key, "title": bookmaker.get("title"), "markets": kept_markets})
        counts = {market: len(companies) for market, companies in company_sets.items()}
        nodes.append({"stage": stage, "target_at": iso_utc(target), "snapshot_at": iso_utc(snapshot),
                      "status": "available" if filtered else "data_missing", "snapshot_lag_seconds": lag,
                      "market_bookmaker_presence": counts, "rejected_market_rows_after_snapshot_or_kickoff": rejected_stale,
                      "all_four_market_keys_present": all(counts.values()),
                      "event": {"id": event_id, "sport_key": sport_key, "commence_time": event.get("commence_time"),
                                "home_team": event.get("home_team"), "away_team": event.get("away_team"), "bookmakers": filtered}})
    return {"ok": True, "schema": "08_historical_market_presence_candidate_v1", "qualification": "RAW_MARKET_PRESENCE_ONLY",
            "event_id": event_id, "sport_key": sport_key, "fixture": payload.get("fixture"),
            "markets": list(MARKETS), "nodes": nodes, "request_count": len(audit), "request_audit": audit,
            "persisted": False, "result_read": False, "prediction_frozen": False}

