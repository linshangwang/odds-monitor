"""The Odds API historical prematch collector.

This module is deliberately independent from the application store.  It turns
provider responses into the canonical ``shadow_prematch_packet_v1`` shape so
the existing import, timing and quality gates remain the single write path.
"""

from __future__ import annotations

import re
import time
import unicodedata
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, List, Optional, Sequence, Tuple


PREMATCH_STAGES: Tuple[str, ...] = (
    "Opening", "T-24h", "T-12h", "T-6h", "T-3h", "T-1h", "T-30m", "Closing"
)
STAGE_OFFSETS: Dict[str, int] = {
    "T-24h": 24 * 3600,
    "T-12h": 12 * 3600,
    "T-6h": 6 * 3600,
    "T-3h": 3 * 3600,
    "T-1h": 3600,
    "T-30m": 1800,
    "Closing": 60,
}
CORE_MARKETS: Tuple[str, ...] = ("h2h", "spreads", "totals")
SPORT_KEY_PATTERN = re.compile(r"^[a-z0-9_]{3,100}$")


def parse_timestamp(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return None


def iso_utc(value: int) -> str:
    return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    return "".join(char for char in text if char.isalnum())


def validate_sport_key(value: Any) -> str:
    sport_key = str(value or "").strip().lower()
    if not SPORT_KEY_PATTERN.fullmatch(sport_key):
        raise ValueError("invalid_the_odds_api_sport_key")
    return sport_key


def _as_float(value: Any) -> Optional[float]:
    try:
        parsed = float(value)
        return parsed if parsed == parsed and parsed not in (float("inf"), float("-inf")) else None
    except (TypeError, ValueError):
        return None


def _name_matches(provider_name: Any, expected: Any, aliases: Sequence[Any]) -> Tuple[bool, str]:
    provider = normalize_name(provider_name)
    candidates = {normalize_name(expected), *(normalize_name(alias) for alias in aliases or [])} - {""}
    if provider in candidates:
        return True, "exact_normalized_alias"
    contained = [candidate for candidate in candidates if min(len(provider), len(candidate)) >= 6 and (provider in candidate or candidate in provider)]
    return (len(contained) == 1, "unique_long_name_containment" if len(contained) == 1 else "no_match")


def select_event(
    rows: Iterable[Dict[str, Any]],
    home_team: str,
    away_team: str,
    kickoff_at: int,
    home_aliases: Sequence[str] = (),
    away_aliases: Sequence[str] = (),
    kickoff_tolerance_seconds: int = 900,
) -> Dict[str, Any]:
    matches: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        event_kickoff = parse_timestamp(row.get("commence_time"))
        if event_kickoff is None or abs(event_kickoff - kickoff_at) > kickoff_tolerance_seconds:
            continue
        home_ok, home_method = _name_matches(row.get("home_team"), home_team, home_aliases)
        away_ok, away_method = _name_matches(row.get("away_team"), away_team, away_aliases)
        if home_ok and away_ok:
            matches.append({
                "event": row,
                "kickoff_delta_seconds": abs(event_kickoff - kickoff_at),
                "name_match_method": {"home": home_method, "away": away_method},
            })
    if len(matches) == 1:
        return {"status": "matched", "decision_eligible": True, **matches[0], "candidate_count": 1}
    if len(matches) > 1:
        return {"status": "ambiguous", "decision_eligible": False, "candidate_count": len(matches), "reason": "multiple_provider_events_match"}
    return {"status": "no_match", "decision_eligible": False, "candidate_count": 0, "reason": "provider_event_not_found"}


def _payload_data(result: Dict[str, Any]) -> Any:
    payload = result.get("data") if isinstance(result, dict) else None
    if isinstance(payload, dict) and "data" in payload:
        return payload.get("data")
    return payload


def _wrapper_timestamp(result: Dict[str, Any]) -> Optional[int]:
    payload = result.get("data") if isinstance(result, dict) else None
    return parse_timestamp(payload.get("timestamp")) if isinstance(payload, dict) else None


def _outcome_by_name(outcomes: Iterable[Dict[str, Any]], expected: str, aliases: Sequence[str] = ()) -> Optional[Dict[str, Any]]:
    exact = []
    for outcome in outcomes or []:
        matched, _ = _name_matches(outcome.get("name"), expected, aliases)
        if matched:
            exact.append(outcome)
    return exact[0] if len(exact) == 1 else None


def _quote(
    bookmaker: Dict[str, Any], market: Dict[str, Any], market_name: str,
    selection: str, line: Optional[float], price: Any, observed_at: str,
) -> Dict[str, Any]:
    return {
        "bookmaker_name": bookmaker.get("title") or bookmaker.get("key") or "unknown",
        "bookmaker_id": bookmaker.get("key"),
        "market": market_name,
        "market_name": market.get("key"),
        "selection": selection,
        "line": line,
        "price": price,
        "observed_at": observed_at,
        "updated_at": market.get("last_update") or bookmaker.get("last_update"),
    }


def normalize_event_quotes(
    event: Dict[str, Any],
    snapshot_at: int,
    kickoff_at: int,
    home_team: str,
    away_team: str,
    home_aliases: Sequence[str] = (),
    away_aliases: Sequence[str] = (),
) -> Dict[str, Any]:
    """Normalize company-level core markets without synthesizing missing sides."""
    observed_at = iso_utc(snapshot_at)
    quotes: List[Dict[str, Any]] = []
    rejected_after_kickoff = 0
    for bookmaker in event.get("bookmakers") or []:
        if not isinstance(bookmaker, dict):
            continue
        for market in bookmaker.get("markets") or []:
            if not isinstance(market, dict):
                continue
            updated_at = parse_timestamp(market.get("last_update") or bookmaker.get("last_update"))
            if updated_at is not None and updated_at >= kickoff_at:
                rejected_after_kickoff += 1
                continue
            key = str(market.get("key") or "").lower()
            outcomes = [row for row in (market.get("outcomes") or []) if isinstance(row, dict)]
            if key == "h2h":
                home = _outcome_by_name(outcomes, home_team, home_aliases)
                away = _outcome_by_name(outcomes, away_team, away_aliases)
                draw = next((row for row in outcomes if normalize_name(row.get("name")) == "draw"), None)
                if home and draw and away:
                    quotes.extend([
                        _quote(bookmaker, market, "1x2", "home", None, home.get("price"), observed_at),
                        _quote(bookmaker, market, "1x2", "draw", None, draw.get("price"), observed_at),
                        _quote(bookmaker, market, "1x2", "away", None, away.get("price"), observed_at),
                    ])
            elif key in ("spreads", "alternate_spreads"):
                home = _outcome_by_name(outcomes, home_team, home_aliases)
                away = _outcome_by_name(outcomes, away_team, away_aliases)
                home_point = _as_float(home.get("point")) if home else None
                away_point = _as_float(away.get("point")) if away else None
                if home and away and home_point is not None and away_point is not None and abs(home_point + away_point) <= 0.011:
                    quotes.extend([
                        _quote(bookmaker, market, "asian_handicap", "home", home_point, home.get("price"), observed_at),
                        _quote(bookmaker, market, "asian_handicap", "away", home_point, away.get("price"), observed_at),
                    ])
            elif key in ("totals", "alternate_totals"):
                grouped: Dict[float, Dict[str, Dict[str, Any]]] = {}
                for outcome in outcomes:
                    point = outcome.get("point")
                    name = str(outcome.get("name") or "").strip().lower()
                    if point is None or name not in ("over", "under"):
                        continue
                    parsed_point = _as_float(point)
                    if parsed_point is None:
                        continue
                    grouped.setdefault(parsed_point, {})[name] = outcome
                for point, pair in grouped.items():
                    if "over" in pair and "under" in pair:
                        quotes.extend([
                            _quote(bookmaker, market, "over_under", "over", point, pair["over"].get("price"), observed_at),
                            _quote(bookmaker, market, "over_under", "under", point, pair["under"].get("price"), observed_at),
                        ])
    return {
        "quotes": quotes,
        "quote_count": len(quotes),
        "bookmaker_count": len({row.get("bookmaker_id") or row.get("bookmaker_name") for row in quotes}),
        "rejected_market_rows_updated_at_or_after_kickoff": rejected_after_kickoff,
    }


def company_coverage(quotes: Sequence[Dict[str, Any]]) -> Dict[str, int]:
    grouped: Dict[Tuple[str, str, str], set] = {}
    for row in quotes:
        market = str(row.get("market") or "")
        bookmaker = str(row.get("bookmaker_id") or row.get("bookmaker_name") or "unknown")
        line = "" if market == "1x2" else str(row.get("line"))
        grouped.setdefault((market, bookmaker, line), set()).add(str(row.get("selection") or ""))
    required = {"1x2": {"home", "draw", "away"}, "asian_handicap": {"home", "away"}, "over_under": {"over", "under"}}
    return {
        market: len({bookmaker for (name, bookmaker, _), selections in grouped.items() if name == market and required[market] <= selections})
        for market in required
    }


def coverage_grade(coverage: Dict[str, int], minimums: Dict[str, int]) -> Dict[str, Any]:
    reasons = [f"{market}_bookmakers_below_{minimum}" for market, minimum in minimums.items() if int(coverage.get(market) or 0) < minimum]
    return {
        "primary_reference_eligible": not reasons,
        "grade": "A" if not reasons else ("B" if all(int(coverage.get(market) or 0) >= max(1, minimum - 2) for market, minimum in minimums.items()) else "C"),
        "reasons": reasons,
        "complete_bookmaker_count_by_market": coverage,
        "minimum_complete_bookmakers": minimums,
    }


def collect_historical_timeline(
    call_api: Callable[[str, Dict[str, Any]], Dict[str, Any]],
    *,
    fixture: str,
    sport_key: str,
    league: str,
    home_team: str,
    away_team: str,
    kickoff_utc: Any,
    home_aliases: Sequence[str] = (),
    away_aliases: Sequence[str] = (),
    regions: str = "eu",
    bookmakers: str = "",
    opening_lookback_days: int = 7,
    opening_scan_hours: int = 12,
    max_requests: int = 48,
    minimums: Optional[Dict[str, int]] = None,
    snapshot_lag_tolerance_seconds: int = 600,
) -> Dict[str, Any]:
    sport_key = validate_sport_key(sport_key)
    kickoff_at = parse_timestamp(kickoff_utc)
    if kickoff_at is None:
        raise ValueError("invalid_kickoff_utc")
    minimums = minimums or {"1x2": 5, "asian_handicap": 3, "over_under": 3}
    request_count = 0
    request_audit: List[Dict[str, Any]] = []
    event_cache: Dict[int, Dict[str, Any]] = {}

    def provider_call(path: str, params: Dict[str, Any]) -> Dict[str, Any]:
        nonlocal request_count
        if request_count >= max_requests:
            return {"ok": False, "error": "the_odds_api_request_budget_exhausted"}
        request_count += 1
        result = call_api(path, params)
        request_audit.append({
            "endpoint_family": "historical_event_odds" if "/odds" in path else "historical_events",
            "ok": bool(result.get("ok")), "status_code": result.get("status_code"),
            "quota_remaining": result.get("quota_remaining"), "quota_used": result.get("quota_used"),
            "quota_last": result.get("quota_last"), "error": result.get("error"),
        })
        return result

    def event_presence(query_at: int) -> Dict[str, Any]:
        query_at = int(query_at // 300 * 300)
        if query_at in event_cache:
            return event_cache[query_at]
        result = provider_call(
            f"/historical/sports/{sport_key}/events",
            {"date": iso_utc(query_at), "dateFormat": "iso"},
        )
        rows = _payload_data(result)
        selected = select_event(
            rows if isinstance(rows, list) else [], home_team, away_team, kickoff_at,
            home_aliases, away_aliases,
        )
        selected.update({
            "request_ok": bool(result.get("ok")), "requested_at": query_at,
            "provider_snapshot_at": _wrapper_timestamp(result), "provider_error": result.get("error"),
        })
        event_cache[query_at] = selected
        return selected

    # Find the first provider snapshot containing this event.  A preceding
    # verified absence is mandatory; otherwise Opening remains left-censored.
    window_start = kickoff_at - max(1, min(opening_lookback_days, 30)) * 86400
    window_end = kickoff_at - 60
    step = max(3600, min(opening_scan_hours, 24) * 3600)
    previous_absent: Optional[int] = None
    first_present: Optional[int] = None
    selected_event: Optional[Dict[str, Any]] = None
    cursor = window_start
    while cursor <= window_end and request_count < max_requests:
        presence = event_presence(cursor)
        if not presence.get("request_ok"):
            return {"ok": False, "error": presence.get("provider_error") or "provider_event_discovery_failed", "request_count": request_count, "request_audit": request_audit}
        if presence.get("status") == "matched":
            first_present, selected_event = cursor, presence.get("event")
            break
        if presence.get("status") == "ambiguous":
            return {"ok": False, "error": "ambiguous_provider_event", "request_count": request_count, "request_audit": request_audit}
        previous_absent = cursor
        cursor += step

    if first_present is None and window_end not in event_cache and request_count < max_requests:
        presence = event_presence(window_end)
        if not presence.get("request_ok"):
            return {"ok": False, "error": presence.get("provider_error") or "provider_event_discovery_failed", "request_count": request_count, "request_audit": request_audit}
        if presence.get("status") == "matched":
            first_present, selected_event = window_end, presence.get("event")
        elif presence.get("status") == "ambiguous":
            return {"ok": False, "error": "ambiguous_provider_event", "request_count": request_count, "request_audit": request_audit}

    if first_present is None or selected_event is None:
        return {"ok": False, "error": "provider_event_not_found_in_opening_window", "request_count": request_count, "request_audit": request_audit}

    opening_verified = previous_absent is not None
    if opening_verified:
        low, high = previous_absent, first_present
        while high - low > 300 and request_count < max_requests:
            midpoint = int(((low + high) // 2) // 300 * 300)
            if midpoint <= low:
                midpoint = low + 300
            presence = event_presence(midpoint)
            if not presence.get("request_ok"):
                return {"ok": False, "error": presence.get("provider_error") or "provider_event_discovery_failed", "request_count": request_count, "request_audit": request_audit}
            if presence.get("status") == "matched":
                high, selected_event = midpoint, presence.get("event")
            elif presence.get("status") == "ambiguous":
                return {"ok": False, "error": "ambiguous_provider_event", "request_count": request_count, "request_audit": request_audit}
            else:
                low = midpoint
        first_present = high

    event_id = str((selected_event or {}).get("id") or "")
    if not event_id:
        return {"ok": False, "error": "provider_event_id_missing", "request_count": request_count, "request_audit": request_audit}

    targets: Dict[str, Optional[int]] = {"Opening": first_present if opening_verified else None}
    targets.update({stage: kickoff_at - offset for stage, offset in STAGE_OFFSETS.items()})
    timeline: List[Dict[str, Any]] = []
    for stage in PREMATCH_STAGES:
        target_at = targets.get(stage)
        if target_at is None:
            timeline.append({
                "stage": stage, "status": "data_missing", "target_at": None,
                "reason": "opening_left_censored_no_verified_preceding_absence",
                "provider_audit": {"source": "the_odds_api", "opening_verified": False},
            })
            continue
        params: Dict[str, Any] = {
            "date": iso_utc(target_at), "dateFormat": "iso", "oddsFormat": "decimal",
            "markets": ",".join(CORE_MARKETS),
        }
        if bookmakers:
            params["bookmakers"] = bookmakers
        else:
            params["regions"] = regions
        result = provider_call(f"/historical/sports/{sport_key}/events/{event_id}/odds", params)
        payload = _payload_data(result)
        snapshot_at = _wrapper_timestamp(result)
        lag = target_at - snapshot_at if snapshot_at is not None else None
        event_match = select_event(
            [payload] if isinstance(payload, dict) else [], home_team, away_team, kickoff_at,
            home_aliases, away_aliases,
        )
        if not result.get("ok") or snapshot_at is None or lag is None or lag < 0 or lag > snapshot_lag_tolerance_seconds or event_match.get("status") != "matched":
            timeline.append({
                "stage": stage, "status": "data_missing", "target_at": iso_utc(target_at),
                "latest_observed_at": iso_utc(snapshot_at) if snapshot_at is not None else None,
                "reason": result.get("error") or ("provider_snapshot_lag_exceeded" if lag is not None and lag > snapshot_lag_tolerance_seconds else event_match.get("reason") or "historical_snapshot_unavailable"),
                "provider_audit": {"source": "the_odds_api", "target_at": target_at, "snapshot_at": snapshot_at, "lag_seconds": lag},
            })
            continue
        normalized = normalize_event_quotes(
            payload, snapshot_at, kickoff_at, home_team, away_team, home_aliases, away_aliases
        )
        coverage = company_coverage(normalized["quotes"])
        quality = coverage_grade(coverage, minimums)
        timeline.append({
            "stage": stage,
            "status": "available" if normalized["quotes"] else "data_missing",
            "target_at": iso_utc(target_at),
            "latest_observed_at": iso_utc(snapshot_at),
            "reason": None if normalized["quotes"] else "no_complete_core_market_quotes",
            "quote_count": normalized["quote_count"],
            "bookmaker_count": normalized["bookmaker_count"],
            "company_market_array": normalized["quotes"],
            "provider_audit": {
                "source": "the_odds_api", "sport_key": sport_key, "event_id": event_id,
                "target_at": target_at, "snapshot_at": snapshot_at, "lag_seconds": lag,
                "coverage": quality,
                "rejected_market_rows_updated_at_or_after_kickoff": normalized["rejected_market_rows_updated_at_or_after_kickoff"],
                "opening_verified": opening_verified if stage == "Opening" else None,
                "opening_preceding_absence_at": previous_absent if stage == "Opening" else None,
            },
        })

    eligible_stages = [row["stage"] for row in timeline if (row.get("provider_audit") or {}).get("coverage", {}).get("primary_reference_eligible")]
    packet = {
        "schema_version": "shadow_prematch_packet_v1",
        "source": "the_odds_api",
        "exported_at": int(time.time()),
        "league": league,
        "match": {
            "match_id": str(fixture), "kickoff_utc": iso_utc(kickoff_at),
            "home_team_name": home_team, "away_team_name": away_team,
            "home_aliases": list(home_aliases), "away_aliases": list(away_aliases),
            "the_odds_api_event_id": event_id, "the_odds_api_sport_key": sport_key,
        },
        "required_timeline": list(PREMATCH_STAGES),
        "timeline": timeline,
        "data_quality": {
            "source": "the_odds_api", "primary_reference_eligible_stages": eligible_stages,
            "primary_reference_eligible_stage_count": len(eligible_stages),
            "required_stage_count": len(PREMATCH_STAGES),
        },
    }
    return {
        "ok": True, "source": "the_odds_api", "sport_key": sport_key,
        "event_id": event_id, "packet": packet, "request_count": request_count,
        "request_audit": request_audit,
        "opening_discovery": {
            "verified": opening_verified, "window_start": window_start,
            "first_present_at": first_present, "preceding_absence_at": previous_absent,
        },
        "quota_last_known": request_audit[-1] if request_audit else None,
    }
