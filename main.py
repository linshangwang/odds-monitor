import json
import gzip
import math
import os
import time
import threading
import hashlib
import hmac
from statistics import median
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

load_dotenv()

VERSION = "1.13.0"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
AUTO_FETCH_DATE = os.getenv("AUTO_FETCH_DATE", "2026-09-28")
AUTO_FETCH_TIMEZONE = os.getenv("AUTO_FETCH_TIMEZONE", "Asia/Shanghai")
AUTO_FETCH_FIXTURE_ID = os.getenv("AUTO_FETCH_FIXTURE_ID", "")
SHADOW_ACCESS_TOKEN = os.getenv("SHADOW_ACCESS_TOKEN", "")
SNAPSHOT_STORE_PATH = os.getenv("SNAPSHOT_STORE_PATH", "/tmp/shadow_snapshots.json")
SNAPSHOT_STORE_GZIP = os.getenv("SNAPSHOT_STORE_GZIP", "true").lower() in ("1", "true", "yes", "on")
EXTERNAL_DATA_STALE_SECONDS = max(60, int(os.getenv("EXTERNAL_DATA_STALE_SECONDS", "1800")))
FUNDAMENTAL_VERSION_RETENTION = max(10, min(int(os.getenv("FUNDAMENTAL_VERSION_RETENTION", "100")), 1000))
PORTFOLIO_RUN_RETENTION = max(10, min(int(os.getenv("PORTFOLIO_RUN_RETENTION", "100")), 1000))
SNAPSHOT_STORE_WARN_BYTES = max(1024 * 1024, int(os.getenv("SNAPSHOT_STORE_WARN_BYTES", str(256 * 1024 * 1024))))
MIN_EDGE = float(os.getenv("MIN_EDGE", "0.03"))
MIN_EV = float(os.getenv("MIN_EV", "0.03"))
MIN_SCRIPT_COVERAGE = float(os.getenv("MIN_SCRIPT_COVERAGE", "0.60"))
MAX_CROWDING = float(os.getenv("MAX_CROWDING", "0.80"))
MIN_LINEUP_CONFIDENCE = float(os.getenv("MIN_LINEUP_CONFIDENCE", "0.70"))
HIGH_VARIANCE_MIN_SCRIPT_COVERAGE = float(os.getenv("HIGH_VARIANCE_MIN_SCRIPT_COVERAGE", "0.40"))
MIN_CONSENSUS_BOOKMAKERS = max(1, int(os.getenv("MIN_CONSENSUS_BOOKMAKERS", "2")))
MAX_CONSENSUS_PRICE_SPREAD = max(0.01, float(os.getenv("MAX_CONSENSUS_PRICE_SPREAD", "0.25")))
MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD = max(0.005, float(os.getenv("MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD", "0.05")))
MIN_MARKET_IMPLIED_PROBABILITY_TOTAL = min(1.0, max(0.5, float(os.getenv("MIN_MARKET_IMPLIED_PROBABILITY_TOTAL", "0.80"))))
MAX_MARKET_IMPLIED_PROBABILITY_TOTAL = max(1.0, min(2.0, float(os.getenv("MAX_MARKET_IMPLIED_PROBABILITY_TOTAL", "1.40"))))
AUTO_SNAPSHOT_ENABLED = os.getenv("AUTO_SNAPSHOT_ENABLED", "true").lower() in ("1", "true", "yes", "on")
AUTO_SNAPSHOT_POLL_SECONDS = max(60, int(os.getenv("AUTO_SNAPSHOT_POLL_SECONDS", "300")))
AUTO_SNAPSHOT_WINDOW_SECONDS = max(60, int(os.getenv("AUTO_SNAPSHOT_WINDOW_SECONDS", "600")))
AUTO_SNAPSHOT_DAYS_AHEAD = max(1, int(os.getenv("AUTO_SNAPSHOT_DAYS_AHEAD", "2")))
AUTO_SNAPSHOT_THREAD_STARTED = False
API_FOOTBALL_RATE_LIMIT_UNTIL = 0
SNAPSHOT_STORE_LOCK = threading.RLock()

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")
THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY", "")
THE_ODDS_API_BASE_URL = "https://api.the-odds-api.com/v4"
NAMI_API_USER = os.getenv("NAMI_API_USER", "")
NAMI_API_SECRET = os.getenv("NAMI_API_SECRET", "")
NAMI_API_BASE_URL = os.getenv("NAMI_API_BASE_URL", "https://open.sportnanoapi.com").rstrip("/")
NAMI_REQUEST_TIMEOUT = max(1, int(os.getenv("NAMI_REQUEST_TIMEOUT", str(min(REQUEST_TIMEOUT, 10)))))
SPORTRADAR_API_KEY = os.getenv("SPORTRADAR_API_KEY", "")
SPORTRADAR_SOCCER_BASE_URL = "https://api.sportradar.com/soccer/trial/v4/en"
ISPORTS_API_KEY = os.getenv("ISPORTS_API_KEY", "")
ISPORTS_BASE_URL = os.getenv("ISPORTS_BASE_URL", "http://isports.feijing88.com").rstrip("/")

DEFAULT_TARGET_LEAGUES: Dict[int, str] = {
    2: "UEFA Champions League",
    3: "UEFA Europa League",
    5: "UEFA Nations League",
    39: "England Premier League",
    61: "France Ligue 1",
    78: "Germany Bundesliga",
    88: "Netherlands Eredivisie",
    94: "Portugal Primeira Liga",
    135: "Italy Serie A",
    140: "Spain La Liga",
    144: "Belgium Pro League",
    203: "Turkey Super Lig",
    848: "UEFA Conference League",
}
raw_target = os.getenv("TARGET_LEAGUE_IDS", "")
TARGET_LEAGUE_IDS = {int(x.strip()) for x in raw_target.split(",") if x.strip().isdigit()} if raw_target.strip() else set(DEFAULT_TARGET_LEAGUES.keys())

TRACKING_STAGES = [
    {"key": "Opening", "label": "Opening 开盘", "offset": timedelta(hours=-48), "purpose": "保存首次真实可得盘口；不得由后续盘口反推"},
    {"key": "T-24h", "label": "T-24h 初判", "offset": timedelta(hours=-24), "purpose": "初始基本面与早盘定位"},
    {"key": "T-12h", "label": "T-12h 早盘确认", "offset": timedelta(hours=-12), "purpose": "早盘/水位第一次确认"},
    {"key": "T-6h", "label": "T-6h 盘口/赔率确认", "offset": timedelta(hours=-6), "purpose": "盘口持续性与赔率结构确认"},
    {"key": "T-3h", "label": "T-3h 临场前修正", "offset": timedelta(hours=-3), "purpose": "盘口跨档、反转、饱和度观察"},
    {"key": "T-1h", "label": "T-1h 阵容伤停确认", "offset": timedelta(hours=-1), "purpose": "首发/伤停/临场水位修正"},
    {"key": "T-15m", "label": "T-15m 最终影子判断", "offset": timedelta(minutes=-15), "purpose": "最终临场确认"},
    {"key": "Closing", "label": "Closing 封盘", "offset": timedelta(minutes=-1), "purpose": "封盘盘口与最终水位"},
    {"key": "FT", "label": "FT 赛后复盘", "offset": timedelta(hours=2), "purpose": "赛后复盘命中/偏差"},
]
STAGE_ORDER = [x["key"] for x in TRACKING_STAGES]
PREMATCH_STAGE_ORDER = [x for x in STAGE_ORDER if x != "FT"]
STAGE_ALIASES = {}
for s in TRACKING_STAGES:
    STAGE_ALIASES[s["key"].lower()] = s["key"]
    STAGE_ALIASES[s["label"].lower()] = s["key"]

app = FastAPI(title="Football Shadow Analysis Data Service", description="Prematch data and shadow snapshot pipeline.", version=VERSION)
LAST_PUSH_EVENTS: List[Dict[str, Any]] = []
LAST_PUSH_STATISTICS: List[Dict[str, Any]] = []
STARTUP_FIXTURES: Dict[str, Any] = {}
STARTUP_COLLECT: Dict[str, Any] = {}


class SnapshotStoreWriteError(RuntimeError):
    pass


class SnapshotStoreReadError(RuntimeError):
    pass


def require_shadow_token(token: Optional[str]) -> None:
    if SHADOW_ACCESS_TOKEN and not hmac.compare_digest(str(token or ""), str(SHADOW_ACCESS_TOKEN)):
        raise HTTPException(status_code=401, detail="Invalid or missing token")


def resolve_shadow_token(query_token: Optional[str], authorization: Optional[str], header_token: Optional[str]) -> Optional[str]:
    if header_token:
        return header_token
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return query_token


def mask_secret(text: str) -> str:
    configured = {str(key) for key in [API_FOOTBALL_KEY, THESTATS_API_KEY, ISPORTS_API_KEY, SHADOW_ACCESS_TOKEN, NAMI_API_USER, NAMI_API_SECRET] if key}
    for key in sorted(configured, key=len, reverse=True):
        text = text.replace(key, "YOUR_SECRET")
    return text


def redact_secrets(value: Any) -> Any:
    """Recursively remove configured credential values from diagnostic payloads."""
    if isinstance(value, dict):
        return {mask_secret(str(key)): redact_secrets(child) for key, child in value.items()}
    if isinstance(value, list):
        return [redact_secrets(child) for child in value]
    if isinstance(value, tuple):
        return [redact_secrets(child) for child in value]
    if isinstance(value, str):
        return mask_secret(value)
    return value


def safe_json_response(resp: requests.Response) -> Any:
    try:
        return redact_secrets(resp.json())
    except Exception:
        return {"raw_text": mask_secret(resp.text[:2000])}


def call_api_football(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    global API_FOOTBALL_RATE_LIMIT_UNTIL
    if not API_FOOTBALL_KEY:
        return {"ok": False, "error": "Missing API_FOOTBALL_KEY"}
    now_ts = int(time.time())
    if now_ts < API_FOOTBALL_RATE_LIMIT_UNTIL:
        return {"ok": False, "status_code": 429, "error": "api_football_rate_limit_cooldown", "retry_after_seconds": API_FOOTBALL_RATE_LIMIT_UNTIL - now_ts}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{API_FOOTBALL_BASE_URL}{path}"
    headers = {"x-apisports-key": API_FOOTBALL_KEY, "Accept": "application/json"}
    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        if resp.status_code == 429:
            API_FOOTBALL_RATE_LIMIT_UNTIL = int(time.time()) + 3600
            print("[API_FOOTBALL] 429 received; cooldown_seconds=3600")
        return {"ok": resp.ok, "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": safe_json_response(resp)}
    except requests.RequestException as exc:
        return {"ok": False, "error": mask_secret(str(exc)), "request_url": mask_secret(url)}


def call_thestats(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not THESTATS_API_KEY:
        return {"ok": False, "error": "Missing THESTATS_API_KEY"}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{THESTATS_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {THESTATS_API_KEY}", "Accept": "application/json"}
    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        return {"ok": resp.ok, "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": safe_json_response(resp)}
    except requests.RequestException as exc:
        return {"ok": False, "error": mask_secret(str(exc)), "request_url": mask_secret(url)}

def call_the_odds_api(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not THE_ODDS_API_KEY:
        return {"ok": False, "error": "Missing THE_ODDS_API_KEY"}

    if not path.startswith("/"):
        path = "/" + path

    url = f"{THE_ODDS_API_BASE_URL}{path}"
    query = dict(params or {})
    query["apiKey"] = THE_ODDS_API_KEY

    try:
        resp = requests.get(url, params=query, timeout=REQUEST_TIMEOUT)
        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "data": safe_json_response(resp),
            "quota_remaining": resp.headers.get("x-requests-remaining"),
            "quota_used": resp.headers.get("x-requests-used")
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": type(exc).__name__}


def call_nami(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Call optional Nami safely; any failure degrades only this data source."""
    if not NAMI_API_USER or not NAMI_API_SECRET:
        return {
            "ok": False, "available": False, "degraded": True, "required": False,
            "error": "nami_not_configured", "fallback": "continue_without_nami",
        }
    if not path.startswith("/"):
        path = "/" + path
    query = dict(params or {})
    query.update({"user": NAMI_API_USER, "secret": NAMI_API_SECRET})
    try:
        response = requests.get(f"{NAMI_API_BASE_URL}{path}", params=query, timeout=NAMI_REQUEST_TIMEOUT)
        try:
            payload = redact_secrets(response.json())
        except Exception:
            payload = {"error": "non_json_response"}
        upstream_error = payload.get("err") if isinstance(payload, dict) else None
        ok = bool(response.ok and not upstream_error)
        return {
            "ok": ok, "available": ok, "degraded": not ok, "required": False,
            "status_code": response.status_code,
            "data": payload, "error": upstream_error,
            "endpoint": path,  # deliberately excludes query credentials
            "fallback": None if ok else "continue_without_nami",
        }
    except Exception as exc:
        return {
            "ok": False, "available": False, "degraded": True, "required": False,
            "error": type(exc).__name__, "endpoint": path,
            "fallback": "continue_without_nami",
        }


def nami_capability_check() -> Dict[str, Any]:
    # The trial subscription is the football real-time package.  Probe an
    # entitled v5 endpoint instead of a basic-data endpoint, since Nami
    # authorizes those product packages independently.
    probe_date = datetime.now(ZoneInfo(AUTO_FETCH_TIMEZONE)).strftime("%Y%m%d")
    endpoint = "/api/v5/football/match/schedule/diary"
    check = call_nami(endpoint, {"date": probe_date})
    data = check.get("data") if isinstance(check.get("data"), dict) else {}
    results = data.get("results") if isinstance(data.get("results"), dict) else {}
    error = str(check.get("error") or "")
    return {
        "configured": bool(NAMI_API_USER and NAMI_API_SECRET), "ok": check.get("ok", False),
        "available": check.get("available", False), "degraded": check.get("degraded", True),
        "required": False, "fallback": "continue_without_nami" if not check.get("ok") else None,
        "status_code": check.get("status_code"),
        "api_version": "v5", "product": "football_realtime", "probe_endpoint": endpoint,
        "probe_date": probe_date,
        "ip_whitelist_required": "ip" in error.lower() and ("授权" in error or "unauthor" in error.lower()),
        "error_category": "ip_not_authorized" if "ip" in error.lower() else ("upstream_error" if error else None),
        "response_fields": sorted(data.keys()),
        "sample_count": len(results.get("match") or []),
        "competition_count": len(results.get("competition") or []),
        "team_count": len(results.get("team") or []),
    }


def response_list(result: Dict[str, Any]) -> List[Any]:
    data = result.get("data") or {}
    return data.get("response") if isinstance(data, dict) and isinstance(data.get("response"), list) else []


def response_first(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rows = response_list(result)
    return rows[0] if rows and isinstance(rows[0], dict) else None


def get_nested(d: Any, path: List[Any], default: Any = None) -> Any:
    cur = d
    for p in path:
        if isinstance(cur, dict):
            cur = cur.get(p)
        elif isinstance(cur, list) and isinstance(p, int) and 0 <= p < len(cur):
            cur = cur[p]
        else:
            return default
    return cur if cur is not None else default


def compact_result(result: Dict[str, Any]) -> Dict[str, Any]:
    data = result.get("data") or {}
    return {"ok": result.get("ok"), "status_code": result.get("status_code"), "request_url": result.get("request_url"), "results": data.get("results") if isinstance(data, dict) else None, "errors": data.get("errors") if isinstance(data, dict) else None, "response": data.get("response") if isinstance(data, dict) else data}


def fixture_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    fixture = row.get("fixture", {}) or {}
    league = row.get("league", {}) or {}
    teams = row.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    status = fixture.get("status", {}) or {}
    goals = row.get("goals", {}) or {}
    league_id = league.get("id")
    return {"fixture_id": fixture.get("id"), "date": fixture.get("date"), "timestamp": fixture.get("timestamp"), "venue": fixture.get("venue"), "referee": fixture.get("referee"), "league_id": league_id, "league": league.get("name"), "league_round": league.get("round"), "league_target_name": DEFAULT_TARGET_LEAGUES.get(league_id), "country": league.get("country"), "season": league.get("season"), "home_id": home.get("id"), "home": home.get("name"), "away_id": away.get("id"), "away": away.get("name"), "status": status.get("short"), "elapsed": status.get("elapsed"), "goals": goals}


def target_fixtures_for_date(date: str, timezone_name: str = "Asia/Shanghai") -> Dict[str, Any]:
    result = call_api_football("/fixtures", {"date": date, "timezone": timezone_name})
    rows = response_list(result)
    targets = [r for r in rows if isinstance(r, dict) and get_nested(r, ["league", "id"]) in TARGET_LEAGUE_IDS]
    summaries = [fixture_summary(r) for r in targets]
    return {"ok": result.get("ok"), "date": date, "timezone": timezone_name, "mode": "target_major_leagues_only", "target_league_ids": sorted(TARGET_LEAGUE_IDS), "all_count": len(rows), "target_count": len(summaries), "fixtures": summaries, "source_status_code": result.get("status_code")}


def parse_fixture_context(fixture_id: int) -> Dict[str, Any]:
    fixture_detail = call_api_football("/fixtures", {"id": fixture_id})
    row = response_first(fixture_detail)
    if not row:
        return {"fixture_id": fixture_id, "error": "fixture_not_found", "fixture_detail": compact_result(fixture_detail)}
    return {"fixture_detail": compact_result(fixture_detail), "fixture_row": row, **fixture_summary(row)}


def recent_form(rows: List[Any], team_id: Optional[int]) -> Dict[str, Any]:
    if not team_id:
        return {"available": False}
    s = {"available": True, "played": 0, "wins": 0, "draws": 0, "losses": 0, "goals_for": 0, "goals_against": 0, "last_results": []}
    for row in rows[:10]:
        teams = row.get("teams", {}) or {}
        goals = row.get("goals", {}) or {}
        home_id = get_nested(teams, ["home", "id"])
        away_id = get_nested(teams, ["away", "id"])
        gh, ga = goals.get("home"), goals.get("away")
        if gh is None or ga is None:
            continue
        if home_id == team_id:
            gf, gc = gh, ga
        elif away_id == team_id:
            gf, gc = ga, gh
        else:
            continue
        if gf > gc:
            res = "W"; s["wins"] += 1
        elif gf < gc:
            res = "L"; s["losses"] += 1
        else:
            res = "D"; s["draws"] += 1
        s["played"] += 1; s["goals_for"] += gf; s["goals_against"] += gc; s["last_results"].append(res)
    s["goal_diff"] = s["goals_for"] - s["goals_against"]
    return s


def standings_for_team(result: Dict[str, Any], team_id: Optional[int]) -> Optional[Dict[str, Any]]:
    groups = get_nested(response_list(result), [0, "league", "standings"], [])
    for group in groups:
        if not isinstance(group, list):
            continue
        for row in group:
            if get_nested(row, ["team", "id"]) == team_id:
                return {"rank": row.get("rank"), "team": get_nested(row, ["team", "name"]), "points": row.get("points"), "goalsDiff": row.get("goalsDiff"), "form": row.get("form"), "description": row.get("description"), "all": row.get("all")}
    return None


def season_stats_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    row = get_nested(result, ["data", "response"], {})
    if not isinstance(row, dict) or not row:
        return {"available": False}
    fixtures = row.get("fixtures", {}) or {}
    goals = row.get("goals", {}) or {}
    return {"available": True, "played_total": get_nested(fixtures, ["played", "total"]), "wins_total": get_nested(fixtures, ["wins", "total"]), "draws_total": get_nested(fixtures, ["draws", "total"]), "loses_total": get_nested(fixtures, ["loses", "total"]), "goals_for_avg": get_nested(goals, ["for", "average", "total"]), "goals_against_avg": get_nested(goals, ["against", "average", "total"]), "clean_sheet": (row.get("clean_sheet") or {}).get("total"), "failed_to_score": (row.get("failed_to_score") or {}).get("total")}


def injuries_summary(result: Dict[str, Any], home_id: Optional[int], away_id: Optional[int]) -> Dict[str, Any]:
    fetched_at = int(time.time())
    if not isinstance(result, dict) or not result.get("ok"):
        error = (result or {}).get("error") if isinstance(result, dict) else None
        status_code = (result or {}).get("status_code") if isinstance(result, dict) else None
        return {
            "available": False, "status": "fetch_failed", "source": "api_football",
            "fetched_at": fetched_at, "home_count": None, "away_count": None,
            "home": [], "away": [],
            "error": error or (f"http_{status_code}" if status_code is not None else "missing_result"),
        }
    home, away = [], []
    for item in response_list(result):
        team_id = get_nested(item, ["team", "id"])
        x = {"team": get_nested(item, ["team", "name"]), "player": get_nested(item, ["player", "name"]), "reason": get_nested(item, ["player", "reason"]), "type": get_nested(item, ["player", "type"])}
        if team_id == home_id:
            home.append(x)
        elif team_id == away_id:
            away.append(x)
    return {
        "available": True, "status": "available" if home or away else "confirmed_empty",
        "source": "api_football", "fetched_at": fetched_at,
        "home_count": len(home), "away_count": len(away), "home": home, "away": away,
    }


def as_float(value: Any) -> Optional[float]:
    try:
        parsed = float(str(value).strip())
        return parsed if math.isfinite(parsed) else None
    except Exception:
        return None


def empty_market_snapshot() -> Dict[str, Any]:
    keys = ["1x2", "asian_handicap", "over_under", "btts", "home_team_total", "away_team_total"]
    return {
        "available": False, "updated_at": None, "bookmaker_count": 0,
        "markets": {key: [] for key in keys},
        # primary is kept as a compatibility alias. It now contains consensus lines.
        "primary": {key: None for key in keys},
        "consensus_main_line": {key: None for key in keys},
        "data_status": {key: "data_missing" for key in keys},
    }


def choose_primary_line(markets: List[Dict[str, Any]], prefer_zero: bool = False, prefer_value: Optional[float] = None) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Dict[str, Any]]] = []
    for market in markets:
        for line in market.get("lines", []) or []:
            lf = as_float(line.get("line"))
            if lf is None:
                continue
            score = abs(lf) if prefer_zero else abs(lf - (prefer_value if prefer_value is not None else lf))
            candidates.append((score, {"bookmaker": market.get("bookmaker"), **line}))
    return sorted(candidates, key=lambda x: x[0])[0][1] if candidates else None


def _median(values: List[Any]) -> Optional[float]:
    nums = [x for x in (as_float(v) for v in values) if x is not None]
    return round(float(median(nums)), 4) if nums else None


def _bookmaker_identity(value: Any) -> str:
    return " ".join(str(value or "unknown").strip().casefold().split())


def _dedupe_bookmaker_rows(rows: List[Dict[str, Any]], keys: Tuple[str, ...]) -> List[Dict[str, Any]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_bookmaker_identity(row.get("bookmaker")), []).append(row)
    deduped = []
    for identity, duplicates in grouped.items():
        merged = {key: _median([row.get(key) for row in duplicates]) for key in keys}
        merged.update({"bookmaker": str(duplicates[0].get("bookmaker") or identity), "bookmaker_identity": identity, "duplicate_quote_count": len(duplicates)})
        deduped.append(merged)
    return deduped


def _price_dispersion(rows: List[Dict[str, Any]], keys: Tuple[str, ...]) -> Dict[str, Any]:
    spreads = {}
    for key in keys:
        values = [as_float(row.get(key)) for row in rows]
        valid = [value for value in values if value is not None and value > 1.0]
        spreads[key] = round(max(valid) - min(valid), 4) if len(valid) >= 2 else 0.0
    maximum = max(spreads.values(), default=0.0)
    probability_rows = []
    for row in rows:
        implied = {key: 1.0 / as_float(row.get(key)) for key in keys if (as_float(row.get(key)) or 0) > 1.0}
        total = sum(implied.values())
        if len(implied) == len(keys) and total > 0:
            probability_rows.append({key: implied[key] / total for key in keys})
    probability_spreads = {
        key: round(max(row[key] for row in probability_rows) - min(row[key] for row in probability_rows), 6)
        if len(probability_rows) >= 2 else 0.0
        for key in keys
    }
    maximum_probability_spread = max(probability_spreads.values(), default=0.0)
    median_probabilities = {key: _median([row[key] for row in probability_rows]) for key in keys} if probability_rows else {}
    median_total = sum(value or 0.0 for value in median_probabilities.values())
    consensus_probabilities = {
        key: round((median_probabilities[key] or 0.0) / median_total, 6) for key in keys
    } if len(median_probabilities) == len(keys) and median_total > 0 else None
    return {
        "price_spread_by_selection": spreads,
        "maximum_price_spread": maximum,
        "price_spread_within_reference": maximum <= MAX_CONSENSUS_PRICE_SPREAD,
        "maximum_allowed_price_spread": MAX_CONSENSUS_PRICE_SPREAD,
        "no_vig_probability_spread_by_selection": probability_spreads,
        "maximum_no_vig_probability_spread": maximum_probability_spread,
        "consensus_no_vig_probabilities": consensus_probabilities,
        "probability_aggregation_method": "median_of_bookmaker_level_no_vig_probabilities_then_normalized",
        "dispersion_eligible": maximum_probability_spread <= MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD,
        "maximum_allowed_no_vig_probability_spread": MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD,
    }


def _consensus_1x2(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    complete = [x for x in _dedupe_bookmaker_rows(rows, ("home", "draw", "away")) if all((as_float(x.get(k)) or 0) > 1.0 for k in ("home", "draw", "away"))]
    if not complete:
        return None
    return {
        "method": "median_all_complete_bookmakers", "source": "complete_company_array", "bookmaker_count": len(complete),
        "home": _median([x.get("home") for x in complete]),
        "draw": _median([x.get("draw") for x in complete]),
        "away": _median([x.get("away") for x in complete]),
        **_price_dispersion(complete, ("home", "draw", "away")),
    }


def _consensus_line(markets: List[Dict[str, Any]], price_keys: Tuple[str, str]) -> Optional[Dict[str, Any]]:
    raw_by_line: Dict[float, List[Dict[str, Any]]] = {}
    for market in markets:
        for line in market.get("lines", []) or []:
            value = as_float(line.get("line"))
            if value is None or any((as_float(line.get(key)) or 0) <= 1.0 for key in price_keys):
                continue
            raw_by_line.setdefault(value, []).append({"bookmaker": market.get("bookmaker"), **line})
    by_line = {value: _dedupe_bookmaker_rows(rows, price_keys) for value, rows in raw_by_line.items()}
    if not by_line:
        return None
    # The main line is the line quoted by the largest number of books. Ties are
    # broken by the most balanced median two-way prices, then proximity to the
    # median quoted line. This avoids a systematic bias toward shallower lines.
    reference_line = float(median([value for value, rows in by_line.items() for _ in rows]))
    ranked = []
    for value, rows in by_line.items():
        left, right = _median([r.get(price_keys[0]) for r in rows]), _median([r.get(price_keys[1]) for r in rows])
        balance = abs(left - right) if left is not None and right is not None else 999.0
        ranked.append((-len(rows), balance, abs(value - reference_line), value, rows, left, right))
    _, _, _, value, rows, left, right = sorted(ranked, key=lambda x: x[:4])[0]
    return {
        "method": "modal_line_then_balanced_median_prices", "source": "complete_company_array", "line": value,
        "tie_break_reference_line": round(reference_line, 4),
        price_keys[0]: left, price_keys[1]: right,
        "bookmaker_count": len(rows), "bookmakers": sorted({str(r.get('bookmaker')) for r in rows if r.get('bookmaker')}),
        **_price_dispersion(rows, price_keys),
    }


def _two_way_market(values: List[Dict[str, Any]], labels: Tuple[str, str]) -> Dict[str, Any]:
    entry = {labels[0]: None, labels[1]: None, "raw_values": values}
    for value in values:
        raw = str(value.get("value", "")).strip().lower()
        if raw == labels[0].lower(): entry[labels[0]] = value.get("odd")
        if raw == labels[1].lower(): entry[labels[1]] = value.get("odd")
    return entry


def _line_market(values: List[Dict[str, Any]], prefixes: Tuple[str, str], keys: Tuple[str, str]) -> List[Dict[str, Any]]:
    lines: Dict[str, Dict[str, Any]] = {}
    for value in values:
        raw = str(value.get("value", "")).strip()
        for prefix, key in zip(prefixes, keys):
            if raw.lower().startswith(prefix.lower() + " "):
                line = raw[len(prefix):].strip()
                lines.setdefault(line, {"line": line, keys[0]: None, keys[1]: None, "raw_values": []})
                lines[line][key] = value.get("odd")
                lines[line]["raw_values"].append(value)
    return list(lines.values())


def extract_market_snapshot(odds_result: Dict[str, Any]) -> Dict[str, Any]:
    rows = response_list(odds_result)
    if not rows:
        return empty_market_snapshot()
    row = rows[0]
    snapshot = empty_market_snapshot()
    bookmakers = row.get("bookmakers", []) or []
    snapshot.update({"available": True, "updated_at": row.get("update"), "bookmaker_count": len(bookmakers)})
    for bm in bookmakers:
        bm_name = bm.get("name")
        for bet in bm.get("bets", []) or []:
            name = bet.get("name")
            values = bet.get("values", []) or []
            if name == "Match Winner":
                entry = {"bookmaker": bm_name, "raw_values": values, "home": None, "draw": None, "away": None}
                for v in values:
                    if str(v.get("value")) == "Home": entry["home"] = v.get("odd")
                    if str(v.get("value")) == "Draw": entry["draw"] = v.get("odd")
                    if str(v.get("value")) == "Away": entry["away"] = v.get("odd")
                snapshot["markets"]["1x2"].append(entry)
            elif name == "Asian Handicap":
                snapshot["markets"]["asian_handicap"].append({"bookmaker": bm_name, "lines": _line_market(values, ("Home", "Away"), ("home", "away"))})
            elif name == "Goals Over/Under":
                snapshot["markets"]["over_under"].append({"bookmaker": bm_name, "lines": _line_market(values, ("Over", "Under"), ("over", "under"))})
            elif name in ("Both Teams Score", "Both Teams To Score"):
                snapshot["markets"]["btts"].append({"bookmaker": bm_name, **_two_way_market(values, ("yes", "no"))})
            elif name in ("Home Team Total Goals", "Home Goals Over/Under"):
                snapshot["markets"]["home_team_total"].append({"bookmaker": bm_name, "lines": _line_market(values, ("Over", "Under"), ("over", "under"))})
            elif name in ("Away Team Total Goals", "Away Goals Over/Under"):
                snapshot["markets"]["away_team_total"].append({"bookmaker": bm_name, "lines": _line_market(values, ("Over", "Under"), ("over", "under"))})
    consensus = {
        "1x2": _consensus_1x2(snapshot["markets"]["1x2"]),
        "asian_handicap": _consensus_line(snapshot["markets"]["asian_handicap"], ("home", "away")),
        "over_under": _consensus_line(snapshot["markets"]["over_under"], ("over", "under")),
        "btts": _consensus_1x2([]),
        "home_team_total": _consensus_line(snapshot["markets"]["home_team_total"], ("over", "under")),
        "away_team_total": _consensus_line(snapshot["markets"]["away_team_total"], ("over", "under")),
    }
    btts = snapshot["markets"]["btts"]
    complete_btts = [row for row in _dedupe_bookmaker_rows(btts, ("yes", "no")) if (as_float(row.get("yes")) or 0) > 1 and (as_float(row.get("no")) or 0) > 1]
    consensus["btts"] = ({"method": "median_all_complete_bookmakers", "source": "complete_company_array", "bookmaker_count": len(complete_btts), "yes": _median([x.get("yes") for x in complete_btts]), "no": _median([x.get("no") for x in complete_btts]), **_price_dispersion(complete_btts, ("yes", "no"))} if complete_btts else None)
    snapshot["consensus_main_line"] = consensus
    snapshot["primary"] = dict(consensus)
    snapshot["data_status"] = {key: ("available" if value else "data_missing") for key, value in consensus.items()}
    return snapshot


def odds_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    rows = response_list(result)
    if not rows:
        return {"available": False}
    bookmakers = rows[0].get("bookmakers", []) or []
    key_markets = []
    wanted = {"Match Winner", "Asian Handicap", "Goals Over/Under", "Both Teams Score", "Both Teams To Score", "Home Team Total Goals", "Away Team Total Goals", "Home Goals Over/Under", "Away Goals Over/Under", "Double Chance"}
    for bm in bookmakers:
        for bet in bm.get("bets", []) or []:
            if bet.get("name") in wanted:
                key_markets.append({"bookmaker": bm.get("name"), "market": bet.get("name"), "values": bet.get("values", [])[:12]})
        if len(key_markets) >= 12:
            break
    return {"available": True, "bookmaker_count": len(bookmakers), "key_markets": key_markets}


def prediction_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    row = response_first(result)
    if not row:
        return {"available": False}
    p = row.get("predictions", {}) or {}
    return {"available": True, "winner": p.get("winner"), "win_or_draw": p.get("win_or_draw"), "advice": p.get("advice"), "percent": p.get("percent"), "comparison": row.get("comparison")}


def data_quality(coverage: Dict[str, Any]) -> Dict[str, Any]:
    must = ["standings", "home_recent_10", "away_recent_10", "home_team_season_stats", "away_team_season_stats"]
    strong = ["odds_prematch", "predictions", "injuries", "lineups"]
    missing_must = [x for x in must if not coverage.get(x, {}).get("has_data")]
    missing_strong = [x for x in strong if not coverage.get(x, {}).get("has_data")]
    level = "high" if not missing_must and coverage.get("odds_prematch", {}).get("has_data") else ("medium" if len(missing_must) <= 1 else "low")
    return {"level": level, "missing_must": missing_must, "missing_strong": missing_strong}


def lineup_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize API-Football confirmed XI without confusing empty/predicted data with fetch failures."""
    fetched_at = int(time.time())
    if not isinstance(result, dict) or not result.get("ok"):
        return {
            "available": False, "confirmed": False, "status": "fetch_failed",
            "source": "api_football", "fetched_at": fetched_at,
            "confidence": 0.0, "error": (result or {}).get("error") or ("http_" + str((result or {}).get("status_code"))) if result else "missing_result",
            "teams": []
        }
    rows = response_list(result)
    if not rows:
        return {
            "available": False, "confirmed": False, "status": "not_available_yet",
            "source": "api_football", "fetched_at": fetched_at,
            "confidence": 0.0, "teams": []
        }
    teams = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        team = row.get("team") or {}
        coach = row.get("coach") or {}
        start = row.get("startXI") or []
        starters = []
        for item in start:
            player = (item or {}).get("player") or {}
            starters.append({
                "id": player.get("id"), "name": player.get("name"),
                "number": player.get("number"), "pos": player.get("pos"),
                "grid": player.get("grid")
            })
        teams.append({
            "team_id": team.get("id"), "team_name": team.get("name"),
            "formation": row.get("formation"),
            "coach": {"id": coach.get("id"), "name": coach.get("name")},
            "starting_xi": starters, "starter_count": len(starters)
        })
    counts = [x.get("starter_count", 0) for x in teams]
    confirmed = len(teams) >= 2 and all(x >= 11 for x in counts[:2])
    confidence = 1.0 if confirmed else (0.8 if len(teams) >= 2 and all(x > 0 for x in counts[:2]) else 0.5)
    return {
        "available": bool(teams), "confirmed": confirmed,
        "status": "confirmed" if confirmed else "partial",
        "source": "api_football", "fetched_at": fetched_at,
        "confidence": confidence, "teams": teams
    }


def team_news_snapshot(data: Dict[str, Any], stage: str) -> Optional[Dict[str, Any]]:
    if stage not in ("T-1h", "T-15m", "Closing"):
        return None
    structured = data.get("structured_inputs") if isinstance(data.get("structured_inputs"), dict) else {}
    return {
        "captured_at": int(data.get("generated_at") or time.time()),
        "stage": stage,
        "injuries": structured.get("injuries") or {"available": False, "reason": "data_missing"},
        "lineups": structured.get("lineups") or {"available": False, "confirmed": False, "status": "data_missing", "teams": []},
    }


def collect_prematch_data(fixture_id: int, include_raw: bool = False) -> Dict[str, Any]:
    ctx = parse_fixture_context(fixture_id)
    if ctx.get("error"):
        return {"ok": False, **ctx}
    home_id, away_id, league_id, season = ctx.get("home_id"), ctx.get("away_id"), ctx.get("league_id"), ctx.get("season")
    calls: Dict[str, Dict[str, Any]] = {"predictions": call_api_football("/predictions", {"fixture": fixture_id}), "odds_prematch": call_api_football("/odds", {"fixture": fixture_id}), "injuries": call_api_football("/injuries", {"fixture": fixture_id}), "lineups": call_api_football("/fixtures/lineups", {"fixture": fixture_id}), "events": call_api_football("/fixtures/events", {"fixture": fixture_id}), "statistics": call_api_football("/fixtures/statistics", {"fixture": fixture_id}), "players": call_api_football("/fixtures/players", {"fixture": fixture_id})}
    if home_id and away_id:
        calls["head_to_head_last_10"] = call_api_football("/fixtures/headtohead", {"h2h": f"{home_id}-{away_id}", "last": 10})
    if home_id:
        calls["home_recent_10"] = call_api_football("/fixtures", {"team": home_id, "last": 10})
    if away_id:
        calls["away_recent_10"] = call_api_football("/fixtures", {"team": away_id, "last": 10})
    if league_id and season and home_id:
        calls["home_team_season_stats"] = call_api_football("/teams/statistics", {"league": league_id, "season": season, "team": home_id})
    if league_id and season and away_id:
        calls["away_team_season_stats"] = call_api_football("/teams/statistics", {"league": league_id, "season": season, "team": away_id})
    if league_id and season:
        calls["standings"] = call_api_football("/standings", {"league": league_id, "season": season})
    coverage = {}
    for name, result in calls.items():
        data = result.get("data") or {}
        response = data.get("response") if isinstance(data, dict) else None
        coverage[name] = {"ok": result.get("ok"), "status_code": result.get("status_code"), "results": data.get("results") if isinstance(data, dict) else None, "has_data": bool(response), "request_url": result.get("request_url")}
    market_snapshot = extract_market_snapshot(calls.get("odds_prematch", {}))
    normalized_lineups = lineup_summary(calls.get("lineups", {}))
    structured = {"standings": {"home": standings_for_team(calls.get("standings", {}), home_id), "away": standings_for_team(calls.get("standings", {}), away_id)}, "recent_form_last_10": {"home": recent_form(response_list(calls.get("home_recent_10", {})), home_id), "away": recent_form(response_list(calls.get("away_recent_10", {})), away_id)}, "season_stats": {"home": season_stats_summary(calls.get("home_team_season_stats", {})), "away": season_stats_summary(calls.get("away_team_season_stats", {}))}, "head_to_head_count": len(response_list(calls.get("head_to_head_last_10", {}))), "injuries": injuries_summary(calls.get("injuries", {}), home_id, away_id), "prediction": prediction_summary(calls.get("predictions", {})), "odds": odds_summary(calls.get("odds_prematch", {})), "odds_market_snapshot": market_snapshot, "lineups": normalized_lineups, "lineups_available": normalized_lineups.get("available", False), "snapshot_requirements": {"required_markets": ["1x2", "asian_handicap", "over_under"], "optional_markets": ["btts", "home_team_total", "away_team_total"], "metrics_supported": ["line_crossing", "continuous_strengthening", "reversal", "market_saturation", "cross_market_divergence", "fundamental_revalidation"]}}
    quality = data_quality(coverage)
    return {"ok": True, "version": VERSION, "generated_at": int(time.time()), "fixture": {k: v for k, v in ctx.items() if k not in ["fixture_detail", "fixture_row"]}, "coverage": coverage, "data_quality": quality, "structured_inputs": structured, "shadow_summary": make_shadow_summary(ctx, structured, quality), "football_ai_prompt": make_prompt(ctx), "raw_data_pack": {"fixture_detail": ctx.get("fixture_detail"), **{k: compact_result(v) for k, v in calls.items()}} if include_raw else None}



def collect_stage_snapshot_data(fixture_id: int, stage: str) -> Dict[str, Any]:
    """Low-cost collection for automatic market snapshots.
    Early stages collect fixture context + odds only. Late stages add injuries/lineups.
    Full fundamentals remain available through /shadow/analyze-fixture.
    """
    ctx = parse_fixture_context(fixture_id)
    if ctx.get("error"):
        return {"ok": False, **ctx}
    calls: Dict[str, Dict[str, Any]] = {
        "odds_prematch": call_api_football("/odds", {"fixture": fixture_id})
    }
    if stage in ("T-1h", "T-15m", "Closing"):
        calls["injuries"] = call_api_football("/injuries", {"fixture": fixture_id})
        calls["lineups"] = call_api_football("/fixtures/lineups", {"fixture": fixture_id})
    coverage = {}
    for name, result in calls.items():
        data = result.get("data") or {}
        response = data.get("response") if isinstance(data, dict) else None
        coverage[name] = {"ok": result.get("ok"), "status_code": result.get("status_code"), "results": data.get("results") if isinstance(data, dict) else None, "has_data": bool(response), "request_url": result.get("request_url")}
    market_snapshot = extract_market_snapshot(calls.get("odds_prematch", {}))
    home_id, away_id = ctx.get("home_id"), ctx.get("away_id")
    normalized_lineups = lineup_summary(calls.get("lineups", {})) if "lineups" in calls else {"available": False, "confirmed": False, "status": "not_requested_at_this_stage", "source": "api_football", "confidence": 0.0, "teams": []}
    return {
        "ok": True,
        "version": VERSION,
        "generated_at": int(time.time()),
        "fixture": {k: v for k, v in ctx.items() if k not in ["fixture_detail", "fixture_row"]},
        "coverage": coverage,
        "data_quality": {"level": "market_snapshot", "missing_must": [], "missing_strong": []},
        "structured_inputs": {
            "odds_market_snapshot": market_snapshot,
            "injuries": injuries_summary(calls.get("injuries", {}), home_id, away_id) if "injuries" in calls else {"available": False, "reason": "not_requested_at_this_stage"},
            "lineups": normalized_lineups,
            "lineups_available": normalized_lineups.get("available", False),
            "collection_profile": "late_market_plus_team_news" if stage in ("T-1h", "T-15m", "Closing") else "market_only"
        },
        "shadow_summary": {
            "directional_notes": ["自动阶段快照：仅采集该阶段必要数据"],
            "risk_flags": [],
            "data_quality": "market_snapshot"
        }
    }

def make_shadow_summary(ctx: Dict[str, Any], structured: Dict[str, Any], quality: Dict[str, Any]) -> Dict[str, Any]:
    notes, risks = [], []
    home, away = ctx.get("home"), ctx.get("away")
    hr = get_nested(structured, ["recent_form_last_10", "home"], {}) or {}
    ar = get_nested(structured, ["recent_form_last_10", "away"], {}) or {}
    if hr.get("available") and ar.get("available"):
        if hr.get("goal_diff", 0) > ar.get("goal_diff", 0): notes.append(f"近10场净胜球偏向 {home}")
        elif ar.get("goal_diff", 0) > hr.get("goal_diff", 0): notes.append(f"近10场净胜球偏向 {away}")
        if hr.get("losses", 0) > ar.get("losses", 0): notes.append(f"近期稳定性偏向 {away}")
        elif ar.get("losses", 0) > hr.get("losses", 0): notes.append(f"近期稳定性偏向 {home}")
    pred = structured.get("prediction", {}) or {}
    if pred.get("available"):
        notes.append(f"官方预测：{pred.get('advice')}，概率 {pred.get('percent')}")
    odds = structured.get("odds", {}) or {}
    if odds.get("available"):
        notes.append(f"赔率覆盖可用，博彩公司数量：{odds.get('bookmaker_count')}")
    if get_nested(structured, ["odds_market_snapshot", "available"]):
        notes.append("已保存 1X2 / Asian Handicap / O/U 市场快照")
    if not structured.get("lineups_available"):
        risks.append("lineups_not_available_yet")
    if quality.get("missing_must"):
        risks.append("missing_must_data:" + ",".join(quality.get("missing_must", [])))
    return {"directional_notes": notes, "risk_flags": risks, "data_quality": quality.get("level")}


def make_prompt(ctx: Dict[str, Any]) -> str:
    return "你是足球AI影子分析。先独立生成 Pure Fundamental Script，再读取完整 T-X 时间轴。盘口差异只能触发事实复核，不能单独改写基本面。读取 1X2/AH/O-U，若可得同时读取 BTTS/Home TT/Away TT；任何缺失必须标记 data_missing，禁止用当前盘口反推历史。最终依次检查 no-vig 概率、Edge、EV、Script Coverage、Crowding、Line Movement、Lineup Confidence 与 Death Path，并允许 PASS。"


def fixture_datetime_utc(summary: Dict[str, Any]) -> Optional[datetime]:
    ts = summary.get("timestamp")
    if ts:
        try:
            return datetime.fromtimestamp(int(ts), tz=timezone.utc)
        except Exception:
            pass
    date_str = summary.get("date")
    if date_str:
        try:
            return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
        except Exception:
            return None
    return None


def normalize_stage(stage: str) -> str:
    raw = (stage or "manual").strip()
    key = STAGE_ALIASES.get(raw.lower())
    if key:
        return key
    for s in TRACKING_STAGES:
        if raw.lower().startswith(s["key"].lower()):
            return s["key"]
    return raw or "manual"


def tracking_plan_for_fixture(summary: Dict[str, Any]) -> Dict[str, Any]:
    dt = fixture_datetime_utc(summary)
    now = datetime.now(timezone.utc)
    if not dt:
        return {"fixture_id": summary.get("fixture_id"), "status": summary.get("status"), "tracking": []}
    items = []
    for stage in TRACKING_STAGES:
        when = dt + stage["offset"]
        items.append({"stage": stage["key"], "label": stage["label"], "purpose": stage["purpose"], "utc_time": when.isoformat(), "status": "due_or_passed" if now >= when else "pending", "action_url": f"/shadow/snapshot?fixture={summary.get('fixture_id')}&stage={stage['key']}"})
    return {"fixture_id": summary.get("fixture_id"), "match": f"{summary.get('home')} vs {summary.get('away')}", "kickoff_utc": dt.isoformat(), "status": summary.get("status"), "tracking": items}


def load_snapshot_store() -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        p = Path(SNAPSHOT_STORE_PATH)
        if not p.exists():
            return {"version": VERSION, "fixtures": {}}
        try:
            raw = p.read_bytes()
            if raw.startswith(b"\x1f\x8b"):
                raw = gzip.decompress(raw)
            store = json.loads(raw.decode("utf-8"))
            if not isinstance(store, dict):
                raise ValueError("snapshot store root must be an object")
            return store
        except Exception as exc:
            print("[SNAPSHOT_STORE] read failed: " + type(exc).__name__)
            raise SnapshotStoreReadError("snapshot_store_read_failed; existing_file_was_not_overwritten") from exc


def snapshot_backup_path() -> Path:
    p = Path(SNAPSHOT_STORE_PATH)
    return p.with_suffix(p.suffix + ".bak")


def load_snapshot_backup_store() -> Dict[str, Any]:
    backup = snapshot_backup_path()
    if not backup.exists():
        raise SnapshotStoreReadError("snapshot_store_backup_not_found")
    try:
        raw = backup.read_bytes()
        if raw.startswith(b"\x1f\x8b"):
            raw = gzip.decompress(raw)
        store = json.loads(raw.decode("utf-8"))
        if not isinstance(store, dict):
            raise ValueError("snapshot backup root must be an object")
        return store
    except SnapshotStoreReadError:
        raise
    except Exception as exc:
        print("[SNAPSHOT_STORE] backup read failed: " + type(exc).__name__)
        raise SnapshotStoreReadError("snapshot_store_backup_read_failed") from exc


def inspect_snapshot_store_file(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {"exists": False, "readable": False, "status": "data_missing"}
    try:
        encoded = path.read_bytes()
        raw = gzip.decompress(encoded) if encoded.startswith(b"\x1f\x8b") else encoded
        store = json.loads(raw.decode("utf-8"))
        if not isinstance(store, dict):
            raise ValueError("store root must be an object")
        fixtures = store.get("fixtures") or {}
        snapshot_count = sum(len(rows or []) for rows in fixtures.values())
        queue = store.get("fundamental_revalidation_queue") or {}
        return {
            "exists": True, "readable": True, "status": "ok", "gzip": encoded.startswith(b"\x1f\x8b"),
            "size_bytes": len(encoded), "raw_size_bytes": len(raw),
            "compression_ratio": round(len(encoded) / len(raw), 6) if raw else 1.0,
            "capacity_state": "warning" if len(encoded) >= SNAPSHOT_STORE_WARN_BYTES else "normal",
            "warning_threshold_bytes": SNAPSHOT_STORE_WARN_BYTES,
            "content_sha256": hashlib.sha256(encoded).hexdigest(),
            "store_version": store.get("version"),
            "fixture_count": len(fixtures), "snapshot_count": snapshot_count,
            "portfolio_count": len(store.get("portfolio_runs") or {}),
            "revalidation_task_count": len(queue),
            "pending_revalidation_count": sum(task.get("status") == "pending" for task in queue.values()),
        }
    except Exception as exc:
        return {"exists": True, "readable": False, "status": "corrupt", "error": type(exc).__name__}


def snapshot_store_integrity() -> Dict[str, Any]:
    primary = inspect_snapshot_store_file(Path(SNAPSHOT_STORE_PATH))
    backup = inspect_snapshot_store_file(snapshot_backup_path())
    return {
        "primary": primary, "backup": backup,
        "operational": primary.get("readable") is True,
        "recovery_ready": backup.get("readable") is True,
        "capacity_ok": primary.get("capacity_state") in (None, "normal"),
        "automatic_restore": False,
    }


def write_snapshot_store(store: Dict[str, Any]) -> bool:
    with SNAPSHOT_STORE_LOCK:
        tmp = None
        backup_tmp = None
        try:
            p = Path(SNAPSHOT_STORE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
            tmp = p.with_suffix(p.suffix + ".tmp")
            backup = snapshot_backup_path()
            backup_tmp = backup.with_suffix(backup.suffix + ".tmp")
            raw = json.dumps(store, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            encoded = gzip.compress(raw, compresslevel=6) if SNAPSHOT_STORE_GZIP else raw
            tmp.write_bytes(encoded)
            # Keep the last successfully committed primary as the rollback point.
            # On the first write, seed the backup with the same valid baseline.
            previous_encoded = p.read_bytes() if p.exists() else encoded
            backup_tmp.write_bytes(previous_encoded)
            backup_tmp.replace(backup)
            tmp.replace(p)
            return True
        except Exception as exc:
            print("[SNAPSHOT_STORE] write failed: " + type(exc).__name__)
            if tmp is not None:
                try:
                    tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            if backup_tmp is not None:
                try:
                    backup_tmp.unlink(missing_ok=True)
                except Exception:
                    pass
            raise SnapshotStoreWriteError("snapshot_store_write_failed") from exc


def get_fixture_snapshots(fixture: Any) -> List[Dict[str, Any]]:
    rows = load_snapshot_store().get("fixtures", {}).get(str(fixture), [])
    return sorted(rows, key=lambda x: (STAGE_ORDER.index(x.get("stage")) if x.get("stage") in STAGE_ORDER else 999, x.get("snapshot_at", 0)))


def latest_prematch_snapshot(rows: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    eligible = [row for row in rows if row.get("stage") in PREMATCH_STAGE_ORDER and get_nested(row, ["stage_timing_audit", "status"]) != "invalid" and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"]
    return max(eligible, key=lambda row: (PREMATCH_STAGE_ORDER.index(row["stage"]), int(row.get("snapshot_at") or 0)), default=None)


def audit_stage_timing(stage: str, observed_at: Any, kickoff_at: Any) -> Dict[str, Any]:
    observed, kickoff = _parse_timestamp(observed_at), _parse_timestamp(kickoff_at)
    if observed is None or kickoff is None:
        return {"status": "data_missing", "decision_eligible": True, "reason": "kickoff_or_observation_time_missing"}
    seconds_before = kickoff - observed
    if stage == "Opening":
        valid = seconds_before > 0
        expected, tolerance = None, None
    else:
        expected_map = {"T-24h": 86400, "T-12h": 43200, "T-6h": 21600, "T-3h": 10800, "T-1h": 3600, "T-15m": 900, "Closing": 0}
        tolerance_map = {"T-24h": 21600, "T-12h": 10800, "T-6h": 5400, "T-3h": 2700, "T-1h": 1800, "T-15m": 900, "Closing": 900}
        expected, tolerance = expected_map.get(stage), tolerance_map.get(stage)
        valid = expected is not None and -300 <= seconds_before and abs(seconds_before - expected) <= tolerance
    return {
        "status": "valid" if valid else "invalid", "decision_eligible": valid,
        "stage": stage, "observed_at": observed, "kickoff_at": kickoff,
        "seconds_before_kickoff": seconds_before, "expected_seconds_before_kickoff": expected,
        "tolerance_seconds": tolerance, "reason": None if valid else "stage_timestamp_mismatch",
    }


def audit_timeline_sequence(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    ordered = sorted((row for row in rows if row.get("stage") in PREMATCH_STAGE_ORDER), key=lambda row: PREMATCH_STAGE_ORDER.index(row["stage"]))
    result: Dict[str, Dict[str, Any]] = {}
    previous_stage, previous_at = None, None
    for row in ordered:
        stage, observed_at = row["stage"], _parse_timestamp(row.get("snapshot_at"))
        explicitly_available = row.get("import_status") == "available"
        native_available = row.get("import_status") is None and bool(get_nested(row, ["market_snapshot", "available"]))
        usable = (explicitly_available or native_available) and get_nested(row, ["stage_timing_audit", "status"]) != "invalid"
        if not usable or observed_at is None:
            result[stage] = {"status": "data_missing", "decision_eligible": False, "reason": "stage_unavailable_or_timestamp_missing"}
            continue
        if previous_at is not None and observed_at <= previous_at:
            result[stage] = {"status": "invalid", "decision_eligible": False, "reason": "non_monotonic_stage_timestamp", "previous_stage": previous_stage, "previous_snapshot_at": previous_at, "snapshot_at": observed_at}
            continue
        result[stage] = {"status": "valid", "decision_eligible": True, "reason": None, "previous_stage": previous_stage, "previous_snapshot_at": previous_at, "snapshot_at": observed_at}
        previous_stage, previous_at = stage, observed_at
    return result


def complete_prematch_timeline(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_stage = {row.get("stage"): row for row in rows if row.get("stage") in PREMATCH_STAGE_ORDER}
    timeline = []
    for stage in PREMATCH_STAGE_ORDER:
        row = by_stage.get(stage)
        if row:
            usable = row.get("import_status") != "data_missing" and bool(get_nested(row, ["market_snapshot", "available"])) and get_nested(row, ["stage_timing_audit", "status"]) != "invalid" and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"
            timeline.append({**row, "timeline_status": "available" if usable else "data_missing", "synthetic_placeholder": False})
        else:
            timeline.append({
                "stage": stage, "timeline_status": "data_missing", "import_status": "data_missing",
                "missing_reason": "historical_stage_not_captured", "snapshot_at": None,
                "market_snapshot": empty_market_snapshot(), "market_dynamics": None,
                "synthetic_placeholder": True, "backfilled_from_current": False,
            })
    return timeline


def audit_line_movement_timeline(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    timeline = complete_prematch_timeline(rows)
    available = [row for row in timeline if row.get("timeline_status") == "available"]
    comparable = [row for row in available if get_nested(row, ["market_dynamics", "comparison_status"]) == "compared"]
    latest = latest_prematch_snapshot(available)
    latest_comparable = get_nested(latest or {}, ["market_dynamics", "comparison_status"]) == "compared"
    eligible = len(available) >= 2 and latest_comparable
    return {
        "decision_eligible": eligible,
        "available_stage_count": len(available), "required_minimum_available_stages": 2,
        "comparable_stage_count": len(comparable), "latest_stage": (latest or {}).get("stage"),
        "latest_comparison_status": get_nested(latest or {}, ["market_dynamics", "comparison_status"]) or "data_missing",
        "reason": None if eligible else "line_movement_requires_two_real_comparable_stages",
        "missing_stages": [row["stage"] for row in timeline if row.get("timeline_status") == "data_missing"],
        "current_odds_used_as_history": False,
    }


def force_pass_decision(decision: Dict[str, Any], reasons: Any) -> Dict[str, Any]:
    additions = reasons if isinstance(reasons, list) else [reasons]
    decision.setdefault("pass_reasons", []).extend(str(reason) for reason in additions if reason)
    decision["pass_reasons"] = list(dict.fromkeys(decision["pass_reasons"]))
    decision["decision"] = "PASS"
    decision["best_market"] = None
    decision["edge"] = None
    decision["ev"] = None
    tiers = decision.get("recommendation_tiers")
    if isinstance(tiers, dict):
        for key in ("first_choice_high_consistency", "second_choice_higher_return", "high_variance_single"):
            tiers[key] = None
    return decision


def apply_line_movement_gate(decision: Dict[str, Any], history: List[Dict[str, Any]]) -> Dict[str, Any]:
    audit = audit_line_movement_timeline(history)
    latest = latest_prematch_snapshot(history)
    decision["line_movement"] = (latest or {}).get("market_dynamics") or {"status": "data_missing"}
    decision["line_movement_audit"] = audit
    if not audit.get("decision_eligible"):
        force_pass_decision(decision, audit.get("reason") or "line_movement_insufficient")
    return decision


def snapshot_stage_usable(row: Dict[str, Any]) -> bool:
    return (
        row.get("import_status") != "data_missing"
        and bool(get_nested(row, ["market_snapshot", "available"]))
        and get_nested(row, ["stage_timing_audit", "status"]) != "invalid"
        and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"
    )


def save_snapshot(record: Dict[str, Any]) -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store(); store.setdefault("fixtures", {}).setdefault(str(record["fixture"]), [])
        existing_stage = next((row for row in store["fixtures"][str(record["fixture"])] if row.get("stage") == record.get("stage")), None)
        if existing_stage and snapshot_stage_usable(existing_stage) and not snapshot_stage_usable(record):
            return {
                "saved": False, "preserved_existing": True, "reason": "snapshot_quality_regression_rejected",
                "path": SNAPSHOT_STORE_PATH, "fixture": record["fixture"], "stage": record["stage"],
                "existing_snapshot_at": existing_stage.get("snapshot_at"), "rejected_snapshot_at": record.get("snapshot_at"),
                "revalidation_task_created": False, "revalidation_task_id": None,
                "downstream_revalidation_tasks_created": 0,
            }
        rows = [r for r in store["fixtures"][str(record["fixture"])] if r.get("stage") != record.get("stage")]
        rows.append(record)
        rows = sorted(rows, key=lambda x: (STAGE_ORDER.index(x.get("stage")) if x.get("stage") in STAGE_ORDER else 999, x.get("snapshot_at", 0)))
        sequence_audit = audit_timeline_sequence(rows)
        for timeline_row in rows:
            if timeline_row.get("stage") in PREMATCH_STAGE_ORDER:
                timeline_row["sequence_timing_audit"] = sequence_audit.get(timeline_row.get("stage"), {"status": "data_missing", "decision_eligible": False})
        changed_index = PREMATCH_STAGE_ORDER.index(record.get("stage")) if record.get("stage") in PREMATCH_STAGE_ORDER else None
        downstream_revalidation_tasks = []
        if changed_index is not None:
            versions = store.get("fundamental_versions", {}).get(str(record["fixture"]), [])
            by_version = {int(version.get("version_number") or 0): version for version in versions}
            for row in rows:
                row_stage = row.get("stage")
                if row_stage not in PREMATCH_STAGE_ORDER or PREMATCH_STAGE_ORDER.index(row_stage) <= changed_index:
                    continue
                timing_invalid = get_nested(row, ["stage_timing_audit", "status"]) == "invalid" or get_nested(row, ["sequence_timing_audit", "status"]) == "invalid"
                if timing_invalid:
                    dynamics = {
                        "stage": row_stage, "comparison_status": "data_missing",
                        "revalidation_trigger": {"triggered": False, "reasons": []},
                        "reason": "invalid_stage_or_sequence_timing",
                        "data_missing": list(((row.get("market_snapshot") or {}).get("data_status") or {}).keys()),
                    }
                else:
                    valid_history = [candidate for candidate in rows if get_nested(candidate, ["stage_timing_audit", "status"]) != "invalid" and get_nested(candidate, ["sequence_timing_audit", "status"]) != "invalid"]
                    dynamics = compare_market_snapshots(valid_history, row.get("market_snapshot") or empty_market_snapshot(), row_stage)
                dynamics["information_search"] = row.get("information_search") or get_nested(row, ["market_dynamics", "information_search"])
                version_number = int(row.get("fundamental_version_number") or 0)
                current_version, previous_version = by_version.get(version_number), by_version.get(version_number - 1)
                old_matches = get_nested(row, ["market_dynamics", "classification_audit", "matched_classifications"], []) or []
                classification = classify_market_move_details(dynamics, previous_version, get_nested(current_version or {}, ["script"]), "Model-Market Divergence" in old_matches)
                if "Fundamental Confirmed" in old_matches and "Fundamental Confirmed" not in classification["matched_classifications"]:
                    classification["matched_classifications"].insert(0, "Fundamental Confirmed")
                    classification["classification_bases"]["Fundamental Confirmed"] = "verified_fundamental_version_changed_preserved_from_persisted_audit"
                    classification["classification"] = "Fundamental Confirmed"
                    classification["basis"] = classification["classification_bases"]["Fundamental Confirmed"]
                dynamics["classification"] = classification["classification"]
                dynamics["classification_audit"] = classification
                row["market_dynamics"] = dynamics
                task = _enqueue_revalidation(store, str(record["fixture"]), row) if not timing_invalid else None
                if task:
                    downstream_revalidation_tasks.append(task.get("task_id"))
        store["fixtures"][str(record["fixture"])] = rows
        revalidation_task = _enqueue_revalidation(store, str(record["fixture"]), record)
        store["version"] = VERSION
        write_snapshot_store(store)
    return {
        "saved": True, "path": SNAPSHOT_STORE_PATH, "fixture": record["fixture"], "stage": record["stage"],
        "revalidation_task_created": bool(revalidation_task),
        "revalidation_task_id": (revalidation_task or {}).get("task_id"),
        "downstream_revalidation_tasks_created": len(downstream_revalidation_tasks),
    }


def _enqueue_revalidation(store: Dict[str, Any], fixture: str, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    trigger = get_nested(row, ["market_dynamics", "revalidation_trigger"], {}) or {}
    if not trigger.get("triggered"):
        return None
    signal_hash = _content_hash({"market": get_nested(row, ["market_snapshot", "consensus_main_line"]), "reasons": trigger.get("reasons"), "classification": get_nested(row, ["market_dynamics", "classification"])})
    task_id = _content_hash({"fixture": fixture, "stage": row.get("stage"), "signal_hash": signal_hash})[:24]
    queue = store.setdefault("fundamental_revalidation_queue", {})
    existing = queue.get(task_id)
    if existing:
        return None
    for older in queue.values():
        if older.get("fixture") == fixture and older.get("stage") == row.get("stage") and older.get("status") == "pending":
            older.update({"status": "superseded", "superseded_at": int(time.time()), "superseded_by": task_id})
    task = {
        "task_id": task_id, "fixture": fixture, "stage": row.get("stage"),
        "snapshot_at": row.get("snapshot_at"), "created_at": int(time.time()), "status": "pending", "signal_hash": signal_hash,
        "reasons": list(trigger.get("reasons") or []),
        "classification": get_nested(row, ["market_dynamics", "classification"]),
        "policy": "fact_recheck_required; market_move_alone_must_not_modify_fundamentals",
    }
    queue[task_id] = task
    _trim_revalidation_queue(queue)
    return task


def _trim_revalidation_queue(queue: Dict[str, Dict[str, Any]], limit: int = 500) -> Dict[str, Any]:
    pending = [task for task in queue.values() if task.get("status") == "pending"]
    terminal = sorted((task for task in queue.values() if task.get("status") != "pending"), key=lambda task: int(task.get("created_at") or 0), reverse=True)
    keep_terminal = max(0, limit - len(pending))
    remove = terminal[keep_terminal:]
    for task in remove:
        queue.pop(task.get("task_id"), None)
    return {
        "limit": limit, "pending_count": len(pending), "terminal_retained": min(len(terminal), keep_terminal),
        "terminal_removed": len(remove), "over_capacity": len(pending) > limit,
        "policy": "pending_tasks_are_never_silently_evicted",
    }


def resolve_revalidation_tasks(fixture: str, version_record: Optional[Dict[str, Any]], model_market_divergence: bool = False, resolved_through_stage: Optional[str] = None) -> int:
    if not version_record or version_record.get("version_number") is None:
        return 0
    changed_information = list(version_record.get("changed_information") or [])
    prior_version_exists = int(version_record.get("version_number") or 0) > 1
    substantive_change = prior_version_exists and bool(changed_information)
    resolution_classification = "Fundamental Confirmed" if substantive_change else ("Model-Market Divergence" if model_market_divergence else "Market-Only Move")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        changed = 0
        through_index = PREMATCH_STAGE_ORDER.index(resolved_through_stage) if resolved_through_stage in PREMATCH_STAGE_ORDER else len(PREMATCH_STAGE_ORDER)
        for task in (store.get("fundamental_revalidation_queue") or {}).values():
            task_stage = task.get("stage")
            task_index = PREMATCH_STAGE_ORDER.index(task_stage) if task_stage in PREMATCH_STAGE_ORDER else len(PREMATCH_STAGE_ORDER)
            if task.get("fixture") == str(fixture) and task.get("status") == "pending" and task_index <= through_index:
                task.update({
                    "status": "revalidated", "resolved_at": int(time.time()),
                    "fundamental_version_number": version_record.get("version_number"),
                    "evidence_status": "verified_fundamental_chain",
                    "resolution_classification": resolution_classification,
                    "fundamental_changed": substantive_change,
                    "changed_information": changed_information if substantive_change else [],
                    "probability_change": version_record.get("probability_change"),
                    "best_market_change": version_record.get("best_market_change"),
                    "resolved_through_stage": resolved_through_stage,
                })
                changed += 1
        if changed:
            store["version"] = VERSION
            write_snapshot_store(store)
        return changed


def record_incomplete_revalidation_attempt(fixture: str, chain_audit: Dict[str, Any], attempted_through_stage: Optional[str] = None) -> int:
    """Keep a triggered task pending while proving that a factual recheck was attempted."""
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        changed = 0
        through_index = PREMATCH_STAGE_ORDER.index(attempted_through_stage) if attempted_through_stage in PREMATCH_STAGE_ORDER else len(PREMATCH_STAGE_ORDER)
        for task in (store.get("fundamental_revalidation_queue") or {}).values():
            task_stage = task.get("stage")
            task_index = PREMATCH_STAGE_ORDER.index(task_stage) if task_stage in PREMATCH_STAGE_ORDER else len(PREMATCH_STAGE_ORDER)
            if task.get("fixture") == str(fixture) and task.get("status") == "pending" and task_index <= through_index:
                task.update({
                    "attempt_count": int(task.get("attempt_count") or 0) + 1,
                    "last_attempt_at": int(time.time()),
                    "last_attempt_outcome": "insufficient_verified_fundamental_evidence",
                    "attempted_through_stage": attempted_through_stage,
                    "required_evidence": sorted(set(
                        list(chain_audit.get("critical_missing") or [])
                        + list(chain_audit.get("critical_provenance_missing") or [])
                        + list((chain_audit.get("critical_semantic_issues") or {}).keys())
                        + list((chain_audit.get("critical_structural_issues") or {}).keys())
                    )),
                })
                changed += 1
        if changed:
            store["version"] = VERSION
            write_snapshot_store(store)
        return changed


def revalidation_queue_view(tasks: List[Dict[str, Any]], now_ts: Optional[int] = None) -> List[Dict[str, Any]]:
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    stage_weight = {stage: index for index, stage in enumerate(PREMATCH_STAGE_ORDER, start=1)}
    reason_weight = {"cross_market_divergence": 5, "significant_line_move": 4, "abnormal_price_move": 3}
    result = []
    for original in tasks:
        task = dict(original)
        age = max(0, now_ts - int(task.get("created_at") or now_ts))
        score = stage_weight.get(task.get("stage"), 0) + max((reason_weight.get(reason, 1) for reason in task.get("reasons") or []), default=0)
        if age >= 1800:
            score += 2
        pending = task.get("status") == "pending"
        attempted = pending and int(task.get("attempt_count") or 0) > 0
        overdue = pending and age >= 1800
        workflow_state = (
            "overdue_awaiting_evidence" if overdue and attempted else
            "overdue_unattempted" if overdue else
            "awaiting_evidence" if attempted else
            "queued" if pending else str(task.get("status") or "unknown")
        )
        task.update({
            "age_seconds": age,
            "overdue": overdue,
            "attempted": attempted,
            "workflow_state": workflow_state,
            "next_action": "collect_required_evidence" if attempted else ("run_fundamental_revalidation" if pending else None),
            "priority_score": score,
            "priority": "critical" if score >= 12 else ("high" if score >= 8 else "normal"),
        })
        result.append(task)
    return sorted(result, key=lambda task: (task.get("status") != "pending", -task["priority_score"], task.get("created_at") or 0))


def _parse_timestamp(value: Any) -> Optional[int]:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value)
    try:
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
    except (TypeError, ValueError):
        return None


def _import_consensus(source: Dict[str, Any]) -> Dict[str, Any]:
    result: Dict[str, Any] = {}
    for market in ("1x2", "asian_handicap", "over_under", "btts", "home_team_total", "away_team_total"):
        row = source.get(market) or {}
        if row.get("status") != "available":
            result[market] = None
            continue
        prices = row.get("median_prices") or {}
        item = {key: as_float(value) for key, value in prices.items()}
        if market not in ("1x2", "btts"):
            item["line"] = as_float(row.get("line"))
        item["method"] = "imported_consensus_main_line"
        item["source"] = "upstream_consensus_fallback"
        item["fallback_reason"] = "complete_company_array_unavailable"
        item["bookmaker_count"] = row.get("bookmaker_coverage")
        result[market] = item
    return result


def _import_company_markets(rows: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    markets = empty_market_snapshot()["markets"]
    grouped: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for raw in rows or []:
        market_name = str(raw.get("market_name") or "").lower()
        if any(token in market_name for token in ("first half", "1st half", "second half", "2nd half")):
            continue
        market = str(raw.get("market") or "").lower()
        if market not in markets:
            continue
        bookmaker = str(raw.get("bookmaker_name") or raw.get("bookmaker_id") or "unknown")
        selection = str(raw.get("selection") or "").strip().lower()
        price = as_float(raw.get("price"))
        if market in ("1x2", "btts"):
            key = (market, bookmaker, "")
            item = grouped.setdefault(key, {"bookmaker": bookmaker, "raw_values": []})
            normalized = {"home": "home", "draw": "draw", "away": "away", "yes": "yes", "no": "no"}.get(selection)
            if normalized:
                item[normalized] = price
            item["raw_values"].append(raw)
        else:
            line = str(raw.get("line") if raw.get("line") is not None else "")
            key = (market, bookmaker, line)
            item = grouped.setdefault(key, {"bookmaker": bookmaker, "line": as_float(line), "raw_values": []})
            side = "home" if selection.startswith("home") else "away" if selection.startswith("away") else "over" if selection.startswith("over") else "under" if selection.startswith("under") else None
            if side:
                item[side] = price
            item["raw_values"].append(raw)
    by_book: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for (market, bookmaker, line), item in grouped.items():
        if market in ("1x2", "btts"):
            markets[market].append(item)
        else:
            book = by_book.setdefault((market, bookmaker), {"bookmaker": bookmaker, "lines": []})
            book["lines"].append({key: value for key, value in item.items() if key != "bookmaker"})
    for (market, _), item in by_book.items():
        markets[market].append(item)
    return markets


def imported_market_snapshot(stage: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = empty_market_snapshot()
    if stage.get("status") != "available":
        return snapshot
    consensus = _import_consensus(stage.get("consensus_main_line") or {})
    markets = _import_company_markets(stage.get("company_market_array") or [])
    complete_imported_btts = [row for row in _dedupe_bookmaker_rows(markets["btts"], ("yes", "no")) if (as_float(row.get("yes")) or 0) > 1 and (as_float(row.get("no")) or 0) > 1]
    recalculated_markets = {
        "1x2": _consensus_1x2(markets["1x2"]),
        "asian_handicap": _consensus_line(markets["asian_handicap"], ("home", "away")),
        "over_under": _consensus_line(markets["over_under"], ("over", "under")),
        "btts": ({
            "method": "median_all_complete_bookmakers",
            "bookmaker_count": len(complete_imported_btts),
            "yes": _median([row.get("yes") for row in complete_imported_btts]),
            "no": _median([row.get("no") for row in complete_imported_btts]),
            **_price_dispersion(complete_imported_btts, ("yes", "no")),
        } if complete_imported_btts else None),
        "home_team_total": _consensus_line(markets["home_team_total"], ("over", "under")),
        "away_team_total": _consensus_line(markets["away_team_total"], ("over", "under")),
    }
    consensus_audit = {}
    for key, recalculated in recalculated_markets.items():
        if recalculated:
            recalculated["source"] = "complete_company_array"
            consensus[key] = recalculated
            consensus_audit[key] = "recalculated_from_company_array"
        elif consensus.get(key):
            consensus_audit[key] = "upstream_fallback_company_array_unavailable"
        else:
            consensus_audit[key] = "data_missing"
    snapshot.update({
        "available": any(consensus.values()),
        "updated_at": stage.get("latest_observed_at"),
        "bookmaker_count": stage.get("bookmaker_count", 0),
        "markets": markets,
        "primary": dict(consensus),
        "consensus_main_line": consensus,
        "consensus_audit": consensus_audit,
        "data_status": {key: ("available" if value else "data_missing") for key, value in consensus.items()},
    })
    return snapshot


def _content_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def normalize_information_search(value: Any, snapshot_at: Optional[int] = None) -> Optional[Dict[str, Any]]:
    if not isinstance(value, dict):
        return None
    allowed_statuses = {"not_searched", "no_evidence_found", "suspected_unconfirmed", "confirmed"}
    status = str(value.get("status") or "not_searched").strip().lower()
    invalid_reasons: List[str] = []
    if status not in allowed_statuses:
        invalid_reasons.append("unsupported_status")
        status = "not_searched"
    raw_refs = value.get("evidence_refs")
    if raw_refs is None:
        raw_refs = []
    if not isinstance(raw_refs, list):
        raw_refs = []
        invalid_reasons.append("evidence_refs_not_array")
    normalized_refs: List[Any] = []
    for ref in raw_refs[:20]:
        if isinstance(ref, str):
            cleaned = ref.strip()
            if cleaned and len(cleaned) <= 500 and not any(ord(char) < 32 for char in cleaned):
                normalized_refs.append(cleaned)
            else:
                invalid_reasons.append("invalid_string_reference")
        elif isinstance(ref, dict):
            source = str(ref.get("source") or "").strip()
            locator = str(ref.get("url") or ref.get("id") or ref.get("title") or "").strip()
            observed_at = ref.get("observed_at")
            observed_ts = _parse_timestamp(observed_at) if observed_at is not None else None
            future = observed_at is not None and snapshot_at is not None and (observed_ts is None or observed_ts > snapshot_at + 300)
            if source and locator and len(source) <= 100 and len(locator) <= 1000 and not future:
                normalized_refs.append({key: ref.get(key) for key in ("source", "url", "id", "title", "observed_at") if ref.get(key) is not None})
            else:
                invalid_reasons.append("invalid_structured_reference")
        else:
            invalid_reasons.append("unsupported_reference_type")
    if len(raw_refs) > 20:
        invalid_reasons.append("evidence_reference_limit_exceeded")
    result = {key: value.get(key) for key in ("query", "searched_at", "notes") if value.get(key) is not None}
    result.update({
        "status": status,
        "evidence_refs": normalized_refs,
        "evidence_audit": {
            "valid_count": len(normalized_refs),
            "invalid_count": len(invalid_reasons),
            "invalid_reasons": sorted(set(invalid_reasons)),
            "decision_eligible": bool(normalized_refs),
        },
    })
    return result


def import_prematch_packet(packet: Dict[str, Any], store_override: Optional[Dict[str, Any]] = None, persist: bool = True) -> Dict[str, Any]:
    if not isinstance(packet, dict):
        raise HTTPException(status_code=400, detail="packet_must_be_an_object")
    if packet.get("schema_version") != "shadow_prematch_packet_v1":
        raise HTTPException(status_code=400, detail="unsupported_schema_version")
    match = packet.get("match") or {}
    fixture = str(match.get("match_id") or "").strip()
    if not fixture:
        raise HTTPException(status_code=400, detail="missing_match_id")
    timeline = packet.get("timeline")
    if not isinstance(timeline, list):
        raise HTTPException(status_code=400, detail="timeline_must_be_an_array")
    seen = set()
    records: List[Dict[str, Any]] = []
    imported = []
    for stage_data in timeline:
        stage = normalize_stage(stage_data.get("stage"))
        if stage not in PREMATCH_STAGE_ORDER or stage in seen:
            raise HTTPException(status_code=400, detail={"error": "invalid_or_duplicate_stage", "stage": stage})
        seen.add(stage)
        status = "available" if stage_data.get("status") == "available" else "data_missing"
        market_snapshot = imported_market_snapshot(stage_data)
        snapshot_at = _parse_timestamp(stage_data.get("latest_observed_at") or stage_data.get("target_at")) or int(time.time())
        information_search = normalize_information_search(stage_data.get("information_search"), snapshot_at)
        source_hash = _content_hash({"stage": stage, "status": status, "market_snapshot": market_snapshot, "missing_reason": stage_data.get("reason"), "latest_observed_at": stage_data.get("latest_observed_at"), "target_at": stage_data.get("target_at"), "kickoff_utc": match.get("kickoff_utc"), "information_search": information_search})
        record = {
            "version": VERSION, "fixture": fixture, "external_fixture_id": fixture, "source": "pang_import",
            "stage": stage, "snapshot_at": snapshot_at,
            "fixture_info": match, "data_quality": packet.get("data_quality"), "coverage": {"source": "pang", "quote_count": stage_data.get("quote_count"), "bookmaker_count": stage_data.get("bookmaker_count")},
            "import_status": status, "missing_reason": stage_data.get("reason") if status == "data_missing" else None,
            "market_snapshot": market_snapshot, "source_content_hash": source_hash, "market_dynamics": None,
            "information_search": information_search,
        }
        record["stage_timing_audit"] = audit_stage_timing(stage, record["snapshot_at"], match.get("kickoff_utc"))
        records.append(record)
    with SNAPSHOT_STORE_LOCK:
        store = store_override if store_override is not None else load_snapshot_store()
        external_prematch = store.setdefault("external_prematch", {})
        previous_meta = external_prematch.get(fixture) or {}
        next_meta_content = {
            "schema_version": packet.get("schema_version"), "league": packet.get("league") or previous_meta.get("league"),
            "exported_at": packet.get("exported_at") or previous_meta.get("exported_at"), "match": match or previous_meta.get("match"),
            "required_timeline": packet.get("required_timeline") or previous_meta.get("required_timeline") or PREMATCH_STAGE_ORDER,
            "lineup_history": packet.get("lineup_history") if "lineup_history" in packet else previous_meta.get("lineup_history", []),
            "data_quality": packet.get("data_quality") or previous_meta.get("data_quality"),
        }
        previous_meta_content = {key: previous_meta.get(key) for key in next_meta_content}
        metadata_changed = not previous_meta or _content_hash(next_meta_content) != _content_hash(previous_meta_content)
        existing = store.setdefault("fixtures", {}).get(fixture, [])
        existing_by_stage = {row.get("stage"): row for row in existing}
        accepted = []
        for record in records:
            old = existing_by_stage.get(record["stage"])
            old_hash = (old or {}).get("source_content_hash")
            if old and not old_hash:
                old_hash = _content_hash({"stage": old.get("stage"), "status": old.get("import_status"), "market_snapshot": old.get("market_snapshot"), "missing_reason": old.get("missing_reason")})
            if old_hash == record["source_content_hash"]:
                action = "unchanged"
            elif old and int(record.get("snapshot_at") or 0) < int(old.get("snapshot_at") or 0):
                action = "stale_skipped"
            else:
                action = "inserted" if not old else "updated"
                accepted.append(record)
                existing_by_stage[record["stage"]] = record
            imported.append({"stage": record["stage"], "status": record["import_status"], "action": action})
        replaced_stages = {row["stage"] for row in accepted}
        merged = [row for row in existing if row.get("stage") not in replaced_stages] + accepted
        merged = sorted(merged, key=lambda x: (STAGE_ORDER.index(x.get("stage")) if x.get("stage") in STAGE_ORDER else 999, x.get("snapshot_at", 0)))
        sequence_audit = audit_timeline_sequence(merged)
        for row in merged:
            row["sequence_timing_audit"] = sequence_audit.get(row.get("stage"), {"status": "data_missing", "decision_eligible": False})
        prior_available: List[Dict[str, Any]] = []
        for row in merged:
            market_snapshot = row.get("market_snapshot") or empty_market_snapshot()
            if row.get("import_status") == "available" and get_nested(row, ["stage_timing_audit", "status"]) != "invalid" and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid":
                dynamics = compare_market_snapshots(prior_available, market_snapshot, row.get("stage"))
                prior_available.append(row)
            else:
                dynamics = {"stage": row.get("stage"), "comparison_status": "data_missing", "revalidation_trigger": {"triggered": False, "reasons": []}, "data_missing": list((market_snapshot.get("data_status") or {}).keys()), "reason": get_nested(row, ["stage_timing_audit", "reason"]) or row.get("missing_reason")}
            dynamics["information_search"] = row.get("information_search")
            classification = classify_market_move_details(dynamics, None, None)
            dynamics["classification"] = classification["classification"]
            dynamics["classification_audit"] = classification
            row["market_dynamics"] = dynamics
        accepted_stages = {row.get("stage") for row in accepted}
        queued = [task for row in merged if row.get("stage") in accepted_stages for task in [_enqueue_revalidation(store, fixture, row)] if task]
        store["fixtures"][fixture] = sorted(merged, key=lambda x: (STAGE_ORDER.index(x.get("stage")) if x.get("stage") in STAGE_ORDER else 999, x.get("snapshot_at", 0)))
        if accepted or metadata_changed:
            external_prematch[fixture] = {**next_meta_content, "imported_at": int(time.time())}
            store["version"] = VERSION
            if persist:
                write_snapshot_store(store)
    counts = {action: sum(1 for row in imported if row["action"] == action) for action in ("inserted", "updated", "unchanged", "stale_skipped")}
    return {"fixture": fixture, "match": f"{match.get('home_team_name')} vs {match.get('away_team_name')}", "stages": imported, "counts": counts, "changed": bool(accepted or metadata_changed), "metadata_changed": metadata_changed, "revalidation_tasks_created": len(queued)}


def import_prematch_packet_batch(packets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Validate and merge a batch in memory, then persist it exactly once."""
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        results = [import_prematch_packet(packet, store_override=store, persist=False) for packet in packets]
        return results, store


def line_from_primary(primary: Optional[Dict[str, Any]]) -> Optional[float]:
    return as_float(primary.get("line")) if primary else None


def odd_from_1x2(primary: Optional[Dict[str, Any]], key: str) -> Optional[float]:
    return as_float(primary.get(key)) if primary else None


def market_saturation(ms: Dict[str, Any]) -> Dict[str, Any]:
    def one(k: str) -> Dict[str, Any]:
        counts: Dict[str, int] = {}
        for bm in get_nested(ms, ["markets", k], []) or []:
            if k in ("1x2", "btts"): counts[k] = counts.get(k, 0) + 1
            else:
                for line in bm.get("lines", []) or []:
                    key = str(line.get("line")); counts[key] = counts.get(key, 0) + 1
        total = sum(counts.values())
        top_line, top_count = max(counts.items(), key=lambda x: x[1]) if counts else (None, 0)
        ratio = top_count / total if total else None
        return {"top_line": top_line, "top_count": top_count, "total": total, "ratio": ratio, "is_saturated": ratio is not None and ratio >= 0.6}
    return {key: one(key) for key in ("1x2", "asian_handicap", "over_under", "btts", "home_team_total", "away_team_total")}


def compare_market_snapshots(history: List[Dict[str, Any]], current: Dict[str, Any], stage: str) -> Dict[str, Any]:
    if stage in PREMATCH_STAGE_ORDER:
        current_index = PREMATCH_STAGE_ORDER.index(stage)
        earlier = [row for row in history if row.get("stage") in PREMATCH_STAGE_ORDER and PREMATCH_STAGE_ORDER.index(row.get("stage")) < current_index]
        previous = max(earlier, key=lambda row: (PREMATCH_STAGE_ORDER.index(row.get("stage")), int(row.get("snapshot_at") or 0)), default=None)
    else:
        previous = max(history, key=lambda row: int(row.get("snapshot_at") or 0), default=None)
    prev = previous.get("market_snapshot") if previous else None
    curp = current.get("primary", {}) if current else {}
    prevp = prev.get("primary", {}) if prev else {}
    def delta(market: str, field: str) -> Optional[float]:
        now, before = as_float(get_nested(curp, [market, field])), as_float(get_nested(prevp, [market, field]))
        return round(now - before, 6) if now is not None and before is not None else None

    movements = {
        "1x2": {field: delta("1x2", field) for field in ("home", "draw", "away")},
        "asian_handicap": {field: delta("asian_handicap", field) for field in ("line", "home", "away")},
        "over_under": {field: delta("over_under", field) for field in ("line", "over", "under")},
        "btts": {field: delta("btts", field) for field in ("yes", "no")},
        "home_team_total": {field: delta("home_team_total", field) for field in ("line", "over", "under")},
        "away_team_total": {field: delta("away_team_total", field) for field in ("line", "over", "under")},
    }
    market_keys = {
        "1x2": ("home", "draw", "away"), "asian_handicap": ("home", "away"),
        "over_under": ("over", "under"), "btts": ("yes", "no"),
        "home_team_total": ("over", "under"), "away_team_total": ("over", "under"),
    }
    probability_movements = {}
    for market, keys in market_keys.items():
        current_probs, current_method = market_no_vig_probabilities(curp.get(market) or {}, list(keys))
        previous_probs, previous_method = market_no_vig_probabilities(prevp.get(market) or {}, list(keys))
        same_line = market in ("1x2", "btts") or line_from_primary(curp.get(market)) == line_from_primary(prevp.get(market))
        comparable = bool(current_probs and previous_probs and same_line)
        probability_movements[market] = {
            "status": "compared" if comparable else ("line_changed" if current_probs and previous_probs and not same_line else "data_missing"),
            "deltas": {key: round(current_probs[key] - previous_probs[key], 6) for key in keys} if comparable else None,
            "current_method": current_method, "previous_method": previous_method,
        }
    ah_now, ah_prev = line_from_primary(curp.get("asian_handicap")), line_from_primary(prevp.get("asian_handicap"))
    ah_delta, ou_delta = movements["asian_handicap"]["line"], movements["over_under"]["line"]
    home_delta, away_delta = movements["1x2"]["home"], movements["1x2"]["away"]
    directions = [line_from_primary(get_nested(r, ["market_snapshot", "primary", "asian_handicap"])) for r in history[-3:]]
    directions = [x for x in directions if x is not None] + ([ah_now] if ah_now is not None else [])
    continuous = len(directions) >= 3 and all(directions[i] <= directions[i+1] for i in range(len(directions)-1))
    reversal = len(directions) >= 3 and ((directions[-3] < directions[-2] and directions[-1] < directions[-2]) or (directions[-3] > directions[-2] and directions[-1] > directions[-2]))
    missing = [key for key, status in (current.get("data_status") or {}).items() if status == "data_missing"]
    def opposed(a: Optional[float], b: Optional[float]) -> bool:
        return a not in (None, 0) and b not in (None, 0) and a * b < 0

    def probability_direction(market: str, selection: str) -> Optional[float]:
        row = probability_movements.get(market) or {}
        return as_float((row.get("deltas") or {}).get(selection)) if row.get("status") == "compared" else None

    directional_signals = {
        "home_strength": probability_direction("1x2", "home") if probability_direction("1x2", "home") is not None else (-home_delta if home_delta is not None else None),
        "away_strength": probability_direction("1x2", "away") if probability_direction("1x2", "away") is not None else (-away_delta if away_delta is not None else None),
        "asian_home_strength": probability_direction("asian_handicap", "home") if probability_direction("asian_handicap", "home") is not None else (-ah_delta if ah_delta is not None else None),
        "total_over_strength": probability_direction("over_under", "over") if probability_direction("over_under", "over") is not None else ou_delta,
        "btts_yes_strength": probability_direction("btts", "yes") if probability_direction("btts", "yes") is not None else (-movements["btts"]["yes"] if movements["btts"]["yes"] is not None else None),
        "home_total_over_strength": probability_direction("home_team_total", "over") if probability_direction("home_team_total", "over") is not None else movements["home_team_total"]["line"],
        "away_total_over_strength": probability_direction("away_team_total", "over") if probability_direction("away_team_total", "over") is not None else movements["away_team_total"]["line"],
    }

    divergence_pairs = {
        "1x2_vs_asian_handicap": opposed(directional_signals["home_strength"], directional_signals["asian_home_strength"]),
        "over_under_vs_btts": opposed(directional_signals["total_over_strength"], directional_signals["btts_yes_strength"]),
        "home_1x2_vs_home_team_total": opposed(directional_signals["home_strength"], directional_signals["home_total_over_strength"]),
        "away_1x2_vs_away_team_total": opposed(directional_signals["away_strength"], directional_signals["away_total_over_strength"]),
    }
    cross_market = any(divergence_pairs.values())
    probability_deltas = [value for row in probability_movements.values() for value in (row.get("deltas") or {}).values()]
    fallback_price_deltas = [
        value
        for market, movement in movements.items()
        if probability_movements.get(market, {}).get("status") == "data_missing"
        for field, value in movement.items() if field != "line"
    ]
    line_deltas = [movements[market]["line"] for market in ("asian_handicap", "over_under", "home_team_total", "away_team_total")]
    probability_price_signal = any(value is not None and abs(value) >= 0.03 for value in probability_deltas)
    decimal_fallback_signal = any(value is not None and abs(value) >= 0.10 for value in fallback_price_deltas)
    significant_price = probability_price_signal or decimal_fallback_signal
    significant_line = any(value is not None and abs(value) >= 0.25 for value in line_deltas)
    reasons = []
    if significant_line: reasons.append("significant_line_move")
    if significant_price: reasons.append("abnormal_price_move")
    if cross_market: reasons.append("cross_market_divergence")
    return {
        "stage": stage, "previous_stage": previous.get("stage") if previous else None,
        "comparison_status": "data_missing" if not previous else "compared",
        "market_movements": movements, "no_vig_probability_movements": probability_movements,
        "line_crossing": {"asian_handicap_delta": ah_delta, "asian_handicap_crossed_025_or_more": abs(ah_delta) >= 0.25 if ah_delta is not None else None, "over_under_delta": ou_delta, "over_under_crossed_025_or_more": abs(ou_delta) >= 0.25 if ou_delta is not None else None},
        "water_movement": {"home_1x2_odd_delta": home_delta, "away_1x2_odd_delta": away_delta},
        "continuous_strengthening": continuous, "reversal": reversal,
        "cross_market_divergence": cross_market, "cross_market_divergence_pairs": divergence_pairs,
        "cross_market_directional_signals": directional_signals,
        "cross_market_signal_policy": "bookmaker_level_no_vig_probability_first; line_or_decimal_price_fallback_only_when_probability_incomparable",
        "revalidation_trigger": {"triggered": bool(reasons), "reasons": reasons, "thresholds": {"line": 0.25, "no_vig_probability": 0.03, "decimal_price_fallback": 0.10}, "signal_audit": {"probability_signal": probability_price_signal, "decimal_fallback_signal": decimal_fallback_signal, "decimal_fallback_used_only_when_probability_missing": True}},
        "data_missing": missing,
        "market_saturation": market_saturation(current)
    }


FUNDAMENTAL_CHAIN = [
    "result_utility", "tactical_risk_appetite", "rotation_quality", "execution_ability",
    "tactical_matchup", "game_state_elasticity", "first_goal_state_transition",
    "open_game_beneficiary", "time_segment_strength", "goal_conversion"
]
CRITICAL_FUNDAMENTAL_MAX_AGE_SECONDS = {
    "result_utility": 48 * 3600,
    "rotation_quality": 24 * 3600,
    "execution_ability": 14 * 24 * 3600,
    "goal_conversion": 14 * 24 * 3600,
}
ROTATION_QUALITY_FIELDS = ["starting_xi_strength", "creativity", "finishing", "chemistry", "bench_strength", "bench_upgrade", "lineup_intent"]
MOVE_CLASSES = ["Fundamental Confirmed", "Likely Information-Driven", "Market-Only Move", "Cross-Market Divergence", "Model-Market Divergence"]


def pure_fundamental_script(data: Dict[str, Any]) -> Dict[str, Any]:
    """Create an odds-independent, evidence-addressable V4 extension.

    Unknown tactical fields remain data_missing instead of being guessed from prices.
    """
    si = data.get("structured_inputs") or {}
    lineup_available = bool(si.get("lineups_available"))
    injuries = si.get("injuries") or {}
    form = si.get("recent_form_last_10") or {}
    stats = si.get("season_stats") or {}
    standings = si.get("standings") or {}
    evidence = {
        "standings": standings, "recent_form_last_10": form, "season_stats": stats,
        "injuries": injuries, "lineups_available": lineup_available,
    }
    chain = {
        "result_utility": {"status": "data_missing", "home_win_draw_loss_utility": None, "away_win_draw_loss_utility": None, "reason": "competition objective/qualification rules are not supplied by current feeds"},
        "tactical_risk_appetite": {"status": "data_missing", "value": None, "depends_on": "result_utility and verified coach intent"},
        "rotation_quality": {"status": "available" if lineup_available else "data_missing", "starting_xi_strength": None, "creativity": None, "finishing": None, "chemistry": None, "bench_strength": None, "bench_upgrade": None, "lineup_intent": None},
        "execution_ability": {"status": "partial" if stats else "data_missing", "source": "season_stats and recent_form; no event-level xG/xThreat feed"},
        "tactical_matchup": {"status": "data_missing", "value": None, "reason": "formation/style/event-level data unavailable"},
        "game_state_elasticity": {"status": "data_missing", "states": {"0_0_persists": None, "home_scores_first": None, "away_scores_first": None, "draw_at_60": None, "trailing_last_30": None}},
        "first_goal_state_transition": {"status": "data_missing", "home_first": None, "away_first": None},
        "open_game_beneficiary": {"status": "data_missing", "team": None, "reason": "requires tactical risk and transition/conversion evidence"},
        "time_segment_strength": {"status": "data_missing", "segments": {"0_15": None, "16_30": None, "31_45": None, "46_60": None, "61_75": None, "76_90": None}},
        "goal_conversion": {"status": "partial" if stats else "data_missing", "strength_edge": None, "goal_edge": None, "margin_edge": None, "warning": "Strength Edge != Goal Edge != Margin Edge"},
    }
    content = json.dumps({"fixture": data.get("fixture"), "evidence": evidence, "chain": chain}, ensure_ascii=False, sort_keys=True, default=str)
    return {
        "schema": "pure_fundamental_script_v4_extension", "odds_independent": True,
        "generated_at": int(time.time()), "evidence": evidence, "chain_order": FUNDAMENTAL_CHAIN,
        "chain": chain, "content_hash": hashlib.sha256(content.encode("utf-8")).hexdigest(),
    }


def get_fundamental_versions(fixture: int) -> List[Dict[str, Any]]:
    rows = load_snapshot_store().get("fundamental_versions", {}).get(str(fixture), [])
    return sorted(rows, key=lambda x: (x.get("version_number", 0), x.get("created_at", 0)))


def normalize_revalidation_trigger(trigger: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    raw = trigger if isinstance(trigger, dict) else {}
    reasons = raw.get("reasons")
    if not isinstance(reasons, list):
        reasons = [reasons] if reasons else []
    normalized = dict(raw)
    normalized["triggered"] = bool(raw.get("triggered"))
    normalized["reasons"] = list(dict.fromkeys(str(reason).strip() for reason in reasons if str(reason).strip()))
    normalized["stage"] = normalize_stage(raw.get("stage")) if raw.get("stage") else None
    normalized["source"] = str(raw.get("source") or ("market_revalidation" if normalized["triggered"] else "pipeline_evaluation"))
    return normalized


def save_fundamental_version(fixture: int, script: Dict[str, Any], trigger: Dict[str, Any], previous: Optional[Dict[str, Any]] = None, probability_change: Optional[Dict[str, Any]] = None, best_market_change: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        rows = store.setdefault("fundamental_versions", {}).setdefault(str(fixture), [])
        requested_previous_version = (previous or {}).get("version_number")
        persisted_previous = max(rows, key=lambda row: (int(row.get("version_number") or 0), int(row.get("created_at") or 0)), default=None)
        previous = persisted_previous or previous
        comparison_rebased = bool(persisted_previous) and requested_previous_version != persisted_previous.get("version_number")
        old_script = (previous or {}).get("script") or {}
        has_previous = bool(previous)
        changed_sections = [key for key in FUNDAMENTAL_CHAIN if has_previous and get_nested(old_script, ["chain", key]) != get_nested(script, ["chain", key])]
        variable_changes = {key: {"before": get_nested(old_script, ["chain", key]), "after": get_nested(script, ["chain", key])} for key in changed_sections}
        if has_previous and old_script.get("estimator") != script.get("estimator"):
            changed_sections.append("fundamental_estimator")
            variable_changes["fundamental_estimator"] = {"before": old_script.get("estimator"), "after": script.get("estimator")}
        next_version = max((int(row.get("version_number") or 0) for row in rows), default=0) + 1
        normalized_trigger = normalize_revalidation_trigger(trigger)
        probability_change = probability_change or {"status": "data_missing", "reason": "no independent model probability supplied"}
        best_market_change = best_market_change or {"status": "data_missing", "reason": "decision inputs incomplete"}
        if comparison_rebased and "after" in probability_change:
            before_probability = get_nested(old_script, ["model", "probabilities", "1x2"])
            after_probability = probability_change.get("after")
            probability_change = {
                "before": before_probability, "after": after_probability,
                "delta": {key: round(after_probability[key] - before_probability[key], 6) for key in ("home", "draw", "away")} if isinstance(before_probability, dict) and isinstance(after_probability, dict) and all(key in before_probability and key in after_probability for key in ("home", "draw", "away")) else None,
            }
        if comparison_rebased and "after" in best_market_change:
            latest_best = get_nested(previous or {}, ["best_market_change", "after"])
            best_market_change = {**best_market_change, "before": latest_best, "changed": latest_best != best_market_change.get("after")}
        prior_version_number = (previous or {}).get("version_number")
        script_changed = has_previous and (old_script.get("content_hash") != script.get("content_hash") or bool(changed_sections))
        record = {
            "version_number": next_version, "previous_version_number": prior_version_number,
            "created_at": int(time.time()), "trigger": normalized_trigger,
            "changed_information": changed_sections, "variable_changes": variable_changes,
            "probability_change": probability_change,
            "best_market_change": best_market_change,
            "recalculation_audit": {
                "performed": True,
                "baseline_created": not has_previous,
                "comparison_available": has_previous,
                "requested_previous_version_number": requested_previous_version,
                "comparison_rebased_to_latest": comparison_rebased,
                "fundamental_changed": script_changed,
                "probability_changed": probability_change.get("before") != probability_change.get("after") if has_previous and "after" in probability_change else None,
                "best_market_changed": best_market_change.get("changed") if has_previous and "changed" in best_market_change else None,
                "triggered_by_market_revalidation": normalized_trigger["triggered"],
                "trigger_reasons": normalized_trigger["reasons"],
                "stage": normalized_trigger.get("stage"),
            },
            "script": script,
        }
        rows.append(record)
        store["fundamental_versions"][str(fixture)] = rows[-FUNDAMENTAL_VERSION_RETENTION:]
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def classify_market_move_details(dynamics: Dict[str, Any], previous_version: Optional[Dict[str, Any]], new_script: Optional[Dict[str, Any]], model_market_divergence: bool = False) -> Dict[str, Any]:
    information = dynamics.get("information_search") if isinstance(dynamics.get("information_search"), dict) else {}
    evidence_refs = information.get("evidence_refs") if isinstance(information.get("evidence_refs"), list) else []
    evidence_audit = information.get("evidence_audit") if isinstance(information.get("evidence_audit"), dict) else None
    evidence_eligible = bool(evidence_audit.get("decision_eligible")) if evidence_audit is not None else any(str(ref).strip() for ref in evidence_refs)
    suspected = information.get("status") == "suspected_unconfirmed" and evidence_eligible and bool(get_nested(dynamics, ["revalidation_trigger", "triggered"]))
    fundamental_changed = bool(previous_version and new_script and get_nested(previous_version, ["script", "content_hash"]) != new_script.get("content_hash"))
    conditions = [
        ("Fundamental Confirmed", fundamental_changed, "verified_fundamental_version_changed"),
        ("Model-Market Divergence", bool(model_market_divergence), "eligible_model_probability_differs_from_market"),
        ("Cross-Market Divergence", bool(dynamics.get("cross_market_divergence")), "cross_market_signals_disagree"),
        ("Likely Information-Driven", suspected, "explicit_unconfirmed_information_with_evidence_reference"),
    ]
    matched = [{"classification": name, "basis": basis} for name, active, basis in conditions if active]
    if not matched:
        matched = [{"classification": "Market-Only Move", "basis": "no_verified_fundamental_change"}]
    primary = matched[0]
    return {
        "classification": primary["classification"], "basis": primary["basis"],
        "matched_classifications": [row["classification"] for row in matched],
        "classification_bases": {row["classification"]: row["basis"] for row in matched},
        "primary_precedence": ["Fundamental Confirmed", "Model-Market Divergence", "Cross-Market Divergence", "Likely Information-Driven", "Market-Only Move"],
        "inferred_without_evidence": False,
    }


def classify_market_move(dynamics: Dict[str, Any], previous_version: Optional[Dict[str, Any]], new_script: Optional[Dict[str, Any]], model_market_divergence: bool = False) -> str:
    return classify_market_move_details(dynamics, previous_version, new_script, model_market_divergence)["classification"]


def no_vig_probabilities(odds: Dict[str, Any], keys: List[str]) -> Optional[Dict[str, float]]:
    implied = {key: (1.0 / as_float(odds.get(key))) for key in keys if as_float(odds.get(key)) and as_float(odds.get(key)) > 1.0}
    if len(implied) != len(keys):
        return None
    total = sum(implied.values())
    if not MIN_MARKET_IMPLIED_PROBABILITY_TOTAL <= total <= MAX_MARKET_IMPLIED_PROBABILITY_TOTAL:
        return None
    return {key: round(value / total, 6) for key, value in implied.items()}


def market_no_vig_probabilities(market: Dict[str, Any], keys: List[str]) -> Tuple[Optional[Dict[str, float]], str]:
    embedded = market.get("consensus_no_vig_probabilities") if isinstance(market.get("consensus_no_vig_probabilities"), dict) else None
    embedded_values = {key: as_float((embedded or {}).get(key)) for key in keys}
    embedded_valid = bool(embedded) and all(value is not None and 0 < value < 1 for value in embedded_values.values()) and abs(sum(embedded_values.values()) - 1.0) <= 0.02
    if embedded_valid:
        price_derived = no_vig_probabilities(market, keys)
        supplied_prices = [as_float(market.get(key)) for key in keys]
        all_prices_present = all(price is not None for price in supplied_prices)
        if all_prices_present and price_derived is None:
            return None, "embedded_probability_prices_invalid"
        if price_derived and max(abs(embedded_values[key] - price_derived[key]) for key in keys) > MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD:
            return None, "embedded_probability_price_mismatch"
        return {key: round(embedded_values[key], 6) for key in keys}, "bookmaker_level_no_vig_consensus"
    fallback = no_vig_probabilities(market, keys)
    return fallback, "no_vig_from_consensus_median_prices" if fallback else "invalid_or_missing_market_prices"


def poisson_probability_model(home_expected_goals: Any, away_expected_goals: Any, input_confidence: Any, provenance: Any, max_goals: int = 10) -> Dict[str, Any]:
    home_xg, away_xg, confidence = as_float(home_expected_goals), as_float(away_expected_goals), as_float(input_confidence)
    errors = []
    if home_xg is None or not 0.05 <= home_xg <= 6.0: errors.append("home_expected_goals_out_of_range")
    if away_xg is None or not 0.05 <= away_xg <= 6.0: errors.append("away_expected_goals_out_of_range")
    if confidence is None or not 0.0 <= confidence <= 1.0: errors.append("input_confidence_out_of_range")
    if not isinstance(provenance, dict) or not provenance.get("source") or provenance.get("uses_market_odds") is not False:
        errors.append("independent_provenance_required")
    if errors:
        return {"ok": False, "status": "invalid_input", "errors": errors}
    home_probs = [math.exp(-home_xg) * home_xg ** goals / math.factorial(goals) for goals in range(max_goals + 1)]
    away_probs = [math.exp(-away_xg) * away_xg ** goals / math.factorial(goals) for goals in range(max_goals + 1)]
    grid = {(home, away): home_probs[home] * away_probs[away] for home in range(max_goals + 1) for away in range(max_goals + 1)}
    mass = sum(grid.values())
    normalized = {score: probability / mass for score, probability in grid.items()}
    one_x_two = {
        "home": sum(p for (home, away), p in normalized.items() if home > away),
        "draw": sum(p for (home, away), p in normalized.items() if home == away),
        "away": sum(p for (home, away), p in normalized.items() if home < away),
    }
    totals = {"over_2_5": sum(p for (home, away), p in normalized.items() if home + away >= 3)}
    totals["under_2_5"] = 1.0 - totals["over_2_5"]
    btts_yes = sum(p for (home, away), p in normalized.items() if home > 0 and away > 0)
    half_lines = (0.5, 1.5, 2.5, 3.5, 4.5)
    team_totals = {}
    for team, index in (("home", 0), ("away", 1)):
        team_totals[team] = {}
        for line in half_lines:
            threshold = int(line + 0.5)
            over = sum(p for score, p in normalized.items() if score[index] >= threshold)
            key = str(line).replace(".", "_")
            team_totals[team][f"over_{key}"] = round(over, 6)
            team_totals[team][f"under_{key}"] = round(1.0 - over, 6)
    top_scores = sorted(normalized.items(), key=lambda item: item[1], reverse=True)[:8]
    distributions = {"goal_difference": {}, "total_goals": {}, "home_goals": {}, "away_goals": {}}
    for (home, away), probability in normalized.items():
        for name, value in (("goal_difference", home - away), ("total_goals", home + away), ("home_goals", home), ("away_goals", away)):
            distributions[name][str(value)] = distributions[name].get(str(value), 0.0) + probability
    status = "ready" if confidence >= 0.6 else "insufficient_confidence"
    return {
        "ok": True, "status": status, "method": "independent_poisson_v1", "uses_market_odds": False,
        "inputs": {"home_expected_goals": home_xg, "away_expected_goals": away_xg, "input_confidence": confidence, "provenance": provenance},
        "probabilities": {
            "1x2": {key: round(value, 6) for key, value in one_x_two.items()},
            "over_under_2_5": {key: round(value, 6) for key, value in totals.items()},
            "btts": {"yes": round(btts_yes, 6), "no": round(1.0 - btts_yes, 6)},
            "home_team_totals": team_totals["home"], "away_team_totals": team_totals["away"],
            "top_scores": [{"score": f"{home}-{away}", "probability": round(probability, 6)} for (home, away), probability in top_scores],
            "settlement_distributions": {name: {key: round(value, 8) for key, value in rows.items()} for name, rows in distributions.items()},
        },
        "truncated_tail_mass": round(1.0 - mass, 10),
        "model_hash": _content_hash({"home_xg": home_xg, "away_xg": away_xg, "confidence": confidence, "provenance": provenance, "method": "independent_poisson_v1"}),
    }


def fundamental_expected_goals(inputs: Dict[str, Any]) -> Dict[str, Any]:
    required_rates = ["league_home_rate", "league_away_rate", "home_attack_rate", "home_defense_rate", "away_attack_rate", "away_defense_rate"]
    rates = {key: as_float(inputs.get(key)) for key in required_rates}
    errors = [f"{key}_out_of_range" for key, value in rates.items() if value is None or not 0.1 <= value <= 5.0]
    samples = {key: as_float(inputs.get(key)) for key in ("home_sample_size", "away_sample_size", "league_sample_size")}
    if samples["home_sample_size"] is None or not 1 <= samples["home_sample_size"] <= 100: errors.append("home_sample_size_out_of_range")
    if samples["away_sample_size"] is None or not 1 <= samples["away_sample_size"] <= 100: errors.append("away_sample_size_out_of_range")
    if samples["league_sample_size"] is None or not 10 <= samples["league_sample_size"] <= 5000: errors.append("league_sample_size_out_of_range")
    metric_type = str(inputs.get("metric_type") or "").lower()
    if metric_type not in ("xg", "goals"): errors.append("metric_type_must_be_xg_or_goals")
    home_adjustment = as_float(inputs.get("home_adjustment", 1.0))
    away_adjustment = as_float(inputs.get("away_adjustment", 1.0))
    if home_adjustment is None or not 0.8 <= home_adjustment <= 1.2: errors.append("home_adjustment_out_of_range")
    if away_adjustment is None or not 0.8 <= away_adjustment <= 1.2: errors.append("away_adjustment_out_of_range")
    lineup_confidence = as_float(inputs.get("lineup_confidence"))
    if lineup_confidence is None or not 0.0 <= lineup_confidence <= 1.0: errors.append("lineup_confidence_out_of_range")
    provenance = inputs.get("provenance")
    if not isinstance(provenance, dict) or not provenance.get("source") or provenance.get("uses_market_odds") is not False:
        errors.append("independent_provenance_required")
    if errors:
        return {"ok": False, "status": "invalid_input", "errors": errors}
    home_base = rates["home_attack_rate"] * rates["away_defense_rate"] / rates["league_home_rate"]
    away_base = rates["away_attack_rate"] * rates["home_defense_rate"] / rates["league_away_rate"]
    home_xg = max(0.05, min(6.0, home_base * home_adjustment))
    away_xg = max(0.05, min(6.0, away_base * away_adjustment))
    sample_confidence = min(1.0, min(samples["home_sample_size"], samples["away_sample_size"]) / 10.0) * min(1.0, samples["league_sample_size"] / 50.0)
    metric_quality = 0.9 if metric_type == "xg" else 0.7
    confidence = round(0.5 * sample_confidence + 0.3 * metric_quality + 0.2 * lineup_confidence, 4)
    status = "ready" if confidence >= 0.6 else "insufficient_confidence"
    audit = {
        "formula": "team_attack_rate * opponent_defense_rate / league_venue_rate * explicit_adjustment",
        "home_base": round(home_base, 6), "away_base": round(away_base, 6),
        "sample_confidence": round(sample_confidence, 6), "metric_quality": metric_quality,
    }
    return {
        "ok": True, "status": status, "method": "fundamental_relative_strength_xg_v1", "uses_market_odds": False,
        "expected_goals": {"home": round(home_xg, 6), "away": round(away_xg, 6)},
        "confidence": confidence, "inputs": {**rates, **samples, "metric_type": metric_type, "home_adjustment": home_adjustment, "away_adjustment": away_adjustment, "lineup_confidence": lineup_confidence, "provenance": provenance},
        "audit": audit,
        "estimator_hash": _content_hash({"rates": rates, "samples": samples, "metric_type": metric_type, "home_adjustment": home_adjustment, "away_adjustment": away_adjustment, "lineup_confidence": lineup_confidence, "provenance": provenance}),
    }
def _model_pair(model_probabilities: Dict[str, Any], market: str, line: Optional[float]) -> Optional[Dict[str, float]]:
    if market == "1x2":
        source = model_probabilities.get("1x2") if isinstance(model_probabilities.get("1x2"), dict) else model_probabilities
        keys = ("home", "draw", "away")
    elif market == "btts":
        source, keys = model_probabilities.get("btts"), ("yes", "no")
    elif market == "over_under" and line == 2.5:
        raw, keys = model_probabilities.get("over_under_2_5"), ("over", "under")
        source = {"over": (raw or {}).get("over_2_5"), "under": (raw or {}).get("under_2_5")}
    elif market in ("home_team_total", "away_team_total") and line is not None and line % 1 == 0.5:
        raw = model_probabilities.get("home_team_totals" if market == "home_team_total" else "away_team_totals") or {}
        suffix, keys = str(line).replace(".", "_"), ("over", "under")
        source = {"over": raw.get(f"over_{suffix}"), "under": raw.get(f"under_{suffix}")}
    else:
        return None
    if not isinstance(source, dict):
        return None
    values = {key: as_float(source.get(key)) for key in keys}
    if not all(value is not None and 0 <= value <= 1 for value in values.values()) or abs(sum(values.values()) - 1.0) > 0.02:
        return None
    return values


def _coverage_for(script_coverage: Dict[str, Any], market: str, selection: str) -> Optional[float]:
    nested = script_coverage.get(market)
    if isinstance(nested, dict) and as_float(nested.get(selection)) is not None:
        return as_float(nested.get(selection))
    return as_float(script_coverage.get(f"{market}.{selection}")) if as_float(script_coverage.get(f"{market}.{selection}")) is not None else as_float(script_coverage.get(selection))


def _split_asian_line(line: float) -> Optional[List[float]]:
    scaled = round(line * 4)
    if abs(line * 4 - scaled) > 1e-6:
        return None
    if abs(scaled) % 2 == 0:
        return [line]
    lower = math.floor(line * 2) / 2.0
    return [lower, lower + 0.5]


def asian_settlement_metrics(distribution: Dict[str, Any], line: float, selection: str, price: float, market: str) -> Optional[Dict[str, float]]:
    split = _split_asian_line(line)
    if not split or not price or price <= 1.0:
        return None
    win_equivalent = loss_equivalent = push_probability = expected_return = 0.0
    for raw_outcome, raw_probability in distribution.items():
        outcome, probability = as_float(raw_outcome), as_float(raw_probability)
        if outcome is None or probability is None:
            continue
        component_weight = probability / len(split)
        for component in split:
            if market == "asian_handicap":
                settled = outcome + component if selection == "home" else -outcome - component
            else:
                settled = outcome - component if selection == "over" else component - outcome
            if settled > 1e-9:
                win_equivalent += component_weight
                expected_return += component_weight * (price - 1.0)
            elif settled < -1e-9:
                loss_equivalent += component_weight
                expected_return -= component_weight
            else:
                push_probability += component_weight
    denominator = win_equivalent + loss_equivalent
    if denominator <= 0:
        return None
    fair_probability = win_equivalent / denominator
    fair_price = 1.0 / fair_probability if fair_probability > 0 else None
    return {
        "model_probability": round(fair_probability, 6), "model_fair_price": round(fair_price, 6) if fair_price else None,
        "win_equivalent": round(win_equivalent, 6), "loss_equivalent": round(loss_equivalent, 6),
        "push_probability": round(push_probability, 6), "ev": round(expected_return, 6),
    }


def decision_layer(market_snapshot: Dict[str, Any], model_probabilities: Optional[Dict[str, Any]] = None, script_coverage: Optional[Dict[str, Any]] = None, crowding: Optional[float] = None, lineup_confidence: Optional[float] = None, death_path: Optional[List[str]] = None) -> Dict[str, Any]:
    consensus = get_nested(market_snapshot, ["consensus_main_line"]) or get_nested(market_snapshot, ["primary"]) or {}
    missing = []
    if not model_probabilities: missing.append("model_probability")
    if script_coverage is None: missing.append("script_coverage")
    if crowding is None: missing.append("crowding")
    if lineup_confidence is None: missing.append("lineup_confidence")
    if death_path is None: missing.append("death_path")
    crowding_value = as_float(crowding)
    lineup_confidence_value = as_float(lineup_confidence)
    crowding_valid = crowding is None or (crowding_value is not None and 0.0 <= crowding_value <= 1.0)
    lineup_confidence_valid = lineup_confidence is None or (lineup_confidence_value is not None and 0.0 <= lineup_confidence_value <= 1.0)
    death_path_valid = death_path is None or (isinstance(death_path, list) and len(death_path) <= 20 and all(isinstance(item, str) and 0 < len(item.strip()) <= 300 for item in death_path))
    normalized_death_path = [item.strip() for item in death_path] if death_path_valid and isinstance(death_path, list) else []
    candidates = []
    market_probabilities = {}
    market_probability_audit = {}
    model_probability_audit = {}
    valid_model_market_count = 0
    invalid_market_price_count = 0
    specs = (("1x2", ("home", "draw", "away")), ("asian_handicap", ("home", "away")), ("over_under", ("over", "under")), ("btts", ("yes", "no")), ("home_team_total", ("over", "under")), ("away_team_total", ("over", "under")))
    for market, keys in specs:
        main = consensus.get(market) or {}
        bookmaker_count = int(as_float(main.get("bookmaker_count")) or 0) if main.get("bookmaker_count") is not None else None
        market_coverage_eligible = bookmaker_count is None or bookmaker_count >= MIN_CONSENSUS_BOOKMAKERS
        consensus_source_eligible = main.get("source") == "complete_company_array"
        dispersion_eligible = main.get("dispersion_eligible") is not False
        line = as_float(main.get("line")) if market not in ("1x2", "btts") else None
        distribution_name = {"asian_handicap": "goal_difference", "over_under": "total_goals", "home_team_total": "home_goals", "away_team_total": "away_goals"}.get(market)
        distribution = get_nested(model_probabilities or {}, ["settlement_distributions", distribution_name]) if distribution_name else None
        market_probability, market_probability_method = market_no_vig_probabilities(main, list(keys))
        market_probability_audit[market] = {
            "status": "available" if market_probability else "data_missing",
            "method": market_probability_method,
            "line": line,
            "selection_count": len(keys),
        }
        settlement_supported = isinstance(distribution, dict) and line is not None and _split_asian_line(line) is not None
        if settlement_supported:
            valid_model_market_count += 1
            model_probability_audit[market] = {"status": "available", "method": "settlement_distribution", "line": line, "reason": None}
        if settlement_supported and market_probability:
            market_probabilities[market] = {"line": line, "probabilities": market_probability, "method": market_probability_method}
            for key in keys:
                price = as_float(main.get(key))
                if price is None or price <= 1.0:
                    invalid_market_price_count += 1
                    continue
                metrics = asian_settlement_metrics(distribution, line, key, price, market)
                if not metrics:
                    continue
                edge = metrics["model_probability"] - market_probability[key]
                candidates.append({"market": market, "selection": key, "line": line, "price": price, **metrics, "market_no_vig_probability": market_probability[key], "edge": round(edge, 6), "script_coverage": _coverage_for(script_coverage or {}, market, key), "settlement_aware": True, "bookmaker_count": bookmaker_count, "market_coverage_eligible": market_coverage_eligible, "consensus_source_eligible": consensus_source_eligible, "dispersion_eligible": dispersion_eligible})
            continue
        if settlement_supported:
            continue
        model_pair = _model_pair(model_probabilities or {}, market, line)
        if model_pair:
            valid_model_market_count += 1
            model_probability_audit[market] = {"status": "available", "method": "direct_market_probability", "line": line, "reason": None}
        else:
            model_probability_audit[market] = {"status": "data_missing", "method": None, "line": line, "reason": "model_probability_not_available_for_market_line" if main else "market_not_available"}
        if market_probability:
            market_probabilities[market] = {"line": line, "probabilities": market_probability, "method": market_probability_method}
        if not model_pair or not market_probability:
            continue
        for key in keys:
            model_p, price = model_pair[key], as_float(main.get(key))
            if price is None or price <= 1.0:
                invalid_market_price_count += 1
                continue
            edge, ev = model_p - market_probability[key], model_p * price - 1.0
            candidates.append({"market": market, "selection": key, "line": line, "price": price, "model_probability": round(model_p, 6), "market_no_vig_probability": market_probability[key], "edge": round(edge, 6), "ev": round(ev, 6), "script_coverage": _coverage_for(script_coverage or {}, market, key), "bookmaker_count": bookmaker_count, "market_coverage_eligible": market_coverage_eligible, "consensus_source_eligible": consensus_source_eligible, "dispersion_eligible": dispersion_eligible})
    if model_probabilities and not valid_model_market_count: missing.append("model_probability_invalid_or_not_normalized")
    if not market_probabilities: missing.append("market_no_vig_probability")
    if not candidates and invalid_market_price_count: missing.append("market_price_for_ev_missing_or_invalid")
    candidates.sort(key=lambda x: (x.get("ev", -999), x.get("edge", -999)), reverse=True)
    best_unfiltered = candidates[0] if candidates else None
    qualified = [row for row in candidates if row["market_coverage_eligible"] and row["consensus_source_eligible"] and row["dispersion_eligible"] and row["edge"] >= MIN_EDGE and row["ev"] >= MIN_EV and as_float(row.get("script_coverage")) is not None and MIN_SCRIPT_COVERAGE <= row["script_coverage"] <= 1.0]
    first_choice = max(qualified, key=lambda row: (row["script_coverage"], row["edge"], row["ev"]), default=None)
    second_pool = [row for row in qualified if not first_choice or (row["market"], row["selection"], row.get("line")) != (first_choice["market"], first_choice["selection"], first_choice.get("line"))]
    second_choice = max(second_pool, key=lambda row: (row["ev"], row["edge"], row["script_coverage"]), default=None)
    high_variance_pool = [row for row in candidates if row["market_coverage_eligible"] and row["consensus_source_eligible"] and row["dispersion_eligible"] and row["edge"] >= MIN_EDGE and row["ev"] >= MIN_EV and as_float(row.get("script_coverage")) is not None and HIGH_VARIANCE_MIN_SCRIPT_COVERAGE <= row["script_coverage"] < MIN_SCRIPT_COVERAGE]
    high_variance = max(high_variance_pool, key=lambda row: (row["ev"], row["edge"]), default=None)
    best = first_choice or best_unfiltered
    pass_reasons = list(missing)
    if best and best["edge"] < MIN_EDGE: pass_reasons.append("edge_below_minimum")
    if best and best["ev"] < MIN_EV: pass_reasons.append("ev_below_minimum")
    if best and as_float(best.get("script_coverage")) is None: pass_reasons.append("script_coverage_for_selection_missing")
    elif best and not 0.0 <= as_float(best.get("script_coverage")) <= 1.0: pass_reasons.append("script_coverage_out_of_range")
    elif best and as_float(best.get("script_coverage")) < MIN_SCRIPT_COVERAGE: pass_reasons.append("script_coverage_below_minimum")
    if best and not best.get("market_coverage_eligible", True): pass_reasons.append("consensus_bookmaker_coverage_below_minimum")
    if best and not best.get("consensus_source_eligible", True): pass_reasons.append("consensus_not_recalculated_from_company_array")
    if best and not best.get("dispersion_eligible", True): pass_reasons.append("consensus_price_dispersion_above_maximum")
    if not crowding_valid: pass_reasons.append("crowding_out_of_range")
    elif crowding_value is not None and crowding_value > MAX_CROWDING: pass_reasons.append("crowding_above_maximum")
    if not lineup_confidence_valid: pass_reasons.append("lineup_confidence_out_of_range")
    elif lineup_confidence_value is not None and lineup_confidence_value < MIN_LINEUP_CONFIDENCE: pass_reasons.append("lineup_confidence_below_minimum")
    if not death_path_valid: pass_reasons.append("death_path_invalid")
    elif normalized_death_path: pass_reasons.append("death_path_present")
    if pass_reasons:
        first_choice = second_choice = None
        if any(reason != "script_coverage_below_minimum" for reason in pass_reasons):
            high_variance = None
    decision = "PASS" if pass_reasons or not first_choice else (first_choice["selection"] if first_choice["market"] == "1x2" else f"{first_choice['market']}:{first_choice['selection']}")
    return {
        "decision": decision, "best_market": first_choice, "candidates": candidates,
        "recommendation_tiers": {
            "first_choice_high_consistency": first_choice,
            "second_choice_higher_return": second_choice,
            "high_variance_single": high_variance,
            "ranking_rule": "consistency first; price second; high-variance candidates are never used to fill the main tier",
        },
        "market_no_vig_probability": market_probabilities, "market_probability_audit": market_probability_audit, "model_probability": model_probabilities, "model_probability_audit": model_probability_audit,
        "edge": first_choice.get("edge") if first_choice else None, "ev": first_choice.get("ev") if first_choice else None,
        "script_coverage": script_coverage, "crowding": crowding_value,
        "line_movement": None, "lineup_confidence": lineup_confidence_value,
        "death_path": normalized_death_path, "death_path_audit": {"valid": death_path_valid, "maximum_items": 20, "maximum_item_length": 300}, "pass_reasons": pass_reasons,
        "candidate_generation_audit": {"generated_candidate_count": len(candidates), "invalid_market_price_count": invalid_market_price_count},
        "settlement_policy": {"supported_line_increment": 0.25, "quarter_lines": "split into adjacent half-lines", "push_half_win_half_loss": "included in model EV", "unsupported_lines": "PASS"},
        "thresholds": {"minimum_edge": MIN_EDGE, "minimum_ev": MIN_EV, "minimum_script_coverage": MIN_SCRIPT_COVERAGE, "high_variance_minimum_script_coverage": HIGH_VARIANCE_MIN_SCRIPT_COVERAGE, "maximum_crowding": MAX_CROWDING, "minimum_lineup_confidence": MIN_LINEUP_CONFIDENCE, "minimum_consensus_bookmakers": MIN_CONSENSUS_BOOKMAKERS, "maximum_consensus_price_spread_reference": MAX_CONSENSUS_PRICE_SPREAD, "maximum_consensus_no_vig_probability_spread": MAX_CONSENSUS_NO_VIG_PROBABILITY_SPREAD, "minimum_market_implied_probability_total": MIN_MARKET_IMPLIED_PROBABILITY_TOTAL, "maximum_market_implied_probability_total": MAX_MARKET_IMPLIED_PROBABILITY_TOTAL},
    }


def _fundamental_evaluation_script(payload: Dict[str, Any], estimator: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    supplied_chain = payload.get("fundamental_chain") if isinstance(payload.get("fundamental_chain"), dict) else {}
    chain = {
        key: supplied_chain.get(key) if isinstance(supplied_chain.get(key), dict)
        else {"status": "data_missing", "reason": "not supplied for this recalculation"}
        for key in FUNDAMENTAL_CHAIN
    }
    independent_content = {
        "fixture": str(payload.get("fixture") or ""), "chain": chain,
        "estimator_method": estimator.get("method"), "estimator_inputs": estimator.get("inputs"),
        "expected_goals": estimator.get("expected_goals"), "model_method": model.get("method"),
        "model_probability": get_nested(model, ["probabilities", "1x2"]),
    }
    return {
        "schema": "prematch_fundamental_evaluation_v1", "odds_independent": True,
        "generated_at": int(time.time()), "chain_order": FUNDAMENTAL_CHAIN, "chain": chain,
        "estimator": {"method": estimator.get("method"), "inputs": estimator.get("inputs"), "expected_goals": estimator.get("expected_goals"), "confidence": estimator.get("confidence"), "audit": estimator.get("audit")},
        "model": {"method": model.get("method"), "model_hash": model.get("model_hash"), "probabilities": model.get("probabilities")},
        "content_hash": _content_hash(independent_content),
    }


def audit_fundamental_chain(script: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    chain = script.get("chain") or {}
    weights = {"available": 1.0, "partial": 0.5}
    allowed_statuses = {"available", "partial", "data_missing"}
    invalid_status_sections = []
    unsubstantiated_sections = []
    provenance_missing_sections = []
    timestamp_missing_sections = []
    scores = {}
    for key in FUNDAMENTAL_CHAIN:
        section = chain.get(key) if isinstance(chain.get(key), dict) else {}
        status = str(section.get("status") or "data_missing").lower()
        if status not in allowed_statuses:
            invalid_status_sections.append(key)
            status = "data_missing"
        substantive_values = [value for field, value in section.items() if field not in {"status", "reason", "warning"} and value not in (None, "", [], {})]
        if status in weights and not substantive_values:
            unsubstantiated_sections.append(key)
            status = "data_missing"
        if status in weights and not (section.get("source") or section.get("provenance")):
            provenance_missing_sections.append(key)
        if status in weights and not (section.get("observed_at") or section.get("as_of")):
            timestamp_missing_sections.append(key)
        scores[key] = weights.get(status, 0.0)
    missing = [key for key, score in scores.items() if score == 0.0]
    partial = [key for key, score in scores.items() if score == 0.5]
    critical = ["result_utility", "rotation_quality", "execution_ability", "goal_conversion"]
    critical_missing = [key for key in critical if scores.get(key, 0.0) == 0.0]
    critical_provenance_missing = [key for key in critical if key in provenance_missing_sections]
    critical_timestamp_issues = {}
    for key in critical:
        section = chain.get(key) if isinstance(chain.get(key), dict) else {}
        raw_timestamp = section.get("observed_at") or section.get("as_of")
        observed_at = _parse_timestamp(raw_timestamp)
        max_age = CRITICAL_FUNDAMENTAL_MAX_AGE_SECONDS[key]
        if observed_at is None:
            issue, age = "missing_or_invalid", None
        else:
            age = now_ts - observed_at
            issue = "future_timestamp" if age < -300 else ("stale" if age > max_age else None)
        if issue:
            critical_timestamp_issues[key] = {"issue": issue, "observed_at": raw_timestamp, "age_seconds": age, "max_age_seconds": max_age}
    semantic_issues = {}
    structural_issues = {}
    market_contaminated_sections = {}
    forbidden_market_keys = {"odds", "market_odds", "bookmaker", "market_probability", "market_no_vig_probability", "implied_probability", "line_movement", "market_snapshot"}
    forbidden_source_tokens = ("bookmaker", "betting odds", "market odds", "no-vig", "implied probability")

    def contamination_paths(value: Any, path: str = "") -> List[str]:
        found = []
        if isinstance(value, dict):
            for field, child in value.items():
                child_path = f"{path}.{field}" if path else str(field)
                if str(field).lower() in forbidden_market_keys:
                    found.append(child_path)
                found.extend(contamination_paths(child, child_path))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                found.extend(contamination_paths(child, f"{path}[{index}]"))
        elif isinstance(value, str) and path.lower().split(".")[-1] in {"source", "provenance"}:
            lowered = value.lower()
            if any(token in lowered for token in forbidden_source_tokens):
                found.append(path)
        return found

    for key in FUNDAMENTAL_CHAIN:
        paths = contamination_paths(chain.get(key) or {})
        if paths:
            market_contaminated_sections[key] = sorted(set(paths))
    result_utility = chain.get("result_utility") or {}
    for side in ("home", "away"):
        side_values = result_utility.get(side) if isinstance(result_utility.get(side), dict) else {}
        missing_fields = [outcome for outcome in ("win", "draw", "loss") if as_float(side_values.get(outcome)) is None]
        if missing_fields:
            semantic_issues.setdefault("result_utility", {})[side] = {"missing_numeric_fields": missing_fields}
        else:
            win, draw, loss = (as_float(side_values[outcome]) for outcome in ("win", "draw", "loss"))
            if not (win >= draw >= loss) or win == loss:
                semantic_issues.setdefault("result_utility", {})[side] = {"reason": "utility_must_satisfy_win_gte_draw_gte_loss_with_nonzero_spread"}
    rotation = chain.get("rotation_quality") or {}
    for side in ("home", "away"):
        side_values = rotation.get(side) if isinstance(rotation.get(side), dict) else {}
        numeric_rotation_fields = [field for field in ROTATION_QUALITY_FIELDS if field != "lineup_intent"]
        valid_numeric = [field for field in numeric_rotation_fields if as_float(side_values.get(field)) is not None and 0 <= as_float(side_values.get(field)) <= 1]
        invalid_numeric = [field for field in numeric_rotation_fields if side_values.get(field) not in (None, "", [], {}) and field not in valid_numeric]
        present = valid_numeric + (["lineup_intent"] if side_values.get("lineup_intent") not in (None, "", [], {}) else [])
        if len(present) < 4:
            semantic_issues.setdefault("rotation_quality", {})[side] = {"required_minimum_fields": 4, "present_fields": present}
        if invalid_numeric:
            semantic_issues.setdefault("rotation_quality", {}).setdefault(side, {})["numeric_fields_outside_0_to_1"] = invalid_numeric
    for section_name in ("execution_ability", "goal_conversion"):
        section = chain.get(section_name) or {}
        for side in ("home", "away"):
            side_values = section.get(side) if isinstance(section.get(side), dict) else {}
            numeric_fields = [field for field, value in side_values.items() if as_float(value) is not None]
            if not numeric_fields:
                semantic_issues.setdefault(section_name, {})[side] = {"reason": "at_least_one_numeric_metric_required"}
            negative_fields = [field for field in numeric_fields if as_float(side_values.get(field)) < 0]
            if negative_fields:
                semantic_issues.setdefault(section_name, {}).setdefault(side, {})["negative_numeric_fields"] = negative_fields
    # These sections are not hard-critical individually, but an "available" claim must
    # describe the complete state model instead of a free-form note.
    required_nested_fields = {
        "game_state_elasticity": ("states", ("0_0_persists", "home_scores_first", "away_scores_first", "draw_at_60", "trailing_last_30")),
        "time_segment_strength": ("segments", ("0_15", "16_30", "31_45", "46_60", "61_75", "76_90")),
    }
    for section_name, (container_name, required_fields) in required_nested_fields.items():
        section = chain.get(section_name) if isinstance(chain.get(section_name), dict) else {}
        if str(section.get("status") or "data_missing").lower() not in weights:
            continue
        container = section.get(container_name) if isinstance(section.get(container_name), dict) else {}
        absent = [field for field in required_fields if container.get(field) in (None, "", [], {})]
        if absent:
            structural_issues[section_name] = {"container": container_name, "missing_fields": absent}
    first_goal = chain.get("first_goal_state_transition") if isinstance(chain.get("first_goal_state_transition"), dict) else {}
    if str(first_goal.get("status") or "data_missing").lower() in weights:
        absent = [field for field in ("home_first", "away_first") if first_goal.get(field) in (None, "", [], {})]
        if absent:
            structural_issues["first_goal_state_transition"] = {"missing_fields": absent}
    open_game = chain.get("open_game_beneficiary") if isinstance(chain.get("open_game_beneficiary"), dict) else {}
    if str(open_game.get("status") or "data_missing").lower() in weights and str(open_game.get("team") or "").lower() not in {"home", "away", "neither", "uncertain"}:
        structural_issues["open_game_beneficiary"] = {"reason": "team_must_be_home_away_neither_or_uncertain"}
    conversion = chain.get("goal_conversion") if isinstance(chain.get("goal_conversion"), dict) else {}
    if str(conversion.get("status") or "data_missing").lower() in weights:
        absent = [field for field in ("strength_edge", "goal_edge", "margin_edge") if conversion.get(field) in (None, "", [], {})]
        if absent:
            structural_issues["goal_conversion_edges"] = {"missing_fields": absent, "rule": "strength_edge_goal_edge_and_margin_edge_are_distinct"}
    completeness = sum(scores.values()) / len(FUNDAMENTAL_CHAIN)
    eligible = completeness >= 0.6 and not critical_missing and not critical_provenance_missing and not critical_timestamp_issues and not semantic_issues and not structural_issues and not market_contaminated_sections
    return {
        "status": "eligible" if eligible else "insufficient",
        "decision_eligible": eligible, "completeness_score": round(completeness, 4),
        "minimum_completeness": 0.6, "critical_sections": critical,
        "critical_missing": critical_missing, "missing_sections": missing, "partial_sections": partial,
        "unsubstantiated_sections": unsubstantiated_sections, "invalid_status_sections": invalid_status_sections,
        "provenance_missing_sections": provenance_missing_sections,
        "critical_provenance_missing": critical_provenance_missing,
        "timestamp_missing_sections": timestamp_missing_sections,
        "critical_timestamp_issues": critical_timestamp_issues,
        "critical_max_age_seconds": CRITICAL_FUNDAMENTAL_MAX_AGE_SECONDS,
        "critical_semantic_issues": semantic_issues,
        "structural_issues": structural_issues,
        "market_contaminated_sections": market_contaminated_sections,
        "critical_schema": {
            "result_utility": "home/away each require numeric win, draw, loss",
            "rotation_quality": "home/away each require at least 4 named quality fields; numeric scores must be 0..1",
            "execution_ability": "home/away each require at least one finite nonnegative numeric metric",
            "goal_conversion": "home/away each require at least one finite nonnegative numeric metric",
        },
        "evidence_rule": "available or partial requires at least one substantive field beyond status/reason/warning",
        "provenance_rule": "critical sections require source or provenance and a valid type-specific observed_at/as_of",
        "odds_independence_rule": "fundamental sections must not contain odds, bookmaker, market probability, implied probability, market snapshot, or line movement inputs",
        "policy": "probability generation remains available; final recommendation must PASS when insufficient or market-contaminated",
    }


def build_decision_summary(decision: Dict[str, Any], chain_audit: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    best = decision.get("best_market") or {}
    passed = decision.get("decision") == "PASS"
    return {
        "decision": decision.get("decision"),
        "status": "pass" if passed else "actionable",
        "recommended_market": None if passed else best.get("market"),
        "recommended_selection": None if passed else best.get("selection"),
        "line": None if passed else best.get("line"),
        "price": None if passed else best.get("price"),
        "edge": None if passed else best.get("edge"),
        "ev": None if passed else best.get("ev"),
        "script_coverage": None if passed else best.get("script_coverage"),
        "model_status": model.get("status"),
        "fundamental_chain_status": chain_audit.get("status"),
        "market_move_classification": decision.get("market_move_classification"),
        "pass_reasons": decision.get("pass_reasons") or [],
        "explanation": "No bet: one or more mandatory gates failed." if passed else "Selection passed model, price, script and risk gates.",
    }


def detect_model_market_divergence(decision: Dict[str, Any], threshold: float = 0.08, model_ready: bool = True, fundamental_eligible: bool = True, market_data_eligible: bool = True) -> Dict[str, Any]:
    raw_comparable = [row for row in decision.get("candidates") or [] if as_float(row.get("edge")) is not None]
    comparable = [row for row in raw_comparable if row.get("market_coverage_eligible", True) and row.get("consensus_source_eligible", True) and row.get("dispersion_eligible", True)]
    strongest = max(comparable, key=lambda row: abs(as_float(row.get("edge")) or 0.0), default=None)
    gap = abs(as_float((strongest or {}).get("edge")) or 0.0) if strongest else None
    lineup_confidence = as_float(decision.get("lineup_confidence"))
    confidence_eligible = lineup_confidence is not None and lineup_confidence >= MIN_LINEUP_CONFIDENCE
    eligibility_reasons = []
    if not model_ready: eligibility_reasons.append("model_not_ready")
    if not fundamental_eligible: eligibility_reasons.append("fundamental_chain_insufficient")
    if not market_data_eligible: eligibility_reasons.append("market_data_not_fresh_or_valid")
    if raw_comparable and not comparable: eligibility_reasons.append("market_consensus_ineligible")
    if not confidence_eligible: eligibility_reasons.append("lineup_confidence_insufficient")
    eligible = not eligibility_reasons
    return {
        "triggered": eligible and gap is not None and gap >= threshold,
        "threshold": threshold,
        "maximum_absolute_probability_gap": round(gap, 6) if gap is not None else None,
        "market": (strongest or {}).get("market"), "selection": (strongest or {}).get("selection"),
        "direction": "model_above_market" if strongest and as_float(strongest.get("edge")) > 0 else ("model_below_market" if strongest else None),
        "comparison_status": "compared" if strongest and eligible else (eligibility_reasons[0] if raw_comparable and eligibility_reasons else "data_missing"),
        "classification_eligible": eligible, "eligibility_reasons": eligibility_reasons,
        "candidate_audit": {"raw_comparable_count": len(raw_comparable), "eligible_comparable_count": len(comparable), "excluded_ineligible_consensus_count": len(raw_comparable) - len(comparable)},
    }


def evaluate_imported_prematch(payload: Dict[str, Any], persist_version: bool = True) -> Dict[str, Any]:
    fixture = str(payload.get("fixture") or "").strip()
    if not fixture:
        raise HTTPException(status_code=422, detail="fixture_required")
    store = load_snapshot_store()
    metadata = (store.get("external_prematch") or {}).get(fixture)
    if not metadata:
        raise HTTPException(status_code=404, detail="imported_fixture_not_found")
    lineup_audit = audit_lineup_confidence(metadata.get("lineup_history"), payload.get("lineup_confidence"))
    effective_payload = dict(payload)
    effective_payload["lineup_confidence"] = lineup_audit.get("effective_confidence")
    estimator = fundamental_expected_goals(effective_payload)
    if not estimator.get("ok"):
        raise HTTPException(status_code=422, detail=estimator)
    xg = estimator["expected_goals"]
    model = poisson_probability_model(xg["home"], xg["away"], estimator["confidence"], payload.get("provenance"))
    if estimator.get("status") != "ready":
        model["status"] = "insufficient_confidence"
    history = get_fixture_snapshots(fixture)
    available = [row for row in history if row.get("import_status") == "available"]
    latest = latest_prematch_snapshot(available)
    market = (latest or {}).get("market_snapshot") or empty_market_snapshot()
    freshness = imported_fixture_freshness(metadata, history)
    decision = decision_layer(
        market, model.get("probabilities"), payload.get("script_coverage"),
        as_float(payload.get("crowding")), lineup_audit.get("effective_confidence"), payload.get("death_path") if "death_path" in payload else None,
    )
    decision["lineup_confidence_audit"] = lineup_audit
    decision = apply_line_movement_gate(decision, history)
    decision["data_freshness"] = freshness
    if model.get("status") != "ready":
        decision["pass_reasons"].append("model_input_confidence_below_0_6")
    if not freshness.get("decision_eligible"):
        decision["pass_reasons"].append(freshness.get("reason") or "data_not_fresh")
    decision["pass_reasons"] = list(dict.fromkeys(decision["pass_reasons"]))
    if decision["pass_reasons"]:
        force_pass_decision(decision, [])

    script = _fundamental_evaluation_script(effective_payload, estimator, model)
    chain_audit = audit_fundamental_chain(script)
    decision["fundamental_chain_audit"] = chain_audit
    if not chain_audit["decision_eligible"]:
        force_pass_decision(decision, "fundamental_chain_insufficient")
    model_market_divergence = detect_model_market_divergence(
        decision,
        model_ready=model.get("status") == "ready",
        fundamental_eligible=chain_audit.get("decision_eligible") is True,
        market_data_eligible=bool(latest) and freshness.get("decision_eligible") is True,
    )
    decision["model_market_divergence"] = model_market_divergence
    decision["market_move_classification"] = "Model-Market Divergence" if model_market_divergence["triggered"] else get_nested(decision, ["line_movement", "classification"])
    versions = get_fundamental_versions(fixture)
    previous = versions[-1] if versions else None
    previous_probability = get_nested(previous or {}, ["script", "model", "probabilities", "1x2"])
    current_probability = get_nested(model, ["probabilities", "1x2"])
    probability_change = {
        "before": previous_probability, "after": current_probability,
        "delta": {key: round(current_probability[key] - previous_probability[key], 6) for key in ("home", "draw", "away")} if previous_probability else None,
    }
    previous_best = get_nested(previous or {}, ["best_market_change", "after"])
    current_best = decision.get("best_market")
    best_market_change = {"before": previous_best, "after": current_best, "changed": previous_best != current_best}
    trigger = payload.get("revalidation_trigger") if isinstance(payload.get("revalidation_trigger"), dict) else {
        "triggered": False, "reasons": ["pipeline_evaluation"]
    }
    version_record = save_fundamental_version(
        fixture, script, trigger, previous=previous,
        probability_change=probability_change, best_market_change=best_market_change,
    ) if persist_version else None
    resolved_revalidations = resolve_revalidation_tasks(fixture, version_record, model_market_divergence["triggered"], (latest or {}).get("stage")) if version_record and trigger.get("triggered") and chain_audit.get("decision_eligible") else 0
    return {
        "ok": True, "version": VERSION, "fixture": fixture, "match": metadata.get("match"), "estimator": estimator, "model": model,
        "decision_layer": decision, "decision_summary": build_decision_summary(decision, chain_audit, model),
        "fundamental_chain_audit": chain_audit, "fundamental_version": version_record,
        "revalidation_tasks_resolved": resolved_revalidations,
    }


def portfolio_selection_label(candidate: Dict[str, Any]) -> str:
    market, selection, line = candidate.get("market"), candidate.get("selection"), candidate.get("line")
    numeric_line = as_float(line)
    if market == "1x2":
        return {"home": "home win", "draw": "draw", "away": "away win"}.get(selection, str(selection or ""))
    if market == "asian_handicap":
        return f"{selection} {numeric_line:+g}" if numeric_line is not None else str(selection or "")
    if market == "over_under":
        return f"{selection} {numeric_line:g}" if numeric_line is not None else str(selection or "")
    if market == "btts":
        return f"BTTS {selection}"
    if market in ("home_team_total", "away_team_total"):
        side = "home team total" if market == "home_team_total" else "away team total"
        return f"{side} {selection} {numeric_line:g}" if numeric_line is not None else f"{side} {selection}"
    return ":".join(str(value) for value in (market, selection) if value)


def _combination_from_rows(rows: List[Dict[str, Any]], tier: str, max_legs: int, allow_fallback: bool = False, risk_preference: str = "balanced") -> Dict[str, Any]:
    candidates = []
    excluded = []
    seen_groups = set()
    upgraded = False
    ordered = sorted(rows, key=lambda row: (
        as_float(get_nested(row, ["evaluation", "decision_layer", "recommendation_tiers", tier, "script_coverage"])) or -1,
        as_float(get_nested(row, ["evaluation", "decision_layer", "recommendation_tiers", tier, "ev"])) or -999,
    ), reverse=True)
    for row in ordered:
        tiers = get_nested(row, ["evaluation", "decision_layer", "recommendation_tiers"]) or {}
        candidate = tiers.get(tier)
        source_tier = tier
        if not candidate and allow_fallback:
            candidate = tiers.get("first_choice_high_consistency")
            source_tier = "first_choice_high_consistency_fallback"
        if not candidate:
            excluded.append({"fixture": row.get("fixture"), "reason": "no_eligible_candidate_for_tier", "requested_tier": tier})
            continue
        group = str(row.get("correlation_group") or row.get("fixture") or "")
        if group in seen_groups:
            excluded.append({"fixture": row.get("fixture"), "correlation_group": group, "reason": "correlation_group_already_selected", "requested_tier": tier})
            continue
        if len(candidates) >= max_legs:
            excluded.append({"fixture": row.get("fixture"), "correlation_group": group, "reason": "max_legs_reached", "requested_tier": tier})
            continue
        seen_groups.add(group)
        decision_layer_result = get_nested(row, ["evaluation", "decision_layer"]) or {}
        match = get_nested(row, ["evaluation", "match"]) or row.get("match") or {}
        match_label = row.get("match_label") or (f"{match.get('home_team_name')} vs {match.get('away_team_name')}" if isinstance(match, dict) and match.get("home_team_name") and match.get("away_team_name") else str(row.get("fixture") or ""))
        enriched = {
            "fixture": row.get("fixture"), "correlation_group": group, "source_tier": source_tier,
            "match_label": match_label,
            "lineup_confidence": decision_layer_result.get("lineup_confidence"),
            "crowding": decision_layer_result.get("crowding"),
            "line_movement": decision_layer_result.get("line_movement"),
            "death_path": decision_layer_result.get("death_path") or [],
            **candidate,
        }
        enriched["selection_label"] = portfolio_selection_label(enriched)
        enriched["display_text"] = f"{match_label} · {enriched['selection_label']}"
        candidates.append(enriched)
        if tier == "second_choice_higher_return" and source_tier == tier:
            upgraded = True
    if len(candidates) < 2 or (tier == "second_choice_higher_return" and not upgraded):
        return {
            "decision": "PASS", "legs": candidates,
            "reason": "fewer_than_two_eligible_independent_legs" if len(candidates) < 2 else "no_higher_return_upgrade_available",
            "selection_audit": {"selected": candidates, "excluded": excluded},
        }
    suggested_options = []
    for leg_count in range(2, len(candidates) + 1):
        option_legs = candidates[:leg_count]
        combined_price = math.prod(candidate["price"] for candidate in option_legs)
        leg_evs = [as_float(candidate.get("ev")) for candidate in option_legs]
        estimated_ev = math.prod(1.0 + value for value in leg_evs) - 1.0 if all(value is not None for value in leg_evs) else None
        binary_probabilities = [as_float(candidate.get("model_probability")) for candidate in option_legs]
        market_probabilities = [as_float(candidate.get("market_no_vig_probability")) for candidate in option_legs]
        has_settlement_aware_leg = any(candidate.get("settlement_aware") for candidate in option_legs)
        full_win_probability = math.prod(binary_probabilities) if not has_settlement_aware_leg and all(value is not None for value in binary_probabilities) else None
        market_combined_probability = math.prod(market_probabilities) if not has_settlement_aware_leg and all(value is not None for value in market_probabilities) else None
        probability_edge = full_win_probability - market_combined_probability if full_win_probability is not None and market_combined_probability is not None else None
        stressed_probabilities = [(market + (model - market) * 0.5) for model, market in zip(binary_probabilities, market_probabilities)] if not has_settlement_aware_leg and all(value is not None for value in binary_probabilities + market_probabilities) else None
        stressed_full_win_probability = math.prod(stressed_probabilities) if stressed_probabilities else None
        stressed_ev = stressed_full_win_probability * combined_price - 1.0 if stressed_full_win_probability is not None else None
        weakest_leg = min(option_legs, key=lambda candidate: (as_float(candidate.get("script_coverage")) if as_float(candidate.get("script_coverage")) is not None else -1, as_float(candidate.get("edge")) if as_float(candidate.get("edge")) is not None else -999))
        risk_warnings = ["residual_cross_match_correlation_not_modeled"]
        if leg_count >= 4:
            risk_warnings.append("four_or_more_legs_materially_increase_variance")
        if has_settlement_aware_leg:
            risk_warnings.append("asian_settlement_can_include_push_half_win_or_half_loss")
        risk = "lower_variance" if leg_count == 2 else ("balanced" if leg_count == 3 else "expanded_high_variance")
        suggested_options.append({
            "leg_count": leg_count, "risk_label": risk, "combined_decimal_price": round(combined_price, 4), "legs": option_legs,
            "estimated_combined_ev": round(estimated_ev, 6) if estimated_ev is not None else {"status": "data_missing", "reason": "one_or_more_leg_ev_missing"},
            "estimated_full_win_probability": round(full_win_probability, 8) if full_win_probability is not None else {"status": "data_missing", "reason": "settlement_aware_asian_leg_prevents_naive_full_win_probability" if has_settlement_aware_leg else "one_or_more_leg_probability_missing"},
            "market_no_vig_combined_probability": round(market_combined_probability, 8) if market_combined_probability is not None else {"status": "data_missing", "reason": "settlement_aware_asian_leg_prevents_naive_probability_multiplication" if has_settlement_aware_leg else "one_or_more_market_probability_missing"},
            "combined_probability_edge": round(probability_edge, 8) if probability_edge is not None else {"status": "data_missing", "reason": "comparable_binary_probabilities_unavailable"},
            "book_price_break_even_probability": round(1.0 / combined_price, 8),
            "half_edge_stress_test": {
                "status": "available", "method": "shrink_each_model_probability_halfway_to_market_no_vig",
                "estimated_full_win_probability": round(stressed_full_win_probability, 8), "estimated_ev": round(stressed_ev, 6),
                "remains_positive_ev": stressed_ev > 0,
            } if stressed_ev is not None else {"status": "data_missing", "reason": "binary_model_and_market_probabilities_required"},
            "weakest_leg": {key: weakest_leg.get(key) for key in ("fixture", "market", "selection", "line", "script_coverage", "edge", "ev", "lineup_confidence", "crowding")},
            "portfolio_context": {
                "minimum_lineup_confidence": min((as_float(candidate.get("lineup_confidence")) for candidate in option_legs if as_float(candidate.get("lineup_confidence")) is not None), default=None),
                "maximum_crowding": max((as_float(candidate.get("crowding")) for candidate in option_legs if as_float(candidate.get("crowding")) is not None), default=None),
                "line_movement_available_for_all_legs": all(isinstance(candidate.get("line_movement"), dict) and candidate.get("line_movement") for candidate in option_legs),
                "death_path_clear_for_all_legs": all(not candidate.get("death_path") for candidate in option_legs),
            },
            "risk_warnings": risk_warnings,
            "calculation_assumption": "cross-match independence after correlation_group screening",
        })
    preferred_counts = {"conservative": 2, "balanced": 3, "aggressive": len(candidates)}
    recommended_count = min(preferred_counts[risk_preference], len(candidates))
    recommended = next(option for option in suggested_options if option["leg_count"] == recommended_count)
    recommendation_reasons = {
        "conservative": "minimum eligible leg count selected to reduce accumulator variance",
        "balanced": "three legs selected when available to balance combined price and variance",
        "aggressive": "all eligible independent legs selected within max_legs; variance increases rapidly",
    }
    priority_ranking = [
        {
            "rank": index,
            "role": "core_top_three" if index <= 3 else "optional_extension",
            **{key: candidate.get(key) for key in ("fixture", "match_label", "market", "selection", "selection_label", "display_text", "line", "price", "script_coverage", "edge", "ev")},
        }
        for index, candidate in enumerate(candidates, start=1)
    ]
    return {
        "decision": "COMBINE", "legs": recommended["legs"], "combined_decimal_price": recommended["combined_decimal_price"],
        "leg_count": recommended_count, "available_leg_count": len(candidates), "suggested_options": suggested_options,
        "risk_preference": risk_preference, "recommended_option": recommended,
        "recommendation_reason": recommendation_reasons[risk_preference],
        "priority_ranking": priority_ranking,
        "core_priority_count": min(3, len(priority_ranking)),
        "optional_extension_count": max(0, len(priority_ranking) - 3),
        "selection_audit": {"selected": candidates, "excluded": excluded},
        "selection_guidance": "ranks 1-3 form the core priority set; rank 4+ are optional extensions with materially higher variance",
        "independence_assumption": "screened by correlation_group; residual correlation is not modeled",
        "combined_ev": recommended["estimated_combined_ev"],
        "combined_ev_status": "estimated_under_independence" if isinstance(recommended["estimated_combined_ev"], float) else "data_missing",
    }


def _risk_adjusted_combination(first: Dict[str, Any], second: Dict[str, Any], risk_preference: str) -> Dict[str, Any]:
    available = [("first_choice_high_consistency", first), ("second_choice_higher_return", second)]
    available = [(name, combination) for name, combination in available if combination.get("decision") == "COMBINE"]
    if not available:
        return {"decision": "PASS", "reason": "no_eligible_combination"}

    def robustness(combination: Dict[str, Any]) -> str:
        stress = get_nested(combination, ["recommended_option", "half_edge_stress_test"]) or {}
        if stress.get("status") != "available":
            return "unknown"
        return "resilient" if stress.get("remains_positive_ev") else "fragile"

    annotated = [(name, combination, robustness(combination)) for name, combination in available]
    if risk_preference == "conservative":
        eligible = [row for row in annotated if row[2] == "resilient"]
        if not eligible:
            return {"decision": "PASS", "reason": "no_combination_passed_half_edge_stress_test", "evaluated": [{"source": name, "robustness": state} for name, _, state in annotated]}
        selected = eligible[0]
    elif risk_preference == "balanced":
        selected = next((row for row in annotated if row[0] == "first_choice_high_consistency" and row[2] == "resilient"), None)
        selected = selected or next((row for row in annotated if row[2] == "resilient"), None) or annotated[0]
    else:
        selected = next((row for row in annotated if row[0] == "second_choice_higher_return"), annotated[0])
    name, combination, state = selected
    warning = "nominal_edge_only; stress_test_not_passed" if state == "fragile" else ("stress_test_unavailable" if state == "unknown" else None)
    return {
        "decision": "COMBINE", "source": name, "robustness": state,
        "warning": warning, "recommended_option": combination.get("recommended_option"),
        "reason": "selected according to risk_preference and half-edge stress result",
    }


def normalize_max_legs(value: Any, default: int = 6) -> int:
    if isinstance(value, bool):
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return max(2, min(parsed, 10))


def _portfolio_recommendation_signature(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    recommendation = portfolio.get("risk_adjusted_recommendation") or {}
    option = recommendation.get("recommended_option") or {}
    return {
        "decision": recommendation.get("decision"), "source": recommendation.get("source"),
        "robustness": recommendation.get("robustness"), "reason": recommendation.get("reason"),
        "legs": [
            {key: leg.get(key) for key in ("fixture", "market", "selection", "line", "price")}
            for leg in (option.get("legs") or [])
        ],
    }


def _portfolio_transition(before: Optional[Dict[str, Any]], after: Dict[str, Any]) -> Dict[str, Any]:
    if before is None:
        return {"classification": "Initial Recommendation", "changed": True, "added_legs": after.get("legs") or [], "removed_legs": []}
    before_decision, after_decision = before.get("decision"), after.get("decision")
    before_legs = before.get("legs") or []
    after_legs = after.get("legs") or []
    leg_key = lambda leg: (str(leg.get("fixture")), str(leg.get("market")), str(leg.get("selection")), str(leg.get("line")))
    before_map, after_map = {leg_key(leg): leg for leg in before_legs}, {leg_key(leg): leg for leg in after_legs}
    added = [after_map[key] for key in after_map.keys() - before_map.keys()]
    removed = [before_map[key] for key in before_map.keys() - after_map.keys()]
    if before == after:
        classification = "No Change"
    elif before_decision != "PASS" and after_decision == "PASS":
        classification = "Risk Downgrade to PASS"
    elif before_decision == "PASS" and after_decision != "PASS":
        classification = "Recovery from PASS"
    elif added or removed:
        classification = "Selection Change"
    elif before.get("robustness") != after.get("robustness"):
        classification = "Robustness Change"
    else:
        classification = "Recommendation Metadata Change"
    return {
        "classification": classification, "changed": before != after, "added_legs": added, "removed_legs": removed,
        "decision_change": {"before": before_decision, "after": after_decision},
        "source_change": {"before": before.get("source"), "after": after.get("source")},
        "robustness_change": {"before": before.get("robustness"), "after": after.get("robustness")},
    }


def _portfolio_change_drivers(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    drivers = []
    for row in rows:
        evaluation = row.get("evaluation") or {}
        decision = evaluation.get("decision_layer") or {}
        trigger = get_nested(evaluation, ["fundamental_version", "trigger"]) or {}
        line_movement = decision.get("line_movement") or {}
        reasons = list(dict.fromkeys([str(reason) for reason in (trigger.get("reasons") or []) + (decision.get("pass_reasons") or []) if reason]))
        classification = line_movement.get("classification")
        evidence_available = bool(reasons or classification or decision.get("best_market"))
        drivers.append({
            "fixture": row.get("fixture"), "evidence_status": "available" if evidence_available else "data_missing",
            "revalidation_triggered": bool(trigger.get("triggered")), "reasons": reasons,
            "market_move_classification": classification or "data_missing",
            "decision": decision.get("decision"), "best_market": decision.get("best_market"),
            "fundamental_version": get_nested(evaluation, ["fundamental_version", "version_number"]),
        })
    return drivers


def save_portfolio_run(portfolio_id: str, fixtures: List[str], portfolio: Dict[str, Any], max_legs: int, trigger_reasons: List[str], stage: str = "manual", change_drivers: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        runs = store.setdefault("portfolio_runs", {}).setdefault(portfolio_id, [])
        signature = _portfolio_recommendation_signature(portfolio)
        previous_signature = (runs[-1].get("recommendation") if runs else None)
        transition = _portfolio_transition(previous_signature, signature)
        next_version = max((int(run.get("version_number") or 0) for run in runs), default=0) + 1
        record = {
            "version_number": next_version, "created_at": int(time.time()), "portfolio_id": portfolio_id,
            "fixtures": fixtures, "risk_preference": portfolio.get("risk_preference"), "max_legs": max_legs,
            "stage": stage,
            "trigger_reasons": trigger_reasons or ["portfolio_evaluation"],
            "recommendation": signature,
            "recommendation_change": {"changed": previous_signature != signature, "before": previous_signature, "after": signature},
            "transition": transition,
            "change_drivers": change_drivers or [],
            "portfolio_decision": portfolio.get("portfolio_decision"),
        }
        runs.append(record)
        store["portfolio_runs"][portfolio_id] = runs[-PORTFOLIO_RUN_RETENTION:]
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def get_portfolio_runs(portfolio_id: str) -> List[Dict[str, Any]]:
    return list(load_snapshot_store().get("portfolio_runs", {}).get(portfolio_id, []))


def portfolio_run_timeline(runs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    latest_by_stage = {}
    for run in runs:
        stage = run.get("stage")
        if stage in PREMATCH_STAGE_ORDER and (stage not in latest_by_stage or int(run.get("created_at") or 0) >= int(latest_by_stage[stage].get("created_at") or 0)):
            latest_by_stage[stage] = run
    return [
        {"stage": stage, "status": "available", "run": latest_by_stage[stage]}
        if stage in latest_by_stage else
        {"stage": stage, "status": "data_missing", "reason": "portfolio was not evaluated at this checkpoint; later recommendations were not backfilled"}
        for stage in PREMATCH_STAGE_ORDER
    ]


def build_portfolio(evaluation_rows: List[Dict[str, Any]], max_legs: int = 6, risk_preference: str = "balanced") -> Dict[str, Any]:
    max_legs = normalize_max_legs(max_legs)
    risk_preference = str(risk_preference or "balanced").strip().lower()
    if risk_preference not in {"conservative", "balanced", "aggressive"}:
        risk_preference = "balanced"
    valid_rows = [row for row in evaluation_rows if row.get("fixture") and isinstance(row.get("evaluation"), dict)]
    high_variance = []
    for row in valid_rows:
        candidate = get_nested(row, ["evaluation", "decision_layer", "recommendation_tiers", "high_variance_single"])
        if candidate:
            high_variance.append({"fixture": row["fixture"], **candidate})
    high_variance.sort(key=lambda row: (row.get("ev", -999), row.get("edge", -999)), reverse=True)
    first = _combination_from_rows(valid_rows, "first_choice_high_consistency", max_legs, risk_preference=risk_preference)
    second = _combination_from_rows(valid_rows, "second_choice_higher_return", max_legs, allow_fallback=True, risk_preference=risk_preference)
    risk_adjusted = _risk_adjusted_combination(first, second, risk_preference)
    return {
        "first_choice_combination": first, "second_choice_combination": second,
        "risk_adjusted_recommendation": risk_adjusted,
        "high_variance_singles": high_variance,
        "portfolio_decision": "PASS" if first["decision"] == "PASS" and second["decision"] == "PASS" and not high_variance else "READY",
        "risk_preference": risk_preference,
        "rules": {"minimum_combination_legs": 2, "maximum_requested_legs": max_legs, "hard_cap": 10, "default_risk_preference": "balanced", "one_leg_per_correlation_group": True, "forced_fill": False},
    }


@app.get("/")
def root():
    return {"service": "football-shadow-data-service", "version": VERSION, "main_endpoints": ["/shadow/target-fixtures", "/shadow/analyze-fixture", "/shadow/tracking-plan", "/shadow/snapshot", "/shadow/snapshots", "/shadow/ai-packet", "/shadow/import-prematch-packets", "/shadow/import-status", "/shadow/data-source-health", "/shadow/model/poisson", "/shadow/model/fundamental-xg", "/shadow/model/prematch-evaluate", "/shadow/portfolio/evaluate", "/shadow/imported-prematch/{fixture}"]}


@app.get("/health")
def health():
    return {"ok": True, "timestamp": int(time.time()), "version": VERSION, "api_football_base_url": API_FOOTBALL_BASE_URL, "thestats_base_url": THESTATS_BASE_URL, "nami_base_url": NAMI_API_BASE_URL, "has_api_football_key": bool(API_FOOTBALL_KEY), "has_thestats_key": bool(THESTATS_API_KEY), "has_nami_credentials": bool(NAMI_API_USER and NAMI_API_SECRET), "nami_optional": True, "nami_failure_policy": "continue_without_nami", "shadow_token_enabled": bool(SHADOW_ACCESS_TOKEN), "auto_fetch_date": AUTO_FETCH_DATE, "auto_fetch_fixture_id": AUTO_FETCH_FIXTURE_ID, "snapshot_store_path": SNAPSHOT_STORE_PATH, "snapshot_store_gzip": SNAPSHOT_STORE_GZIP, "snapshot_store_warn_bytes": SNAPSHOT_STORE_WARN_BYTES, "fundamental_version_retention": FUNDAMENTAL_VERSION_RETENTION, "portfolio_run_retention": PORTFOLIO_RUN_RETENTION, "external_data_stale_seconds": EXTERNAL_DATA_STALE_SECONDS, "tracking_stages": STAGE_ORDER, "target_leagues": {str(k): v for k, v in DEFAULT_TARGET_LEAGUES.items() if k in TARGET_LEAGUE_IDS}}


@app.get("/shadow/nami-capabilities")
def shadow_nami_capabilities(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(nami_capability_check())


@app.get("/api-football/live")
def api_football_live():
    return JSONResponse(call_api_football("/fixtures", {"live": "all"}))


@app.get("/api-football/fixtures")
def api_football_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone")):
    return JSONResponse(call_api_football("/fixtures", {"date": date, "timezone": timezone_name}))


@app.get("/thestats/raw")
def thestats_raw(path: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(call_thestats(path))

@app.get("/shadow/historical-odds-test")
def shadow_historical_odds_test(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))

    auth = call_the_odds_api("/sports")

    result = {
        "ok": False,
        "auth_valid": auth.get("status_code") == 200,
        "historical_access": False,
        "sports_status": auth.get("status_code"),
        "historical_status": None,
        "quota_remaining": auth.get("quota_remaining"),
        "quota_used": auth.get("quota_used")
    }

    if not result["auth_valid"]:
        return JSONResponse(result)

def sportradar_live_selfcheck() -> Dict[str, Any]:
    if not SPORTRADAR_API_KEY:
        return {"ok": False, "error": "Missing SPORTRADAR_API_KEY"}
    try:
        resp = requests.get(
            f"{SPORTRADAR_SOCCER_BASE_URL}/schedules/live/schedules.json",
            headers={"x-api-key": SPORTRADAR_API_KEY},
            timeout=REQUEST_TIMEOUT,
        )
        data = safe_json_response(resp) if resp.ok else None
        events = data.get("schedules", []) if isinstance(data, dict) else []
        sample = None
        if events:
            event = events[0] if isinstance(events[0], dict) else {}
            sport_event = event.get("sport_event", {}) if isinstance(event, dict) else {}
            status = event.get("sport_event_status", {}) if isinstance(event, dict) else {}
            competitors = sport_event.get("competitors", []) if isinstance(sport_event, dict) else []
            sample = {
                "sport_event_id": sport_event.get("id"),
                "start_time": sport_event.get("start_time"),
                "status": status.get("status"),
                "match_status": status.get("match_status"),
                "home_score": status.get("home_score"),
                "away_score": status.get("away_score"),
                "competitors": [{"name": x.get("name"), "qualifier": x.get("qualifier")} for x in competitors[:2] if isinstance(x, dict)],
            }
        return {"ok": resp.ok, "status_code": resp.status_code, "live_event_count": len(events), "sample": sample}
    except requests.RequestException as exc:
        return {"ok": False, "error": type(exc).__name__}


# One-time startup-safe historical odds self-check helper.
def historical_odds_selfcheck() -> Dict[str, Any]:
    auth = call_the_odds_api("/sports")
    result = {
        "auth_valid": auth.get("status_code") == 200,
        "historical_access": False,
        "sports_status": auth.get("status_code"),
        "historical_status": None,
        "quota_remaining": auth.get("quota_remaining"),
        "quota_used": auth.get("quota_used"),
    }
    if not result["auth_valid"]:
        return result
    historical = call_the_odds_api("/historical/sports/soccer_epl/odds", {
        "regions": "eu", "markets": "h2h", "oddsFormat": "decimal",
        "date": "2024-01-01T12:00:00Z"
    })
    result["historical_status"] = historical.get("status_code")
    result["historical_access"] = historical.get("status_code") == 200
    result["quota_remaining"] = historical.get("quota_remaining")
    result["quota_used"] = historical.get("quota_used")
    return result


    historical = call_the_odds_api(
        "/historical/sports/soccer_epl/odds",
        {
            "regions": "eu",
            "markets": "h2h",
            "oddsFormat": "decimal",
            "date": "2024-01-01T12:00:00Z"
        }
    )

    result["historical_status"] = historical.get("status_code")
    result["historical_access"] = historical.get("status_code") == 200
    result["ok"] = result["historical_access"]
    result["quota_remaining"] = historical.get("quota_remaining")
    result["quota_used"] = historical.get("quota_used")

    return JSONResponse(result)


@app.get("/prematch/target-fixtures")
def prematch_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone")):
    return JSONResponse(target_fixtures_for_date(date, timezone_name))


@app.get("/shadow/target-fixtures")
def shadow_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(target_fixtures_for_date(date, timezone_name))


@app.get("/shadow/analyze-fixture")
def shadow_analyze_fixture(fixture: int, raw: bool = False, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(collect_prematch_data(fixture, include_raw=raw))


@app.get("/shadow/analyze")
def shadow_analyze(date: str, max_games: int = 5, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    fixtures = target_fixtures_for_date(date, timezone_name)
    rows = fixtures.get("fixtures", [])
    selected = ([x for x in rows if x.get("status") in ["NS", "TBD"]] or rows)[:max_games]
    analyses = [collect_prematch_data(int(x["fixture_id"]), include_raw=False) for x in selected if x.get("fixture_id")]
    return JSONResponse({"ok": True, "date": date, "selected_count": len(selected), "fixtures": selected, "analyses": analyses})


@app.get("/shadow/tracking-plan")
def shadow_tracking_plan(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    fixtures = target_fixtures_for_date(date, timezone_name)
    plans = [tracking_plan_for_fixture(x) for x in fixtures.get("fixtures", [])]
    return JSONResponse({"ok": True, "version": VERSION, "date": date, "target_count": fixtures.get("target_count"), "stage_order": STAGE_ORDER, "plans": plans})


@app.get("/shadow/snapshot")
def shadow_snapshot(fixture: int, stage: str = "manual", raw: bool = False, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    normalized = normalize_stage(stage)
    if normalized not in STAGE_ORDER and normalized != "manual":
        raise HTTPException(status_code=400, detail={"error": "unsupported_stage", "allowed": STAGE_ORDER, "received": stage})
    data = collect_prematch_data(fixture, include_raw=raw)
    history = get_fixture_snapshots(fixture)
    market_snapshot = get_nested(data, ["structured_inputs", "odds_market_snapshot"], empty_market_snapshot())
    dynamics = compare_market_snapshots(history, market_snapshot, normalized)
    versions = get_fundamental_versions(fixture)
    previous_version = versions[-1] if versions else None
    script = pure_fundamental_script(data)
    trigger = get_nested(dynamics, ["revalidation_trigger"], {}) or {}
    should_version = not versions or bool(trigger.get("triggered")) or normalized in ("T-1h", "T-15m", "Closing")
    fundamental_version = save_fundamental_version(fixture, script, {"stage": normalized, **trigger}, previous_version) if should_version else previous_version
    classification = classify_market_move_details(dynamics, previous_version, script)
    dynamics["classification"] = classification["classification"]
    dynamics["classification_audit"] = classification
    snapshot_at = int(time.time())
    record = {"version": VERSION, "fixture": fixture, "stage": normalized, "requested_stage": stage, "snapshot_at": snapshot_at, "fixture_info": data.get("fixture"), "data_quality": data.get("data_quality"), "coverage": data.get("coverage"), "market_snapshot": market_snapshot, "market_dynamics": dynamics, "team_news_snapshot": team_news_snapshot(data, normalized), "pure_fundamental_script_hash": script.get("content_hash"), "fundamental_version_number": (fundamental_version or {}).get("version_number"), "shadow_summary": data.get("shadow_summary"), "stage_timing_audit": audit_stage_timing(normalized, snapshot_at, get_nested(data, ["fixture", "date"]))}
    saved = save_snapshot(record)
    chain_audit = audit_fundamental_chain(script)
    if saved.get("revalidation_task_created"):
        if chain_audit.get("decision_eligible"):
            saved["revalidation_tasks_resolved"] = resolve_revalidation_tasks(str(fixture), fundamental_version, False, normalized)
        else:
            saved["revalidation_attempts_recorded"] = record_incomplete_revalidation_attempt(str(fixture), chain_audit, normalized)
    print("[SHADOW_SNAPSHOT] " + json.dumps({"stage": normalized, "fixture": fixture, "data_quality": data.get("data_quality"), "market_dynamics": dynamics, "summary": data.get("shadow_summary")}, ensure_ascii=False)[:6000])
    return JSONResponse({"ok": True, "saved": saved, "stage": normalized, "snapshot_at": record["snapshot_at"], "fixture": fixture, "market_snapshot": market_snapshot, "market_dynamics": dynamics, "pure_fundamental_script": script, "fundamental_version": fundamental_version, "data": data})


@app.get("/shadow/snapshots")
def shadow_snapshots(fixture: int, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    rows = get_fixture_snapshots(fixture)
    complete_timeline = complete_prematch_timeline(rows)
    return JSONResponse({"ok": True, "version": VERSION, "fixture": fixture, "stage_order": STAGE_ORDER, "count": len(rows), "snapshots": rows, "complete_prematch_timeline": complete_timeline, "timeline_coverage": {"available": sum(row["timeline_status"] == "available" for row in complete_timeline), "data_missing": sum(row["timeline_status"] == "data_missing" for row in complete_timeline), "required": len(PREMATCH_STAGE_ORDER)}})


@app.post("/shadow/import-prematch-packets")
async def shadow_import_prematch_packets(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    """Import canonical prematch packets without calling or changing upstream providers."""
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    body = await request.body()
    if len(body) > 30 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="request_body_too_large")
    try:
        if request.headers.get("content-encoding", "").lower() == "gzip":
            body = gzip.decompress(body)
            if len(body) > 150 * 1024 * 1024:
                raise HTTPException(status_code=413, detail="decompressed_body_too_large")
        payload = json.loads(body.decode("utf-8"))
    except HTTPException:
        raise
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise HTTPException(status_code=400, detail="invalid_json_or_gzip_body")
    packets = payload.get("packets") if isinstance(payload, dict) and "packets" in payload else payload
    if isinstance(packets, dict):
        packets = [packets]
    if not isinstance(packets, list) or not packets:
        raise HTTPException(status_code=400, detail="one_or_more_packets_required")
    if len(packets) > 100:
        raise HTTPException(status_code=413, detail="maximum_100_packets_per_request")
    with SNAPSHOT_STORE_LOCK:
        results, store = import_prematch_packet_batch(packets)
        previous = store.get("import_sync_status") or {}
        totals = {key: sum(row.get("counts", {}).get(key, 0) for row in results) for key in ("inserted", "updated", "unchanged", "stale_skipped")}
        store["import_sync_status"] = {
            "status": "ok", "last_success_at": int(time.time()), "previous_success_at": previous.get("last_success_at"),
            "packet_count": len(results), "fixtures": [row.get("fixture") for row in results],
            "request_bytes": len(await request.body()), "content_encoding": request.headers.get("content-encoding") or "identity",
            "mode": request.headers.get("x-sync-mode") or "batch", "source": request.headers.get("x-sync-source") or "external",
            "stage_counts": totals, "changed_fixture_count": sum(1 for row in results if row.get("changed")),
        }
        write_snapshot_store(store)
    return JSONResponse({"ok": True, "version": VERSION, "imported_count": len(results), "stage_counts": totals, "results": results})


@app.get("/shadow/import-status")
def shadow_import_status(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    store = load_snapshot_store()
    metadata = store.get("external_prematch", {})
    queue = list((store.get("fundamental_revalidation_queue") or {}).values())
    queue_view = revalidation_queue_view(queue)
    return JSONResponse({
        "ok": True, "version": VERSION, "sync": store.get("import_sync_status") or {"status": "never_imported"},
        "revalidation": {
            "pending": sum(task.get("status") == "pending" for task in queue_view),
            "unattempted": sum(task.get("workflow_state") in ("queued", "overdue_unattempted") for task in queue_view),
            "awaiting_evidence": sum(task.get("workflow_state") in ("awaiting_evidence", "overdue_awaiting_evidence") for task in queue_view),
            "overdue": sum(bool(task.get("overdue")) for task in queue_view if task.get("status") == "pending"),
            "over_capacity": sum(task.get("status") == "pending" for task in queue) > 500,
            "retention_policy": "pending_tasks_are_never_silently_evicted",
        },
        "fixture_count": len(metadata), "fixtures": [
            {"fixture": fixture, "league": row.get("league"), "imported_at": row.get("imported_at"), "match": row.get("match")}
            for fixture, row in metadata.items()
        ],
    })


@app.get("/shadow/revalidation-queue")
def shadow_revalidation_queue(status: str = "pending", fixture: Optional[str] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    if status not in {"pending", "revalidated", "superseded", "all"}:
        raise HTTPException(status_code=422, detail="status_must_be_pending_revalidated_superseded_or_all")
    tasks = list((load_snapshot_store().get("fundamental_revalidation_queue") or {}).values())
    if status != "all":
        tasks = [task for task in tasks if task.get("status") == status]
    if fixture:
        tasks = [task for task in tasks if task.get("fixture") == str(fixture)]
    tasks = revalidation_queue_view(tasks)
    return JSONResponse({"ok": True, "version": VERSION, "status": status, "fixture": fixture, "count": len(tasks), "tasks": tasks})


def imported_fixture_freshness(metadata: Dict[str, Any], history: List[Dict[str, Any]], now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(now_ts or time.time())
    kickoff = _parse_timestamp(get_nested(metadata, ["match", "kickoff_utc"]))
    available = [row for row in history if row.get("import_status") == "available" and get_nested(row, ["market_snapshot", "available"]) is not False]
    latest_row = latest_prematch_snapshot(available)
    latest = int((latest_row or {}).get("snapshot_at") or 0) or None
    age = now_ts - latest if latest else None
    if not latest_row or not latest:
        state, eligible, reason = "data_missing", False, "no_available_market_snapshot"
    elif age < -300:
        state, eligible, reason = "invalid_timestamp", False, "latest_market_snapshot_is_in_future"
    elif kickoff and now_ts >= kickoff:
        state, eligible, reason = "historical", False, "fixture_is_not_prematch"
    elif age > EXTERNAL_DATA_STALE_SECONDS:
        state, eligible, reason = "stale", False, "latest_market_snapshot_exceeds_freshness_threshold"
    else:
        state, eligible, reason = "fresh", True, None
    return {
        "state": state, "decision_eligible": eligible, "reason": reason,
        "latest_snapshot_at": latest, "age_seconds": age, "stale_after_seconds": EXTERNAL_DATA_STALE_SECONDS,
        "latest_stage": (latest_row or {}).get("stage"), "selection_policy": "latest_prematch_stage_not_latest_write_time",
        "future_tolerance_seconds": 300, "kickoff_at": kickoff,
    }


def audit_lineup_confidence(lineup_history: Any, submitted_confidence: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    submitted = as_float(submitted_confidence)
    rows = lineup_history if isinstance(lineup_history, list) else []
    valid_rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        observed_at = _parse_timestamp(row.get("observed_at") or row.get("as_of") or row.get("updated_at"))
        if observed_at is None or observed_at > now_ts + 300:
            continue
        label = str(row.get("status") or row.get("type") or row.get("lineup_type") or "").lower()
        official = row.get("is_official") is True or label in {"official", "confirmed", "starting_xi"}
        valid_rows.append({"observed_at": observed_at, "official": official})
    latest = max(valid_rows, key=lambda row: row["observed_at"], default=None)
    if latest:
        age = now_ts - latest["observed_at"]
        if latest["official"] and age <= 6 * 3600:
            evidence_status, cap = "official_fresh", .95
        elif not latest["official"] and age <= 24 * 3600:
            evidence_status, cap = "predicted_fresh", .80
        else:
            evidence_status, cap = "stale", .60 if latest["official"] else .50
    else:
        age, evidence_status, cap = None, "data_missing", .40
    effective = min(submitted, cap) if submitted is not None else None
    return {
        "evidence_status": evidence_status, "submitted_confidence": submitted,
        "evidence_cap": cap, "effective_confidence": effective,
        "latest_observed_at": latest["observed_at"] if latest else None,
        "age_seconds": age, "valid_observation_count": len(valid_rows),
        "capped": submitted is not None and effective < submitted,
    }


@app.get("/shadow/data-source-health")
def shadow_data_source_health(probe_nami: bool = False, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    store = load_snapshot_store()
    fixtures = []
    for fixture, metadata in (store.get("external_prematch", {}) or {}).items():
        freshness = imported_fixture_freshness(metadata, get_fixture_snapshots(fixture))
        fixtures.append({"fixture": fixture, "match": metadata.get("match"), "freshness": freshness})
    state_counts = {state: sum(1 for row in fixtures if row["freshness"]["state"] == state) for state in ("fresh", "stale", "historical", "invalid_timestamp", "data_missing")}
    nami = nami_capability_check() if probe_nami else {
        "configured": bool(NAMI_API_USER and NAMI_API_SECRET), "status": "not_probed",
        "optional": True, "failure_policy": "continue_without_nami",
    }
    return JSONResponse({
        "ok": True, "version": VERSION, "generated_at": int(time.time()),
        "pang": {"write_policy": "read_only_source_no_remote_tasks", "sync": store.get("import_sync_status") or {"status": "never_imported"}, "fixture_state_counts": state_counts, "fixtures": fixtures},
        "nami": nami, "decision_gate": {"requires_fresh_prematch_data": True, "stale_action": "PASS"},
    })


@app.get("/shadow/store-health")
def shadow_store_health(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "version": VERSION, "integrity": snapshot_store_integrity()})


@app.post("/shadow/model/poisson")
async def shadow_poisson_model(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json_body")
    model = poisson_probability_model(
        payload.get("home_expected_goals"), payload.get("away_expected_goals"),
        payload.get("input_confidence"), payload.get("provenance"),
    )
    if not model.get("ok"):
        raise HTTPException(status_code=422, detail=model)
    fixture = str(payload.get("fixture") or "").strip()
    market = empty_market_snapshot()
    freshness = None
    if fixture:
        store = load_snapshot_store()
        metadata = store.get("external_prematch", {}).get(fixture)
        if not metadata:
            raise HTTPException(status_code=404, detail="imported_fixture_not_found")
        history = get_fixture_snapshots(fixture)
        available = [row for row in history if row.get("import_status") == "available"]
        latest = latest_prematch_snapshot(available)
        market = (latest or {}).get("market_snapshot") or empty_market_snapshot()
        freshness = imported_fixture_freshness(metadata, history)
    decision = decision_layer(
        market, model.get("probabilities"), payload.get("script_coverage"),
        as_float(payload.get("crowding")), as_float(payload.get("lineup_confidence")), payload.get("death_path") if "death_path" in payload else None,
    )
    if model.get("status") != "ready":
        force_pass_decision(decision, "model_input_confidence_below_0_6")
    if freshness and not freshness.get("decision_eligible"):
        reason = freshness.get("reason") or "data_not_fresh"
        force_pass_decision(decision, reason)
    decision["data_freshness"] = freshness
    return JSONResponse({"ok": True, "version": VERSION, "fixture": fixture or None, "model": model, "decision_layer": decision})


@app.post("/shadow/model/fundamental-xg")
async def shadow_fundamental_xg(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json_body")
    estimator = fundamental_expected_goals(payload)
    if not estimator.get("ok"):
        raise HTTPException(status_code=422, detail=estimator)
    xg = estimator["expected_goals"]
    model = poisson_probability_model(xg["home"], xg["away"], estimator["confidence"], payload.get("provenance"))
    if estimator["status"] != "ready":
        model["status"] = "insufficient_confidence"
    return JSONResponse({"ok": True, "version": VERSION, "estimator": estimator, "model": model, "decision": "PASS", "reason": "probability_generation_only; bind a fresh fixture and complete decision inputs before evaluation"})


@app.post("/shadow/model/prematch-evaluate")
async def shadow_prematch_evaluate(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    """Run and audit the complete odds-independent model-to-market decision pipeline."""
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json_body")
    return JSONResponse(evaluate_imported_prematch(payload, persist_version=True))


@app.post("/shadow/portfolio/evaluate")
async def shadow_portfolio_evaluate(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json_body")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    matches = payload.get("matches")
    if not isinstance(matches, list) or not 1 <= len(matches) <= 10:
        raise HTTPException(status_code=422, detail="matches_must_contain_1_to_10_items")
    fixtures = [str(row.get("fixture") or "").strip() for row in matches if isinstance(row, dict)]
    if len(fixtures) != len(matches) or any(not fixture for fixture in fixtures) or len(set(fixtures)) != len(fixtures):
        raise HTTPException(status_code=422, detail="unique_fixture_required_for_each_match")
    raw_max_legs = payload.get("max_legs", 6)
    if isinstance(raw_max_legs, bool):
        raise HTTPException(status_code=422, detail="max_legs_must_be_an_integer_from_2_to_10")
    try:
        requested_max_legs = int(raw_max_legs)
    except (TypeError, ValueError):
        raise HTTPException(status_code=422, detail="max_legs_must_be_an_integer_from_2_to_10")
    if requested_max_legs < 2 or requested_max_legs > 10:
        raise HTTPException(status_code=422, detail="max_legs_must_be_an_integer_from_2_to_10")
    risk_preference = str(payload.get("risk_preference", "balanced")).strip().lower()
    if risk_preference not in {"conservative", "balanced", "aggressive"}:
        raise HTTPException(status_code=422, detail="risk_preference_must_be_conservative_balanced_or_aggressive")
    max_legs = requested_max_legs
    supplied_id = str(payload.get("portfolio_id") or "").strip()
    portfolio_id = supplied_id or "auto-" + _content_hash({"fixtures": sorted(fixtures)})[:16]
    if len(portfolio_id) > 100 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in portfolio_id):
        raise HTTPException(status_code=422, detail="portfolio_id_must_use_1_to_100_safe_characters")
    trigger_reasons = []
    for match in matches:
        reasons = get_nested(match, ["revalidation_trigger", "reasons"], [])
        if reasons is None:
            continue
        if not isinstance(reasons, list):
            raise HTTPException(status_code=422, detail="revalidation_trigger_reasons_must_be_an_array")
        trigger_reasons.extend(str(reason) for reason in reasons if reason)
    trigger_reasons = list(dict.fromkeys(trigger_reasons))
    stage = normalize_stage(str(payload.get("stage") or "manual"))
    if stage != "manual" and stage not in PREMATCH_STAGE_ORDER:
        raise HTTPException(status_code=422, detail="stage_must_be_opening_or_supported_t_minus_checkpoint")
    rows = []
    for match in matches:
        evaluation = evaluate_imported_prematch(match, persist_version=True)
        rows.append({"fixture": evaluation["fixture"], "match": evaluation.get("match"), "correlation_group": match.get("correlation_group") or evaluation["fixture"], "evaluation": evaluation})
    portfolio = build_portfolio(rows, max_legs, risk_preference)
    portfolio_run = save_portfolio_run(portfolio_id, fixtures, portfolio, max_legs, trigger_reasons, stage, _portfolio_change_drivers(rows))
    return JSONResponse({"ok": True, "version": VERSION, "portfolio_id": portfolio_id, "evaluations": rows, "portfolio": portfolio, "portfolio_run": portfolio_run})


@app.get("/shadow/portfolio/history/{portfolio_id}")
def shadow_portfolio_history(portfolio_id: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    runs = get_portfolio_runs(portfolio_id)
    if not runs:
        raise HTTPException(status_code=404, detail="portfolio_history_not_found")
    return JSONResponse({"ok": True, "version": VERSION, "portfolio_id": portfolio_id, "count": len(runs), "timeline": portfolio_run_timeline(runs), "manual_runs": [run for run in runs if run.get("stage") == "manual"], "runs": runs})


def build_imported_ai_packet(fixture: str, include_companies: bool = False, include_lineups: bool = False) -> Dict[str, Any]:
    store = load_snapshot_store()
    metadata = store.get("external_prematch", {}).get(str(fixture))
    if not metadata:
        raise HTTPException(status_code=404, detail="imported_fixture_not_found")
    history = get_fixture_snapshots(fixture)
    complete_timeline = complete_prematch_timeline(history)
    by_stage = {row.get("stage"): row for row in complete_timeline}
    timeline = []
    for stage in PREMATCH_STAGE_ORDER:
        row = by_stage.get(stage)
        if not row or row.get("timeline_status") != "available":
            timeline.append({
                "stage": stage, "status": "data_missing",
                "reason": (row or {}).get("missing_reason") or "historical checkpoint was not captured; current odds were not backfilled",
            })
            continue
        market = row.get("market_snapshot") or empty_market_snapshot()
        timeline.append({
            "stage": stage, "status": "available", "snapshot_at": row.get("snapshot_at"),
            "consensus_main_line": market.get("consensus_main_line"),
            **({"company_market_array": market.get("markets")} if include_companies else {}),
            "market_dynamics": row.get("market_dynamics"), "collection_profile": row.get("coverage"),
        })
    available = [row for row in complete_timeline if row.get("timeline_status") == "available"]
    latest = latest_prematch_snapshot(available)
    match = metadata.get("match") or {}
    lineup_history = metadata.get("lineup_history") or []
    fundamentals = {
        "status": "partial" if metadata.get("lineup_history") else "data_missing",
        "lineup_history_count": len(lineup_history),
        "lineup_history": lineup_history if include_lineups else None,
        "result_utility": {"status": "data_missing"}, "tactical_risk_appetite": {"status": "data_missing"},
        "rotation_quality": {"status": "partial" if metadata.get("lineup_history") else "data_missing"},
        "execution_ability": {"status": "data_missing"}, "tactical_matchup": {"status": "data_missing"},
        "game_state_elasticity": {"status": "data_missing"}, "first_goal_state_transition": {"status": "data_missing"},
        "open_game_beneficiary": {"status": "data_missing"}, "time_segment_strength": {"status": "data_missing"},
        "goal_conversion": {"status": "data_missing"},
    }
    current = (latest or {}).get("market_snapshot") or empty_market_snapshot()
    current_output = current if include_companies else {key: value for key, value in current.items() if key != "markets"}
    decision = decision_layer(current)
    decision = apply_line_movement_gate(decision, history)
    freshness = imported_fixture_freshness(metadata, history)
    decision["data_freshness"] = freshness
    if not freshness["decision_eligible"]:
        reason = freshness.get("reason") or "data_not_fresh"
        force_pass_decision(decision, reason)
    return {
        "ok": True, "status": "ready", "version": VERSION, "generated_at": int(time.time()),
        "source": "pang_import", "fixture": match, "data_quality": metadata.get("data_quality"), "data_freshness": freshness,
        "fundamentals": fundamentals,
        "market": {
            "current": current_output, "saved_stage_count": len(history), "available_prematch_stage_count": len(available), "saved_stages": [x.get("stage") for x in history],
            "required_timeline": PREMATCH_STAGE_ORDER,
            "missing_stages": [row.get("stage") for row in complete_timeline if row.get("timeline_status") != "available"],
            "timeline": timeline, "latest_dynamics": (latest or {}).get("market_dynamics"),
        },
        "analysis_rules": {
            "order": ["pure_fundamental_script", *FUNDAMENTAL_CHAIN, "market_timeline", "fundamental_revalidation", "model_probability_vs_market_no_vig_probability", "edge", "ev", "script_coverage", "crowding", "line_movement", "lineup_confidence", "death_path", "bet_or_pass"],
            "missing_data_rule": "Missing historical checkpoints and facts remain data_missing; never backfill them from current odds.",
            "prematch_only": True, "line_move_is_not_edge": True,
        },
        "market_move_classes": MOVE_CLASSES, "decision_layer": decision,
    }


@app.get("/shadow/imported-prematch/{fixture}")
def shadow_imported_prematch(fixture: str, include_companies: bool = False, include_lineups: bool = False, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(build_imported_ai_packet(fixture, include_companies=include_companies, include_lineups=include_lineups))



def build_shadow_ai_packet(fixture: int) -> Dict[str, Any]:
    """Build one AI-ready prematch packet from persisted market history + one current fundamentals fetch."""
    data = collect_prematch_data(fixture, include_raw=False)
    history = get_fixture_snapshots(fixture)
    complete_timeline = complete_prematch_timeline(history)
    by_stage = {row.get("stage"): row for row in complete_timeline}
    timeline = []
    for stage in PREMATCH_STAGE_ORDER:
        row = by_stage.get(stage)
        if not row or row.get("timeline_status") != "available":
            timeline.append({"stage": stage, "status": "data_missing", "reason": (row or {}).get("missing_reason") or "historical checkpoint was not captured or failed timing audit; current odds were not backfilled"})
            continue
        ms = row.get("market_snapshot") or {}
        primary = ms.get("consensus_main_line") or ms.get("primary") or {}
        timeline.append({
            "stage": row.get("stage"), "status": "available",
            "snapshot_at": row.get("snapshot_at"),
            "asian_handicap": primary.get("asian_handicap"),
            "over_under": primary.get("over_under"),
            "1x2": primary.get("1x2"),
            "btts": primary.get("btts"),
            "home_team_total": primary.get("home_team_total"),
            "away_team_total": primary.get("away_team_total"),
            "market_dynamics": row.get("market_dynamics"),
            "collection_profile": get_nested(row, ["coverage"], {})
        })
    available_history = [row for row in complete_timeline if row.get("timeline_status") == "available"]
    latest = latest_prematch_snapshot(available_history)
    first = min(available_history, key=lambda row: PREMATCH_STAGE_ORDER.index(row.get("stage")), default=None)
    first_ah = line_from_primary(get_nested(first or {}, ["market_snapshot", "primary", "asian_handicap"]))
    latest_ah = line_from_primary(get_nested(latest or {}, ["market_snapshot", "primary", "asian_handicap"]))
    total_ah_move = latest_ah - first_ah if first_ah is not None and latest_ah is not None else None
    si = data.get("structured_inputs") or {}
    script = pure_fundamental_script(data)
    versions = get_fundamental_versions(fixture)
    decision = decision_layer(si.get("odds_market_snapshot") or empty_market_snapshot())
    decision = apply_line_movement_gate(decision, history)
    upstream_ok = bool(data.get("ok")) and bool(data.get("fixture"))
    return {
        "ok": upstream_ok,
        "status": "ready" if upstream_ok else "upstream_unavailable_or_data_missing",
        "version": VERSION,
        "generated_at": int(time.time()),
        "fixture": data.get("fixture"),
        "data_quality": data.get("data_quality"),
        "coverage": data.get("coverage"),
        "fundamentals": {
            "standings": si.get("standings"),
            "recent_form_last_10": si.get("recent_form_last_10"),
            "season_stats": si.get("season_stats"),
            "injuries": si.get("injuries"),
            "lineups_available": si.get("lineups_available"),
            "prediction": si.get("prediction")
        },
        "pure_fundamental_script": script,
        "fundamental_versions": versions,
        "market": {
            "current": si.get("odds_market_snapshot"),
            "saved_stage_count": len(history),
            "available_prematch_stage_count": len(available_history),
            "saved_stages": [x.get("stage") for x in history],
            "required_timeline": PREMATCH_STAGE_ORDER,
            "missing_stages": [row.get("stage") for row in complete_timeline if row.get("timeline_status") != "available"],
            "timeline": timeline,
            "total_asian_handicap_move": total_ah_move,
            "latest_dynamics": latest.get("market_dynamics") if latest else None,
            "latest_saturation": get_nested(latest or {}, ["market_dynamics", "market_saturation"])
        },
        "analysis_rules": {
            "order": ["pure_fundamental_script", *FUNDAMENTAL_CHAIN, "market_timeline", "fundamental_revalidation", "model_probability_vs_market_no_vig_probability", "edge", "ev", "script_coverage", "crowding", "line_movement", "lineup_confidence", "death_path", "bet_or_pass"],
            "missing_data_rule": "Any unavailable injuries, lineups, odds, standings or other inputs must be marked 数据缺失; never infer missing facts.",
            "prematch_only": True,
            "line_move_is_not_edge": True
        },
        "market_move_classes": MOVE_CLASSES,
        "decision_layer": decision,
        "shadow_summary": data.get("shadow_summary")
    }


@app.get("/shadow/ai-packet")
def shadow_ai_packet(fixture: int, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(build_shadow_ai_packet(fixture))


@app.post("/shadow/evaluate")
async def shadow_evaluate(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    """Evaluate explicit independent model inputs without mutating fundamentals."""
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    fixture = int(payload.get("fixture"))
    history = get_fixture_snapshots(fixture)
    latest = latest_prematch_snapshot(history)
    current = ((latest or {}).get("market_snapshot")) or empty_market_snapshot()
    result = decision_layer(
        current, payload.get("model_probabilities"), payload.get("script_coverage"),
        as_float(payload.get("crowding")), as_float(payload.get("lineup_confidence")), payload.get("death_path") if "death_path" in payload else None
    )
    result = apply_line_movement_gate(result, history)
    return JSONResponse({"ok": True, "version": VERSION, "fixture": fixture, "evaluation": result})


@app.get("/shadow/report", response_class=PlainTextResponse)
def shadow_report(fixture: int, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    data = collect_prematch_data(fixture, include_raw=False)
    fx, si = data.get("fixture", {}), data.get("structured_inputs", {})
    lines = [f"【比赛】{fx.get('home')} vs {fx.get('away')} / {fx.get('league')} / {fx.get('league_round')}", f"【状态】{fx.get('status')}  开赛时间UTC：{fx.get('date')}", f"【数据完整度】{data.get('data_quality')}", f"【近期状态】主队：{get_nested(si, ['recent_form_last_10', 'home'])}", f"【近期状态】客队：{get_nested(si, ['recent_form_last_10', 'away'])}", f"【积分】主队：{get_nested(si, ['standings', 'home'])}", f"【积分】客队：{get_nested(si, ['standings', 'away'])}", f"【赛季统计】主队：{get_nested(si, ['season_stats', 'home'])}", f"【赛季统计】客队：{get_nested(si, ['season_stats', 'away'])}", f"【伤停】{si.get('injuries')}", f"【预测】{si.get('prediction')}", f"【赔率摘要】{si.get('odds')}", f"【盘口快照】{si.get('odds_market_snapshot')}", f"【影子摘要】{data.get('shadow_summary')}", "【注意】这是数据摘要，不是最终投注建议；临场前必须按 T-24h → T-12h → T-6h → T-3h → T-1h → T-15m → Closing → FT 追踪刷新。"]
    return "\n".join(lines)


@app.post("/sportradar/push/events")
async def sportradar_push_events(request: Request):
    payload = await request.json()
    LAST_PUSH_EVENTS.append({"received_at": int(time.time()), "payload": payload})
    del LAST_PUSH_EVENTS[:-50]
    return {"ok": True, "received": "events", "count": len(LAST_PUSH_EVENTS)}


@app.post("/sportradar/push/statistics")
async def sportradar_push_statistics(request: Request):
    payload = await request.json()
    LAST_PUSH_STATISTICS.append({"received_at": int(time.time()), "payload": payload})
    del LAST_PUSH_STATISTICS[:-50]
    return {"ok": True, "received": "statistics", "count": len(LAST_PUSH_STATISTICS)}


def auto_snapshot_stage_due(now: datetime, kickoff: datetime, stage: Dict[str, Any]) -> bool:
    due = kickoff + stage["offset"]
    delta = (now - due).total_seconds()
    if abs(delta) > AUTO_SNAPSHOT_WINDOW_SECONDS:
        return False
    return audit_stage_timing(stage["key"], int(now.timestamp()), int(kickoff.timestamp())).get("status") != "invalid"


def completed_auto_snapshot_stages(history: List[Dict[str, Any]]) -> set:
    return {
        row.get("stage") for row in history
        if row.get("stage") in PREMATCH_STAGE_ORDER
        and row.get("import_status") != "data_missing"
        and bool(get_nested(row, ["market_snapshot", "available"]))
        and get_nested(row, ["stage_timing_audit", "status"]) != "invalid"
        and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"
    }


def auto_snapshot_cycle() -> None:
    now = datetime.now(timezone.utc)
    dates = sorted({(now + timedelta(days=i)).astimezone(ZoneInfo(AUTO_FETCH_TIMEZONE)).date().isoformat() for i in range(AUTO_SNAPSHOT_DAYS_AHEAD + 1)})
    for date_str in dates:
        try:
            fixtures = target_fixtures_for_date(date_str, AUTO_FETCH_TIMEZONE)
            for fx in fixtures.get("fixtures", []):
                if fx.get("status") != "NS" or not fx.get("fixture_id"):
                    continue
                kickoff = fixture_datetime_utc(fx)
                if not kickoff:
                    continue
                history = get_fixture_snapshots(int(fx["fixture_id"]))
                saved_stages = completed_auto_snapshot_stages(history)
                for stage in TRACKING_STAGES:
                    key = stage["key"]
                    if key == "FT" or key in saved_stages:
                        continue
                    due = kickoff + stage["offset"]
                    if auto_snapshot_stage_due(now, kickoff, stage):
                        try:
                            data = collect_stage_snapshot_data(int(fx["fixture_id"]), key)
                            if not data.get("ok"):
                                print("[AUTO_SNAPSHOT] skipped invalid collection " + json.dumps({"fixture": fx["fixture_id"], "stage": key, "reason": data.get("error")}, ensure_ascii=False))
                                continue
                            odds_cov = get_nested(data, ["coverage", "odds_prematch"], {}) or {}
                            if not odds_cov.get("ok") or not odds_cov.get("has_data"):
                                print("[AUTO_SNAPSHOT] skipped missing odds " + json.dumps({"fixture": fx["fixture_id"], "stage": key, "status_code": odds_cov.get("status_code")}, ensure_ascii=False))
                                continue
                            history = get_fixture_snapshots(int(fx["fixture_id"]))
                            market_snapshot = get_nested(data, ["structured_inputs", "odds_market_snapshot"], empty_market_snapshot())
                            dynamics = compare_market_snapshots(history, market_snapshot, key)
                            versions = get_fundamental_versions(int(fx["fixture_id"]))
                            previous_version = versions[-1] if versions else None
                            trigger = get_nested(dynamics, ["revalidation_trigger"], {}) or {}
                            full_data = collect_prematch_data(int(fx["fixture_id"]), include_raw=False) if (not versions or trigger.get("triggered") or key in ("T-1h", "T-15m", "Closing")) else None
                            script = pure_fundamental_script(full_data) if full_data else get_nested(previous_version or {}, ["script"], {})
                            fundamental_version = save_fundamental_version(int(fx["fixture_id"]), script, {"stage": key, **trigger}, previous_version) if full_data else previous_version
                            classification = classify_market_move_details(dynamics, previous_version, script)
                            dynamics["classification"] = classification["classification"]
                            dynamics["classification_audit"] = classification
                            snapshot_at = int(time.time())
                            record = {"version": VERSION, "fixture": int(fx["fixture_id"]), "stage": key, "requested_stage": "auto", "snapshot_at": snapshot_at, "fixture_info": data.get("fixture"), "data_quality": data.get("data_quality"), "coverage": data.get("coverage"), "market_snapshot": market_snapshot, "market_dynamics": dynamics, "team_news_snapshot": team_news_snapshot(data, key), "pure_fundamental_script_hash": script.get("content_hash"), "fundamental_version_number": (fundamental_version or {}).get("version_number"), "shadow_summary": data.get("shadow_summary"), "stage_timing_audit": audit_stage_timing(key, snapshot_at, fx.get("date") or get_nested(data, ["fixture", "date"]))}
                            saved = save_snapshot(record)
                            saved_stages.add(key)
                            if saved.get("revalidation_task_created"):
                                chain_audit = audit_fundamental_chain(script)
                                if chain_audit.get("decision_eligible"):
                                    resolve_revalidation_tasks(str(fx["fixture_id"]), fundamental_version, False, key)
                                else:
                                    record_incomplete_revalidation_attempt(str(fx["fixture_id"]), chain_audit, key)
                            print("[AUTO_SNAPSHOT] saved " + json.dumps({"fixture": fx["fixture_id"], "stage": key, "due": due.isoformat()}, ensure_ascii=False))
                        except Exception as exc:
                            print("[AUTO_SNAPSHOT] fixture failed: " + str(exc))
        except Exception as exc:
            print("[AUTO_SNAPSHOT] date failed: " + date_str + " " + str(exc))


def auto_snapshot_worker() -> None:
    while True:
        try:
            auto_snapshot_cycle()
        except Exception as exc:
            print("[AUTO_SNAPSHOT] cycle failed: " + str(exc))
        time.sleep(AUTO_SNAPSHOT_POLL_SECONDS)


def start_auto_snapshot_worker() -> None:
    global AUTO_SNAPSHOT_THREAD_STARTED
    if not AUTO_SNAPSHOT_ENABLED or AUTO_SNAPSHOT_THREAD_STARTED:
        return
    AUTO_SNAPSHOT_THREAD_STARTED = True
    threading.Thread(target=auto_snapshot_worker, name="shadow-auto-snapshot", daemon=True).start()
    print("[AUTO_SNAPSHOT] worker started poll_seconds=" + str(AUTO_SNAPSHOT_POLL_SECONDS) + " window_seconds=" + str(AUTO_SNAPSHOT_WINDOW_SECONDS))



def validate_ai_packet_structure(fixture: int) -> Dict[str, Any]:
    """Local structural self-check; does not call external APIs."""
    history = get_fixture_snapshots(fixture)
    required_stages = [x for x in STAGE_ORDER if x != "FT"]
    checks = {
        "snapshot_store_persistent": str(SNAPSHOT_STORE_PATH).startswith("/data/"),
        "history_readable": isinstance(history, list),
        "stage_values_valid": all(x.get("stage") in STAGE_ORDER or x.get("stage") == "manual" for x in history),
        "market_snapshot_present": all("market_snapshot" in x for x in history),
        "market_dynamics_present": all("market_dynamics" in x for x in history),
    }
    saved_stages = [x.get("stage") for x in history]
    return {
        "fixture": fixture,
        "pass": all(checks.values()),
        "checks": checks,
        "saved_stage_count": len(history),
        "saved_stages": saved_stages,
        "missing_scheduled_stages": [x for x in required_stages if x not in saved_stages],
        "snapshot_store_path": SNAPSHOT_STORE_PATH,
    }




def choose_bootstrap_candidate() -> Optional[Dict[str, Any]]:
    """Find the nearest real NS target fixture inside the active tracking window."""
    now = datetime.now(timezone.utc)
    candidates: List[Dict[str, Any]] = []
    for day_delta in range(0, AUTO_SNAPSHOT_DAYS_AHEAD + 2):
        date_str = (now.date() + timedelta(days=day_delta)).isoformat()
        batch = target_fixtures_for_date(date_str, "UTC")
        for fx in batch.get("fixtures", []) or []:
            if fx.get("status") != "NS":
                continue
            kickoff = fixture_datetime_utc(fx)
            if not kickoff or kickoff <= now:
                continue
            hours = (kickoff - now).total_seconds() / 3600
            if 0 < hours <= 24.25:
                candidates.append({**fx, "_hours_to_kickoff": hours})
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["_hours_to_kickoff"])
    return candidates[0]


def bootstrap_current_snapshot_once() -> Dict[str, Any]:
    """Seed one truthful persisted baseline for the nearest real NS target fixture."""
    fx = choose_bootstrap_candidate()
    if not fx:
        return {"status": "skipped", "reason": "no_real_ns_fixture_inside_24h"}
    fixture_id = int(fx["fixture_id"])
    history = get_fixture_snapshots(fixture_id)
    if history:
        return {"fixture": fixture_id, "match": f"{fx.get('home')} vs {fx.get('away')}", "status": "skipped", "reason": "history_exists", "saved_stage_count": len(history)}
    kickoff = fixture_datetime_utc(fx)
    now = datetime.now(timezone.utc)
    prematch_stages = [x for x in TRACKING_STAGES if x["key"] != "FT"]
    due = [x for x in prematch_stages if now >= kickoff + x["offset"]]
    if not due:
        return {"fixture": fixture_id, "status": "skipped", "reason": "before_t24h"}
    stage = due[-1]["key"]
    data = collect_stage_snapshot_data(fixture_id, stage)
    if not data.get("ok"):
        return {"fixture": fixture_id, "status": "skipped", "reason": data.get("error") or "collection_failed"}
    odds_coverage = get_nested(data, ["coverage", "odds_prematch"], {}) or {}
    market_snapshot = get_nested(data, ["structured_inputs", "odds_market_snapshot"], empty_market_snapshot())
    if not odds_coverage.get("ok") or not odds_coverage.get("has_data") or not market_snapshot.get("available"):
        return {"fixture": fixture_id, "status": "skipped", "reason": "odds_data_missing"}
    dynamics = compare_market_snapshots([], market_snapshot, stage)
    snapshot_at = int(time.time())
    record = {
        "version": VERSION, "fixture": fixture_id, "stage": stage, "requested_stage": "bootstrap_current_once",
        "snapshot_at": snapshot_at, "fixture_info": fx, "data_quality": data.get("data_quality"),
        "coverage": data.get("coverage"), "market_snapshot": market_snapshot,
        "market_dynamics": dynamics, "team_news_snapshot": team_news_snapshot(data, stage),
        "shadow_summary": data.get("shadow_summary"),
        "stage_timing_audit": audit_stage_timing(stage, snapshot_at, fx.get("date")),
    }
    saved = save_snapshot(record)
    return {"fixture": fixture_id, "match": f"{fx.get('home')} vs {fx.get('away')}", "status": "saved", "stage": stage, "hours_to_kickoff": round(fx.get("_hours_to_kickoff", 0), 2), "saved": saved}


def validate_persistent_system() -> Dict[str, Any]:
    store = load_snapshot_store()
    fixtures = store.get("fixtures", {}) if isinstance(store, dict) else {}
    total = sum(len(v or []) for v in fixtures.values())
    return {
        "pass": str(SNAPSHOT_STORE_PATH).startswith("/data/") and isinstance(fixtures, dict),
        "snapshot_store_path": SNAPSHOT_STORE_PATH,
        "fixture_count": len(fixtures),
        "snapshot_count": total,
        "fixtures": {k: [x.get("stage") for x in (v or [])] for k, v in fixtures.items()}
    }


def startup_ai_packet_selfcheck() -> None:
    result = validate_persistent_system()
    print("[AI_PACKET_SELFCHECK] " + json.dumps(result, ensure_ascii=False))


@app.on_event("startup")
def startup_fetch():
    # Production startup is intentionally API-light. The background scheduler owns
    # fixture discovery and stage collection; startup only validates local persistence.
    start_auto_snapshot_worker()
    startup_ai_packet_selfcheck()
    live_check = sportradar_live_selfcheck()
    print("[SPORTRADAR_LIVE_SELFCHECK] " + json.dumps(live_check, ensure_ascii=False))
    print("[STARTUP] API-light mode enabled; no duplicate prematch/bootstrap fetches")
