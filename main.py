import json
import copy
import gzip
import io
import math
import os
import time
import threading
import hashlib
import hmac
import unicodedata
from statistics import median
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import requests
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from the_odds_api_provider import collect_historical_timeline

load_dotenv()

VERSION = "2.19.10"
RELEASE_CHANNEL = "shadow-usable"
RELEASE_SOURCE_REVISION = os.getenv("RELEASE_COMMIT_SHA", "").strip().lower()
PROVIDER_RECONCILIATION_SCHEMA_VERSION = 2
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
AUTO_FETCH_DATE = os.getenv("AUTO_FETCH_DATE", "2026-09-28")
AUTO_FETCH_TIMEZONE = os.getenv("AUTO_FETCH_TIMEZONE", "Asia/Shanghai")
AUTO_FETCH_FIXTURE_ID = os.getenv("AUTO_FETCH_FIXTURE_ID", "")
SHADOW_ACCESS_TOKEN = os.getenv("SHADOW_ACCESS_TOKEN", "")
SNAPSHOT_STORE_PATH = os.getenv("SNAPSHOT_STORE_PATH", "/tmp/shadow_snapshots.json")
SNAPSHOT_STORE_GZIP = os.getenv("SNAPSHOT_STORE_GZIP", "true").lower() in ("1", "true", "yes", "on")
try:
    SNAPSHOT_STORE_WRITER_WORKERS = int(os.getenv("UVICORN_WORKERS", "1"))
except (TypeError, ValueError):
    SNAPSHOT_STORE_WRITER_WORKERS = 0
EXTERNAL_DATA_STALE_SECONDS = max(60, int(os.getenv("EXTERNAL_DATA_STALE_SECONDS", "1800")))
FUNDAMENTAL_VERSION_RETENTION = max(10, min(int(os.getenv("FUNDAMENTAL_VERSION_RETENTION", "100")), 1000))
PORTFOLIO_RUN_RETENTION = max(10, min(int(os.getenv("PORTFOLIO_RUN_RETENTION", "100")), 1000))
MAX_IMPORT_DECOMPRESSED_BYTES = 150 * 1024 * 1024


def bounded_gzip_decompress(raw: bytes, max_bytes: int) -> bytes:
    """Decompress at most max_bytes, avoiding unbounded gzip expansion."""
    with gzip.GzipFile(fileobj=io.BytesIO(raw), mode="rb") as stream:
        decoded = stream.read(max_bytes + 1)
    if len(decoded) > max_bytes:
        raise ValueError("decompressed_size_limit_exceeded")
    return decoded
SNAPSHOT_STORE_WARN_BYTES = max(1024 * 1024, int(os.getenv("SNAPSHOT_STORE_WARN_BYTES", str(256 * 1024 * 1024))))
SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES = max(
    1024 * 1024,
    int(os.getenv("SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES", str(512 * 1024 * 1024))),
)
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
REAL_MONEY_MAX_AGE_SECONDS = max(300, int(os.getenv("REAL_MONEY_MAX_AGE_SECONDS", "21600")))
REAL_MONEY_SOURCE_TYPES = {"bookmaker_official", "betting_exchange", "regulated_market_data", "verified_money_vendor"}
try:
    _real_money_registry_raw = json.loads(os.getenv("REAL_MONEY_SOURCE_REGISTRY_JSON", "{}"))
    REAL_MONEY_SOURCE_REGISTRY = {
        str(domain).strip().lower(): entry
        for domain, entry in (_real_money_registry_raw.items() if isinstance(_real_money_registry_raw, dict) else [])
        if str(domain).strip() and isinstance(entry, dict)
    }
except (TypeError, ValueError, json.JSONDecodeError):
    REAL_MONEY_SOURCE_REGISTRY = {}
REAL_MONEY_MARKET_SELECTIONS = {
    "1x2": ("home", "draw", "away"), "asian_handicap": ("home", "away"),
    "over_under": ("over", "under"), "btts": ("yes", "no"),
    "home_team_total": ("over", "under"), "away_team_total": ("over", "under"),
}
AUTO_SNAPSHOT_ENABLED = os.getenv("AUTO_SNAPSHOT_ENABLED", "true").lower() in ("1", "true", "yes", "on")
AUTO_SNAPSHOT_POLL_SECONDS = max(60, int(os.getenv("AUTO_SNAPSHOT_POLL_SECONDS", "300")))
AUTO_SNAPSHOT_WINDOW_SECONDS = max(60, int(os.getenv("AUTO_SNAPSHOT_WINDOW_SECONDS", "600")))
AUTO_SNAPSHOT_DAYS_AHEAD = max(1, int(os.getenv("AUTO_SNAPSHOT_DAYS_AHEAD", "2")))
AUTO_SNAPSHOT_THREAD_STARTED = False
AUTO_SNAPSHOT_THREAD: Optional[threading.Thread] = None
AUTO_SNAPSHOT_LAST_CYCLE_AT: Optional[int] = None
AUTO_SNAPSHOT_LAST_ERROR: Optional[str] = None
AUTO_RECONCILIATION_LAST_RESULT: Optional[Dict[str, Any]] = None
AUTO_LEARNING_LAST_RESULT: Optional[Dict[str, Any]] = None
LEARNING_SHADOW_MODEL_RUNNER: Optional[Any] = None
LEARNING_CHALLENGER_MAX_MODULE_LOG_RATE_DELTA = 0.25
LEARNING_CHALLENGER_MAX_COMBINED_LOG_RATE_DELTA = 0.40
THESTATS_FIXTURE_DAY_CACHE_TTL_SECONDS = max(60, min(int(os.getenv("THESTATS_FIXTURE_DAY_CACHE_TTL_SECONDS", "600")), 3600))
THESTATS_FIXTURE_DAY_CACHE_LOCK = threading.RLock()
THESTATS_FIXTURE_DAY_CACHE: Dict[str, Dict[str, Any]] = {}
NAMI_ODDS_STARTUP_PROBE: Dict[str, Any] = {"status": "pending", "decision_use": False}
NAMI_ODDS_STARTUP_PROBE_STARTED = False
NAMI_ODDS_PROBE_TTL_SECONDS = max(3600, int(os.getenv("NAMI_ODDS_PROBE_TTL_SECONDS", str(7 * 24 * 3600))))
CALIBRATION_MIN_SAMPLE = max(1, int(os.getenv("CALIBRATION_MIN_SAMPLE", "30")))
LEARNING_MIN_VALIDATION_SAMPLES = max(2, int(os.getenv("LEARNING_MIN_VALIDATION_SAMPLES", "30")))
LEARNING_RESEARCH_PROPOSAL_MIN_MATCHES = max(3, int(os.getenv("LEARNING_RESEARCH_PROPOSAL_MIN_MATCHES", "5")))
LEARNING_RESEARCH_PROPOSAL_MIN_FAILURES = max(2, int(os.getenv("LEARNING_RESEARCH_PROPOSAL_MIN_FAILURES", "3")))
LEARNING_RESEARCH_PROPOSAL_MIN_FAILURE_RATE = min(1.0, max(0.05, float(os.getenv("LEARNING_RESEARCH_PROPOSAL_MIN_FAILURE_RATE", "0.25"))))
LEARNING_PROCESS_CLASSES = {
    "PROCESS_CORRECT_RESULT_WIN", "PROCESS_CORRECT_RESULT_LOSS",
    "PROCESS_ERROR_RESULT_WIN", "PROCESS_ERROR_RESULT_LOSS",
    "EVENT_CONTAMINATED", "DATA_INSUFFICIENT",
}
LEARNING_HYPOTHESIS_TYPES = {"HYPOTHESIS_ONLY", "LEAGUE_TAG_CANDIDATE"}
LEARNING_REVIEW_STATUSES = {"passed", "failed", "inconclusive", "data_missing", "not_applicable"}
LEARNING_REVIEW_SECTIONS = (
    "match_selection_quality", "fundamental_chain_audit", "state_tree_coverage",
    "market_language_audit", "expression_audit", "price_execution_audit",
)
LEAGUE_DNA_CATEGORIES = {
    "goal_environment", "handicap_and_parity", "corner_environment",
    "tempo_and_state_elasticity", "match_context",
    "discipline_and_officiating", "market_microstructure",
}
LEAGUE_DNA_MARKETS = {
    "goals", "1x2", "asian_handicap", "over_under", "btts",
    "corners", "cards", "state_tree", "market_expression",
}
LEARNING_PROMOTION_REQUIRED_GATES = (
    "pre_registration", "pit_integrity", "event_pollution_audit",
    "out_of_sample_shadow", "ablation", "calibration",
    "clv_or_price_quality", "process_accuracy", "risk_review",
)
LEARNING_ABLATION_INTERVENTIONS = {
    "MSCB": "remove_mscb_adjustment",
    "STATE_TREE": "remove_state_tree_layer",
    "IEH": "neutralize_ieh_state_parameter",
    "TAC": "neutralize_tac_state_parameter",
    "TDD": "neutralize_tdd_state_parameter",
    "LET": "neutralize_let_state_parameter",
    "LPS": "neutralize_lps_state_parameter",
    "OCR": "neutralize_ocr_state_parameter",
}
API_FOOTBALL_RATE_LIMIT_UNTIL = 0
API_FOOTBALL_STARTUP_PROBE_STARTED = False
API_FOOTBALL_STARTUP_PROBE: Dict[str, Any] = {"status": "not_started", "authenticated": False}
SNAPSHOT_STORE_LOCK = threading.RLock()

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")
THE_ODDS_API_KEY = os.getenv("THE_ODDS_API_KEY", "")
THE_ODDS_API_BASE_URL = os.getenv("THE_ODDS_API_BASE_URL", "https://api.the-odds-api.com/v4").rstrip("/")
THE_ODDS_API_REGIONS = os.getenv("THE_ODDS_API_REGIONS", "fi,eu")
THE_ODDS_API_BOOKMAKERS = os.getenv("THE_ODDS_API_BOOKMAKERS", "")
THE_ODDS_API_OPENING_LOOKBACK_DAYS = max(1, min(int(os.getenv("THE_ODDS_API_OPENING_LOOKBACK_DAYS", "7")), 30))
THE_ODDS_API_OPENING_SCAN_HOURS = max(1, min(int(os.getenv("THE_ODDS_API_OPENING_SCAN_HOURS", "12")), 24))
THE_ODDS_API_MAX_HISTORY_REQUESTS = max(16, min(int(os.getenv("THE_ODDS_API_MAX_HISTORY_REQUESTS", "48")), 100))
THE_ODDS_API_MIN_1X2_BOOKMAKERS = max(1, int(os.getenv("THE_ODDS_API_MIN_1X2_BOOKMAKERS", "5")))
THE_ODDS_API_MIN_AH_BOOKMAKERS = max(1, int(os.getenv("THE_ODDS_API_MIN_AH_BOOKMAKERS", "3")))
THE_ODDS_API_MIN_OU_BOOKMAKERS = max(1, int(os.getenv("THE_ODDS_API_MIN_OU_BOOKMAKERS", "3")))
THE_ODDS_API_DISCOVERY_MAX_ATTEMPTS = max(1, min(int(os.getenv("THE_ODDS_API_DISCOVERY_MAX_ATTEMPTS", "3")), 5))
THE_ODDS_API_DISCOVERY_RETRY_SECONDS = max(300, min(int(os.getenv("THE_ODDS_API_DISCOVERY_RETRY_SECONDS", "900")), 3600))
THE_ODDS_API_WATCHLIST_ARCHIVE_RETENTION = max(100, min(int(os.getenv("THE_ODDS_API_WATCHLIST_ARCHIVE_RETENTION", "5000")), 50000))
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
# Curated major-league learning scope is intentionally separate from the
# broader analysis target list above. Being a domestic tier-one league is only
# a necessary condition: competitions outside this explicit user-approved
# allowlist remain available for ordinary analysis and market-language
# settlement, but can never enter model learning automatically.
LEARNING_MAJOR_LEAGUES_VERSION = os.getenv("LEARNING_MAJOR_LEAGUES_VERSION", "2026-10-08-v1").strip() or "2026-10-08-v1"
LEARNING_MAJOR_LEAGUES: Dict[int, Dict[str, str]] = {
    39: {"name": "England Premier League", "country": "England"},
    61: {"name": "France Ligue 1", "country": "France"},
    71: {"name": "Brazil Serie A", "country": "Brazil"},
    78: {"name": "Germany Bundesliga", "country": "Germany"},
    88: {"name": "Netherlands Eredivisie", "country": "Netherlands"},
    94: {"name": "Portugal Primeira Liga", "country": "Portugal"},
    103: {"name": "Norway Eliteserien", "country": "Norway"},
    113: {"name": "Sweden Allsvenskan", "country": "Sweden"},
    128: {"name": "Argentina Liga Profesional", "country": "Argentina"},
    135: {"name": "Italy Serie A", "country": "Italy"},
    140: {"name": "Spain La Liga", "country": "Spain"},
    144: {"name": "Belgium Pro League", "country": "Belgium"},
    203: {"name": "Turkey Super Lig", "country": "Turkey"},
    253: {"name": "USA Major League Soccer", "country": "USA"},
}
# Explicit cross-provider registry for odds-only fixture discovery. The
# registry is deliberately coupled to the user-approved major-league pool:
# unmapped or newly added competitions cannot enter the watchlist silently.
THE_ODDS_API_MAJOR_LEAGUE_SPORT_KEYS: Dict[int, str] = {
    39: "soccer_epl",
    61: "soccer_france_ligue_one",
    71: "soccer_brazil_campeonato",
    78: "soccer_germany_bundesliga",
    88: "soccer_netherlands_eredivisie",
    94: "soccer_portugal_primeira_liga",
    103: "soccer_norway_eliteserien",
    113: "soccer_sweden_allsvenskan",
    128: "soccer_argentina_primera_division",
    135: "soccer_italy_serie_a",
    140: "soccer_spain_la_liga",
    144: "soccer_belgium_first_div",
    203: "soccer_turkey_super_league",
    253: "soccer_usa_mls",
}
# Backward-compatible alias. New code and external contracts should use the
# major-league name because domestic tier one alone is not sufficient.
LEARNING_TOP_FLIGHT_LEAGUES = LEARNING_MAJOR_LEAGUES
LEARNING_DISCOVERY_HORIZON_HOURS = max(1, min(int(os.getenv("LEARNING_DISCOVERY_HORIZON_HOURS", "24")), 72))
LEARNING_POSTMATCH_LOOKBACK_HOURS = max(1, min(int(os.getenv("LEARNING_POSTMATCH_LOOKBACK_HOURS", "36")), 168))
LEARNING_NODE_LEASE_SECONDS = max(60, min(int(os.getenv("LEARNING_NODE_LEASE_SECONDS", "900")), 3600))
LEARNING_NODE_RETRY_BASE_SECONDS = max(60, min(int(os.getenv("LEARNING_NODE_RETRY_BASE_SECONDS", "300")), 3600))
LEARNING_NODE_RETRY_MAX_SECONDS = max(
    LEARNING_NODE_RETRY_BASE_SECONDS,
    min(int(os.getenv("LEARNING_NODE_RETRY_MAX_SECONDS", "3600")), 12 * 3600),
)
LEARNING_NODE_ATTEMPT_RETENTION = max(100, min(int(os.getenv("LEARNING_NODE_ATTEMPT_RETENTION", "5000")), 50000))
LEARNING_RULES_VERSION = os.getenv("LEARNING_RULES_VERSION", "MODEL_RULES.md@2026-10-10-v2.19.10").strip() or "MODEL_RULES.md@2026-10-10-v2.19.10"
raw_target = os.getenv("TARGET_LEAGUE_IDS", "")
TARGET_LEAGUE_IDS = {int(x.strip()) for x in raw_target.split(",") if x.strip().isdigit()} if raw_target.strip() else set(DEFAULT_TARGET_LEAGUES.keys())
raw_nami_target = os.getenv("NAMI_TARGET_COMPETITION_IDS", "")
NAMI_TARGET_COMPETITION_IDS = {str(x).strip() for x in raw_nami_target.split(",") if str(x).strip()}
DEFAULT_NAMI_TARGET_COMPETITIONS: Dict[str, str] = {
    "2906": "UEFA Nations League",
}
NAMI_TARGET_COMPETITION_IDS.update(DEFAULT_NAMI_TARGET_COMPETITIONS)

TRACKING_STAGES = [
    {"key": "Opening", "label": "Opening 开盘", "offset": timedelta(hours=-48), "purpose": "仅保存有来源证明的真实开盘；自动当前赔率不得冒充开盘"},
    {"key": "T-24h", "label": "T-24h 初判", "offset": timedelta(hours=-24), "purpose": "初始基本面与早盘定位"},
    {"key": "T-12h", "label": "T-12h 早盘确认", "offset": timedelta(hours=-12), "purpose": "早盘/水位第一次确认"},
    {"key": "T-6h", "label": "T-6h 盘口/赔率确认", "offset": timedelta(hours=-6), "purpose": "盘口持续性与赔率结构确认"},
    {"key": "T-3h", "label": "T-3h 临场前修正", "offset": timedelta(hours=-3), "purpose": "盘口跨档、反转、饱和度观察"},
    {"key": "T-1h", "label": "T-1h 阵容伤停确认", "offset": timedelta(hours=-1), "purpose": "首发/伤停/临场水位修正"},
    {"key": "T-30m", "label": "T-30m 最终影子判断", "offset": timedelta(minutes=-30), "purpose": "最终临场确认"},
    {"key": "Closing", "label": "Closing 封盘", "offset": timedelta(minutes=-1), "purpose": "封盘盘口与最终水位"},
    {"key": "FT", "label": "FT 赛后复盘", "offset": timedelta(hours=2), "purpose": "赛后复盘命中/偏差"},
]
STAGE_ORDER = [x["key"] for x in TRACKING_STAGES]
PREMATCH_STAGE_ORDER = [x for x in STAGE_ORDER if x != "FT"]
LEARNING_PREMATCH_STAGE_ORDER = ["Opening", "T-12h", "T-6h", "T-1h"]
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


def require_configured_shadow_token(token: Optional[str], unavailable_detail: str = "shadow_access_token_required") -> None:
    """Fail closed for endpoints that can mutate learning state or consume paid quota."""
    if not SHADOW_ACCESS_TOKEN:
        raise HTTPException(status_code=503, detail=unavailable_detail)
    require_shadow_token(token)


def require_learning_token(token: Optional[str]) -> None:
    """Learning writes and discovery fail closed until an access token exists."""
    require_configured_shadow_token(token, "shadow_access_token_required_for_learning")


def require_paid_odds_token(token: Optional[str]) -> None:
    """Paid historical-odds requests must never become public when auth is unset."""
    require_configured_shadow_token(token, "shadow_access_token_required_for_paid_odds")


def resolve_shadow_token(query_token: Optional[str], authorization: Optional[str], header_token: Optional[str]) -> Optional[str]:
    if header_token:
        return header_token
    if authorization and authorization.lower().startswith("bearer "):
        return authorization[7:].strip()
    return query_token


def mask_secret(text: str) -> str:
    configured = {str(key) for key in [API_FOOTBALL_KEY, THESTATS_API_KEY, THE_ODDS_API_KEY, ISPORTS_API_KEY, SHADOW_ACCESS_TOKEN, NAMI_API_USER, NAMI_API_SECRET] if key}
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


def api_football_business_error(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return "invalid_payload"
    errors = payload.get("errors")
    if errors in (None, {}, []):
        return None
    return "api_football_business_error"


def root_business_error(payload: Any, source: str) -> Optional[str]:
    if not isinstance(payload, (dict, list)):
        return f"{source}_invalid_payload"
    if isinstance(payload, list):
        return None
    if payload.get("error") not in (None, "", {}, []):
        return f"{source}_business_error"
    if payload.get("errors") not in (None, "", {}, []):
        return f"{source}_business_error"
    if payload.get("success") is False or str(payload.get("status") or "").lower() in ("error", "failed", "failure"):
        return f"{source}_business_error"
    return None


def nami_business_error(payload: Any) -> Optional[str]:
    if not isinstance(payload, dict):
        return "nami_invalid_payload"
    for key in ("err", "error", "errors"):
        if payload.get(key) not in (None, "", {}, []):
            return mask_secret(str(payload.get(key)))
    code = payload.get("code")
    if code not in (None, 0, "0", ""):
        return mask_secret(str(payload.get("message") or payload.get("msg") or f"nami_code_{code}"))
    if payload.get("success") is False or str(payload.get("status") or "").lower() in ("error", "failed", "failure"):
        return mask_secret(str(payload.get("message") or payload.get("msg") or "nami_business_error"))
    return None


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
        payload = safe_json_response(resp)
        business_error = api_football_business_error(payload)
        if resp.status_code == 429:
            API_FOOTBALL_RATE_LIMIT_UNTIL = int(time.time()) + 3600
            print("[API_FOOTBALL] 429 received; cooldown_seconds=3600")
        elif business_error and any(word in str((payload or {}).get("errors", "")).lower() for word in ("limit", "quota", "rate")):
            API_FOOTBALL_RATE_LIMIT_UNTIL = int(time.time()) + 3600
            print("[API_FOOTBALL] business rate-limit received; cooldown_seconds=3600")
        return {"ok": bool(resp.ok and not business_error), "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": payload, "error": business_error}
    except requests.RequestException as exc:
        return {"ok": False, "error": mask_secret(str(exc)), "request_url": mask_secret(url)}


def api_football_capability_check() -> Dict[str, Any]:
    """Verify the configured credential without exposing it or consuming fixture quota."""
    if not API_FOOTBALL_KEY:
        return {"status": "skipped", "configured": False, "authenticated": False, "reason": "api_football_not_configured"}
    result = call_api_football("/status")
    payload = result.get("data") if isinstance(result.get("data"), dict) else {}
    errors = payload.get("errors") if isinstance(payload, dict) else None
    error_text = str(errors or result.get("error") or "").lower()
    if result.get("ok"):
        category = None
    elif any(token in error_text for token in ("missing application key", "invalid", "token", "unauthor")):
        category = "credential_rejected"
    elif any(token in error_text for token in ("limit", "quota", "rate")):
        category = "quota_or_rate_limit"
    else:
        category = "upstream_error"
    return {
        "status": "completed", "checked_at": int(time.time()),
        "configured": True, "authenticated": result.get("ok") is True,
        "status_code": result.get("status_code"), "error_category": category,
    }


def run_api_football_startup_probe() -> None:
    global API_FOOTBALL_STARTUP_PROBE
    try:
        API_FOOTBALL_STARTUP_PROBE = api_football_capability_check()
    except Exception as exc:
        API_FOOTBALL_STARTUP_PROBE = {
            "status": "error", "checked_at": int(time.time()),
            "configured": bool(API_FOOTBALL_KEY), "authenticated": False,
            "error_type": type(exc).__name__,
        }


def start_api_football_startup_probe() -> None:
    global API_FOOTBALL_STARTUP_PROBE_STARTED, API_FOOTBALL_STARTUP_PROBE
    if API_FOOTBALL_STARTUP_PROBE_STARTED:
        return
    API_FOOTBALL_STARTUP_PROBE_STARTED = True
    if not API_FOOTBALL_KEY:
        API_FOOTBALL_STARTUP_PROBE = {"status": "skipped", "configured": False, "authenticated": False, "reason": "api_football_not_configured"}
        return
    threading.Thread(target=run_api_football_startup_probe, name="api-football-startup-probe", daemon=True).start()


def call_thestats(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not THESTATS_API_KEY:
        return {"ok": False, "error": "Missing THESTATS_API_KEY"}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{THESTATS_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {THESTATS_API_KEY}", "Accept": "application/json"}
    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        payload = safe_json_response(resp)
        business_error = root_business_error(payload, "thestats")
        return {"ok": bool(resp.ok and not business_error), "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": payload, "error": business_error}
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
        payload = safe_json_response(resp)
        business_error = root_business_error(payload, "the_odds_api")
        return {
            "ok": bool(resp.ok and not business_error),
            "status_code": resp.status_code,
            "data": payload,
            "error": business_error,
            "quota_remaining": resp.headers.get("x-requests-remaining"),
            "quota_used": resp.headers.get("x-requests-used"),
            "quota_last": resp.headers.get("x-requests-last"),
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
        upstream_error = nami_business_error(payload)
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


def _nami_name(row: Dict[str, Any]) -> Optional[str]:
    for key in ("name_zh", "name_zht", "name_en", "name", "short_name"):
        if row.get(key):
            return str(row[key])
    return None


def _nami_names(row: Dict[str, Any]) -> List[str]:
    values = []
    for key in ("name_zh", "name_zht", "name_en", "name", "short_name"):
        value = str(row.get(key) or "").strip()
        if value and value not in values:
            values.append(value)
    return values


def parse_nami_schedule(result: Dict[str, Any]) -> Dict[str, Any]:
    """Parse Nami relation arrays while preserving its provider id namespace."""
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    results = data.get("results") if isinstance(data.get("results"), dict) else {}
    matches = results.get("match") if isinstance(results.get("match"), list) else []
    competitions = {str(row.get("id")): row for row in (results.get("competition") or []) if isinstance(row, dict) and row.get("id") is not None}
    teams = {str(row.get("id")): row for row in (results.get("team") or []) if isinstance(row, dict) and row.get("id") is not None}
    target_names = {normalize_fixture_identity_name(name) for name in DEFAULT_TARGET_LEAGUES.values()}
    fixtures = []
    for row in matches:
        if not isinstance(row, dict):
            continue
        competition_id = str(row.get("competition_id") or "")
        competition_name = _nami_name(competitions.get(competition_id, {}))
        canonical_competition = DEFAULT_NAMI_TARGET_COMPETITIONS.get(competition_id) or competition_name
        home = teams.get(str(row.get("home_team_id") or ""), {})
        away = teams.get(str(row.get("away_team_id") or ""), {})
        match = {
            "id": row.get("id"), "match_time": row.get("match_time"),
            "home": _nami_name(home), "away": _nami_name(away),
            "home_aliases": _nami_names(home), "away_aliases": _nami_names(away),
            "league_name": canonical_competition,
        }
        identity = fixture_identity_from_match(match, canonical_competition, "nami")
        target_candidate = competition_id in NAMI_TARGET_COMPETITION_IDS or normalize_fixture_identity_name(competition_name) in target_names
        fixtures.append({
            "provider": "nami", "provider_fixture_id": identity.get("source_fixture_id"),
            "competition_id": competition_id or None, "competition": competition_name, "canonical_competition": canonical_competition,
            "home_team_id": str(row.get("home_team_id")) if row.get("home_team_id") is not None else None,
            "home": _nami_name(home), "away_team_id": str(row.get("away_team_id")) if row.get("away_team_id") is not None else None,
            "away": _nami_name(away), "kickoff_at": identity.get("kickoff_at"), "status_id": row.get("status_id"),
            "target_candidate": target_candidate, "fixture_identity": identity,
            "decision_eligible": False, "reason": "provider_identity_requires_unique_reconciliation_and_market_data",
        })
    return {
        "ok": bool(result.get("ok")), "status_code": result.get("status_code"),
        "data_status": "available" if result.get("ok") else "data_missing",
        "fixture_count": len(fixtures), "target_candidate_count": sum(bool(row["target_candidate"]) for row in fixtures),
        "fixtures": fixtures, "error": result.get("error"),
    }


def payload_structure_fingerprint(value: Any, max_depth: int = 5, max_fields: int = 100) -> Dict[str, Any]:
    """Return schema-like paths and collection sizes without returning provider values."""
    paths: List[Dict[str, Any]] = []

    def walk(node: Any, path: str, depth: int) -> None:
        if len(paths) >= max_fields:
            return
        if isinstance(node, dict):
            paths.append({"path": path or "$", "type": "object", "field_count": len(node)})
            if depth < max_depth:
                for key in sorted(node.keys(), key=str):
                    walk(node[key], f"{path}.{key}" if path else str(key), depth + 1)
        elif isinstance(node, list):
            paths.append({"path": path or "$", "type": "array", "length": len(node)})
            if node and depth < max_depth:
                walk(node[0], f"{path}[]" if path else "[]", depth + 1)
        elif node is None:
            paths.append({"path": path or "$", "type": "null"})
        elif isinstance(node, bool):
            paths.append({"path": path or "$", "type": "boolean"})
        elif isinstance(node, (int, float)):
            paths.append({"path": path or "$", "type": "number"})
        else:
            paths.append({"path": path or "$", "type": "string"})

    walk(value, "results", 0)
    return {"paths": paths, "path_count": len(paths), "truncated": len(paths) >= max_fields}


def nami_odds_capability_check() -> Dict[str, Any]:
    """Probe Nami football odds entitlement without making it a system dependency.

    The provider documents odds as a separate product family.  This probe only
    reports entitlement and response shape; it does not import quotes or allow
    them to influence a recommendation.
    """
    endpoint = "/api/v5/football/odds/live"
    check = call_nami(endpoint)
    data = check.get("data") if isinstance(check.get("data"), dict) else {}
    results = data.get("results")
    if isinstance(results, list):
        result_shape, sample_count = "array", len(results)
    elif isinstance(results, dict):
        result_shape = "object"
        sample_count = sum(len(value) for value in results.values() if isinstance(value, list))
    elif results is None:
        result_shape, sample_count = "missing", 0
    else:
        result_shape, sample_count = type(results).__name__, 0
    error = str(check.get("error") or "")
    error_lower = error.lower()
    not_entitled = any(token in error_lower for token in ("entitle", "permission", "product", "套餐", "权限", "未开通"))
    structure = payload_structure_fingerprint(results)
    return {
        "configured": bool(NAMI_API_USER and NAMI_API_SECRET),
        "ok": check.get("ok", False), "available": check.get("available", False),
        "degraded": check.get("degraded", True), "required": False,
        "fallback": "continue_without_nami_odds" if not check.get("ok") else None,
        "status_code": check.get("status_code"), "api_version": "v5",
        "product": "football_odds", "probe_endpoint": endpoint,
        "entitlement": "available" if check.get("ok") else ("not_entitled" if not_entitled else "unknown"),
        "error_category": "product_not_entitled" if not_entitled else ("upstream_error" if error else None),
        "response_fields": sorted(data.keys()), "results_shape": result_shape,
        "sample_count": sample_count, "structure_fingerprint": structure,
        "integration_status": "capability_probe_only",
        "decision_use": False,
        "required_before_import": ["timestamp_semantics_verified", "company_array_verified", "history_coverage_verified"],
    }


def run_nami_odds_startup_probe() -> None:
    """Run one non-blocking, redacted capability probe per service process."""
    global NAMI_ODDS_STARTUP_PROBE
    try:
        now_ts = int(time.time())
        store = load_snapshot_store()
        cached = get_nested(store, ["provider_capability_cache", "nami_football_odds"], {}) or {}
        checked_at = int(cached.get("checked_at") or 0)
        if checked_at > 0 and 0 <= now_ts - checked_at < NAMI_ODDS_PROBE_TTL_SECONDS:
            NAMI_ODDS_STARTUP_PROBE = {
                **cached, "status": "completed", "source": "persistent_cache",
                "age_seconds": now_ts - checked_at, "decision_use": False,
            }
            return
        result = nami_odds_capability_check()
        summary = {
            "status": "completed", "checked_at": now_ts,
            "configured": result.get("configured"), "available": result.get("available"),
            "entitlement": result.get("entitlement"), "error_category": result.get("error_category"),
            "results_shape": result.get("results_shape"), "sample_count": result.get("sample_count"),
            "structure_fingerprint": result.get("structure_fingerprint"),
            "integration_status": result.get("integration_status"),
            "source": "live_probe", "age_seconds": 0, "decision_use": False,
        }
        with SNAPSHOT_STORE_LOCK:
            latest_store = load_snapshot_store()
            latest_store.setdefault("provider_capability_cache", {})["nami_football_odds"] = {
                key: value for key, value in summary.items() if key not in ("source", "age_seconds")
            }
            latest_store["version"] = VERSION
            write_snapshot_store(latest_store)
        NAMI_ODDS_STARTUP_PROBE = summary
    except Exception as exc:
        NAMI_ODDS_STARTUP_PROBE = {
            "status": "error", "checked_at": int(time.time()),
            "error_type": type(exc).__name__, "decision_use": False,
        }


def start_nami_odds_startup_probe() -> None:
    global NAMI_ODDS_STARTUP_PROBE_STARTED, NAMI_ODDS_STARTUP_PROBE
    if NAMI_ODDS_STARTUP_PROBE_STARTED:
        return
    NAMI_ODDS_STARTUP_PROBE_STARTED = True
    if not (NAMI_API_USER and NAMI_API_SECRET):
        NAMI_ODDS_STARTUP_PROBE = {"status": "skipped", "reason": "nami_not_configured", "decision_use": False}
        return
    threading.Thread(target=run_nami_odds_startup_probe, name="nami-odds-startup-probe", daemon=True).start()


def nami_fixtures_for_date(date: str) -> Dict[str, Any]:
    digits = str(date or "").replace("-", "")
    if len(digits) != 8 or not digits.isdigit():
        return {"ok": False, "data_status": "data_missing", "fixture_count": 0, "target_candidate_count": 0, "fixtures": [], "error": "invalid_date"}
    return parse_nami_schedule(call_nami("/api/v5/football/match/schedule/diary", {"date": digits}))


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


def target_fixtures_for_date(date: str, timezone_name: str = "Asia/Shanghai", include_supplemental: bool = False) -> Dict[str, Any]:
    result = call_api_football("/fixtures", {"date": date, "timezone": timezone_name})
    rows = response_list(result)
    targets = [r for r in rows if isinstance(r, dict) and get_nested(r, ["league", "id"]) in TARGET_LEAGUE_IDS]
    summaries = [fixture_summary(r) for r in targets]
    configured = bool(API_FOOTBALL_KEY)
    if not configured:
        discovery_status, blocker = "data_missing", "api_football_not_configured"
    elif not result.get("ok"):
        discovery_status, blocker = "upstream_unavailable", result.get("error") or "api_football_request_failed"
    elif not rows:
        discovery_status, blocker = "available_empty", "no_fixtures_returned_for_date"
    elif not summaries:
        discovery_status, blocker = "available_no_targets", "no_target_league_fixtures_for_date"
    else:
        discovery_status, blocker = "available", None
    nami_schedule = nami_fixtures_for_date(date) if include_supplemental else None
    source_audit = {
        "selected_source": "api_football",
        "selected_source_configured": configured,
        "discovery_status": discovery_status,
        "blocker": blocker,
        "decision_eligible": bool(result.get("ok") and summaries),
        "nami": {
            "configured": bool(NAMI_API_USER and NAMI_API_SECRET),
            "role": "optional_supplemental_source",
            "fixture_id_namespace_compatible": False,
            "fallback_used": False,
            "schedule_probe": ({key: nami_schedule.get(key) for key in ("ok", "status_code", "data_status", "fixture_count", "target_candidate_count", "error")} if nami_schedule else None),
            "reason": "nami_fixture_ids_must_not_be_sent_to_api_football_endpoints",
        },
        "policy": "never_cross_join_fixture_ids_between_providers; missing_discovery_data_remains_data_missing",
    }
    return {
        "ok": result.get("ok"), "date": date, "timezone": timezone_name,
        "mode": "target_major_leagues_only", "target_league_ids": sorted(TARGET_LEAGUE_IDS),
        "all_count": len(rows), "target_count": len(summaries), "fixtures": summaries,
        "source_status_code": result.get("status_code"), "source_audit": source_audit,
        "data_status": discovery_status, "data_missing": not configured,
        "supplemental_fixtures": (nami_schedule or {}).get("fixtures", []),
        "supplemental_target_candidate_count": (nami_schedule or {}).get("target_candidate_count", 0),
    }


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


def independent_model_inputs_from_standings(
    result: Dict[str, Any],
    home_id: Optional[int],
    away_id: Optional[int],
    observed_at: Optional[int] = None,
    lineup_confidence: Optional[float] = None,
) -> Dict[str, Any]:
    """Build odds-independent, venue-split model inputs from one PIT standings response.

    This deliberately refuses total-only or incomplete tables.  It never substitutes
    bookmaker prices, provider predictions, later results, or another observation.
    """
    observed_at = int(observed_at or time.time())
    groups = get_nested(response_list(result), [0, "league", "standings"], [])
    rows = [row for group in groups if isinstance(group, list) for row in group if isinstance(row, dict)]
    home_row = next((row for row in rows if get_nested(row, ["team", "id"]) == home_id), None)
    away_row = next((row for row in rows if get_nested(row, ["team", "id"]) == away_id), None)

    def venue_values(row: Optional[Dict[str, Any]], venue: str) -> Optional[Dict[str, float]]:
        played = as_float(get_nested(row or {}, [venue, "played"]))
        goals_for = as_float(get_nested(row or {}, [venue, "goals", "for"]))
        goals_against = as_float(get_nested(row or {}, [venue, "goals", "against"]))
        if played is None or played < 1 or goals_for is None or goals_against is None:
            return None
        return {
            "played": played,
            "goals_for": goals_for,
            "goals_against": goals_against,
            "attack_rate": goals_for / played,
            "defense_rate": goals_against / played,
        }

    home_values = venue_values(home_row, "home")
    away_values = venue_values(away_row, "away")
    league_home_goals = league_away_goals = league_matches = 0.0
    usable_league_rows = 0
    for row in rows:
        home_split = venue_values(row, "home")
        away_split = venue_values(row, "away")
        if not home_split or not away_split:
            continue
        # Every league match appears once in the aggregate home split.  Requiring
        # equal home/away sample counts prevents silently accepting malformed tables.
        if abs(home_split["played"] - away_split["played"]) > 1:
            continue
        league_matches += home_split["played"]
        league_home_goals += home_split["goals_for"]
        league_away_goals += away_split["goals_for"]
        usable_league_rows += 1

    blockers = []
    if not rows:
        blockers.append("standings_response_missing")
    if home_values is None:
        blockers.append("home_team_home_split_missing")
    if away_values is None:
        blockers.append("away_team_away_split_missing")
    if usable_league_rows < 2 or league_matches < 10:
        blockers.append("league_venue_baseline_sample_insufficient")
    if blockers:
        return {
            "status": "data_missing",
            "decision_eligible": False,
            "blockers": blockers,
            "observed_at": observed_at,
            "uses_market_odds": False,
        }

    relevant_table = [{
        "team_id": get_nested(row, ["team", "id"]),
        "home": row.get("home"),
        "away": row.get("away"),
    } for row in rows]
    source_hash = _content_hash(relevant_table)
    lineup_confidence = as_float(lineup_confidence)
    if lineup_confidence is None or not 0.0 <= lineup_confidence <= 1.0:
        lineup_confidence = 0.0
    model_inputs = {
        "league_home_rate": round(league_home_goals / league_matches, 6),
        "league_away_rate": round(league_away_goals / league_matches, 6),
        "home_attack_rate": round(home_values["attack_rate"], 6),
        "home_defense_rate": round(home_values["defense_rate"], 6),
        "away_attack_rate": round(away_values["attack_rate"], 6),
        "away_defense_rate": round(away_values["defense_rate"], 6),
        "home_sample_size": int(home_values["played"]),
        "away_sample_size": int(away_values["played"]),
        "league_sample_size": int(league_matches),
        "metric_type": "goals",
        "home_adjustment": 1.0,
        "away_adjustment": 1.0,
        "lineup_confidence": lineup_confidence,
        "provenance": {
            "source": "api_football_standings_venue_split",
            "uses_market_odds": False,
            "observed_at": observed_at,
            "source_content_hash": source_hash,
        },
    }
    return {
        "status": "ready",
        "decision_eligible": True,
        "observed_at": observed_at,
        "uses_market_odds": False,
        "usable_league_team_count": usable_league_rows,
        "source_content_hash": source_hash,
        "inputs": model_inputs,
        "input_hash": _content_hash(model_inputs),
    }


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


def _bookmaker_coverage_audit(raw_rows: List[Dict[str, Any]], deduped_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "raw_quote_count": len(raw_rows),
        "unique_bookmaker_count": len(deduped_rows),
        "duplicate_quote_count": max(0, len(raw_rows) - len(deduped_rows)),
        "missing_bookmaker_name_quote_count": sum(not str(row.get("bookmaker") or "").strip() for row in raw_rows),
        "ambiguous_duplicate_selection_quote_count": sum(bool(row.get("ambiguous_duplicate_selection")) for row in raw_rows),
        "identity_method": "trimmed_casefolded_bookmaker_name; missing names share one unknown identity",
        "duplicate_quotes_count_as_additional_bookmakers": False,
    }


def _complete_deduped_bookmaker_rows(rows: List[Dict[str, Any]], keys: Tuple[str, ...]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[str]]:
    deduped = _dedupe_bookmaker_rows(rows, keys)
    complete_raw_rows = [row for row in rows if not row.get("ambiguous_duplicate_selection") and all((as_float(row.get(key)) or 0) > 1.0 for key in keys)]
    raw_complete_identities = {_bookmaker_identity(row.get("bookmaker")) for row in complete_raw_rows}
    complete = _dedupe_bookmaker_rows(complete_raw_rows, keys)
    fragmented = [
        str(row.get("bookmaker_identity")) for row in deduped
        if row.get("bookmaker_identity") not in raw_complete_identities
        and all((as_float(row.get(key)) or 0) > 1.0 for key in keys)
    ]
    return complete, deduped, fragmented


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
    complete, deduped, fragmented = _complete_deduped_bookmaker_rows(rows, ("home", "draw", "away"))
    if not complete:
        return None
    return {
        "method": "median_all_complete_bookmakers", "source": "complete_company_array", "bookmaker_count": len(complete),
        "home": _median([x.get("home") for x in complete]),
        "draw": _median([x.get("draw") for x in complete]),
        "away": _median([x.get("away") for x in complete]),
        "bookmaker_coverage_audit": {**_bookmaker_coverage_audit(rows, deduped), "eligible_unique_bookmaker_count": len(complete), "incomplete_unique_bookmaker_count": len(deduped) - len(complete), "fragmented_synthetic_complete_identities_rejected": fragmented},
        **_price_dispersion(complete, ("home", "draw", "away")),
    }


def _consensus_line(markets: List[Dict[str, Any]], price_keys: Tuple[str, str]) -> Optional[Dict[str, Any]]:
    raw_by_line: Dict[float, List[Dict[str, Any]]] = {}
    for market in markets:
        for line in market.get("lines", []) or []:
            value = as_float(line.get("line"))
            if value is None or line.get("ambiguous_duplicate_selection") or any((as_float(line.get(key)) or 0) <= 1.0 for key in price_keys):
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
        "bookmaker_coverage_audit": _bookmaker_coverage_audit(raw_by_line[value], rows),
        **_price_dispersion(rows, price_keys),
    }


def _two_way_market(values: List[Dict[str, Any]], labels: Tuple[str, str]) -> Dict[str, Any]:
    entry = {labels[0]: None, labels[1]: None, "raw_values": values}
    counts = {labels[0]: 0, labels[1]: 0}
    for value in values:
        raw = str(value.get("value", "")).strip().lower()
        if raw == labels[0].lower():
            counts[labels[0]] += 1
            entry[labels[0]] = value.get("odd")
        if raw == labels[1].lower():
            counts[labels[1]] += 1
            entry[labels[1]] = value.get("odd")
    entry["ambiguous_duplicate_selection"] = any(count > 1 for count in counts.values())
    entry["selection_counts"] = counts
    return entry


def _line_market(values: List[Dict[str, Any]], prefixes: Tuple[str, str], keys: Tuple[str, str]) -> List[Dict[str, Any]]:
    lines: Dict[str, Dict[str, Any]] = {}
    for value in values:
        raw = str(value.get("value", "")).strip()
        for prefix, key in zip(prefixes, keys):
            if raw.lower().startswith(prefix.lower() + " "):
                line = raw[len(prefix):].strip()
                lines.setdefault(line, {"line": line, keys[0]: None, keys[1]: None, "raw_values": [], "selection_counts": {keys[0]: 0, keys[1]: 0}})
                lines[line][key] = value.get("odd")
                lines[line]["selection_counts"][key] += 1
                lines[line]["raw_values"].append(value)
    for row in lines.values():
        row["ambiguous_duplicate_selection"] = any(count > 1 for count in row["selection_counts"].values())
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
                selection_counts = {"home": 0, "draw": 0, "away": 0}
                for v in values:
                    if str(v.get("value")) == "Home": entry["home"] = v.get("odd"); selection_counts["home"] += 1
                    if str(v.get("value")) == "Draw": entry["draw"] = v.get("odd"); selection_counts["draw"] += 1
                    if str(v.get("value")) == "Away": entry["away"] = v.get("odd"); selection_counts["away"] += 1
                entry["selection_counts"] = selection_counts
                entry["ambiguous_duplicate_selection"] = any(count > 1 for count in selection_counts.values())
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
    complete_btts, deduped_btts, fragmented_btts = _complete_deduped_bookmaker_rows(btts, ("yes", "no"))
    consensus["btts"] = ({"method": "median_all_complete_bookmakers", "source": "complete_company_array", "bookmaker_count": len(complete_btts), "yes": _median([x.get("yes") for x in complete_btts]), "no": _median([x.get("no") for x in complete_btts]), "bookmaker_coverage_audit": {**_bookmaker_coverage_audit(btts, deduped_btts), "eligible_unique_bookmaker_count": len(complete_btts), "incomplete_unique_bookmaker_count": len(deduped_btts) - len(complete_btts), "fragmented_synthetic_complete_identities_rejected": fragmented_btts}, **_price_dispersion(complete_btts, ("yes", "no"))} if complete_btts else None)
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


def coverage_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    result = result if isinstance(result, dict) else {}
    data = result.get("data") if isinstance(result.get("data"), dict) else {}
    response = data.get("response")
    request_ok = result.get("ok") is True
    return {
        "ok": request_ok,
        "status_code": result.get("status_code"),
        "results": data.get("results"),
        "has_data": bool(response) and request_ok,
        "response_present_but_unusable": bool(response) and not request_ok,
        "request_url": result.get("request_url"),
    }


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
    if stage not in ("T-1h", "T-30m", "Closing"):
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
    coverage = {name: coverage_summary(result) for name, result in calls.items()}
    market_snapshot = extract_market_snapshot(calls.get("odds_prematch", {}))
    normalized_lineups = lineup_summary(calls.get("lineups", {}))
    generated_at = int(time.time())
    independent_model_inputs = independent_model_inputs_from_standings(
        calls.get("standings", {}), home_id, away_id, generated_at,
        normalized_lineups.get("confidence"),
    )
    structured = {"standings": {"home": standings_for_team(calls.get("standings", {}), home_id), "away": standings_for_team(calls.get("standings", {}), away_id)}, "recent_form_last_10": {"home": recent_form(response_list(calls.get("home_recent_10", {})), home_id), "away": recent_form(response_list(calls.get("away_recent_10", {})), away_id)}, "season_stats": {"home": season_stats_summary(calls.get("home_team_season_stats", {})), "away": season_stats_summary(calls.get("away_team_season_stats", {}))}, "head_to_head_count": len(response_list(calls.get("head_to_head_last_10", {}))), "injuries": injuries_summary(calls.get("injuries", {}), home_id, away_id), "prediction": prediction_summary(calls.get("predictions", {})), "independent_model_inputs": independent_model_inputs, "odds": odds_summary(calls.get("odds_prematch", {})), "odds_market_snapshot": market_snapshot, "lineups": normalized_lineups, "lineups_available": normalized_lineups.get("available", False), "lineups_confirmed": normalized_lineups.get("confirmed", False), "snapshot_requirements": {"required_markets": ["1x2", "asian_handicap", "over_under"], "optional_markets": ["btts", "home_team_total", "away_team_total"], "metrics_supported": ["line_crossing", "continuous_strengthening", "reversal", "market_saturation", "cross_market_divergence", "fundamental_revalidation"]}}
    quality = data_quality(coverage)
    return {"ok": True, "version": VERSION, "generated_at": generated_at, "fixture": {k: v for k, v in ctx.items() if k not in ["fixture_detail", "fixture_row"]}, "coverage": coverage, "data_quality": quality, "structured_inputs": structured, "shadow_summary": make_shadow_summary(ctx, structured, quality), "football_ai_prompt": make_prompt(ctx), "raw_data_pack": {"fixture_detail": ctx.get("fixture_detail"), **{k: compact_result(v) for k, v in calls.items()}} if include_raw else None}



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
    if stage in ("T-1h", "T-30m", "Closing"):
        calls["injuries"] = call_api_football("/injuries", {"fixture": fixture_id})
        calls["lineups"] = call_api_football("/fixtures/lineups", {"fixture": fixture_id})
    coverage = {name: coverage_summary(result) for name, result in calls.items()}
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
            "lineups_confirmed": normalized_lineups.get("confirmed", False),
            "collection_profile": "late_market_plus_team_news" if stage in ("T-1h", "T-30m", "Closing") else "market_only"
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
        external_only = stage["key"] == "Opening"
        items.append({"stage": stage["key"], "label": stage["label"], "purpose": stage["purpose"], "utc_time": None if external_only else when.isoformat(), "status": "requires_verified_opening_source" if external_only else ("due_or_passed" if now >= when else "pending"), "automatic_collection": not external_only, "action_url": None if external_only else f"/shadow/snapshot?fixture={summary.get('fixture_id')}&stage={stage['key']}"})
    return {"fixture_id": summary.get("fixture_id"), "match": f"{summary.get('home')} vs {summary.get('away')}", "kickoff_utc": dt.isoformat(), "status": summary.get("status"), "tracking": items}


def load_snapshot_store() -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        p = Path(SNAPSHOT_STORE_PATH)
        if not p.exists():
            return {"version": VERSION, "fixtures": {}}
        try:
            raw = p.read_bytes()
            if raw.startswith(b"\x1f\x8b"):
                raw = bounded_gzip_decompress(raw, SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES)
            elif len(raw) > SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES:
                raise ValueError("snapshot_store_size_limit_exceeded")
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
            raw = bounded_gzip_decompress(raw, SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES)
        elif len(raw) > SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES:
            raise ValueError("snapshot_store_backup_size_limit_exceeded")
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
        raw = (
            bounded_gzip_decompress(encoded, SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES)
            if encoded.startswith(b"\x1f\x8b") else encoded
        )
        if len(raw) > SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES:
            raise ValueError("snapshot_store_size_limit_exceeded")
        store = json.loads(raw.decode("utf-8"))
        if not isinstance(store, dict):
            raise ValueError("store root must be an object")
        fixtures = store.get("fixtures") or {}
        snapshot_count = sum(len(rows or []) for rows in fixtures.values())
        queue = store.get("fundamental_revalidation_queue") or {}
        node_attempts = store.get("learning_node_attempts") or {}
        return {
            "exists": True, "readable": True, "status": "ok", "gzip": encoded.startswith(b"\x1f\x8b"),
            "size_bytes": len(encoded), "raw_size_bytes": len(raw),
            "compression_ratio": round(len(encoded) / len(raw), 6) if raw else 1.0,
            "capacity_state": "warning" if len(encoded) >= SNAPSHOT_STORE_WARN_BYTES else "normal",
            "warning_threshold_bytes": SNAPSHOT_STORE_WARN_BYTES,
            "maximum_decompressed_bytes": SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES,
            "content_sha256": hashlib.sha256(encoded).hexdigest(),
            "store_version": store.get("version"),
            "fixture_count": len(fixtures), "snapshot_count": snapshot_count,
            "portfolio_count": len(store.get("portfolio_runs") or {}),
            "revalidation_task_count": len(queue),
            "pending_revalidation_count": sum(task.get("status") == "pending" for task in queue.values()),
            "learning_admission_count": len(store.get("learning_admissions") or {}),
            "learning_node_attempt_count": len(node_attempts),
            "learning_node_failed_attempt_count": sum(
                isinstance(row, dict) and row.get("status") == "failed" for row in node_attempts.values()
            ),
        }
    except Exception as exc:
        return {"exists": True, "readable": False, "status": "corrupt", "error": type(exc).__name__}


def snapshot_store_integrity() -> Dict[str, Any]:
    primary = inspect_snapshot_store_file(Path(SNAPSHOT_STORE_PATH))
    backup = inspect_snapshot_store_file(snapshot_backup_path())
    manual_recovery_required = primary.get("readable") is not True and backup.get("readable") is True
    return {
        "primary": primary, "backup": backup,
        "operational": primary.get("readable") is True,
        "recovery_ready": backup.get("readable") is True,
        "manual_recovery_required": manual_recovery_required,
        "manual_recovery_available": manual_recovery_required,
        "manual_recovery_requires_exact_preview_token": True,
        "capacity_ok": primary.get("capacity_state") in (None, "normal"),
        "automatic_restore": False,
    }


def _snapshot_store_file_fingerprint(path: Path) -> Optional[str]:
    """Hash encoded bytes without attempting to interpret a damaged store."""
    if not path.exists():
        return None
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except Exception as exc:
        raise SnapshotStoreReadError("snapshot_store_fingerprint_failed") from exc


def snapshot_store_recovery_preview() -> Dict[str, Any]:
    """Build a state-bound manual recovery token; never modify either store file."""
    with SNAPSHOT_STORE_LOCK:
        primary_path = Path(SNAPSHOT_STORE_PATH)
        backup_path = snapshot_backup_path()
        integrity = snapshot_store_integrity()
        primary_fingerprint = _snapshot_store_file_fingerprint(primary_path)
        backup_fingerprint = _snapshot_store_file_fingerprint(backup_path)
        eligible = bool(integrity.get("manual_recovery_required"))
        token_content = {
            "action": "restore_snapshot_backup_to_primary",
            "primary_exists": primary_path.exists(),
            "primary_status": get_nested(integrity, ["primary", "status"]),
            "primary_fingerprint_sha256": primary_fingerprint,
            "backup_status": get_nested(integrity, ["backup", "status"]),
            "backup_fingerprint_sha256": backup_fingerprint,
        }
        recovery_token = _content_hash(token_content) if eligible else None
        confirmation_phrase = (
            "RESTORE_SNAPSHOT_BACKUP:" + str(backup_fingerprint)[:16]
            if eligible and backup_fingerprint else None
        )
        return {
            "eligible": eligible,
            "reason": (
                "primary_unreadable_and_backup_verified"
                if eligible else (
                    "primary_is_operational"
                    if integrity.get("operational") else "readable_backup_not_available"
                )
            ),
            "primary": integrity.get("primary"),
            "backup": integrity.get("backup"),
            "primary_fingerprint_sha256": primary_fingerprint,
            "backup_fingerprint_sha256": backup_fingerprint,
            "recovery_token": recovery_token,
            "required_confirmation": confirmation_phrase,
            "automatic_restore": False,
            "state_bound": True,
            "policy": "manual protected recovery only; automatic workers never invoke restoration",
        }


def restore_snapshot_store_from_backup(payload: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Restore only an unreadable/missing primary from the exact previewed backup state."""
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="snapshot_recovery_payload_must_be_object")
    supplied_token = str(payload.get("recovery_token") or "")
    supplied_confirmation = str(payload.get("confirmation") or "")
    now_ts = int(now_ts or time.time())
    with SNAPSHOT_STORE_LOCK:
        preview = snapshot_store_recovery_preview()
        if not preview["eligible"]:
            raise HTTPException(status_code=409, detail="snapshot_store_manual_recovery_not_eligible")
        expected_token = str(preview.get("recovery_token") or "")
        expected_confirmation = str(preview.get("required_confirmation") or "")
        if not supplied_token or not hmac.compare_digest(supplied_token, expected_token):
            raise HTTPException(status_code=409, detail="snapshot_recovery_preview_token_mismatch")
        if not supplied_confirmation or not hmac.compare_digest(supplied_confirmation, expected_confirmation):
            raise HTTPException(status_code=422, detail="snapshot_recovery_confirmation_mismatch")

        primary_path = Path(SNAPSHOT_STORE_PATH)
        backup_path = snapshot_backup_path()
        backup_encoded = backup_path.read_bytes()
        backup_fingerprint = hashlib.sha256(backup_encoded).hexdigest()
        if not hmac.compare_digest(backup_fingerprint, str(preview.get("backup_fingerprint_sha256") or "")):
            raise HTTPException(status_code=409, detail="snapshot_recovery_backup_changed_after_preview")
        recovered_store = load_snapshot_backup_store()
        audit = {
            "recovered_at": now_ts,
            "action": "manual_restore_backup_to_primary",
            "automatic": False,
            "preview_token": expected_token,
            "recovered_from_backup_sha256": backup_fingerprint,
            "displaced_primary_sha256": preview.get("primary_fingerprint_sha256"),
            "confirmation_method": "exact_state_bound_phrase",
        }
        recovery_audit = list(recovered_store.get("snapshot_store_recovery_audit") or [])
        recovery_audit.append(audit)
        recovered_store["snapshot_store_recovery_audit"] = recovery_audit[-100:]
        recovered_store["version"] = VERSION
        raw = json.dumps(recovered_store, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(raw) > SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES:
            raise SnapshotStoreWriteError("snapshot_store_recovery_size_limit_exceeded")
        restored_encoded = gzip.compress(raw, compresslevel=6) if SNAPSHOT_STORE_GZIP else raw

        primary_path.parent.mkdir(parents=True, exist_ok=True)
        restore_tmp = primary_path.with_suffix(primary_path.suffix + ".restore.tmp")
        restore_tmp.write_bytes(restored_encoded)
        tmp_audit = inspect_snapshot_store_file(restore_tmp)
        if tmp_audit.get("readable") is not True:
            restore_tmp.unlink(missing_ok=True)
            raise SnapshotStoreWriteError("snapshot_store_recovery_candidate_invalid")

        quarantine_path = None
        if primary_path.exists():
            displaced = str(preview.get("primary_fingerprint_sha256") or "unknown")[:16]
            quarantine_path = primary_path.with_suffix(
                primary_path.suffix + f".quarantine.{now_ts}.{displaced}"
            )
            if quarantine_path.exists():
                restore_tmp.unlink(missing_ok=True)
                raise HTTPException(status_code=409, detail="snapshot_recovery_quarantine_target_exists")
            primary_path.replace(quarantine_path)
        try:
            restore_tmp.replace(primary_path)
        except Exception:
            if quarantine_path is not None and quarantine_path.exists() and not primary_path.exists():
                quarantine_path.replace(primary_path)
            restore_tmp.unlink(missing_ok=True)
            raise

        restored_integrity = inspect_snapshot_store_file(primary_path)
        if restored_integrity.get("readable") is not True:
            raise SnapshotStoreWriteError("snapshot_store_recovery_verification_failed")
        return {
            "restored": True,
            "automatic": False,
            "recovered_at": now_ts,
            "recovered_from_backup_sha256": backup_fingerprint,
            "restored_primary_sha256": restored_integrity.get("content_sha256"),
            "displaced_primary_quarantined": quarantine_path is not None,
            "displaced_primary_sha256": preview.get("primary_fingerprint_sha256"),
            "backup_preserved": _snapshot_store_file_fingerprint(backup_path) == backup_fingerprint,
            "integrity": snapshot_store_integrity(),
            "recovery_audit_hash": _content_hash(audit),
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
            if len(raw) > SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES:
                raise ValueError("snapshot_store_size_limit_exceeded")
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
        expected_map = {"T-24h": 86400, "T-12h": 43200, "T-6h": 21600, "T-3h": 10800, "T-1h": 3600, "T-30m": 1800, "Closing": 0}
        tolerance_map = {"T-24h": 21600, "T-12h": 10800, "T-6h": 5400, "T-3h": 2700, "T-1h": 1800, "T-30m": 900, "Closing": 900}
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
    timestamps = [int(row.get("snapshot_at")) for row in available if isinstance(row.get("snapshot_at"), (int, float)) and int(row.get("snapshot_at")) > 0]
    distinct_timestamps = sorted(set(timestamps))
    all_times_valid = len(timestamps) == len(available)
    strictly_chronological = all_times_valid and all(timestamps[index] < timestamps[index + 1] for index in range(len(timestamps) - 1))
    latest_dynamics = (latest or {}).get("market_dynamics") if isinstance((latest or {}).get("market_dynamics"), dict) else {}
    probability_comparisons = [market for market, detail in (latest_dynamics.get("no_vig_probability_movements") or {}).items() if isinstance(detail, dict) and detail.get("status") == "compared"]
    movement_values = [value for detail in (latest_dynamics.get("market_movements") or {}).values() if isinstance(detail, dict) for value in detail.values() if as_float(value) is not None]
    actual_comparison = bool(probability_comparisons or movement_values)
    blockers = []
    if len(available) < 2:
        blockers.append("fewer_than_two_available_stages")
    if len(distinct_timestamps) < 2:
        blockers.append("fewer_than_two_distinct_observation_times")
    if len(available) >= 2 and not strictly_chronological:
        blockers.append("observation_times_not_strictly_in_stage_order")
    if not latest_comparable:
        blockers.append("latest_stage_not_marked_compared")
    if latest_comparable and not actual_comparison:
        blockers.append("latest_stage_has_no_shared_comparable_market")
    eligible = not blockers
    return {
        "decision_eligible": eligible,
        "available_stage_count": len(available), "required_minimum_available_stages": 2,
        "comparable_stage_count": len(comparable), "latest_stage": (latest or {}).get("stage"),
        "distinct_observation_time_count": len(distinct_timestamps),
        "all_observation_times_valid": all_times_valid,
        "observation_times_strictly_chronological": strictly_chronological,
        "latest_actual_comparison": actual_comparison,
        "latest_probability_compared_markets": probability_comparisons,
        "latest_comparable_value_count": len(movement_values),
        "blockers": blockers,
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
    decision["execution_action"] = "PASS"
    decision["best_market"] = None
    decision["edge"] = None
    decision["ev"] = None
    tiers = decision.get("recommendation_tiers")
    if isinstance(tiers, dict):
        for key in ("first_choice_high_consistency", "second_choice_higher_return", "high_variance_single"):
            tiers[key] = None
    optimizer = decision.get("expression_optimizer") if isinstance(decision.get("expression_optimizer"), dict) else {}
    decision["expression_optimizer"] = {
        **optimizer,
        "action": "PASS", "selected_expression": None,
        "reason": "mandatory_gate_failed",
        "automatic_direction_reversal": False,
    }
    return decision


def _normalize_real_money_percentages(value: Any, selections: Tuple[str, ...], field: str) -> Tuple[Optional[Dict[str, float]], List[str]]:
    if value in (None, {}):
        return None, []
    if not isinstance(value, dict):
        return None, [f"{field}_must_be_object"]
    unknown = sorted(str(key) for key in value if key not in selections)
    missing = [key for key in selections if key not in value]
    normalized = {key: as_float(value.get(key)) for key in selections}
    invalid = [key for key, number in normalized.items() if number is None or not 0 <= number <= 100]
    reasons = []
    if unknown:
        reasons.append(f"{field}_unknown_selections:" + ",".join(unknown))
    if missing:
        reasons.append(f"{field}_missing_selections:" + ",".join(missing))
    if invalid:
        reasons.append(f"{field}_percent_out_of_range:" + ",".join(invalid))
    if not reasons and abs(sum(normalized.values()) - 100.0) > 1.5:
        reasons.append(f"{field}_percentages_must_sum_to_100")
    return ({key: round(float(normalized[key]), 4) for key in selections} if not reasons else None), reasons


def audit_real_money_data(payload: Any, expected_fixture: Any = None, data_cutoff_at: Any = None, kickoff_at: Any = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Validate a separately sourced A-grade money packet; odds movements can never satisfy this schema."""
    if payload in (None, {}):
        return {
            "schema": "real_money_v1", "status": "data_missing", "decision_eligible": False,
            "evidence_grade": "data_missing", "is_real_money": False,
            "money_percent": None, "bet_percent": None, "turnover": None,
            "markets": {}, "reasons": ["real_money_packet_not_supplied"],
        }
    if not isinstance(payload, dict):
        return {
            "schema": "real_money_v1", "status": "rejected", "decision_eligible": False,
            "evidence_grade": "rejected", "is_real_money": False, "markets": {},
            "reasons": ["real_money_packet_must_be_object"],
        }
    reasons = []
    if payload.get("schema") != "real_money_v1":
        reasons.append("real_money_schema_must_be_real_money_v1")
    fixture = str(payload.get("fixture") or "").strip()
    if expected_fixture is not None and fixture != str(expected_fixture):
        reasons.append("real_money_fixture_mismatch")
    source = payload.get("source") if isinstance(payload.get("source"), dict) else {}
    source_name = str(source.get("name") or "").strip()
    source_type = str(source.get("type") or "").strip().lower()
    evidence_ref = str(source.get("evidence_ref") or "").strip()
    methodology = str(source.get("methodology") or "").strip()
    parsed_ref = urlparse(evidence_ref)
    if not source_name:
        reasons.append("real_money_source_name_required")
    if source_type not in REAL_MONEY_SOURCE_TYPES:
        reasons.append("real_money_source_type_not_a_grade")
    if parsed_ref.scheme not in {"http", "https"} or not parsed_ref.hostname or parsed_ref.path in {"", "/"}:
        reasons.append("real_money_locatable_http_evidence_required")
    authority_domain = parsed_ref.hostname.lower() if parsed_ref.hostname else None
    registry_entry = REAL_MONEY_SOURCE_REGISTRY.get(authority_domain) if authority_domain else None
    if not isinstance(registry_entry, dict):
        reasons.append("real_money_source_not_in_verified_registry")
    else:
        if not str(registry_entry.get("registry_id") or "").strip():
            reasons.append("real_money_registered_source_requires_stable_id")
        allowed_types = registry_entry.get("allowed_types") if isinstance(registry_entry.get("allowed_types"), list) else []
        if source_type not in allowed_types:
            reasons.append("real_money_source_type_not_allowed_for_registered_domain")
    if source.get("authority_verified") is not True:
        reasons.append("real_money_source_authority_verification_required")
    if source.get("methodology_verified") is not True or len(methodology) < 20:
        reasons.append("real_money_verified_methodology_required")
    observed_at = _parse_timestamp(payload.get("observed_at"))
    cutoff_at = _parse_timestamp(data_cutoff_at) or int(now_ts or time.time())
    kickoff_ts = _parse_timestamp(kickoff_at)
    if observed_at is None:
        reasons.append("real_money_observed_at_required")
        age_seconds = None
    else:
        age_seconds = cutoff_at - observed_at
        if observed_at > cutoff_at:
            reasons.append("real_money_observation_after_data_cutoff")
        if kickoff_ts is not None and observed_at >= kickoff_ts:
            reasons.append("real_money_observation_not_prematch")
        if age_seconds > REAL_MONEY_MAX_AGE_SECONDS:
            reasons.append("real_money_observation_stale")
        if age_seconds < -300:
            reasons.append("real_money_observation_in_future")
    raw_markets = payload.get("markets") if isinstance(payload.get("markets"), dict) else {}
    if not raw_markets:
        reasons.append("real_money_markets_required")
    unknown_markets = sorted(str(key) for key in raw_markets if key not in REAL_MONEY_MARKET_SELECTIONS)
    if unknown_markets:
        reasons.append("real_money_unknown_markets:" + ",".join(unknown_markets))
    markets = {}
    for market, selections in REAL_MONEY_MARKET_SELECTIONS.items():
        if market not in raw_markets:
            continue
        row = raw_markets.get(market) if isinstance(raw_markets.get(market), dict) else {}
        if not row:
            reasons.append(f"real_money_{market}_must_be_object")
            continue
        money, money_reasons = _normalize_real_money_percentages(row.get("money_percent"), selections, f"{market}_money_percent")
        bets, bet_reasons = _normalize_real_money_percentages(row.get("bet_percent"), selections, f"{market}_bet_percent")
        reasons.extend(money_reasons + bet_reasons)
        if money is None and bets is None:
            reasons.append(f"real_money_{market}_money_or_bet_percent_required")
        line = as_float(row.get("line")) if market not in {"1x2", "btts"} else None
        if market not in {"1x2", "btts"} and line is None:
            reasons.append(f"real_money_{market}_line_required")
        turnover = as_float(row.get("turnover"))
        currency = str(row.get("currency") or "").strip().upper() or None
        if row.get("turnover") is not None and (turnover is None or turnover < 0 or currency is None):
            reasons.append(f"real_money_{market}_turnover_requires_nonnegative_value_and_currency")
        concentration = money or bets or {}
        ordered = sorted(concentration.items(), key=lambda item: item[1], reverse=True)
        direction = ordered[0][0] if ordered and (len(ordered) == 1 or ordered[0][1] > ordered[1][1]) else None
        concentration_gap = round((ordered[0][1] - ordered[1][1]) / 100.0, 6) if len(ordered) >= 2 and direction else None
        markets[market] = {
            "line": line, "money_percent": money, "bet_percent": bets,
            "turnover": turnover, "currency": currency,
            "direction": direction, "direction_basis": "money_percent_top_share" if money else ("bet_percent_top_share" if bets else None),
            "concentration_gap": concentration_gap,
        }
    eligible = not reasons and bool(markets)
    normalized = {
        "schema": "real_money_v1", "fixture": fixture,
        "source": {
            "name": source_name, "type": source_type, "evidence_ref": evidence_ref,
            "authority_domain": authority_domain,
            "registry_id": str((registry_entry or {}).get("registry_id") or "").strip() or None,
            "authority_verified": source.get("authority_verified") is True,
            "methodology_verified": source.get("methodology_verified") is True,
            "methodology": methodology,
        },
        "observed_at": observed_at, "data_cutoff_at": cutoff_at, "age_seconds": age_seconds,
        "maximum_age_seconds": REAL_MONEY_MAX_AGE_SECONDS,
        "markets": markets,
    }
    return {
        **normalized,
        "status": "available" if eligible else "rejected",
        "decision_eligible": eligible,
        "evidence_grade": "A" if eligible else "rejected",
        "is_real_money": eligible,
        "reasons": reasons,
        "evidence_hash": _content_hash(normalized) if eligible else None,
        "policy": "only this independently audited schema may populate real Money%/Bet%/turnover fields; odds paths remain a separate proxy",
    }


def _movement_probability_delta(dynamics: Dict[str, Any], market: str, selection: str) -> Optional[float]:
    detail = (dynamics.get("no_vig_probability_movements") or {}).get(market) or {}
    if detail.get("status") != "compared":
        return None
    return as_float((detail.get("deltas") or {}).get(selection))


def _market_axis_for_candidate(candidate: Dict[str, Any]) -> str:
    market, selection = candidate.get("market"), candidate.get("selection")
    if market in ("1x2", "asian_handicap") and selection in ("home", "away"):
        return str(selection).title()
    if market == "over_under" and selection in ("over", "under"):
        return str(selection).title()
    if market == "btts" and selection in ("yes", "no"):
        return "BTTS Yes" if selection == "yes" else "BTTS No"
    if market in ("home_team_total", "away_team_total") and selection in ("over", "under"):
        side = "Home TT" if market == "home_team_total" else "Away TT"
        return f"{side} {str(selection).title()}"
    return f"{market}:{selection}"


def _candidate_real_money_signal(candidate: Dict[str, Any], real_money_data: Dict[str, Any]) -> Tuple[Optional[float], str, Optional[Dict[str, Any]]]:
    if not isinstance(real_money_data, dict) or real_money_data.get("decision_eligible") is not True:
        return None, "data_missing", None
    market, selection = str(candidate.get("market") or ""), str(candidate.get("selection") or "")
    row = (real_money_data.get("markets") or {}).get(market)
    if not isinstance(row, dict) or row.get("direction") is None:
        return None, "data_missing", None
    candidate_line, evidence_line = as_float(candidate.get("line")), as_float(row.get("line"))
    if market not in {"1x2", "btts"} and (candidate_line is None or evidence_line is None or abs(candidate_line - evidence_line) > 1e-9):
        return None, "real_money_line_mismatch", row
    magnitude = as_float(row.get("concentration_gap"))
    if magnitude is None:
        return None, "real_money_direction_concentration_missing", row
    signed = max(0.005, magnitude) if row.get("direction") == selection else -max(0.005, magnitude)
    return signed, f"a_grade_{market}_money_distribution", row


def _candidate_pressure_signal(candidate: Dict[str, Any], dynamics: Dict[str, Any]) -> Tuple[Optional[float], str]:
    """Return signed proxy pressure toward this selection, never a claim about actual money flow."""
    market, selection = candidate.get("market"), candidate.get("selection")
    direct = _movement_probability_delta(dynamics, str(market), str(selection))
    if direct is not None:
        return direct, f"{market}_same_line_no_vig_probability_delta"
    if market == "asian_handicap" and selection in ("home", "away"):
        companion = _movement_probability_delta(dynamics, "1x2", str(selection))
        if companion is not None:
            return companion, "1x2_no_vig_probability_companion"
    if market == "over_under" and selection in ("over", "under"):
        btts_yes = _movement_probability_delta(dynamics, "btts", "yes")
        if btts_yes is not None:
            return btts_yes if selection == "over" else -btts_yes, "btts_no_vig_probability_companion"
    return None, "data_missing"


def _candidate_line_response(candidate: Dict[str, Any], dynamics: Dict[str, Any], pressure: Optional[float]) -> Dict[str, Any]:
    market, selection = candidate.get("market"), candidate.get("selection")
    movements = dynamics.get("market_movements") or {}
    structural_market = market
    structural_selection = selection
    if market == "1x2" and selection in ("home", "away"):
        structural_market = "asian_handicap"
    line_delta = as_float((movements.get(structural_market) or {}).get("line"))
    signed_response = None
    if line_delta is not None:
        if structural_market == "asian_handicap" and structural_selection in ("home", "away"):
            signed_response = -line_delta if structural_selection == "home" else line_delta
        elif structural_selection in ("over", "under"):
            signed_response = line_delta if structural_selection == "over" else -line_delta
    if signed_response is not None:
        response = "upgrade" if signed_response >= 0.249 else ("downgrade" if signed_response <= -0.249 else "static")
        return {
            "status": "available", "response": response, "signed_line_response": round(signed_response, 6),
            "raw_line_delta": line_delta, "structural_market": structural_market,
            "basis": "main_line_change_toward_candidate",
        }
    if pressure is not None and market in ("1x2", "btts"):
        return {
            "status": "partial", "response": "price_only", "signed_line_response": None,
            "raw_line_delta": None, "structural_market": market,
            "basis": "line_free_market_price_confirmation_only",
        }
    return {
        "status": "data_missing", "response": "data_missing", "signed_line_response": None,
        "raw_line_delta": line_delta, "structural_market": structural_market,
        "basis": "structural_line_response_unavailable",
    }


def _pressure_strength(value: Optional[float]) -> str:
    if value is None:
        return "data_missing"
    magnitude = abs(value)
    if magnitude >= 0.03:
        return "strong"
    if magnitude >= 0.015:
        return "medium"
    if magnitude >= 0.005:
        return "weak"
    return "neutral"


def build_repricing_attribution(
    dynamics: Dict[str, Any],
    capital_pressure: Optional[Dict[str, Any]] = None,
    market_response_diagnostic: Optional[str] = None,
) -> Dict[str, Any]:
    """Separate observed market response from evidence about why repricing occurred."""
    classification_audit = dynamics.get("classification_audit") if isinstance(dynamics.get("classification_audit"), dict) else {}
    fallback_classification = "Market-Only Move" if dynamics.get("comparison_status") == "compared" else "data_missing"
    classification = str(classification_audit.get("classification") or dynamics.get("classification") or fallback_classification)
    matched = classification_audit.get("matched_classifications") if isinstance(classification_audit.get("matched_classifications"), list) else []
    if classification not in {"", "data_missing", "None"} and classification not in matched:
        matched = [classification, *matched]
    matched = list(dict.fromkeys(str(value) for value in matched if value))
    trigger_verified = get_nested(dynamics, ["revalidation_trigger", "triggered"]) is True
    fundamental_change_verified = "Fundamental Confirmed" in matched
    likely_information = "Likely Information-Driven" in matched
    pressure = capital_pressure if isinstance(capital_pressure, dict) else {}
    real_money_present = pressure.get("is_real_money") is True and bool(pressure.get("real_money_evidence_hash"))

    if fundamental_change_verified and trigger_verified:
        status = "fundamental_repricing_confirmed"
        basis = "verified_substantive_fundamental_change_after_market_revalidation_trigger"
        source_attribution_verified = True
    elif fundamental_change_verified:
        status = "verified_fundamental_change_without_market_trigger"
        basis = "fundamental_change_is_verified_but_market_repricing_causation_is_not_established"
        source_attribution_verified = False
    elif likely_information:
        status = "likely_information_driven_unconfirmed"
        basis = "bounded_information_evidence_exists_but_the_underlying_fact_is_unconfirmed"
        source_attribution_verified = False
    elif classification == "data_missing" or dynamics.get("comparison_status") != "compared":
        status = "data_missing"
        basis = "comparable_market_timeline_or_classification_unavailable"
        source_attribution_verified = False
    else:
        status = "market_move_cause_unverified"
        basis = "market_path_observed_without_verified_fundamental_cause"
        source_attribution_verified = False

    return {
        "schema": "repricing_attribution_v1",
        "status": status,
        "market_move_classification": classification,
        "matched_classifications": matched,
        "market_response_diagnostic": market_response_diagnostic,
        "market_response_is_cause_evidence": False,
        "source_attribution_verified": source_attribution_verified,
        "verified_fundamental_change_present": fundamental_change_verified,
        "market_revalidation_triggered": trigger_verified,
        "verified_real_money_pressure_present": real_money_present,
        "real_money_pressure_proves_repricing_cause": False,
        "pressure_evidence_type": "real_money_a_grade" if real_money_present else ("capital_pressure_proxy" if pressure.get("status") == "proxy_available" else "data_missing"),
        "basis": basis,
        "policy": "market acceptance, money pressure, and repricing cause are separate claims; Accepted Repricing never proves a cause by itself",
    }


def attach_repricing_attribution(decision: Dict[str, Any], classification_audit: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    dynamics = decision.get("line_movement") if isinstance(decision.get("line_movement"), dict) else {}
    if isinstance(classification_audit, dict):
        dynamics = dict(dynamics)
        dynamics["classification"] = classification_audit.get("classification") or dynamics.get("classification")
        dynamics["classification_audit"] = classification_audit
        decision["line_movement"] = dynamics
    fallback_classification = "Market-Only Move" if dynamics.get("comparison_status") == "compared" else "data_missing"
    effective_audit = dynamics.get("classification_audit") if isinstance(dynamics.get("classification_audit"), dict) else {
        "classification": dynamics.get("classification") or fallback_classification,
        "matched_classifications": [dynamics.get("classification") or fallback_classification],
        "basis": "no_verified_fundamental_change" if fallback_classification == "Market-Only Move" else "comparable_market_timeline_unavailable",
        "inferred_without_evidence": False,
    }
    decision["market_move_classification"] = effective_audit.get("classification") or "data_missing"
    decision["market_move_classification_audit"] = effective_audit

    for candidate in decision.get("candidates") or []:
        language = candidate.get("market_language") if isinstance(candidate.get("market_language"), dict) else None
        if language is not None:
            language["repricing_attribution"] = build_repricing_attribution(
                dynamics, language.get("capital_pressure"), language.get("diagnostic")
            )
    market_language = decision.get("market_language") if isinstance(decision.get("market_language"), dict) else None
    if market_language is not None:
        for language in (market_language.get("axes") or {}).values():
            if isinstance(language, dict):
                language["repricing_attribution"] = build_repricing_attribution(
                    dynamics, language.get("capital_pressure"), language.get("diagnostic")
                )
        selected_raw = decision.get("best_market") if isinstance(decision.get("best_market"), dict) else None
        selected = next((
            row for row in (decision.get("candidates") or [])
            if isinstance(row, dict) and selected_raw
            and (row.get("market"), row.get("selection"), row.get("line"))
            == (selected_raw.get("market"), selected_raw.get("selection"), selected_raw.get("line"))
        ), selected_raw)
        selected_language = selected.get("market_language") if isinstance((selected or {}).get("market_language"), dict) else None
        market_language["selected_repricing_attribution"] = (
            selected_language.get("repricing_attribution") if selected_language else None
        )
        market_language["repricing_policy"] = "acceptance describes market response; attribution requires separate fundamental or information evidence"
    return decision


def market_language_for_candidate(candidate: Dict[str, Any], dynamics: Dict[str, Any], timeline_audit: Dict[str, Any], real_money_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    axis = _market_axis_for_candidate(candidate)
    if not timeline_audit.get("decision_eligible") or dynamics.get("comparison_status") != "compared":
        real_pressure, real_basis, real_market = _candidate_real_money_signal(candidate, real_money_data or {})
        result = {
            "axis": axis,
            "capital_pressure": {
                "status": "real_money_available" if real_pressure is not None else "data_missing",
                "label": "Real Funds" if real_pressure is not None else "data_missing",
                "signed_toward_candidate": round(real_pressure, 6) if real_pressure is not None else None,
                "strength": _pressure_strength(real_pressure), "basis": real_basis,
                "evidence_grade": "A" if real_pressure is not None else "C_or_missing",
                "is_real_money": real_pressure is not None,
                "money_percent": (real_market or {}).get("money_percent") if real_pressure is not None else None,
                "bet_percent": (real_market or {}).get("bet_percent") if real_pressure is not None else None,
                "turnover": (real_market or {}).get("turnover") if real_pressure is not None else None,
                "currency": (real_market or {}).get("currency") if real_pressure is not None else None,
                "real_money_evidence_hash": (real_money_data or {}).get("evidence_hash") if real_pressure is not None else None,
            },
            "line_response": {"status": "data_missing", "response": "data_missing"},
            "market_acceptance": "data_missing", "diagnostic": "insufficient_comparable_timeline",
            "expression_risk": "unknown",
        }
        result["repricing_attribution"] = build_repricing_attribution(dynamics, result["capital_pressure"], result["diagnostic"])
        return result
    real_pressure, real_basis, real_market = _candidate_real_money_signal(candidate, real_money_data or {})
    if real_pressure is not None:
        pressure, basis, pressure_is_real = real_pressure, real_basis, True
    else:
        pressure, basis = _candidate_pressure_signal(candidate, dynamics)
        pressure_is_real = False
    response = _candidate_line_response(candidate, dynamics, pressure)
    strength = _pressure_strength(pressure)
    if pressure is None:
        acceptance, diagnostic, risk = "data_missing", "capital_pressure_proxy_unavailable", "unknown"
    elif pressure <= -0.005:
        acceptance, diagnostic, risk = "Rejected", "Opposing Capital Pressure Proxy", "high"
    elif strength == "neutral":
        acceptance, diagnostic, risk = "Partial", "No Material Capital Pressure Proxy", "medium"
    elif response["response"] == "upgrade":
        acceptance, diagnostic, risk = "Accepted", "Accepted Repricing", "low"
    elif response["response"] == "static":
        acceptance, diagnostic, risk = "Resistance", "Market Resistance", "high"
    elif response["response"] == "downgrade":
        acceptance, diagnostic, risk = "Rejected", "Strong Resistance / Divergence", "very_high"
    elif response["response"] == "price_only":
        acceptance, diagnostic, risk = "Partial", "Price Confirmation Without Structural Line", "medium_low"
    else:
        acceptance, diagnostic, risk = "data_missing", "line_response_unavailable", "unknown"
    opposite = None
    if axis == "Home": opposite = "Away"
    elif axis == "Away": opposite = "Home"
    elif axis == "Over": opposite = "Under"
    elif axis == "Under": opposite = "Over"
    elif axis == "BTTS Yes": opposite = "BTTS No"
    elif axis == "BTTS No": opposite = "BTTS Yes"
    elif axis.endswith(" Over"): opposite = axis[:-5] + " Under"
    elif axis.endswith(" Under"): opposite = axis[:-6] + " Over"
    result = {
        "axis": axis,
        "capital_pressure": {
            "status": "real_money_available" if pressure is not None and pressure_is_real else ("proxy_available" if pressure is not None else "data_missing"),
            "label": "Real Funds" if pressure is not None and pressure_is_real else ("Capital Pressure Proxy" if pressure is not None else "data_missing"),
            "signed_toward_candidate": round(pressure, 6) if pressure is not None else None,
            "direction": axis if pressure is not None and pressure >= 0.005 else (opposite if pressure is not None and pressure <= -0.005 else "neutral"),
            "strength": strength, "basis": basis, "evidence_grade": "A" if pressure_is_real else ("B" if pressure is not None else "C_or_missing"),
            "is_real_money": pressure_is_real,
            "money_percent": (real_market or {}).get("money_percent") if pressure_is_real else None,
            "bet_percent": (real_market or {}).get("bet_percent") if pressure_is_real else None,
            "turnover": (real_market or {}).get("turnover") if pressure_is_real else None,
            "currency": (real_market or {}).get("currency") if pressure_is_real else None,
            "real_money_evidence_hash": (real_money_data or {}).get("evidence_hash") if pressure_is_real else None,
        },
        "line_response": response,
        "market_acceptance": acceptance,
        "diagnostic": diagnostic,
        "expression_risk": risk,
    }
    result["repricing_attribution"] = build_repricing_attribution(dynamics, result["capital_pressure"], result["diagnostic"])
    return result


def _expression_family(candidate: Dict[str, Any]) -> str:
    market, selection = candidate.get("market"), candidate.get("selection")
    if market in ("1x2", "asian_handicap") and selection in ("home", "away"):
        return f"team_result_{selection}"
    if market in ("over_under", "btts"):
        if (market == "over_under" and selection == "over") or (market == "btts" and selection == "yes"):
            return "open_goal_script"
        if (market == "over_under" and selection == "under") or (market == "btts" and selection == "no"):
            return "closed_goal_script"
    return f"{market}_{selection}"


def _candidate_is_qualified(candidate: Dict[str, Any]) -> bool:
    coverage = as_float(candidate.get("script_coverage"))
    return bool(
        candidate.get("market_coverage_eligible", True)
        and candidate.get("consensus_source_eligible", True)
        and candidate.get("dispersion_eligible", True)
        and as_float(candidate.get("edge")) is not None and as_float(candidate.get("edge")) >= MIN_EDGE
        and as_float(candidate.get("ev")) is not None and as_float(candidate.get("ev")) >= MIN_EV
        and coverage is not None and MIN_SCRIPT_COVERAGE <= coverage <= 1.0
    )


def apply_market_language_and_expression_optimizer(decision: Dict[str, Any], history: List[Dict[str, Any]], real_money_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    audit = decision.get("line_movement_audit") or audit_line_movement_timeline(history)
    latest = latest_prematch_snapshot(history)
    dynamics = (latest or {}).get("market_dynamics") if isinstance((latest or {}).get("market_dynamics"), dict) else {}
    axis_language = {
        "Home": market_language_for_candidate({"market": "asian_handicap", "selection": "home"}, dynamics, audit, real_money_data),
        "Away": market_language_for_candidate({"market": "asian_handicap", "selection": "away"}, dynamics, audit, real_money_data),
        "Over": market_language_for_candidate({"market": "over_under", "selection": "over"}, dynamics, audit, real_money_data),
        "Under": market_language_for_candidate({"market": "over_under", "selection": "under"}, dynamics, audit, real_money_data),
    }
    normalized_real_money = real_money_data if isinstance(real_money_data, dict) else {
        "status": "data_missing", "money_percent": None, "bet_percent": None, "turnover": None,
        "policy": "real funds require an independently sourced and timestamped A-grade feed",
    }
    real_money_used = any(get_nested(row, ["capital_pressure", "is_real_money"]) is True for row in axis_language.values())
    candidates = []
    for raw in decision.get("candidates") or []:
        candidate = dict(raw)
        candidate["market_language"] = market_language_for_candidate(candidate, dynamics, audit, real_money_data)
        candidate["expression_family"] = _expression_family(candidate)
        candidate["expression_qualified"] = _candidate_is_qualified(candidate)
        candidates.append(candidate)
    real_money_used = real_money_used or any(get_nested(row, ["market_language", "capital_pressure", "is_real_money"]) is True for row in candidates)
    decision["candidates"] = candidates
    candidate_map = {(row.get("market"), row.get("selection"), row.get("line")): row for row in candidates}
    original_raw = decision.get("best_market") if isinstance(decision.get("best_market"), dict) else None
    original = candidate_map.get(((original_raw or {}).get("market"), (original_raw or {}).get("selection"), (original_raw or {}).get("line"))) if original_raw else None
    if decision.get("decision") == "PASS" or original is None:
        decision["execution_action"] = "PASS"
        decision["market_language"] = {
            "status": "data_missing" if not audit.get("decision_eligible") else "available_no_eligible_expression",
            "axes": axis_language, "real_money_data": normalized_real_money, "capital_pressure_is_proxy_only": not real_money_used,
        }
        decision["expression_optimizer"] = {"action": "PASS", "original_expression": original, "selected_expression": None, "switch_type": None, "reason": "mandatory_gate_failed_or_no_eligible_expression", "automatic_direction_reversal": False}
        return attach_repricing_attribution(decision)
    original_language = original["market_language"]
    acceptance_rank = {"Accepted": 4, "Partial": 3, "data_missing": 2, "Resistance": 1, "Rejected": 0}
    original_acceptance = original_language.get("market_acceptance", "data_missing")
    same_family = [
        row for row in candidates
        if row.get("expression_qualified") and row.get("expression_family") == original.get("expression_family")
        and (row.get("market"), row.get("selection"), row.get("line")) != (original.get("market"), original.get("selection"), original.get("line"))
        and acceptance_rank.get(get_nested(row, ["market_language", "market_acceptance"]), -1) > acceptance_rank.get(original_acceptance, -1)
        and get_nested(row, ["market_language", "market_acceptance"]) in ("Accepted", "Partial")
    ]
    selected = max(same_family, key=lambda row: (acceptance_rank.get(get_nested(row, ["market_language", "market_acceptance"]), -1), as_float(row.get("script_coverage")) or -1, as_float(row.get("ev")) or -999), default=None)
    switched = selected is not None
    selected = selected or original
    if original_acceptance in ("Accepted", "Partial") or switched:
        action = "BET"
        reason = "current_expression_market_accepted" if not switched else "same_script_lower_resistance_expression_selected"
    else:
        action = "WAIT"
        reason = "market_resistance_requires_better_expression_or_new_evidence"
    if switched:
        decision["best_market"] = selected
        decision["edge"], decision["ev"] = selected.get("edge"), selected.get("ev")
        decision["decision"] = selected.get("selection") if selected.get("market") == "1x2" else f"{selected.get('market')}:{selected.get('selection')}"
    tiers = decision.get("recommendation_tiers") if isinstance(decision.get("recommendation_tiers"), dict) else {}
    for tier_name in ("first_choice_high_consistency", "second_choice_higher_return", "high_variance_single"):
        tier_raw = tiers.get(tier_name)
        tier = candidate_map.get(((tier_raw or {}).get("market"), (tier_raw or {}).get("selection"), (tier_raw or {}).get("line"))) if isinstance(tier_raw, dict) else None
        if tier and get_nested(tier, ["market_language", "market_acceptance"]) in ("Accepted", "Partial"):
            tiers[tier_name] = tier
        else:
            tiers[tier_name] = None
    if action == "BET":
        tiers["first_choice_high_consistency"] = selected
    decision["recommendation_tiers"] = tiers
    decision["execution_action"] = action
    decision["market_language"] = {
        "status": "available", "axes": axis_language,
        "real_money_data": normalized_real_money, "capital_pressure_is_proxy_only": not real_money_used,
        "selected_axis": get_nested(selected, ["market_language", "axis"]),
        "selected_acceptance": get_nested(selected, ["market_language", "market_acceptance"]),
        "selected_diagnostic": get_nested(selected, ["market_language", "diagnostic"]),
        "policy": "odds path is Capital Pressure Proxy; only explicit Money%/Bet%/turnover data may be called real funds",
    }
    decision["expression_optimizer"] = {
        "action": action, "original_expression": original, "selected_expression": selected,
        "switch_type": "cross_market_same_script" if switched else "original_expression",
        "reason": reason, "automatic_direction_reversal": False,
        "wait_conditions": ["structural_line_accepts_pressure", "lower_resistance_same_script_expression_appears", "new_verified_fundamental_evidence"] if action == "WAIT" else [],
    }
    return attach_repricing_attribution(decision)


def apply_line_movement_gate(decision: Dict[str, Any], history: List[Dict[str, Any]], real_money_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    audit = audit_line_movement_timeline(history)
    latest = latest_prematch_snapshot(history)
    decision["line_movement"] = (latest or {}).get("market_dynamics") or {"status": "data_missing"}
    decision["line_movement_audit"] = audit
    if not audit.get("decision_eligible"):
        force_pass_decision(decision, audit.get("reason") or "line_movement_insufficient")
    return apply_market_language_and_expression_optimizer(decision, history, real_money_data)


def snapshot_stage_usable(row: Dict[str, Any]) -> bool:
    return (
        row.get("import_status") != "data_missing"
        and bool(get_nested(row, ["market_snapshot", "available"]))
        and get_nested(row, ["stage_timing_audit", "status"]) != "invalid"
        and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"
    )


def preserve_higher_quality_team_news(existing: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
    old_news = existing.get("team_news_snapshot") if isinstance(existing.get("team_news_snapshot"), dict) else {}
    new_news = incoming.get("team_news_snapshot") if isinstance(incoming.get("team_news_snapshot"), dict) else {}
    if not old_news:
        return incoming
    old_lineups = old_news.get("lineups") if isinstance(old_news.get("lineups"), dict) else {}
    new_lineups = new_news.get("lineups") if isinstance(new_news.get("lineups"), dict) else {}
    old_injuries = old_news.get("injuries") if isinstance(old_news.get("injuries"), dict) else {}
    new_injuries = new_news.get("injuries") if isinstance(new_news.get("injuries"), dict) else {}
    lineup_score = lambda value: 2 if value.get("confirmed") else (1 if value.get("available") else 0)
    injury_score = lambda value: 1 if value.get("available") else 0
    preserved_components = []
    merged = dict(new_news)
    if lineup_score(old_lineups) > lineup_score(new_lineups):
        merged["lineups"] = old_lineups
        preserved_components.append("lineups")
    if injury_score(old_injuries) > injury_score(new_injuries):
        merged["injuries"] = old_injuries
        preserved_components.append("injuries")
    if preserved_components:
        merged["preservation_audit"] = {
            "preserved": True, "reason": "higher_quality_same_stage_team_news",
            "components": preserved_components,
            "source_snapshot_at": existing.get("snapshot_at"),
            "market_snapshot_at": incoming.get("snapshot_at"),
            "source_team_news_captured_at": old_news.get("captured_at"),
        }
        return {**incoming, "team_news_snapshot": merged}
    return incoming


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
        if existing_stage:
            record = preserve_higher_quality_team_news(existing_stage, record)
        existing_at = _parse_timestamp((existing_stage or {}).get("snapshot_at"))
        incoming_at = _parse_timestamp(record.get("snapshot_at"))
        if existing_stage and existing_at is not None and (incoming_at is None or incoming_at < existing_at):
            return {
                "saved": False, "preserved_existing": True, "reason": "stale_snapshot_rejected",
                "path": SNAPSHOT_STORE_PATH, "fixture": record["fixture"], "stage": record["stage"],
                "existing_snapshot_at": existing_stage.get("snapshot_at"), "rejected_snapshot_at": record.get("snapshot_at"),
                "revalidation_task_created": False, "revalidation_task_id": None,
                "downstream_revalidation_tasks_created": 0,
            }
        if existing_stage and existing_at is not None and incoming_at == existing_at and _content_hash(existing_stage.get("market_snapshot")) != _content_hash(record.get("market_snapshot")):
            return {
                "saved": False, "preserved_existing": True, "reason": "snapshot_timestamp_conflict_rejected",
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
    evidence_eligible = get_nested(version_record, ["fundamental_chain_audit", "decision_eligible"]) is True
    if not evidence_eligible:
        record_incomplete_revalidation_attempt(fixture, version_record.get("fundamental_chain_audit") or {}, resolved_through_stage)
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


def revalidation_required_evidence(chain_audit: Dict[str, Any]) -> List[str]:
    required = (
        list(chain_audit.get("critical_missing") or [])
        + list(chain_audit.get("critical_provenance_missing") or [])
        + list((chain_audit.get("critical_timestamp_issues") or {}).keys())
        + list((chain_audit.get("critical_semantic_issues") or {}).keys())
        + list((chain_audit.get("structural_issues") or {}).keys())
        + list((chain_audit.get("critical_structural_issues") or {}).keys())
        + list((chain_audit.get("market_contaminated_sections") or {}).keys())
    )
    return sorted(set(required)) or ["fundamental_chain_completeness"]


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
                    "required_evidence": revalidation_required_evidence(chain_audit),
                    "last_attempt_blocker_categories": [key for key in (
                        "critical_missing", "critical_provenance_missing", "critical_timestamp_issues",
                        "critical_semantic_issues", "structural_issues", "market_contaminated_sections",
                    ) if chain_audit.get(key)],
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


def version_at_least(value: Any, minimum: Tuple[int, int, int]) -> bool:
    parts = str(value or "").split(".")
    try:
        parsed = tuple(int("".join(char for char in part if char.isdigit()) or "0") for part in parts[:3])
    except (TypeError, ValueError):
        return False
    return (parsed + (0, 0, 0))[:3] >= minimum


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


IMPORTED_QUOTE_FIELDS = (
    "bookmaker_name", "bookmaker_id", "market", "market_name", "selection", "line", "price",
    "observed_at", "updated_at",
)


def _compact_imported_quote(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {key: raw.get(key) for key in IMPORTED_QUOTE_FIELDS if raw.get(key) is not None}


def _import_company_array_compaction_audit(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    input_fields = sum(len(row) for row in rows if isinstance(row, dict))
    retained_fields = sum(len(_compact_imported_quote(row)) for row in rows if isinstance(row, dict))
    return {
        "quote_count": sum(isinstance(row, dict) for row in rows),
        "input_field_count": input_fields,
        "retained_field_count": retained_fields,
        "dropped_unneeded_field_count": max(0, input_fields - retained_fields),
        "retained_fields": list(IMPORTED_QUOTE_FIELDS),
        "policy": "raw_values retain only fields needed for calculation, provenance and audit",
    }


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
        bookmaker_identity = _bookmaker_identity(bookmaker)
        selection = str(raw.get("selection") or "").strip().lower()
        price = as_float(raw.get("price"))
        if market in ("1x2", "btts"):
            key = (market, bookmaker_identity, "")
            item = grouped.setdefault(key, {"bookmaker": bookmaker, "raw_values": [], "selection_counts": {}})
            normalized = {"home": "home", "draw": "draw", "away": "away", "yes": "yes", "no": "no"}.get(selection)
            if normalized:
                item["selection_counts"][normalized] = item["selection_counts"].get(normalized, 0) + 1
                if item["selection_counts"][normalized] > 1:
                    item["ambiguous_duplicate_selection"] = True
                item[normalized] = price
            item["raw_values"].append(_compact_imported_quote(raw))
        else:
            line = str(raw.get("line") if raw.get("line") is not None else "")
            key = (market, bookmaker_identity, line)
            item = grouped.setdefault(key, {"bookmaker": bookmaker, "line": as_float(line), "raw_values": [], "selection_counts": {}})
            side = "home" if selection.startswith("home") else "away" if selection.startswith("away") else "over" if selection.startswith("over") else "under" if selection.startswith("under") else None
            if side:
                item["selection_counts"][side] = item["selection_counts"].get(side, 0) + 1
                if item["selection_counts"][side] > 1:
                    item["ambiguous_duplicate_selection"] = True
                item[side] = price
            item["raw_values"].append(_compact_imported_quote(raw))
    by_book: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for (market, bookmaker_identity, line), item in grouped.items():
        if market in ("1x2", "btts"):
            markets[market].append(item)
        else:
            book = by_book.setdefault((market, bookmaker_identity), {"bookmaker": item["bookmaker"], "lines": []})
            book["lines"].append({key: value for key, value in item.items() if key != "bookmaker"})
    for (market, _), item in by_book.items():
        markets[market].append(item)
    return markets


def _import_company_array_quality_audit(markets: Dict[str, List[Dict[str, Any]]]) -> Dict[str, Any]:
    duplicate_groups = []
    group_count_by_market = {market: 0 for market in markets}
    eligible_group_count_by_market = {market: 0 for market in markets}
    invalid_group_count_by_market = {market: 0 for market in markets}
    rejection_reason_count_by_market = {market: {} for market in markets}
    required_prices = {
        "1x2": ("home", "draw", "away"), "btts": ("yes", "no"),
        "asian_handicap": ("home", "away"), "over_under": ("over", "under"),
        "home_team_total": ("over", "under"), "away_team_total": ("over", "under"),
    }
    for market, bookmaker_rows in markets.items():
        for bookmaker_row in bookmaker_rows:
            candidates = [bookmaker_row] if market in ("1x2", "btts") else list(bookmaker_row.get("lines") or [])
            for candidate in candidates:
                group_count_by_market[market] += 1
                complete = (
                    not candidate.get("ambiguous_duplicate_selection")
                    and all((as_float(candidate.get(key)) or 0) > 1.0 for key in required_prices[market])
                    and (market in ("1x2", "btts") or as_float(candidate.get("line")) is not None)
                )
                if complete:
                    eligible_group_count_by_market[market] += 1
                else:
                    invalid_group_count_by_market[market] += 1
                    if candidate.get("ambiguous_duplicate_selection"):
                        rejection_reason = "ambiguous_duplicate_selection"
                    elif market not in ("1x2", "btts") and as_float(candidate.get("line")) is None:
                        rejection_reason = "missing_or_invalid_line"
                    else:
                        rejection_reason = "missing_or_invalid_selection_price"
                    reason_counts = rejection_reason_count_by_market[market]
                    reason_counts[rejection_reason] = reason_counts.get(rejection_reason, 0) + 1
                if not candidate.get("ambiguous_duplicate_selection"):
                    continue
                duplicate_groups.append({
                    "market": market,
                    "bookmaker": bookmaker_row.get("bookmaker"),
                    "line": candidate.get("line"),
                    "duplicate_selections": sorted(
                        key for key, count in (candidate.get("selection_counts") or {}).items() if count > 1
                    ),
                })
    duplicate_group_count_by_market = {
        market: sum(group["market"] == market for group in duplicate_groups)
        for market in markets
    }
    return {
        "ambiguous_duplicate_selection_group_count": len(duplicate_groups),
        "ambiguous_duplicate_selection_group_count_by_market": duplicate_group_count_by_market,
        "quote_group_count_by_market": group_count_by_market,
        "eligible_complete_quote_group_count_by_market": eligible_group_count_by_market,
        "incomplete_or_invalid_quote_group_count_by_market": invalid_group_count_by_market,
        "rejection_reason_count_by_market": rejection_reason_count_by_market,
        "ambiguous_duplicate_selection_groups": duplicate_groups[:50],
        "ambiguous_duplicate_selection_groups_truncated": len(duplicate_groups) > 50,
        "policy": "ambiguous duplicate selections are excluded from company-array consensus",
    }


def imported_market_snapshot(stage: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = empty_market_snapshot()
    if stage.get("status") != "available":
        return snapshot
    upstream_consensus = stage.get("consensus_main_line") or {}
    if not isinstance(upstream_consensus, dict):
        raise HTTPException(status_code=400, detail="consensus_main_line_must_be_an_object")
    company_market_array = stage.get("company_market_array") or []
    if not isinstance(company_market_array, list):
        raise HTTPException(status_code=400, detail="company_market_array_must_be_an_array")
    invalid_row_indexes = [index for index, row in enumerate(company_market_array) if not isinstance(row, dict)]
    if invalid_row_indexes:
        raise HTTPException(status_code=400, detail={
            "error": "company_market_array_rows_must_be_objects",
            "invalid_row_indexes": invalid_row_indexes[:50],
            "invalid_row_count": len(invalid_row_indexes),
            "indexes_truncated": len(invalid_row_indexes) > 50,
        })
    consensus = _import_consensus(upstream_consensus)
    markets = _import_company_markets(company_market_array)
    company_array_quality_audit = _import_company_array_quality_audit(markets)
    complete_imported_btts, deduped_imported_btts, fragmented_imported_btts = _complete_deduped_bookmaker_rows(markets["btts"], ("yes", "no"))
    recalculated_markets = {
        "1x2": _consensus_1x2(markets["1x2"]),
        "asian_handicap": _consensus_line(markets["asian_handicap"], ("home", "away")),
        "over_under": _consensus_line(markets["over_under"], ("over", "under")),
        "btts": ({
            "method": "median_all_complete_bookmakers",
            "bookmaker_count": len(complete_imported_btts),
            "yes": _median([row.get("yes") for row in complete_imported_btts]),
            "no": _median([row.get("no") for row in complete_imported_btts]),
            "bookmaker_coverage_audit": {
                **_bookmaker_coverage_audit(markets["btts"], deduped_imported_btts),
                "eligible_unique_bookmaker_count": len(complete_imported_btts),
                "incomplete_unique_bookmaker_count": len(deduped_imported_btts) - len(complete_imported_btts),
                "fragmented_synthetic_complete_identities_rejected": fragmented_imported_btts,
            },
            **_price_dispersion(complete_imported_btts, ("yes", "no")),
        } if complete_imported_btts else None),
        "home_team_total": _consensus_line(markets["home_team_total"], ("over", "under")),
        "away_team_total": _consensus_line(markets["away_team_total"], ("over", "under")),
    }
    consensus_audit = {}
    for key, recalculated in recalculated_markets.items():
        ambiguous_company_rows = (company_array_quality_audit.get("ambiguous_duplicate_selection_group_count_by_market") or {}).get(key, 0)
        if recalculated:
            recalculated["source"] = "complete_company_array"
            consensus[key] = recalculated
            consensus_audit[key] = "recalculated_from_company_array"
        elif ambiguous_company_rows:
            consensus[key] = None
            consensus_audit[key] = "data_missing_company_array_ambiguous_duplicate_selection"
        elif markets.get(key):
            consensus[key] = None
            consensus_audit[key] = "data_missing_company_array_no_complete_quote"
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
        "company_array_quality_audit": company_array_quality_audit,
        "company_array_compaction_audit": _import_company_array_compaction_audit(company_market_array),
        "data_status": {key: ("available" if value else "data_missing") for key, value in consensus.items()},
    })
    return snapshot


def _content_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_learning_probability_replay(
    fixture: Any,
    model_input_bundle: Any,
    generated_at: Optional[int] = None,
) -> Dict[str, Any]:
    """Run the built-in odds-independent calculator and retain every replay input."""
    generated_at = int(generated_at or time.time())
    fixture_key = str(fixture or "").strip()
    bundle = model_input_bundle if isinstance(model_input_bundle, dict) else {}
    if bundle.get("status") != "ready" or not isinstance(bundle.get("inputs"), dict):
        return {
            "schema": "learning_probability_replay_v1",
            "status": "data_missing",
            "decision_eligible": False,
            "fixture": fixture_key or None,
            "generated_at": generated_at,
            "uses_market_odds": False,
            "blockers": list(bundle.get("blockers") or ["independent_model_inputs_missing"]),
        }
    inputs = copy.deepcopy(bundle["inputs"])
    estimator = fundamental_expected_goals(inputs)
    if estimator.get("ok") is not True:
        return {
            "schema": "learning_probability_replay_v1",
            "status": "data_missing",
            "decision_eligible": False,
            "fixture": fixture_key or None,
            "generated_at": generated_at,
            "uses_market_odds": False,
            "input_hash": _content_hash(inputs),
            "inputs": inputs,
            "blockers": ["independent_estimator_input_invalid", *(estimator.get("errors") or [])],
        }
    expected_goals = estimator["expected_goals"]
    model = poisson_probability_model(
        expected_goals["home"], expected_goals["away"], estimator["confidence"], inputs.get("provenance")
    )
    if model.get("ok") is not True:
        return {
            "schema": "learning_probability_replay_v1",
            "status": "data_missing",
            "decision_eligible": False,
            "fixture": fixture_key or None,
            "generated_at": generated_at,
            "uses_market_odds": False,
            "input_hash": _content_hash(inputs),
            "inputs": inputs,
            "estimator": estimator,
            "blockers": ["probability_model_input_invalid", *(model.get("errors") or [])],
        }
    contract = {
        "schema": "learning_probability_replay_v1",
        "status": "ready" if estimator.get("status") == "ready" and model.get("status") == "ready" else "insufficient_confidence",
        "decision_eligible": estimator.get("status") == "ready" and model.get("status") == "ready",
        "fixture": fixture_key,
        "generated_at": generated_at,
        "calculator": {"runner_id": "builtin_fundamental_poisson", "runner_version": "1", "uses_market_odds": False},
        "uses_market_odds": False,
        "input_hash": _content_hash(inputs),
        "inputs": inputs,
        "estimator": estimator,
        "model": model,
        "source_content_hash": bundle.get("source_content_hash"),
    }
    contract["replay_hash"] = _content_hash(contract)
    return contract


def audit_learning_probability_replay(
    replay: Any,
    expected_fixture: Any,
    data_cutoff_at: Any,
    kickoff_at: Any,
) -> Dict[str, Any]:
    """Recompute a frozen probability contract; caller-supplied outputs never suffice."""
    replay = replay if isinstance(replay, dict) else {}
    if replay.get("status") == "data_missing":
        return {
            "status": "data_missing",
            "decision_eligible": False,
            "blockers": list(replay.get("blockers") or ["learning_probability_replay_inputs_missing"]),
            "recomputed": False,
        }
    issues = []
    if replay.get("schema") != "learning_probability_replay_v1":
        issues.append("unsupported_replay_schema")
    if str(replay.get("fixture") or "") != str(expected_fixture or ""):
        issues.append("replay_fixture_mismatch")
    generated_at = _parse_timestamp(replay.get("generated_at"))
    cutoff = _parse_timestamp(data_cutoff_at)
    kickoff = _parse_timestamp(kickoff_at)
    if generated_at is None:
        issues.append("replay_generated_at_missing")
    elif cutoff is not None and generated_at > cutoff:
        issues.append("replay_generated_after_packet_cutoff")
    if generated_at is not None and kickoff is not None and generated_at >= kickoff:
        issues.append("replay_generated_after_kickoff")
    inputs = replay.get("inputs") if isinstance(replay.get("inputs"), dict) else {}
    provenance = inputs.get("provenance") if isinstance(inputs.get("provenance"), dict) else {}
    allowed_input_fields = {
        "league_home_rate", "league_away_rate", "home_attack_rate", "home_defense_rate",
        "away_attack_rate", "away_defense_rate", "home_sample_size", "away_sample_size",
        "league_sample_size", "metric_type", "home_adjustment", "away_adjustment",
        "lineup_confidence", "provenance",
    }
    unexpected_input_fields = sorted(set(inputs) - allowed_input_fields)
    if unexpected_input_fields:
        issues.append("unexpected_model_input_fields:" + ",".join(unexpected_input_fields))
    forbidden_tokens = ("odd", "bookmaker", "market_probability", "implied_probability", "line_movement")
    if any(any(token in str(field).lower() for token in forbidden_tokens) for field in inputs):
        issues.append("market_derived_model_input_field_forbidden")
    observed_at = _parse_timestamp(provenance.get("observed_at"))
    if replay.get("uses_market_odds") is not False or provenance.get("uses_market_odds") is not False:
        issues.append("market_odds_forbidden_in_probability_calculator")
    if observed_at is None:
        issues.append("model_input_observed_at_missing")
    elif cutoff is not None and observed_at > cutoff:
        issues.append("model_input_observed_after_packet_cutoff")
    if observed_at is not None and kickoff is not None and observed_at >= kickoff:
        issues.append("model_input_observed_after_kickoff")
    if replay.get("input_hash") != _content_hash(inputs):
        issues.append("model_input_hash_mismatch")
    source_content_hash = str(provenance.get("source_content_hash") or "")
    if len(source_content_hash) != 64 or any(char not in "0123456789abcdef" for char in source_content_hash.lower()):
        issues.append("model_input_source_content_hash_invalid")
    if replay.get("source_content_hash") != source_content_hash:
        issues.append("replay_source_content_hash_mismatch")
    calculator = replay.get("calculator") if isinstance(replay.get("calculator"), dict) else {}
    if calculator != {"runner_id": "builtin_fundamental_poisson", "runner_version": "1", "uses_market_odds": False}:
        issues.append("untrusted_probability_calculator")
    supplied_hash = replay.get("replay_hash")
    replay_without_hash = {key: value for key, value in replay.items() if key != "replay_hash"}
    if supplied_hash != _content_hash(replay_without_hash):
        issues.append("replay_hash_mismatch")
    estimator = fundamental_expected_goals(inputs) if inputs else {"ok": False}
    recomputed_model = {"ok": False}
    if estimator.get("ok") is True:
        xg = estimator["expected_goals"]
        recomputed_model = poisson_probability_model(
            xg["home"], xg["away"], estimator["confidence"], inputs.get("provenance")
        )
    supplied_estimator = replay.get("estimator") if isinstance(replay.get("estimator"), dict) else {}
    supplied_model = replay.get("model") if isinstance(replay.get("model"), dict) else {}
    if estimator.get("ok") is not True:
        issues.append("recomputed_estimator_invalid")
    elif supplied_estimator.get("estimator_hash") != estimator.get("estimator_hash") or supplied_estimator.get("expected_goals") != estimator.get("expected_goals"):
        issues.append("estimator_output_mismatch")
    if recomputed_model.get("ok") is not True:
        issues.append("recomputed_model_invalid")
    elif supplied_model.get("model_hash") != recomputed_model.get("model_hash") or supplied_model.get("probabilities") != recomputed_model.get("probabilities"):
        issues.append("probability_output_mismatch")
    expected_eligible = estimator.get("status") == "ready" and recomputed_model.get("status") == "ready"
    if bool(replay.get("decision_eligible")) != expected_eligible:
        issues.append("replay_eligibility_mismatch")
    return {
        "status": "invalid" if issues else ("ready" if expected_eligible else "insufficient_confidence"),
        "decision_eligible": not issues and expected_eligible,
        "issues": issues,
        "recomputed": True,
        "input_hash": _content_hash(inputs) if inputs else None,
        "estimator_hash": estimator.get("estimator_hash"),
        "model_hash": recomputed_model.get("model_hash"),
    }


def normalize_fixture_identity_name(value: Any) -> str:
    """Normalize a display name for comparison without altering provider-owned ids."""
    text = unicodedata.normalize("NFKD", str(value or "")).casefold()
    return "".join(char for char in text if char.isalnum())


def fixture_identity_from_match(match: Dict[str, Any], league: Any = None, source: str = "unknown") -> Dict[str, Any]:
    kickoff = _parse_timestamp(match.get("kickoff_utc") or match.get("date") or match.get("match_time"))
    home = match.get("home_team_name") or match.get("home") or get_nested(match, ["teams", "home", "name"])
    away = match.get("away_team_name") or match.get("away") or get_nested(match, ["teams", "away", "name"])
    league_name = league or match.get("league_name") or match.get("league")
    normalized = {
        "home": normalize_fixture_identity_name(home),
        "away": normalize_fixture_identity_name(away),
        "league": normalize_fixture_identity_name(league_name),
        "home_aliases": sorted({normalize_fixture_identity_name(value) for value in ([home] + list(match.get("home_aliases") or [])) if normalize_fixture_identity_name(value)}),
        "away_aliases": sorted({normalize_fixture_identity_name(value) for value in ([away] + list(match.get("away_aliases") or [])) if normalize_fixture_identity_name(value)}),
    }
    complete = bool(kickoff is not None and normalized["home"] and normalized["away"])
    canonical_key = _content_hash({"kickoff_minute": kickoff // 60 if kickoff is not None else None, "home": normalized["home"], "away": normalized["away"], "league": normalized["league"]})[:32] if complete else None
    return {
        "source": source, "source_fixture_id": str(match.get("match_id") or match.get("fixture_id") or match.get("id") or "") or None,
        "kickoff_at": kickoff, "normalized": normalized, "canonical_key": canonical_key,
        "status": "complete" if complete else "data_missing",
        "decision_eligible": complete,
        "missing": [key for key, value in {"kickoff": kickoff, "home": normalized["home"], "away": normalized["away"]}.items() if value in (None, "")],
    }


def reconcile_fixture_identity(incoming: Dict[str, Any], candidates: List[Dict[str, Any]], kickoff_tolerance_seconds: int = 900) -> Dict[str, Any]:
    """Strictly reconcile provider fixtures; ambiguity never resolves to a guessed match."""
    if incoming.get("status") != "complete":
        return {"status": "data_missing", "decision_eligible": False, "matches": [], "reason": "incoming_identity_incomplete"}
    matches = []
    for candidate in candidates or []:
        if candidate.get("status") != "complete":
            continue
        left, right = incoming.get("normalized") or {}, candidate.get("normalized") or {}
        left_home = set(left.get("home_aliases") or [left.get("home")]) - {None, ""}
        right_home = set(right.get("home_aliases") or [right.get("home")]) - {None, ""}
        left_away = set(left.get("away_aliases") or [left.get("away")]) - {None, ""}
        right_away = set(right.get("away_aliases") or [right.get("away")]) - {None, ""}
        same_teams = bool(left_home & right_home) and bool(left_away & right_away)
        league_compatible = not left.get("league") or not right.get("league") or left.get("league") == right.get("league")
        kickoff_delta = abs(int(incoming.get("kickoff_at")) - int(candidate.get("kickoff_at")))
        if same_teams and league_compatible and kickoff_delta <= kickoff_tolerance_seconds:
            matches.append({"canonical_key": candidate.get("canonical_key"), "source": candidate.get("source"), "source_fixture_id": candidate.get("source_fixture_id"), "kickoff_delta_seconds": kickoff_delta})
    if len(matches) == 1:
        return {"status": "matched", "decision_eligible": True, "match": matches[0], "matches": matches, "reason": None}
    if len(matches) > 1:
        return {"status": "ambiguous", "decision_eligible": False, "matches": matches, "reason": "multiple_provider_candidates"}
    return {"status": "no_match", "decision_eligible": False, "matches": [], "reason": "no_strict_provider_match"}


def provider_reconciliation_report(date: str, store_override: Optional[Dict[str, Any]] = None, nami_schedule_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Compare Nami target candidates with persisted pang identities without mutating either source."""
    store = store_override if store_override is not None else load_snapshot_store()
    pang_candidates = []
    for fixture, metadata in (store.get("external_prematch") or {}).items():
        identity = metadata.get("fixture_identity")
        if not isinstance(identity, dict):
            identity = fixture_identity_from_match(metadata.get("match") or {}, metadata.get("league"), "pang")
        identity = {**identity, "source": "pang", "source_fixture_id": str(fixture)}
        pang_candidates.append(identity)
    nami_schedule = nami_schedule_override if nami_schedule_override is not None else nami_fixtures_for_date(date)
    target_rows = [row for row in (nami_schedule.get("fixtures") or []) if row.get("target_candidate")]
    rows = []
    for row in target_rows:
        reconciliation = reconcile_fixture_identity(row.get("fixture_identity") or {}, pang_candidates)
        rows.append({
            "nami_fixture_id": row.get("provider_fixture_id"), "competition": row.get("canonical_competition") or row.get("competition"),
            "home": row.get("home"), "away": row.get("away"), "kickoff_at": row.get("kickoff_at"),
            "status": reconciliation.get("status"), "decision_eligible": reconciliation.get("decision_eligible", False),
            "matched_pang_fixture_id": get_nested(reconciliation, ["match", "source_fixture_id"]),
            "reason": reconciliation.get("reason"), "candidate_count": len(reconciliation.get("matches") or []),
        })
    counts = {status: sum(row["status"] == status for row in rows) for status in ("matched", "ambiguous", "no_match", "data_missing")}
    return {
        "ok": bool(nami_schedule.get("ok")), "version": VERSION, "date": date,
        "pang_fixture_count": len(pang_candidates), "nami_target_candidate_count": len(target_rows),
        "counts": counts, "rows": rows,
        "decision_gate": "only_unique_matched_rows_can_continue; market_and_timeline_gates_still_apply",
        "mutation_policy": "read_only_reconciliation_no_pang_writes",
    }


def apply_provider_reconciliation(date: str, store_override: Optional[Dict[str, Any]] = None, nami_schedule_override: Optional[Dict[str, Any]] = None, persist: bool = True) -> Dict[str, Any]:
    """Persist only unique, conflict-free provider ids in the local Railway store."""
    with SNAPSHOT_STORE_LOCK:
        store = store_override if store_override is not None else load_snapshot_store()
        report = provider_reconciliation_report(date, store_override=store, nami_schedule_override=nami_schedule_override)
        applied, unchanged, rejected = [], [], []
        metadata_by_fixture = store.setdefault("external_prematch", {})
        now_ts = int(time.time())
        for row in report["rows"]:
            if row.get("status") != "matched" or not row.get("decision_eligible"):
                rejected.append({"nami_fixture_id": row.get("nami_fixture_id"), "reason": row.get("reason") or row.get("status")})
                continue
            pang_fixture = str(row.get("matched_pang_fixture_id") or "")
            nami_fixture = str(row.get("nami_fixture_id") or "")
            metadata = metadata_by_fixture.get(pang_fixture)
            if not metadata or not nami_fixture:
                rejected.append({"nami_fixture_id": nami_fixture or None, "reason": "matched_metadata_missing"})
                continue
            provider_ids = metadata.setdefault("provider_fixture_ids", {"pang": pang_fixture})
            existing = str(provider_ids.get("nami") or "")
            if existing and existing != nami_fixture:
                rejected.append({"nami_fixture_id": nami_fixture, "pang_fixture_id": pang_fixture, "reason": "existing_nami_id_conflict"})
                continue
            record = {"pang_fixture_id": pang_fixture, "nami_fixture_id": nami_fixture}
            if existing == nami_fixture:
                unchanged.append(record)
                continue
            provider_ids["nami"] = nami_fixture
            metadata["provider_reconciliation"] = {
                "status": "matched", "matched_at": now_ts, "match_date": date,
                "method": "strict_team_league_kickoff_unique_match", "kickoff_tolerance_seconds": 900,
            }
            applied.append(record)
        audit = {
            "run_at": now_ts, "date": date, "applied_count": len(applied), "unchanged_count": len(unchanged),
            "rejected_count": len(rejected), "source_counts": report.get("counts"),
            "matcher_schema_version": PROVIDER_RECONCILIATION_SCHEMA_VERSION,
            "policy": "local_mapping_only; no_pang_writes; conflicts_never_overwritten",
        }
        history = store.setdefault("provider_reconciliation_audit", [])
        history.append(audit)
        store["provider_reconciliation_audit"] = history[-500:]
        store["version"] = VERSION
        if persist and store_override is None:
            write_snapshot_store(store)
        return {"ok": report.get("ok"), "version": VERSION, "date": date, "applied": applied, "unchanged": unchanged, "rejected": rejected, "audit": audit}


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
    packet_source = str(packet.get("source") or "pang").strip().lower()
    if packet_source not in {"pang", "the_odds_api"}:
        raise HTTPException(status_code=400, detail="unsupported_prematch_packet_source")
    match = packet.get("match") or {}
    if not isinstance(match, dict):
        raise HTTPException(status_code=400, detail="match_must_be_an_object")
    fixture = str(match.get("match_id") or "").strip()
    if not fixture:
        raise HTTPException(status_code=400, detail="missing_match_id")
    fixture_identity = fixture_identity_from_match(match, packet.get("league"), packet_source)
    timeline = packet.get("timeline")
    if not isinstance(timeline, list):
        raise HTTPException(status_code=400, detail="timeline_must_be_an_array")
    invalid_stage_indexes = [index for index, row in enumerate(timeline) if not isinstance(row, dict)]
    if invalid_stage_indexes:
        raise HTTPException(status_code=400, detail={
            "error": "timeline_rows_must_be_objects",
            "invalid_row_indexes": invalid_stage_indexes[:50],
            "invalid_row_count": len(invalid_stage_indexes),
            "indexes_truncated": len(invalid_stage_indexes) > 50,
        })
    lineup_history = packet.get("lineup_history")
    if lineup_history is not None and not isinstance(lineup_history, list):
        raise HTTPException(status_code=400, detail="lineup_history_must_be_an_array")
    invalid_lineup_indexes = [index for index, row in enumerate(lineup_history or []) if not isinstance(row, dict)]
    if invalid_lineup_indexes:
        raise HTTPException(status_code=400, detail={
            "error": "lineup_history_rows_must_be_objects",
            "invalid_row_indexes": invalid_lineup_indexes[:50],
            "invalid_row_count": len(invalid_lineup_indexes),
            "indexes_truncated": len(invalid_lineup_indexes) > 50,
        })
    seen = set()
    records: List[Dict[str, Any]] = []
    imported = []
    for stage_data in timeline:
        stage = normalize_stage(stage_data.get("stage"))
        if stage not in PREMATCH_STAGE_ORDER or stage in seen:
            raise HTTPException(status_code=400, detail={"error": "invalid_or_duplicate_stage", "stage": stage})
        seen.add(stage)
        status = "available" if stage_data.get("status") == "available" else "data_missing"
        observed_value = stage_data.get("latest_observed_at") or stage_data.get("target_at")
        observed_at = _parse_timestamp(observed_value)
        opening_source_audit = None
        provider_audit = stage_data.get("provider_audit") if isinstance(stage_data.get("provider_audit"), dict) else None
        market_snapshot = imported_market_snapshot(stage_data)
        if stage == "Opening":
            verified_markets = [
                market for market, row in (market_snapshot.get("consensus_main_line") or {}).items()
                if isinstance(row, dict) and row.get("source") == "complete_company_array" and int(row.get("bookmaker_count") or 0) >= 1
            ]
            opening_source_audit = {
                "verified": bool(
                    observed_at is not None and verified_markets
                    and (packet_source != "the_odds_api" or get_nested(provider_audit or {}, ["opening_verified"]) is True)
                ),
                "observed_at_present": observed_at is not None,
                "verified_markets": verified_markets,
                "provider_first_seen_verified": get_nested(provider_audit or {}, ["opening_verified"]),
                "provider_preceding_absence_at": get_nested(provider_audit or {}, ["opening_preceding_absence_at"]),
                "policy": "opening_requires_observation_time_recalculated_company_array_and_provider_first_seen_proof_when_source_is_the_odds_api",
            }
            if status == "available" and not opening_source_audit["verified"]:
                status = "data_missing"
                market_snapshot = empty_market_snapshot()
        snapshot_at = observed_at or int(time.time())
        information_search = normalize_information_search(stage_data.get("information_search"), snapshot_at)
        missing_reason = "opening_source_unverified" if stage == "Opening" and status == "data_missing" and opening_source_audit and not opening_source_audit["verified"] else stage_data.get("reason")
        source_hash = _content_hash({"stage": stage, "status": status, "market_snapshot": market_snapshot, "missing_reason": missing_reason, "latest_observed_at": stage_data.get("latest_observed_at"), "target_at": stage_data.get("target_at"), "kickoff_utc": match.get("kickoff_utc"), "information_search": information_search, "opening_source_audit": opening_source_audit, "provider_audit": provider_audit})
        record = {
            "version": VERSION, "fixture": fixture, "external_fixture_id": fixture, "source": f"{packet_source}_import",
            "stage": stage, "snapshot_at": snapshot_at,
            "fixture_info": match, "data_quality": packet.get("data_quality"), "coverage": {"source": packet_source, "quote_count": stage_data.get("quote_count"), "bookmaker_count": stage_data.get("bookmaker_count")},
            "import_status": status, "missing_reason": missing_reason if status == "data_missing" else None,
            "market_snapshot": market_snapshot, "source_content_hash": source_hash, "market_dynamics": None,
            "information_search": information_search, "opening_source_audit": opening_source_audit,
            "provider_audit": provider_audit,
        }
        record["stage_timing_audit"] = audit_stage_timing(stage, record["snapshot_at"], match.get("kickoff_utc"))
        records.append(record)
    with SNAPSHOT_STORE_LOCK:
        store = store_override if store_override is not None else load_snapshot_store()
        external_prematch = store.setdefault("external_prematch", {})
        previous_meta = external_prematch.get(fixture) or {}
        provider_fixture_ids = dict(previous_meta.get("provider_fixture_ids") or {})
        if packet_source == "pang":
            provider_fixture_ids["pang"] = fixture
        else:
            provider_fixture_ids["the_odds_api"] = str(match.get("the_odds_api_event_id") or "") or None
        next_data_quality = packet.get("data_quality") or previous_meta.get("data_quality")
        if packet_source == "the_odds_api" and isinstance(packet.get("data_quality"), dict) and isinstance(previous_meta.get("data_quality"), dict):
            previous_quality = previous_meta["data_quality"]
            incoming_quality = packet["data_quality"]
            eligible_stages = [
                stage for stage in PREMATCH_STAGE_ORDER
                if stage in set(previous_quality.get("primary_reference_eligible_stages") or [])
                or stage in set(incoming_quality.get("primary_reference_eligible_stages") or [])
            ]
            next_data_quality = {
                **previous_quality,
                **incoming_quality,
                "primary_reference_eligible_stages": eligible_stages,
                "primary_reference_eligible_stage_count": len(eligible_stages),
                "required_stage_count": len(PREMATCH_STAGE_ORDER),
                "last_requested_stages": list(incoming_quality.get("requested_stages") or []),
                "incremental_collection": bool(incoming_quality.get("incremental_collection")),
            }
        next_meta_content = {
            "schema_version": packet.get("schema_version"), "league": packet.get("league") or previous_meta.get("league"),
            "exported_at": packet.get("exported_at") or previous_meta.get("exported_at"), "match": match or previous_meta.get("match"),
            "required_timeline": packet.get("required_timeline") or previous_meta.get("required_timeline") or PREMATCH_STAGE_ORDER,
            "lineup_history": lineup_history if "lineup_history" in packet else previous_meta.get("lineup_history", []),
            "data_quality": next_data_quality,
            "fixture_identity": fixture_identity,
            "source": packet_source,
            "provider_fixture_ids": {key: value for key, value in provider_fixture_ids.items() if value},
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
            elif old and snapshot_stage_usable(old) and not snapshot_stage_usable(record):
                action = "quality_regression_skipped"
            elif old and int(record.get("snapshot_at") or 0) < int(old.get("snapshot_at") or 0):
                action = "stale_skipped"
            elif old and int(record.get("snapshot_at") or 0) == int(old.get("snapshot_at") or 0) and _content_hash(old.get("market_snapshot")) != _content_hash(record.get("market_snapshot")):
                action = "timestamp_conflict_skipped"
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
    counts = {action: sum(1 for row in imported if row["action"] == action) for action in ("inserted", "updated", "unchanged", "stale_skipped", "quality_regression_skipped", "timestamp_conflict_skipped")}
    return {"fixture": fixture, "match": f"{match.get('home_team_name')} vs {match.get('away_team_name')}", "stages": imported, "counts": counts, "changed": bool(accepted or metadata_changed), "metadata_changed": metadata_changed, "revalidation_tasks_created": len(queued)}


def import_prematch_packet_batch(packets: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    """Validate and merge a batch in memory, then persist it exactly once."""
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        results = [import_prematch_packet(packet, store_override=store, persist=False) for packet in packets]
        return results, store


def collect_the_odds_api_timeline(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Collect and optionally persist missing nodes of a verified timeline."""
    if not THE_ODDS_API_KEY:
        raise HTTPException(status_code=503, detail="the_odds_api_not_configured")
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="request_body_must_be_an_object")
    required = ("fixture", "sport_key", "league", "home_team", "away_team", "kickoff_utc")
    missing = [key for key in required if payload.get(key) in (None, "")]
    if missing:
        raise HTTPException(status_code=400, detail={"error": "missing_required_fields", "fields": missing})
    fixture = str(payload.get("fixture")).strip()
    if not fixture or len(fixture) > 128:
        raise HTTPException(status_code=400, detail="invalid_fixture")
    requested_stages = payload.get("requested_stages")
    if requested_stages is not None:
        if not isinstance(requested_stages, list) or not requested_stages or any(not isinstance(stage, str) for stage in requested_stages):
            raise HTTPException(status_code=400, detail="invalid_requested_stages")
        requested_stages = list(dict.fromkeys(requested_stages))
        invalid_stages = [stage for stage in requested_stages if stage not in PREMATCH_STAGE_ORDER]
        if invalid_stages:
            raise HTTPException(status_code=400, detail={"error": "invalid_requested_stage", "stages": invalid_stages})
    else:
        requested_stages = list(PREMATCH_STAGE_ORDER)
    only_missing = payload.get("only_missing", True)
    retry_data_missing = payload.get("retry_data_missing", False)
    if not isinstance(only_missing, bool) or not isinstance(retry_data_missing, bool):
        raise HTTPException(status_code=400, detail="incremental_flags_must_be_boolean")
    store = load_snapshot_store()
    existing_rows = list((store.get("fixtures") or {}).get(fixture) or [])
    if retry_data_missing:
        completed_stages = {row.get("stage") for row in existing_rows if snapshot_stage_usable(row)}
    else:
        completed_stages = {row.get("stage") for row in existing_rows if row.get("stage") in PREMATCH_STAGE_ORDER}
    stages_to_fetch = [stage for stage in requested_stages if not only_missing or stage not in completed_stages]
    metadata = (store.get("external_prematch") or {}).get(fixture) or {}
    provider_ids = metadata.get("provider_fixture_ids") if isinstance(metadata.get("provider_fixture_ids"), dict) else {}
    known_event_id = str(payload.get("the_odds_api_event_id") or provider_ids.get("the_odds_api") or "").strip()
    if len(known_event_id) > 200 or any(ord(char) < 32 for char in known_event_id):
        raise HTTPException(status_code=400, detail="invalid_the_odds_api_event_id")
    if not stages_to_fetch:
        return {
            "ok": True, "source": "the_odds_api", "fixture": fixture,
            "request_count": 0, "request_audit": [], "requested_stages": requested_stages,
            "stages_to_fetch": [], "skipped_existing_stages": requested_stages,
            "incremental_noop": True, "persist_requested": payload.get("persist", True) is not False,
            "persist_result": None, "credential_exposed": False,
        }
    aliases = {}
    for side in ("home", "away"):
        raw = payload.get(f"{side}_aliases") or []
        if not isinstance(raw, list) or len(raw) > 20 or any(not isinstance(value, str) or len(value) > 120 for value in raw):
            raise HTTPException(status_code=400, detail=f"invalid_{side}_aliases")
        aliases[side] = raw
    regions = str(payload.get("regions") or THE_ODDS_API_REGIONS).strip().lower()
    region_values = [value.strip() for value in regions.split(",") if value.strip()]
    if not region_values or len(region_values) > 3 or any(not value.replace("_", "").isalnum() for value in region_values):
        raise HTTPException(status_code=400, detail="invalid_the_odds_api_regions")
    bookmakers = str(payload.get("bookmakers") or THE_ODDS_API_BOOKMAKERS).strip().lower()
    bookmaker_values = [value.strip() for value in bookmakers.split(",") if value.strip()]
    if len(bookmaker_values) > 20 or any(not value.replace("_", "").isalnum() for value in bookmaker_values):
        raise HTTPException(status_code=400, detail="invalid_the_odds_api_bookmakers")
    try:
        result = collect_historical_timeline(
            call_the_odds_api,
            fixture=fixture,
            sport_key=payload.get("sport_key"),
            league=str(payload.get("league")),
            home_team=str(payload.get("home_team")),
            away_team=str(payload.get("away_team")),
            kickoff_utc=payload.get("kickoff_utc"),
            home_aliases=aliases["home"],
            away_aliases=aliases["away"],
            regions=",".join(region_values),
            bookmakers=",".join(bookmaker_values),
            opening_lookback_days=max(1, min(int(payload.get("opening_lookback_days") or THE_ODDS_API_OPENING_LOOKBACK_DAYS), 30)),
            opening_scan_hours=max(1, min(int(payload.get("opening_scan_hours") or THE_ODDS_API_OPENING_SCAN_HOURS), 24)),
            max_requests=THE_ODDS_API_MAX_HISTORY_REQUESTS,
            minimums={
                "1x2": THE_ODDS_API_MIN_1X2_BOOKMAKERS,
                "asian_handicap": THE_ODDS_API_MIN_AH_BOOKMAKERS,
                "over_under": THE_ODDS_API_MIN_OU_BOOKMAKERS,
            },
            requested_stages=stages_to_fetch,
            known_event_id=known_event_id or None,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    persisted = None
    if result.get("ok") and payload.get("persist", True) is not False:
        persisted = import_prematch_packet(result["packet"])
    return {
        **result,
        "stages_to_fetch": stages_to_fetch,
        "skipped_existing_stages": [stage for stage in requested_stages if stage not in stages_to_fetch],
        "incremental_noop": False,
        "persist_requested": payload.get("persist", True) is not False,
        "persist_result": persisted,
        "credential_exposed": False,
    }


def server_import_preflight(packets: List[Dict[str, Any]], expected_date: Optional[str] = None, require_prematch: bool = False, now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    seen, rows = set(), []
    for packet in packets:
        match = packet.get("match") if isinstance(packet, dict) else {}
        match = match if isinstance(match, dict) else {}
        fixture = str(match.get("match_id") or "").strip()
        kickoff_raw = match.get("kickoff_utc")
        try:
            kickoff_dt = datetime.fromisoformat(str(kickoff_raw).replace("Z", "+00:00"))
            if kickoff_dt.tzinfo is None:
                kickoff_dt = kickoff_dt.replace(tzinfo=timezone.utc)
            kickoff_ts, kickoff_date = int(kickoff_dt.timestamp()), kickoff_dt.date().isoformat()
        except (TypeError, ValueError):
            kickoff_ts, kickoff_date = None, None
        reasons = []
        if not fixture:
            reasons.append("missing_match_id")
        elif fixture in seen:
            reasons.append("duplicate_match_id")
        seen.add(fixture)
        if kickoff_ts is None:
            reasons.append("missing_or_invalid_kickoff")
        if expected_date and kickoff_date != expected_date:
            reasons.append("kickoff_date_mismatch")
        if require_prematch and kickoff_ts is not None and kickoff_ts <= now_ts:
            reasons.append("fixture_not_prematch")
        rows.append({"fixture": fixture or None, "kickoff_date": kickoff_date, "status": "rejected" if reasons else "ready", "reasons": reasons})
    rejected = [row for row in rows if row["status"] == "rejected"]
    return {"status": "ready" if packets and not rejected else "rejected", "packet_count": len(packets), "ready_count": len(rows) - len(rejected), "rejected_count": len(rejected), "expected_date": expected_date, "require_prematch": require_prematch, "rows": rows}


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
    shared_markets = sorted({
        market for market, movement in movements.items()
        if any(as_float(value) is not None for value in movement.values())
    } | {
        market for market, detail in probability_movements.items()
        if detail.get("status") in ("compared", "line_changed")
    })
    actual_comparison = bool(previous and shared_markets)
    return {
        "stage": stage, "previous_stage": previous.get("stage") if previous else None,
        "comparison_status": "compared" if actual_comparison else "data_missing",
        "comparison_audit": {
            "previous_stage_available": bool(previous),
            "shared_comparable_markets": shared_markets,
            "shared_comparable_market_count": len(shared_markets),
            "reason": None if actual_comparison else ("previous_stage_missing" if not previous else "no_shared_comparable_market"),
        },
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
    lineup_confirmed = bool(si.get("lineups_confirmed") or get_nested(si, ["lineups", "confirmed"]))
    injuries = si.get("injuries") or {}
    form = si.get("recent_form_last_10") or {}
    stats = si.get("season_stats") or {}
    standings = si.get("standings") or {}
    usable_stats_sides = [side for side in ("home", "away") if isinstance(stats.get(side), dict) and stats[side].get("available") is True]
    usable_form_sides = [side for side in ("home", "away") if isinstance(form.get(side), dict) and form[side].get("available") is True]
    execution_evidence_available = bool(usable_stats_sides or usable_form_sides)
    conversion_evidence_available = any(
        as_float((stats.get(side) or {}).get(field)) is not None
        for side in usable_stats_sides for field in ("goals_for_avg", "failed_to_score")
    )
    evidence = {
        "standings": standings, "recent_form_last_10": form, "season_stats": stats,
        "injuries": injuries, "lineups_available": lineup_available, "lineups_confirmed": lineup_confirmed,
    }
    chain = {
        "result_utility": {"status": "data_missing", "home_win_draw_loss_utility": None, "away_win_draw_loss_utility": None, "reason": "competition objective/qualification rules are not supplied by current feeds"},
        "tactical_risk_appetite": {"status": "data_missing", "value": None, "depends_on": "result_utility and verified coach intent"},
        "rotation_quality": {"status": "partial" if lineup_confirmed else "data_missing", "starting_xi_strength": None, "creativity": None, "finishing": None, "chemistry": None, "bench_strength": None, "bench_upgrade": None, "lineup_intent": None, "current_athletic_level": {"home": None, "away": None}, "structural_replacement": {"home": None, "away": None}, "reason": "confirmed_xi_present_but_quality_dimensions_not_scored" if lineup_confirmed else ("partial_or_unconfirmed_lineup_not_sufficient" if lineup_available else "lineup_data_missing")},
        "execution_ability": {"status": "partial" if execution_evidence_available else "data_missing", "source": "season_stats and recent_form; no event-level xG/xThreat feed", "usable_stats_sides": usable_stats_sides, "usable_form_sides": usable_form_sides, "absolute_attack_quality": {"home": None, "away": None}},
        "tactical_matchup": {"status": "data_missing", "value": None, "reason": "formation/style/event-level data unavailable"},
        "game_state_elasticity": {"status": "data_missing", "states": {"0_0_persists": None, "home_scores_first": None, "away_scores_first": None, "draw_at_60": None, "trailing_last_30": None}},
        "first_goal_state_transition": {"status": "data_missing", "home_first": None, "away_first": None},
        "open_game_beneficiary": {"status": "data_missing", "team": None, "two_way": {"home_attack_gain": None, "home_defensive_exposure": None, "away_attack_gain": None, "away_defensive_exposure": None}, "reason": "requires tactical risk and transition/conversion evidence"},
        "time_segment_strength": {"status": "data_missing", "segments": {"0_15": None, "16_30": None, "31_45": None, "46_60": None, "61_75": None, "76_90": None}, "late_game_resistance": {"home": None, "away": None}},
        "goal_conversion": {"status": "partial" if conversion_evidence_available else "data_missing", "strength_edge": None, "goal_edge": None, "margin_edge": None, "usable_stats_sides": usable_stats_sides if conversion_evidence_available else [], "warning": "Strength Edge != Goal Edge != Margin Edge"},
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


FUNDAMENTAL_EVIDENCE_METADATA_FIELDS = {"observed_at", "as_of", "source", "provenance", "evidence", "evidence_refs", "notes", "reason", "warning"}


def substantive_fundamental_section(section: Any) -> Any:
    if isinstance(section, dict):
        return {
            key: substantive_fundamental_section(value)
            for key, value in section.items()
            if key not in FUNDAMENTAL_EVIDENCE_METADATA_FIELDS
        }
    if isinstance(section, list):
        return [substantive_fundamental_section(value) for value in section]
    return section


def fundamental_chain_change_sets(old_chain: Dict[str, Any], new_chain: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    raw_changed = [key for key in FUNDAMENTAL_CHAIN if old_chain.get(key) != new_chain.get(key)]
    substantive = [key for key in raw_changed if substantive_fundamental_section(old_chain.get(key)) != substantive_fundamental_section(new_chain.get(key))]
    metadata_only = [key for key in raw_changed if key not in substantive]
    return substantive, metadata_only


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
        old_chain = old_script.get("chain") if isinstance(old_script.get("chain"), dict) else {}
        new_chain = script.get("chain") if isinstance(script.get("chain"), dict) else {}
        changed_sections, evidence_metadata_changes = fundamental_chain_change_sets(old_chain, new_chain) if has_previous else ([], [])
        variable_changes = {key: {"before": old_chain.get(key), "after": new_chain.get(key)} for key in changed_sections}
        estimator_changed = bool(has_previous and old_script.get("estimator") != script.get("estimator"))
        if estimator_changed:
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
        script_changed = has_previous and bool(changed_sections)
        chain_audit = audit_fundamental_chain(script)
        probability_changed = probability_change.get("before") != probability_change.get("after") if has_previous and "after" in probability_change else None
        best_market_changed = best_market_change.get("changed") if has_previous and "changed" in best_market_change else None
        change_types = []
        if not has_previous:
            change_types.append("baseline")
        if changed_sections:
            change_types.append("fundamental_variables")
        if evidence_metadata_changes:
            change_types.append("evidence_metadata")
        if estimator_changed:
            change_types.append("estimator")
        if probability_changed:
            change_types.append("model_probability")
        if best_market_changed:
            change_types.append("best_market_expression")
        if has_previous and not change_types:
            change_types.append("no_change")
        record = {
            "version_number": next_version, "previous_version_number": prior_version_number,
            "created_at": int(time.time()), "trigger": normalized_trigger,
            "changed_information": changed_sections, "variable_changes": variable_changes,
            "evidence_metadata_changes": evidence_metadata_changes,
            "probability_change": probability_change,
            "best_market_change": best_market_change,
            "change_types": change_types,
            "primary_change_type": next((kind for kind in ("fundamental_variables", "evidence_metadata", "estimator", "model_probability", "best_market_expression", "baseline", "no_change") if kind in change_types), "no_change"),
            "fundamental_chain_audit": {
                "status": chain_audit.get("status"),
                "decision_eligible": chain_audit.get("decision_eligible") is True,
                "critical_missing": chain_audit.get("critical_missing") or [],
                "critical_provenance_missing": chain_audit.get("critical_provenance_missing") or [],
                "critical_timestamp_issues": chain_audit.get("critical_timestamp_issues") or {},
                "critical_semantic_issues": chain_audit.get("critical_semantic_issues") or {},
                "structural_issues": chain_audit.get("structural_issues") or {},
                "market_contaminated_sections": chain_audit.get("market_contaminated_sections") or {},
            },
            "recalculation_audit": {
                "performed": True,
                "baseline_created": not has_previous,
                "comparison_available": has_previous,
                "requested_previous_version_number": requested_previous_version,
                "comparison_rebased_to_latest": comparison_rebased,
                "fundamental_changed": script_changed,
                "fundamental_evidence_eligible": chain_audit.get("decision_eligible") is True,
                "estimator_changed": estimator_changed,
                "evidence_metadata_refreshed": bool(evidence_metadata_changes),
                "probability_changed": probability_changed,
                "best_market_changed": best_market_changed,
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
    previous_chain = get_nested(previous_version or {}, ["script", "chain"]) or {}
    new_chain = (new_script or {}).get("chain") if isinstance((new_script or {}).get("chain"), dict) else {}
    changed_fundamental_sections, evidence_metadata_only_sections = fundamental_chain_change_sets(previous_chain, new_chain) if previous_version and new_script else ([], [])
    new_chain_audit = audit_fundamental_chain(new_script or {}) if changed_fundamental_sections else {"decision_eligible": False, "status": "not_changed"}
    fundamental_changed = bool(changed_fundamental_sections and new_chain_audit.get("decision_eligible"))
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
        "fundamental_change_audit": {
            "changed_sections": changed_fundamental_sections,
            "evidence_metadata_only_sections": evidence_metadata_only_sections,
            "evidence_eligible": bool(new_chain_audit.get("decision_eligible")),
            "rule": "verified_substantive_chain_change_required; evidence timestamp/source refresh or model/estimator hash change alone is not fundamental confirmation",
        },
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
        "decision": decision, "execution_action": "PASS" if decision == "PASS" else "PENDING_MARKET_LANGUAGE",
        "best_market": first_choice, "candidates": candidates,
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
    extended_paths = {
        "current_athletic_level": (("rotation_quality", "current_athletic_level", "home"), ("rotation_quality", "current_athletic_level", "away")),
        "structural_replacement": (("rotation_quality", "structural_replacement", "home"), ("rotation_quality", "structural_replacement", "away")),
        "absolute_attack_quality": (("execution_ability", "absolute_attack_quality", "home"), ("execution_ability", "absolute_attack_quality", "away")),
        "two_way_open_game": tuple(("open_game_beneficiary", "two_way", field) for field in ("home_attack_gain", "home_defensive_exposure", "away_attack_gain", "away_defensive_exposure")),
        "late_game_resistance": (("time_segment_strength", "late_game_resistance", "home"), ("time_segment_strength", "late_game_resistance", "away")),
    }
    extended_dimension_audit = {}
    for dimension, paths in extended_paths.items():
        missing_paths = [".".join(path) for path in paths if get_nested(chain, list(path)) in (None, "", [], {})]
        extended_dimension_audit[dimension] = {
            "status": "available" if not missing_paths else ("partial" if len(missing_paths) < len(paths) else "data_missing"),
            "missing_paths": missing_paths,
        }
    extended_ready = all(row["status"] == "available" for row in extended_dimension_audit.values())
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
        "extended_dimension_audit": extended_dimension_audit,
        "extended_dimensions_ready": extended_ready,
        "extended_dimensions_policy": "compatibility audit in V1.49; missing legacy fields do not alone invalidate an otherwise eligible V4 packet",
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


def freeze_fundamental_stage(
    fixture: Any,
    script: Dict[str, Any],
    chain_audit: Dict[str, Any],
    estimator: Dict[str, Any],
    model: Dict[str, Any],
    frozen_at: int,
) -> Dict[str, Any]:
    script_hash = str(script.get("content_hash") or "").strip() or None
    recomputed_script_hash = _content_hash({
        "fixture": str(fixture), "chain": script.get("chain") or {},
        "estimator_method": get_nested(script, ["estimator", "method"]),
        "estimator_inputs": get_nested(script, ["estimator", "inputs"]),
        "expected_goals": get_nested(script, ["estimator", "expected_goals"]),
        "model_method": get_nested(script, ["model", "method"]),
        "model_probability": get_nested(script, ["model", "probabilities", "1x2"]),
    })
    script_hash_valid = bool(script_hash and script_hash == recomputed_script_hash)
    estimator_independent = estimator.get("uses_market_odds") is False
    chain_independent = not bool(chain_audit.get("market_contaminated_sections"))
    odds_independent = script.get("odds_independent") is True and estimator_independent and chain_independent and script_hash_valid
    normalized = {
        "schema": "fundamental_first_freeze_v1",
        "fixture": str(fixture),
        "frozen_at": int(frozen_at),
        "script_hash": script_hash,
        "recomputed_script_hash": recomputed_script_hash,
        "script_hash_valid": script_hash_valid,
        "estimator_hash": _content_hash({
            "method": estimator.get("method"), "inputs": estimator.get("inputs"),
            "expected_goals": estimator.get("expected_goals"), "confidence": estimator.get("confidence"),
        }),
        "model_hash": model.get("model_hash"),
        "chain_audit_hash": _content_hash(chain_audit),
        "odds_independent": odds_independent,
        "fundamental_decision_eligible": chain_audit.get("decision_eligible") is True,
    }
    eligible = bool(script_hash and odds_independent and normalized["fundamental_decision_eligible"])
    return {
        **normalized,
        "status": "ready" if eligible else "diagnostic_only",
        "market_decision_authorized": eligible,
        "freeze_hash": _content_hash(normalized),
        "policy": "this immutable fundamental stage must complete before market comparison; ineligible fundamentals allow diagnostic market output only",
    }


def audit_fundamental_first_pipeline(fundamental_freeze: Dict[str, Any], script: Dict[str, Any]) -> Dict[str, Any]:
    post_market_script_hash = str(script.get("content_hash") or "").strip() or None
    freeze_hash_payload = {
        key: fundamental_freeze.get(key) for key in (
            "schema", "fixture", "frozen_at", "script_hash", "recomputed_script_hash",
            "script_hash_valid", "estimator_hash", "model_hash", "chain_audit_hash",
            "odds_independent", "fundamental_decision_eligible",
        )
    }
    freeze_hash_valid = bool(
        fundamental_freeze.get("freeze_hash")
        and _content_hash(freeze_hash_payload) == fundamental_freeze.get("freeze_hash")
    )
    freeze_valid = (
        fundamental_freeze.get("schema") == "fundamental_first_freeze_v1"
        and freeze_hash_valid
        and fundamental_freeze.get("script_hash_valid") is True
        and fundamental_freeze.get("odds_independent") is True
    )
    script_preserved = bool(post_market_script_hash and post_market_script_hash == fundamental_freeze.get("script_hash"))
    return {
        "schema": "fundamental_first_pipeline_audit_v1",
        "status": "passed" if freeze_valid and script_preserved else "failed",
        "decision_eligible": freeze_valid and script_preserved,
        "required_order": [
            "fundamental_estimator", "independent_probability_model", "pure_fundamental_script",
            "fundamental_chain_audit", "fundamental_freeze", "market_snapshot",
            "market_candidate_generation", "market_language", "expression_optimizer",
        ],
        "observed_order": [
            "fundamental_estimator", "independent_probability_model", "pure_fundamental_script",
            "fundamental_chain_audit", "fundamental_freeze", "market_snapshot",
            "market_candidate_generation", "market_language", "expression_optimizer",
        ],
        "fundamental_stage_completed_before_market": True,
        "fundamental_freeze_hash": fundamental_freeze.get("freeze_hash"),
        "fundamental_freeze_hash_valid": freeze_hash_valid,
        "pre_market_script_hash": fundamental_freeze.get("script_hash"),
        "post_market_script_hash": post_market_script_hash,
        "market_processing_mutated_fundamentals": not script_preserved,
        "market_evaluation_mode": "decision_eligible" if fundamental_freeze.get("market_decision_authorized") is True else "diagnostic_only",
        "policy": "market processing may confirm, resist, or optimize an expression but cannot create or rewrite the frozen fundamental script",
    }


def build_decision_summary(decision: Dict[str, Any], chain_audit: Dict[str, Any], model: Dict[str, Any]) -> Dict[str, Any]:
    best = decision.get("best_market") or {}
    execution_action = decision.get("execution_action") or ("PASS" if decision.get("decision") == "PASS" else "BET")
    passed = execution_action == "PASS"
    return {
        "decision": decision.get("decision"), "execution_action": execution_action,
        "status": "pass" if passed else ("wait" if execution_action == "WAIT" else "actionable"),
        "recommended_market": None if passed else best.get("market"),
        "recommended_selection": None if passed else best.get("selection"),
        "line": None if passed else best.get("line"),
        "price": None if passed else best.get("price"),
        "edge": None if passed else best.get("edge"),
        "ev": None if passed else best.get("ev"),
        "script_coverage": None if passed else best.get("script_coverage"),
        "model_status": model.get("status"),
        "fundamental_chain_status": chain_audit.get("status"),
        "fundamental_first_pipeline_status": get_nested(decision, ["fundamental_first_pipeline_audit", "status"]),
        "market_evaluation_mode": get_nested(decision, ["fundamental_first_pipeline_audit", "market_evaluation_mode"]),
        "market_move_classification": decision.get("market_move_classification"),
        "market_acceptance": get_nested(decision, ["market_language", "selected_acceptance"]),
        "repricing_attribution": get_nested(decision, ["market_language", "selected_repricing_attribution"]),
        "pass_reasons": decision.get("pass_reasons") or [],
        "explanation": "No bet: one or more mandatory gates failed." if passed else ("Wait: model direction is retained but the market has not accepted this expression." if execution_action == "WAIT" else "Selection passed model, price, script, market-language and risk gates."),
    }


def audit_decision_output(decision: Dict[str, Any]) -> Dict[str, Any]:
    required_fields = (
        "decision", "model_probability", "market_no_vig_probability", "edge", "ev",
        "script_coverage", "crowding", "line_movement", "lineup_confidence", "death_path",
        "pass_reasons", "best_market",
    )
    missing_fields = [field for field in required_fields if field not in decision]
    decision_name = decision.get("decision")
    execution_action = decision.get("execution_action") or ("PASS" if decision_name == "PASS" else "BET")
    pass_mode = execution_action == "PASS" or decision_name == "PASS"
    consistency_issues = []
    if "fundamental_freeze" in decision or "fundamental_first_pipeline_audit" in decision:
        freeze = decision.get("fundamental_freeze") if isinstance(decision.get("fundamental_freeze"), dict) else {}
        pipeline = decision.get("fundamental_first_pipeline_audit") if isinstance(decision.get("fundamental_first_pipeline_audit"), dict) else {}
        if freeze.get("schema") != "fundamental_first_freeze_v1" or not freeze.get("freeze_hash"):
            consistency_issues.append("fundamental_first_freeze_v1_required")
        if pipeline.get("schema") != "fundamental_first_pipeline_audit_v1" or pipeline.get("status") != "passed":
            consistency_issues.append("fundamental_first_pipeline_audit_must_pass")
        if pipeline.get("fundamental_stage_completed_before_market") is not True:
            consistency_issues.append("fundamental_stage_must_complete_before_market")
        if pipeline.get("market_processing_mutated_fundamentals") is not False:
            consistency_issues.append("market_processing_must_not_mutate_fundamentals")
        if execution_action in ("BET", "WAIT") and freeze.get("market_decision_authorized") is not True:
            consistency_issues.append("actionable_market_decision_requires_eligible_fundamental_freeze")
    if "line_movement_audit" in decision:
        if not isinstance(decision.get("market_language"), dict):
            consistency_issues.append("terminal_decision_requires_market_language")
        if not isinstance(decision.get("expression_optimizer"), dict):
            consistency_issues.append("terminal_decision_requires_expression_optimizer")
        axes = get_nested(decision, ["market_language", "axes"])
        if not isinstance(axes, dict) or any(axis not in axes for axis in ("Home", "Away", "Over", "Under")):
            consistency_issues.append("terminal_market_language_requires_four_axes")
        any_real_axis = isinstance(axes, dict) and any(get_nested(axes, [axis, "capital_pressure", "is_real_money"]) is True for axis in ("Home", "Away", "Over", "Under"))
        any_real_candidate = any(get_nested(row, ["market_language", "capital_pressure", "is_real_money"]) is True for row in (decision.get("candidates") or []) if isinstance(row, dict))
        any_real_pressure = any_real_axis or any_real_candidate
        proxy_only_flag = get_nested(decision, ["market_language", "capital_pressure_is_proxy_only"])
        if proxy_only_flag != (not any_real_pressure):
            consistency_issues.append("capital_pressure_proxy_flag_mismatch")
        if any_real_pressure and (
            get_nested(decision, ["market_language", "real_money_data", "decision_eligible"]) is not True
            or not get_nested(decision, ["market_language", "real_money_data", "evidence_hash"])
        ):
            consistency_issues.append("real_money_axis_requires_audited_evidence_hash")
        if get_nested(decision, ["expression_optimizer", "automatic_direction_reversal"]) is not False:
            consistency_issues.append("expression_optimizer_must_forbid_automatic_direction_reversal")
        attribution_rows = [
            get_nested(row, ["market_language", "repricing_attribution"])
            for row in (decision.get("candidates") or []) if isinstance(row, dict)
        ] + [
            get_nested(axes or {}, [axis, "repricing_attribution"])
            for axis in ("Home", "Away", "Over", "Under")
        ]
        if any(not isinstance(row, dict) or row.get("schema") != "repricing_attribution_v1" for row in attribution_rows):
            consistency_issues.append("terminal_market_language_requires_repricing_attribution_v1")
        for attribution in [row for row in attribution_rows if isinstance(row, dict)]:
            if attribution.get("market_response_is_cause_evidence") is not False:
                consistency_issues.append("market_response_must_not_be_treated_as_cause_evidence")
            if attribution.get("real_money_pressure_proves_repricing_cause") is not False:
                consistency_issues.append("real_money_pressure_must_not_claim_repricing_causation")
            if attribution.get("source_attribution_verified") is True and not (
                attribution.get("status") == "fundamental_repricing_confirmed"
                and attribution.get("verified_fundamental_change_present") is True
                and attribution.get("market_revalidation_triggered") is True
            ):
                consistency_issues.append("repricing_source_verification_requires_fundamental_change_and_market_trigger")
        if execution_action in ("BET", "WAIT") and not isinstance(get_nested(decision, ["market_language", "selected_repricing_attribution"]), dict):
            consistency_issues.append("selected_expression_requires_repricing_attribution")
        optimizer_action = get_nested(decision, ["expression_optimizer", "action"])
        if optimizer_action != execution_action:
            consistency_issues.append("expression_optimizer_action_mismatch")
    if pass_mode:
        if decision.get("best_market") is not None:
            consistency_issues.append("pass_must_not_have_best_market")
        if not isinstance(decision.get("pass_reasons"), list) or not decision.get("pass_reasons"):
            consistency_issues.append("pass_requires_at_least_one_reason")
    else:
        best = decision.get("best_market") if isinstance(decision.get("best_market"), dict) else {}
        if not best:
            consistency_issues.append("actionable_decision_requires_best_market")
        for field in ("market", "selection", "price", "model_probability", "market_no_vig_probability", "edge", "ev", "script_coverage"):
            if best.get(field) is None:
                consistency_issues.append(f"actionable_best_market_missing_{field}")
        if decision.get("pass_reasons"):
            consistency_issues.append("actionable_decision_must_not_have_pass_reasons")
        if execution_action not in ("BET", "WAIT", "PENDING_MARKET_LANGUAGE"):
            consistency_issues.append("actionable_execution_action_must_be_bet_or_wait")
        if execution_action == "BET" and isinstance(decision.get("market_language"), dict) and decision["market_language"].get("status") == "available" and get_nested(decision, ["market_language", "selected_acceptance"]) not in ("Accepted", "Partial"):
            consistency_issues.append("bet_requires_accepted_or_partial_market_expression")
    eligible = not missing_fields and not consistency_issues
    return {
        "status": "complete" if eligible else "incomplete",
        "decision_eligible": eligible,
        "mode": "pass" if pass_mode else "actionable",
        "required_fields": list(required_fields),
        "missing_fields": missing_fields,
        "consistency_issues": consistency_issues,
        "policy": "incomplete or inconsistent final output must be downgraded to PASS",
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
    evaluation_cutoff = int(time.time())
    script = _fundamental_evaluation_script(effective_payload, estimator, model)
    chain_audit = audit_fundamental_chain(script, now_ts=evaluation_cutoff)
    fundamental_freeze = freeze_fundamental_stage(
        fixture, script, chain_audit, estimator, model, evaluation_cutoff
    )
    history = get_fixture_snapshots(fixture)
    available = [row for row in history if row.get("import_status") == "available"]
    latest = latest_prematch_snapshot(available)
    market = (latest or {}).get("market_snapshot") or empty_market_snapshot()
    freshness = imported_fixture_freshness(metadata, history)
    match_metadata = metadata.get("match") if isinstance(metadata.get("match"), dict) else {}
    kickoff_at = match_metadata.get("kickoff_utc") or match_metadata.get("date") or match_metadata.get("kickoff_at")
    real_money_audit = audit_real_money_data(
        payload.get("real_money_data"), expected_fixture=fixture,
        data_cutoff_at=evaluation_cutoff, kickoff_at=kickoff_at, now_ts=evaluation_cutoff,
    )
    decision = decision_layer(
        market, model.get("probabilities"), payload.get("script_coverage"),
        as_float(payload.get("crowding")), lineup_audit.get("effective_confidence"), payload.get("death_path") if "death_path" in payload else None,
    )
    decision["lineup_confidence_audit"] = lineup_audit
    decision["fundamental_chain_audit"] = chain_audit
    decision["fundamental_freeze"] = fundamental_freeze
    if not fundamental_freeze["market_decision_authorized"]:
        force_pass_decision(decision, "fundamental_chain_insufficient")
    decision = apply_line_movement_gate(decision, history, real_money_audit)
    pipeline_order_audit = audit_fundamental_first_pipeline(fundamental_freeze, script)
    decision["fundamental_first_pipeline_audit"] = pipeline_order_audit
    if not pipeline_order_audit["decision_eligible"]:
        force_pass_decision(decision, "fundamental_first_pipeline_failed")
    decision["data_freshness"] = freshness
    if payload.get("real_money_data") not in (None, {}) and real_money_audit.get("status") == "rejected":
        decision["pass_reasons"].append("real_money_data_rejected")
    if model.get("status") != "ready":
        decision["pass_reasons"].append("model_input_confidence_below_0_6")
    if not freshness.get("decision_eligible"):
        decision["pass_reasons"].append(freshness.get("reason") or "data_not_fresh")
    decision["pass_reasons"] = list(dict.fromkeys(decision["pass_reasons"]))
    if decision["pass_reasons"]:
        force_pass_decision(decision, [])

    model_market_divergence = detect_model_market_divergence(
        decision,
        model_ready=model.get("status") == "ready",
        fundamental_eligible=chain_audit.get("decision_eligible") is True,
        market_data_eligible=bool(latest) and freshness.get("decision_eligible") is True,
    )
    decision["model_market_divergence"] = model_market_divergence
    versions = get_fundamental_versions(fixture)
    previous = versions[-1] if versions else None
    move_classification = classify_market_move_details(
        decision.get("line_movement") or {}, previous, script, model_market_divergence["triggered"]
    )
    decision = attach_repricing_attribution(decision, move_classification)
    output_audit = audit_decision_output(decision)
    if not output_audit["decision_eligible"]:
        force_pass_decision(decision, "final_output_contract_incomplete")
    decision["output_contract_audit"] = output_audit
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
        "fundamental_chain_audit": chain_audit, "fundamental_freeze": fundamental_freeze,
        "fundamental_first_pipeline_audit": pipeline_order_audit, "fundamental_version": version_record,
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


def _portfolio_prematch_priority(row: Dict[str, Any], tier: str) -> Dict[str, Any]:
    """Build an outcome-blind, fundamentals-first cross-match ordering vector."""
    evaluation = row.get("evaluation") if isinstance(row.get("evaluation"), dict) else {}
    decision = evaluation.get("decision_layer") if isinstance(evaluation.get("decision_layer"), dict) else {}
    tiers = decision.get("recommendation_tiers") if isinstance(decision.get("recommendation_tiers"), dict) else {}
    candidate = tiers.get(tier) if isinstance(tiers.get(tier), dict) else {}
    fundamental = evaluation.get("fundamental_chain_audit") if isinstance(evaluation.get("fundamental_chain_audit"), dict) else {}
    fundamental_eligible = fundamental.get("decision_eligible") is True
    fundamental_completeness = as_float(fundamental.get("completeness_score"))
    lineup_confidence = as_float(decision.get("lineup_confidence"))
    script_coverage = as_float(candidate.get("script_coverage"))
    acceptance = get_nested(candidate, ["market_language", "market_acceptance"])
    if acceptance is None:
        acceptance = get_nested(decision, ["market_language", "selected_acceptance"])
    acceptance_rank = {
        "Accepted": 4, "Partial": 3, "data_missing": 2,
        "Resistance": 1, "Rejected": 0,
    }.get(str(acceptance or "data_missing"), 2)
    edge = as_float(candidate.get("edge"))
    ev = as_float(candidate.get("ev"))
    # Lexicographic ordering intentionally avoids inventing postmatch-fitted weights.
    # Fundamental eligibility/completeness and lineup certainty always precede
    # script/market/value fields. No result, settlement, score or hit-rate field
    # is read by this function.
    sort_key = (
        -int(fundamental_eligible),
        -(fundamental_completeness if fundamental_completeness is not None else -1.0),
        -(lineup_confidence if lineup_confidence is not None else -1.0),
        -(script_coverage if script_coverage is not None else -1.0),
        -acceptance_rank,
        -(edge if edge is not None else -999.0),
        -(ev if ev is not None else -999.0),
        str(row.get("fixture") or ""),
    )
    return {
        "schema": "prematch_priority_vector_v1",
        "match_priority": {
            "fundamental_decision_eligible": fundamental_eligible,
            "fundamental_completeness": fundamental_completeness,
            "lineup_confidence": lineup_confidence,
        },
        "expression_priority": {
            "script_coverage": script_coverage,
            "market_acceptance": acceptance or "data_missing",
            "market_acceptance_rank": acceptance_rank,
            "edge": edge,
            "ev": ev,
        },
        "fundamental_decision_eligible": fundamental_eligible,
        "fundamental_completeness": fundamental_completeness,
        "lineup_confidence": lineup_confidence,
        "script_coverage": script_coverage,
        "market_acceptance": acceptance or "data_missing",
        "market_acceptance_rank": acceptance_rank,
        "edge": edge,
        "ev": ev,
        "comparison_order": [
            "fundamental_decision_eligible", "fundamental_completeness",
            "lineup_confidence", "script_coverage", "market_acceptance",
            "edge", "ev", "fixture_stable_tiebreak",
        ],
        "outcome_fields_used": False,
        "result_backfit_allowed": False,
        "sort_key": sort_key,
    }


def _combination_from_rows(rows: List[Dict[str, Any]], tier: str, max_legs: int, allow_fallback: bool = False, risk_preference: str = "balanced") -> Dict[str, Any]:
    candidates = []
    excluded = []
    seen_groups = set()
    upgraded = False
    ordered = sorted(rows, key=lambda row: _portfolio_prematch_priority(row, tier)["sort_key"])
    for row in ordered:
        execution_action = get_nested(row, ["evaluation", "decision_layer", "execution_action"])
        if execution_action is None:
            execution_action = "BET"
        if execution_action != "BET":
            excluded.append({"fixture": row.get("fixture"), "reason": "execution_action_not_bet", "execution_action": execution_action or "data_missing", "requested_tier": tier})
            continue
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
        priority_tier = tier if source_tier == tier else "first_choice_high_consistency"
        priority_basis = _portfolio_prematch_priority(row, priority_tier)
        priority_basis.pop("sort_key", None)
        enriched["priority_basis"] = priority_basis
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
            "priority_basis": candidate.get("priority_basis"),
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
        "priority_policy": {
            "schema": "prematch_priority_vector_v1",
            "order": ["fundamental eligibility", "fundamental completeness", "lineup confidence", "script coverage", "market acceptance", "edge", "EV"],
            "outcome_fields_used": False,
            "result_backfit_allowed": False,
            "rule": "cross-match order is frozen from prematch evidence; settlement never reorders the board",
        },
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


def _portfolio_priority_evidence(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Bind a priority board to the exact prematch evidence available at evaluation time."""
    evidence = []
    for row in rows:
        evaluation = row.get("evaluation") if isinstance(row.get("evaluation"), dict) else {}
        decision = evaluation.get("decision_layer") if isinstance(evaluation.get("decision_layer"), dict) else {}
        market_binding = decision.get("market_evidence_binding") if isinstance(decision.get("market_evidence_binding"), dict) else {}
        freeze = evaluation.get("fundamental_freeze") if isinstance(evaluation.get("fundamental_freeze"), dict) else {}
        evidence.append({
            "fixture": row.get("fixture"),
            "fundamental_freeze_hash": freeze.get("freeze_hash"),
            "fundamental_script_hash": freeze.get("script_hash"),
            "fundamental_version": get_nested(evaluation, ["fundamental_version", "version_number"]),
            "probability_replay_hash": decision.get("probability_replay_hash"),
            "market_stage": market_binding.get("stage"),
            "market_snapshot_at": market_binding.get("snapshot_at"),
            "market_source_content_hash": market_binding.get("source_content_hash"),
            "current_unpersisted_quote_used": market_binding.get("current_unpersisted_quote_used") is True,
        })
    return evidence


def build_portfolio_priority_board(
    portfolio: Dict[str, Any], evidence_bindings: Optional[List[Dict[str, Any]]],
    stage: str, created_at: int,
) -> Dict[str, Any]:
    """Create a result-blind immutable board with match and expression ranks separated."""
    tiers = {
        "first_choice": get_nested(portfolio, ["first_choice_combination", "priority_ranking"], []) or [],
        "second_choice": get_nested(portfolio, ["second_choice_combination", "priority_ranking"], []) or [],
    }
    candidates_by_fixture: Dict[str, Dict[str, Any]] = {}
    for tier_name in ("first_choice", "second_choice"):
        for row in tiers[tier_name]:
            fixture = str(row.get("fixture") or "").strip()
            if not fixture or fixture in candidates_by_fixture:
                continue
            basis = row.get("priority_basis") if isinstance(row.get("priority_basis"), dict) else {}
            match_priority = basis.get("match_priority") if isinstance(basis.get("match_priority"), dict) else {
                key: basis.get(key) for key in ("fundamental_decision_eligible", "fundamental_completeness", "lineup_confidence")
            }
            candidates_by_fixture[fixture] = {
                "fixture": fixture, "match_label": row.get("match_label"),
                "match_priority": match_priority,
            }

    def match_sort_key(row: Dict[str, Any]) -> tuple:
        priority = row.get("match_priority") or {}
        eligible = priority.get("fundamental_decision_eligible") is True
        completeness = as_float(priority.get("fundamental_completeness"))
        confidence = as_float(priority.get("lineup_confidence"))
        return (
            -int(eligible),
            -(completeness if completeness is not None else -1.0),
            -(confidence if confidence is not None else -1.0),
            row.get("fixture") or "",
        )

    match_ranking = []
    for rank, row in enumerate(sorted(candidates_by_fixture.values(), key=match_sort_key), start=1):
        match_ranking.append({"rank": rank, **row})
    expression_rankings = {}
    for tier_name, rows in tiers.items():
        expression_rankings[tier_name] = [
            {
                "rank": index,
                **{key: row.get(key) for key in (
                    "fixture", "match_label", "market", "selection", "selection_label",
                    "line", "price", "role",
                )},
                "expression_priority": get_nested(row, ["priority_basis", "expression_priority"], {
                    key: get_nested(row, ["priority_basis", key]) for key in (
                        "script_coverage", "market_acceptance", "market_acceptance_rank", "edge", "ev",
                    )
                }),
            }
            for index, row in enumerate(rows, start=1)
        ]
    immutable_content = {
        "schema": "prematch_priority_board_v1",
        "created_at": int(created_at),
        "stage": stage,
        "match_priority_ranking": match_ranking,
        "expression_priority_rankings": expression_rankings,
        "evidence_bindings": evidence_bindings or [],
        "comparison_policy": {
            "match_order": ["fundamental eligibility", "fundamental completeness", "lineup confidence"],
            "expression_order": ["script coverage", "market acceptance", "edge", "EV"],
            "match_priority_precedes_expression_priority": True,
        },
        "outcome_fields_used": False,
        "result_backfit_allowed": False,
    }
    return {**immutable_content, "board_hash": _content_hash(immutable_content), "immutable": True}


def _priority_match_sort_key(fixture: str, priority: Dict[str, Any]) -> tuple:
    eligible = priority.get("fundamental_decision_eligible") is True
    completeness = as_float(priority.get("fundamental_completeness"))
    confidence = as_float(priority.get("lineup_confidence"))
    return (
        -int(eligible),
        -(completeness if completeness is not None else -1.0),
        -(confidence if confidence is not None else -1.0),
        str(fixture or ""),
    )


def _priority_combined_sort_key(fixture: str, match_priority: Dict[str, Any], expression_priority: Dict[str, Any]) -> tuple:
    acceptance_rank = as_float(expression_priority.get("market_acceptance_rank"))
    coverage = as_float(expression_priority.get("script_coverage"))
    edge = as_float(expression_priority.get("edge"))
    ev = as_float(expression_priority.get("ev"))
    return (
        *_priority_match_sort_key(fixture, match_priority)[:-1],
        -(coverage if coverage is not None else -1.0),
        -(acceptance_rank if acceptance_rank is not None else 2.0),
        -(edge if edge is not None else -999.0),
        -(ev if ev is not None else -999.0),
        str(fixture or ""),
    )


def audit_portfolio_priority_board(board: Any) -> Dict[str, Any]:
    if not isinstance(board, dict):
        return {"status": "failed", "hash_valid": False, "issues": ["priority_board_missing"]}
    expected = str(board.get("board_hash") or "")
    immutable_content = {key: value for key, value in board.items() if key not in {"board_hash", "immutable"}}
    issues = []
    hash_valid = bool(expected and hmac.compare_digest(expected, _content_hash(immutable_content)))
    if board.get("schema") != "prematch_priority_board_v1":
        issues.append("priority_board_schema_invalid")
    if not hash_valid:
        issues.append("priority_board_hash_invalid")
    if board.get("outcome_fields_used") is not False or board.get("result_backfit_allowed") is not False:
        issues.append("priority_board_outcome_isolation_invalid")
    match_rows = board.get("match_priority_ranking")
    expression_groups = board.get("expression_priority_rankings")
    if not isinstance(match_rows, list):
        issues.append("priority_board_match_ranking_invalid")
        match_rows = []
    if not isinstance(expression_groups, dict):
        issues.append("priority_board_expression_rankings_invalid")
        expression_groups = {}
    match_fixtures = [str(row.get("fixture") or "") for row in match_rows if isinstance(row, dict)]
    if [row.get("rank") for row in match_rows if isinstance(row, dict)] != list(range(1, len(match_rows) + 1)):
        issues.append("priority_board_match_ranks_not_contiguous")
    if any(not fixture for fixture in match_fixtures) or len(match_fixtures) != len(set(match_fixtures)):
        issues.append("priority_board_match_fixtures_not_unique")
    match_priority = {
        str(row.get("fixture") or ""): row.get("match_priority")
        for row in match_rows if isinstance(row, dict) and isinstance(row.get("match_priority"), dict)
    }
    expected_match_order = sorted(
        match_fixtures,
        key=lambda fixture: _priority_match_sort_key(fixture, match_priority.get(fixture) or {}),
    )
    if match_fixtures != expected_match_order:
        issues.append("priority_board_match_order_invalid")
    for tier, rows in expression_groups.items():
        if not isinstance(rows, list):
            issues.append(f"priority_board_{tier}_ranking_invalid")
            continue
        fixtures = [str(row.get("fixture") or "") for row in rows if isinstance(row, dict)]
        if [row.get("rank") for row in rows if isinstance(row, dict)] != list(range(1, len(rows) + 1)):
            issues.append(f"priority_board_{tier}_ranks_not_contiguous")
        if any(not fixture for fixture in fixtures) or len(fixtures) != len(set(fixtures)):
            issues.append(f"priority_board_{tier}_fixtures_not_unique")
        if any(fixture not in set(match_fixtures) for fixture in fixtures):
            issues.append(f"priority_board_{tier}_fixture_missing_match_rank")
        expected_fixtures = sorted(
            fixtures,
            key=lambda fixture: _priority_combined_sort_key(
                fixture, match_priority.get(fixture) or {},
                next((row.get("expression_priority") or {} for row in rows if isinstance(row, dict) and str(row.get("fixture") or "") == fixture), {}),
            ),
        )
        if fixtures != expected_fixtures:
            issues.append(f"priority_board_{tier}_order_invalid")
    if any(row.get("current_unpersisted_quote_used") is True for row in (board.get("evidence_bindings") or []) if isinstance(row, dict)):
        issues.append("priority_board_unpersisted_quote_binding_forbidden")
    return {"status": "passed" if not issues else "failed", "hash_valid": hash_valid, "issues": issues}


def _learning_priority_candidate(decision: Dict[str, Any]) -> Dict[str, Any]:
    tiers = decision.get("recommendation_tiers") if isinstance(decision.get("recommendation_tiers"), dict) else {}
    for candidate in (
        decision.get("selected_expression"), decision.get("best_market"),
        tiers.get("first_choice_high_consistency"), tiers.get("second_choice_higher_return"),
    ):
        if isinstance(candidate, dict):
            return candidate
    return {}


def _learning_freeze_priority_row(freeze: Dict[str, Any]) -> Dict[str, Any]:
    fixture = str(freeze.get("fixture") or "")
    analysis = freeze.get("analysis") if isinstance(freeze.get("analysis"), dict) else {}
    decision = freeze.get("decision") if isinstance(freeze.get("decision"), dict) else {}
    candidate = _learning_priority_candidate(decision)
    chain_audit = decision.get("fundamental_chain_audit") if isinstance(decision.get("fundamental_chain_audit"), dict) else {}
    if not chain_audit:
        fundamental = analysis.get("fundamental_chain") if isinstance(analysis.get("fundamental_chain"), dict) else {}
        chain_audit = audit_fundamental_chain(fundamental, now_ts=int(freeze.get("data_cutoff_at") or freeze.get("captured_at") or time.time()))
    synthetic = {
        "fixture": fixture,
        "evaluation": {
            "fundamental_chain_audit": chain_audit,
            "decision_layer": {
                **decision,
                "recommendation_tiers": {"first_choice_high_consistency": candidate},
            },
        },
    }
    vector = _portfolio_prematch_priority(synthetic, "first_choice_high_consistency")
    sort_key = vector.pop("sort_key")
    execution_action = decision.get("execution_action") or decision.get("decision")
    return {
        "fixture": fixture,
        "freeze_id": freeze.get("freeze_id"),
        "freeze_hash": freeze.get("content_hash"),
        "captured_at": freeze.get("captured_at"),
        "kickoff_at": freeze.get("kickoff_at"),
        "analysis_node": analysis.get("analysis_node"),
        "match_rating": _learning_frozen_rating(decision, "match_rating"),
        "execution_action": execution_action,
        "participation_eligible": execution_action == "BET" and bool(candidate),
        "market": candidate.get("market"), "selection": candidate.get("selection"),
        "line": candidate.get("line"), "price": candidate.get("price"),
        "priority_basis": vector,
        "sort_key": sort_key,
    }


def build_learning_priority_board(board_id: str, freezes: List[Dict[str, Any]], created_at: int) -> Dict[str, Any]:
    """Freeze one cross-match board from PIT learning samples; realised outcomes are unavailable by contract."""
    latest_by_fixture: Dict[str, Dict[str, Any]] = {}
    for freeze in freezes:
        if not isinstance(freeze, dict):
            continue
        fixture = str(freeze.get("fixture") or "").strip()
        captured_at = int(freeze.get("captured_at") or freeze.get("data_cutoff_at") or 0)
        kickoff_at = int(freeze.get("kickoff_at") or 0)
        if not fixture or captured_at > int(created_at) or kickoff_at <= int(created_at):
            continue
        existing = latest_by_fixture.get(fixture)
        if not existing or captured_at > int(existing.get("captured_at") or existing.get("data_cutoff_at") or 0):
            latest_by_fixture[fixture] = freeze
    priority_rows = [_learning_freeze_priority_row(freeze) for freeze in latest_by_fixture.values()]
    participating_rows = [row for row in priority_rows if row.get("participation_eligible") is True]
    non_participating_rows = [row for row in priority_rows if row.get("participation_eligible") is not True]

    def match_key(row: Dict[str, Any]) -> tuple:
        match = get_nested(row, ["priority_basis", "match_priority"], {}) or {}
        eligible = match.get("fundamental_decision_eligible") is True
        completeness = as_float(match.get("fundamental_completeness"))
        confidence = as_float(match.get("lineup_confidence"))
        return (-int(eligible), -(completeness if completeness is not None else -1.0), -(confidence if confidence is not None else -1.0), row["fixture"])

    match_rows = sorted(participating_rows, key=match_key)
    expression_rows = sorted(participating_rows, key=lambda row: row["sort_key"])
    match_ranking = [
        {
            "rank": index, "role": "core_top_three" if index <= 3 else "optional_extension",
            **{key: row.get(key) for key in ("fixture", "freeze_id", "freeze_hash", "captured_at", "kickoff_at", "analysis_node", "match_rating", "execution_action")},
            "match_priority": get_nested(row, ["priority_basis", "match_priority"], {}),
        }
        for index, row in enumerate(match_rows, start=1)
    ]
    expression_ranking = [
        {
            "rank": index,
            **{key: row.get(key) for key in ("fixture", "freeze_id", "freeze_hash", "market", "selection", "line", "price")},
            "expression_priority": get_nested(row, ["priority_basis", "expression_priority"], {}),
        }
        for index, row in enumerate(expression_rows, start=1)
    ]
    non_participating = [
        {
            **{key: row.get(key) for key in ("fixture", "freeze_id", "freeze_hash", "captured_at", "kickoff_at", "analysis_node", "match_rating", "execution_action")},
            "reason": "execution_action_not_bet" if row.get("execution_action") != "BET" else "actionable_expression_missing",
        }
        for row in sorted(non_participating_rows, key=lambda row: row.get("fixture") or "")
    ]
    immutable_content = {
        "schema": "learning_prematch_priority_board_v1",
        "board_id": board_id,
        "created_at": int(created_at),
        "match_priority_ranking": match_ranking,
        "expression_priority_ranking": expression_ranking,
        "non_participating_records": non_participating,
        "comparison_policy": {
            "match_order": ["fundamental eligibility", "fundamental completeness", "lineup confidence"],
            "expression_order": ["script coverage", "market acceptance", "edge", "EV"],
            "core_priority_count": min(3, len(match_ranking)),
        },
        "outcome_fields_used": False,
        "result_backfit_allowed": False,
    }
    return {**immutable_content, "board_hash": _content_hash(immutable_content), "immutable": True}


def audit_learning_priority_board(board: Any) -> Dict[str, Any]:
    if not isinstance(board, dict):
        return {"status": "failed", "hash_valid": False, "issues": ["learning_priority_board_missing"]}
    expected = str(board.get("board_hash") or "")
    content = {key: value for key, value in board.items() if key not in {"board_hash", "immutable", "action"}}
    issues = []
    hash_valid = bool(expected and hmac.compare_digest(expected, _content_hash(content)))
    if board.get("schema") != "learning_prematch_priority_board_v1":
        issues.append("learning_priority_board_schema_invalid")
    if not hash_valid:
        issues.append("learning_priority_board_hash_invalid")
    if board.get("outcome_fields_used") is not False or board.get("result_backfit_allowed") is not False:
        issues.append("learning_priority_board_outcome_isolation_invalid")
    board_id = str(board.get("board_id") or "")
    if not board_id or len(board_id) > 100 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in board_id):
        issues.append("learning_priority_board_id_invalid")
    match_rows = board.get("match_priority_ranking")
    expression_rows = board.get("expression_priority_ranking")
    non_participating = board.get("non_participating_records")
    if not isinstance(match_rows, list):
        issues.append("learning_priority_match_ranking_invalid")
        match_rows = []
    if not isinstance(expression_rows, list):
        issues.append("learning_priority_expression_ranking_invalid")
        expression_rows = []
    if not isinstance(non_participating, list):
        issues.append("learning_priority_non_participating_invalid")
        non_participating = []
    if [row.get("rank") for row in match_rows if isinstance(row, dict)] != list(range(1, len(match_rows) + 1)):
        issues.append("learning_priority_match_ranks_not_contiguous")
    if [row.get("rank") for row in expression_rows if isinstance(row, dict)] != list(range(1, len(expression_rows) + 1)):
        issues.append("learning_priority_expression_ranks_not_contiguous")
    match_keys = [
        (str(row.get("fixture") or ""), str(row.get("freeze_id") or ""), str(row.get("freeze_hash") or ""))
        for row in match_rows if isinstance(row, dict)
    ]
    expression_keys = [
        (str(row.get("fixture") or ""), str(row.get("freeze_id") or ""), str(row.get("freeze_hash") or ""))
        for row in expression_rows if isinstance(row, dict)
    ]
    non_participating_keys = [
        (str(row.get("fixture") or ""), str(row.get("freeze_id") or ""), str(row.get("freeze_hash") or ""))
        for row in non_participating if isinstance(row, dict)
    ]
    if any(not all(key) for key in match_keys + expression_keys + non_participating_keys):
        issues.append("learning_priority_freeze_binding_incomplete")
    if len(match_keys) != len(set(match_keys)) or len(expression_keys) != len(set(expression_keys)) or len(non_participating_keys) != len(set(non_participating_keys)):
        issues.append("learning_priority_freeze_binding_not_unique")
    if set(match_keys) != set(expression_keys):
        issues.append("learning_priority_match_expression_membership_mismatch")
    if set(match_keys) & set(non_participating_keys):
        issues.append("learning_priority_participation_sets_overlap")
    all_fixture_ids = [key[0] for key in match_keys + non_participating_keys]
    if len(all_fixture_ids) != len(set(all_fixture_ids)):
        issues.append("learning_priority_fixture_not_unique_across_participation_sets")
    core_count = get_nested(board, ["comparison_policy", "core_priority_count"])
    if core_count != min(3, len(match_rows)):
        issues.append("learning_priority_core_count_invalid")
    for index, row in enumerate(match_rows, start=1):
        if not isinstance(row, dict):
            issues.append("learning_priority_match_row_invalid")
            continue
        expected_role = "core_top_three" if index <= 3 else "optional_extension"
        if row.get("role") != expected_role:
            issues.append("learning_priority_match_role_invalid")
        if row.get("execution_action") != "BET":
            issues.append("learning_priority_non_bet_ranked")
        captured_at = int(row.get("captured_at") or 0)
        kickoff_at = int(row.get("kickoff_at") or 0)
        if captured_at <= 0 or kickoff_at <= captured_at or int(board.get("created_at") or 0) >= kickoff_at:
            issues.append("learning_priority_prematch_timing_invalid")
    for row in non_participating:
        if not isinstance(row, dict):
            issues.append("learning_priority_non_participating_row_invalid")
            continue
        action = row.get("execution_action")
        reason = row.get("reason")
        if (action == "BET" and reason != "actionable_expression_missing") or (action != "BET" and reason != "execution_action_not_bet"):
            issues.append("learning_priority_non_participating_reason_invalid")
        captured_at = int(row.get("captured_at") or 0)
        kickoff_at = int(row.get("kickoff_at") or 0)
        if captured_at <= 0 or kickoff_at <= captured_at or int(board.get("created_at") or 0) >= kickoff_at:
            issues.append("learning_priority_prematch_timing_invalid")
    match_priority_by_key = {
        key: row.get("match_priority") or {}
        for key, row in zip(match_keys, match_rows) if isinstance(row, dict)
    }
    expression_priority_by_key = {
        key: row.get("expression_priority") or {}
        for key, row in zip(expression_keys, expression_rows) if isinstance(row, dict)
    }
    expected_match_keys = sorted(match_keys, key=lambda key: _priority_match_sort_key(key[0], match_priority_by_key.get(key) or {}))
    if match_keys != expected_match_keys:
        issues.append("learning_priority_match_order_invalid")
    expected_expression_keys = sorted(
        expression_keys,
        key=lambda key: _priority_combined_sort_key(
            key[0], match_priority_by_key.get(key) or {}, expression_priority_by_key.get(key) or {},
        ),
    )
    if expression_keys != expected_expression_keys:
        issues.append("learning_priority_expression_order_invalid")
    return {"status": "passed" if not issues else "failed", "hash_valid": hash_valid, "issues": list(dict.fromkeys(issues))}


def persist_learning_priority_board(board: Dict[str, Any]) -> Dict[str, Any]:
    audit = audit_learning_priority_board(board)
    if audit.get("status") != "passed":
        raise HTTPException(status_code=422, detail={"error": "learning_priority_board_invalid", "audit": audit})
    board_id = str(board.get("board_id") or "").strip()
    if not board_id:
        raise HTTPException(status_code=400, detail="learning_priority_board_id_required")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        boards = store.setdefault("learning_priority_boards", {})
        existing = boards.get(board_id)
        if existing:
            if existing.get("board_hash") == board.get("board_hash"):
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="learning_priority_board_id_already_recorded")
        boards[board_id] = board
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**board, "action": "frozen"}


def _learning_priority_context(store: Dict[str, Any], freeze: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    fixture = str(freeze.get("fixture") or "")
    freeze_id = str(freeze.get("freeze_id") or "")
    candidates = []
    for board in (store.get("learning_priority_boards") or {}).values():
        if audit_learning_priority_board(board).get("status") != "passed":
            continue
        row = next((item for item in board.get("match_priority_ranking") or [] if str(item.get("fixture") or "") == fixture and str(item.get("freeze_id") or "") == freeze_id), None)
        if row:
            candidates.append({"board": board, "row": row})
    if not candidates:
        return None
    selected = max(candidates, key=lambda item: int(item["board"].get("created_at") or 0))
    return {
        "board_id": selected["board"].get("board_id"),
        "board_hash": selected["board"].get("board_hash"),
        "rank": selected["row"].get("rank"),
        "role": selected["row"].get("role"),
        "core_priority_count": get_nested(selected, ["board", "comparison_policy", "core_priority_count"]),
    }


def learning_priority_board_report(board_id: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
    """Return bounded, hash-audited priority boards from the local immutable store."""
    try:
        parsed_limit = int(limit)
    except (TypeError, ValueError):
        parsed_limit = 0
    if isinstance(limit, bool) or not 1 <= parsed_limit <= 50:
        raise HTTPException(status_code=400, detail="limit_must_be_between_1_and_50")
    board_id = str(board_id or "").strip() or None
    if board_id and (len(board_id) > 100 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in board_id)):
        raise HTTPException(status_code=400, detail="priority_board_id_must_use_1_to_100_safe_characters")
    boards = load_snapshot_store().get("learning_priority_boards") or {}
    if board_id:
        board = boards.get(board_id)
        if not isinstance(board, dict):
            raise HTTPException(status_code=404, detail="learning_priority_board_not_found")
        return {
            "version": VERSION, "board_id": board_id, "board": board,
            "audit": audit_learning_priority_board(board),
            "outcome_fields_used": False, "result_backfit_allowed": False,
        }
    rows = sorted(
        (row for row in boards.values() if isinstance(row, dict)),
        key=lambda row: (int(row.get("created_at") or 0), str(row.get("board_id") or "")),
        reverse=True,
    )
    selected = rows[:parsed_limit]
    return {
        "version": VERSION, "count": len(rows), "returned_count": len(selected),
        "boards": [{**row, "audit": audit_learning_priority_board(row)} for row in selected],
        "truncated": len(rows) > len(selected),
        "outcome_fields_used": False, "result_backfit_allowed": False,
    }


def save_portfolio_run(portfolio_id: str, fixtures: List[str], portfolio: Dict[str, Any], max_legs: int, trigger_reasons: List[str], stage: str = "manual", change_drivers: Optional[List[Dict[str, Any]]] = None, priority_evidence: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        runs = store.setdefault("portfolio_runs", {}).setdefault(portfolio_id, [])
        signature = _portfolio_recommendation_signature(portfolio)
        previous_signature = (runs[-1].get("recommendation") if runs else None)
        transition = _portfolio_transition(previous_signature, signature)
        next_version = max((int(run.get("version_number") or 0) for run in runs), default=0) + 1
        created_at = int(time.time())
        priority_board = build_portfolio_priority_board(portfolio, priority_evidence, stage, created_at)
        priority_board_audit = audit_portfolio_priority_board(priority_board)
        if priority_board_audit.get("status") != "passed":
            raise HTTPException(status_code=422, detail={
                "error": "portfolio_priority_board_invalid",
                "audit": priority_board_audit,
            })
        record = {
            "version_number": next_version, "created_at": created_at, "portfolio_id": portfolio_id,
            "fixtures": fixtures, "risk_preference": portfolio.get("risk_preference"), "max_legs": max_legs,
            "stage": stage,
            "trigger_reasons": trigger_reasons or ["portfolio_evaluation"],
            "recommendation": signature,
            "recommendation_change": {"changed": previous_signature != signature, "before": previous_signature, "after": signature},
            "transition": transition,
            "change_drivers": change_drivers or [],
            "portfolio_decision": portfolio.get("portfolio_decision"),
            "priority_board": priority_board,
            "priority_board_audit": priority_board_audit,
        }
        runs.append(record)
        store["portfolio_runs"][portfolio_id] = runs[-PORTFOLIO_RUN_RETENTION:]
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def get_portfolio_runs(portfolio_id: str) -> List[Dict[str, Any]]:
    rows = copy.deepcopy(load_snapshot_store().get("portfolio_runs", {}).get(portfolio_id, []))
    for row in rows:
        row["priority_board_audit"] = audit_portfolio_priority_board(row.get("priority_board"))
    return rows


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
        execution_action = get_nested(row, ["evaluation", "decision_layer", "execution_action"])
        if execution_action is not None and execution_action != "BET":
            continue
        candidate = get_nested(row, ["evaluation", "decision_layer", "recommendation_tiers", "high_variance_single"])
        if candidate:
            priority = _portfolio_prematch_priority(row, "high_variance_single")
            sort_key = priority.pop("sort_key")
            high_variance.append({"fixture": row["fixture"], **candidate, "priority_basis": priority, "_sort_key": sort_key})
    high_variance.sort(key=lambda row: row["_sort_key"])
    for row in high_variance:
        row.pop("_sort_key", None)
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
    return {"service": "football-shadow-data-service", "version": VERSION, "main_endpoints": ["/shadow/target-fixtures", "/shadow/analyze-fixture", "/shadow/tracking-plan", "/shadow/snapshot", "/shadow/snapshots", "/shadow/ai-packet", "/shadow/import-prematch-packets", "/shadow/the-odds-api/collect-timeline", "/shadow/import-status", "/shadow/data-source-health", "/shadow/store-health", "/shadow/store-recovery-preview", "/shadow/store-recover", "/shadow/nami-odds-capabilities", "/shadow/model/poisson", "/shadow/model/fundamental-xg", "/shadow/model/prematch-evaluate", "/shadow/portfolio/evaluate", "/shadow/imported-prematch/{fixture}", "/shadow/market-language/freeze", "/shadow/market-language/collect-facts", "/shadow/market-language/settlement-plan", "/shadow/market-language/settle", "/shadow/market-language/status", "/shadow/learning/registry", "/shadow/learning/activation-plan", "/shadow/learning/node-plan", "/shadow/learning/node-run", "/shadow/learning/priority-boards", "/shadow/learning/data-health", "/shadow/learning/postmatch-recovery", "/shadow/learning/collect-facts", "/shadow/learning/cycle-plan", "/shadow/learning/run", "/shadow/learning/review-queue", "/shadow/learning/review-draft", "/shadow/learning/complete-review", "/shadow/learning/quality-cards/refresh", "/shadow/learning/quality-calibration", "/shadow/learning/research-proposals", "/shadow/learning/research-proposals/refresh", "/shadow/learning/validation-queue", "/shadow/learning/freeze", "/shadow/learning/settle", "/shadow/learning/hypotheses", "/shadow/learning/hypotheses/{id}/shadow-lock", "/shadow/learning/hypotheses/{id}/promotion-evidence", "/shadow/learning/league-dna", "/shadow/learning/league-dna/{tag_id}/activation-candidate", "/shadow/learning/league-dna/{tag_id}/confirm", "/shadow/learning/selection-quality", "/shadow/learning/status"]}


@app.get("/health")
def health():
    route = market_data_route_report()
    route_summary = {
        "status": route.get("status"), "decision_eligible": route.get("decision_eligible"),
        "decision_route": route.get("decision_route"), "collector_candidates": route.get("collector_candidates"),
        "fresh_fixture_count": get_nested(route, ["pang", "fresh_fixture_count"], 0),
        "missing_action": route.get("missing_action"),
    }
    return {"ok": True, "timestamp": int(time.time()), "version": VERSION, "api_football_base_url": API_FOOTBALL_BASE_URL, "thestats_base_url": THESTATS_BASE_URL, "the_odds_api_base_url": THE_ODDS_API_BASE_URL, "nami_base_url": NAMI_API_BASE_URL, "has_api_football_key": bool(API_FOOTBALL_KEY), "api_football_startup_probe": API_FOOTBALL_STARTUP_PROBE, "has_thestats_key": bool(THESTATS_API_KEY), "thestats_fixture_day_cache_ttl_seconds": THESTATS_FIXTURE_DAY_CACHE_TTL_SECONDS, "thestats_fixture_day_cache_entries": len(THESTATS_FIXTURE_DAY_CACHE), "has_the_odds_api_key": bool(THE_ODDS_API_KEY), "the_odds_api_watchlist": the_odds_api_watchlist_health(), "has_nami_credentials": bool(NAMI_API_USER and NAMI_API_SECRET), "nami_optional": True, "nami_failure_policy": "continue_without_nami", "nami_odds_startup_probe": NAMI_ODDS_STARTUP_PROBE, "nami_odds_probe_ttl_seconds": NAMI_ODDS_PROBE_TTL_SECONDS, "market_data_route": route_summary, "shadow_token_enabled": bool(SHADOW_ACCESS_TOKEN), "auto_fetch_date": AUTO_FETCH_DATE, "auto_fetch_fixture_id": AUTO_FETCH_FIXTURE_ID, "snapshot_store_path": SNAPSHOT_STORE_PATH, "snapshot_store_gzip": SNAPSHOT_STORE_GZIP, "snapshot_store_writer_workers": SNAPSHOT_STORE_WRITER_WORKERS, "snapshot_store_single_writer_required": True, "snapshot_store_warn_bytes": SNAPSHOT_STORE_WARN_BYTES, "snapshot_store_max_decompressed_bytes": SNAPSHOT_STORE_MAX_DECOMPRESSED_BYTES, "fundamental_version_retention": FUNDAMENTAL_VERSION_RETENTION, "portfolio_run_retention": PORTFOLIO_RUN_RETENTION, "external_data_stale_seconds": EXTERNAL_DATA_STALE_SECONDS, "tracking_stages": STAGE_ORDER, "target_leagues": {str(k): v for k, v in DEFAULT_TARGET_LEAGUES.items() if k in TARGET_LEAGUE_IDS}, "auto_provider_reconciliation": AUTO_RECONCILIATION_LAST_RESULT}


@app.get("/shadow/nami-capabilities")
def shadow_nami_capabilities(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(nami_capability_check())


@app.get("/shadow/nami-odds-capabilities")
def shadow_nami_odds_capabilities(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(nami_odds_capability_check())


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
    require_paid_odds_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(historical_odds_selfcheck())


@app.post("/shadow/the-odds-api/collect-timeline")
async def shadow_the_odds_api_collect_timeline(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_paid_odds_token(resolve_shadow_token(token, authorization, x_shadow_token))
    body = await request.body()
    if len(body) > 100 * 1024:
        raise HTTPException(status_code=413, detail="request_body_too_large")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid_json_body") from exc
    return JSONResponse(collect_the_odds_api_timeline(payload))

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
    # The historical events endpoint costs one credit and is enough to verify
    # paid historical entitlement without burning a market request.
    historical = call_the_odds_api("/historical/sports/soccer_epl/events", {
        "dateFormat": "iso", "date": "2024-01-01T12:00:00Z"
    })
    result["historical_status"] = historical.get("status_code")
    result["historical_access"] = historical.get("status_code") == 200
    result["ok"] = result["historical_access"]
    result["quota_remaining"] = historical.get("quota_remaining")
    result["quota_used"] = historical.get("quota_used")
    result["quota_last"] = historical.get("quota_last")
    return result


@app.get("/prematch/target-fixtures")
def prematch_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone")):
    return JSONResponse(target_fixtures_for_date(date, timezone_name, include_supplemental=True))


@app.get("/shadow/target-fixtures")
def shadow_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(target_fixtures_for_date(date, timezone_name, include_supplemental=True))


@app.get("/shadow/provider-reconciliation")
def shadow_provider_reconciliation(date: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(provider_reconciliation_report(date))


@app.post("/shadow/provider-reconciliation/apply")
def shadow_apply_provider_reconciliation(date: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(apply_provider_reconciliation(date))


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
    if normalized == "Opening":
        raise HTTPException(status_code=400, detail={"error": "verified_opening_source_required", "reason": "current_odds_cannot_be_saved_as_opening", "allowed_path": "/shadow/import-prematch-packets"})
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
    should_version = not versions or bool(trigger.get("triggered")) or normalized in ("T-1h", "T-30m", "Closing")
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
    request_bytes = len(body)
    if len(body) > 30 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="request_body_too_large")
    try:
        if request.headers.get("content-encoding", "").lower() == "gzip":
            try:
                body = bounded_gzip_decompress(body, MAX_IMPORT_DECOMPRESSED_BYTES)
            except ValueError:
                raise HTTPException(status_code=413, detail="decompressed_body_too_large")
        payload_sha256 = hashlib.sha256(body).hexdigest()
        supplied_payload_sha256 = str(request.headers.get("x-payload-sha256") or "").lower()
        if supplied_payload_sha256 and supplied_payload_sha256 != payload_sha256:
            raise HTTPException(status_code=422, detail="payload_sha256_mismatch")
        source_sha256 = str(request.headers.get("x-source-sha256") or "").lower()
        if source_sha256 and (len(source_sha256) != 64 or any(char not in "0123456789abcdef" for char in source_sha256)):
            raise HTTPException(status_code=400, detail="invalid_source_sha256")
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
    expected_date = request.headers.get("x-expected-match-date") or None
    require_prematch = str(request.headers.get("x-require-prematch") or "false").lower() in ("1", "true", "yes", "on")
    preflight = server_import_preflight(packets, expected_date=expected_date, require_prematch=require_prematch)
    if preflight["status"] != "ready":
        raise HTTPException(status_code=422, detail={"error": "import_preflight_rejected", "preflight": preflight})
    with SNAPSHOT_STORE_LOCK:
        results, store = import_prematch_packet_batch(packets)
        previous = store.get("import_sync_status") or {}
        totals = {key: sum(row.get("counts", {}).get(key, 0) for row in results) for key in ("inserted", "updated", "unchanged", "stale_skipped")}
        store["import_sync_status"] = {
            "status": "ok", "last_success_at": int(time.time()), "previous_success_at": previous.get("last_success_at"),
            "packet_count": len(results), "fixtures": [row.get("fixture") for row in results],
            "request_bytes": request_bytes, "content_encoding": request.headers.get("content-encoding") or "identity",
            "payload_sha256": payload_sha256, "source_sha256": source_sha256 or None,
            "mode": request.headers.get("x-sync-mode") or "batch", "source": request.headers.get("x-sync-source") or "external",
            "stage_counts": totals, "changed_fixture_count": sum(1 for row in results if row.get("changed")),
        }
        write_snapshot_store(store)
    return JSONResponse({"ok": True, "version": VERSION, "imported_count": len(results), "stage_counts": totals, "preflight": preflight, "results": results})


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
    provider_primary_eligible = True
    if str(metadata.get("source") or "pang") == "the_odds_api" and latest_row:
        provider_primary_eligible = get_nested(latest_row, ["provider_audit", "coverage", "primary_reference_eligible"]) is True
    if not latest_row or not latest:
        state, eligible, reason = "data_missing", False, "no_available_market_snapshot"
    elif not provider_primary_eligible:
        state, eligible, reason = "data_missing", False, "the_odds_api_primary_coverage_gate_failed"
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
        "provider_source": metadata.get("source") or "pang", "provider_primary_reference_eligible": provider_primary_eligible,
    }


def market_data_route_report(store_override: Optional[Dict[str, Any]] = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Describe verified decision data separately from merely configured collectors."""
    store = store_override if store_override is not None else load_snapshot_store()
    state_counts = {state: 0 for state in ("fresh", "stale", "historical", "invalid_timestamp", "data_missing")}
    fresh_by_source: Dict[str, int] = {}
    for fixture, metadata in (store.get("external_prematch") or {}).items():
        history = (store.get("fixtures") or {}).get(str(fixture), []) or []
        freshness = imported_fixture_freshness(metadata, history, now_ts=now_ts)
        state = freshness.get("state")
        if state in state_counts:
            state_counts[state] += 1
        if freshness.get("decision_eligible"):
            source = str(metadata.get("source") or "pang")
            fresh_by_source[source] = fresh_by_source.get(source, 0) + 1
    fresh_count = state_counts["fresh"]
    nami_cache = get_nested(store, ["provider_capability_cache", "nami_football_odds"], {}) or {}
    nami_available = nami_cache.get("available") is True and nami_cache.get("entitlement") == "available"
    collector_candidates = []
    if API_FOOTBALL_KEY:
        collector_candidates.append("api_football_configured_unverified_for_current_fixture")
    if THE_ODDS_API_KEY:
        collector_candidates.append("the_odds_api_configured_requires_verified_persisted_timeline")
    if nami_available:
        collector_candidates.append("nami_odds_entitled_unverified_for_current_fixture")
    decision_route = (
        "the_odds_api_persisted_timeline" if fresh_by_source.get("the_odds_api") else
        "pang_persisted_snapshot" if fresh_count else None
    )
    if decision_route:
        status = "decision_data_available"
    elif collector_candidates:
        status = "awaiting_verified_snapshot"
    else:
        status = "market_source_blocked"
    return {
        "status": status, "decision_eligible": bool(decision_route),
        "decision_route": decision_route, "collector_candidates": collector_candidates,
        "pang": {"policy": "read_only", "fresh_fixture_count": fresh_count, "state_counts": state_counts},
        "the_odds_api": {
            "configured": bool(THE_ODDS_API_KEY),
            "fresh_fixture_count": fresh_by_source.get("the_odds_api", 0),
            "role": "primary_reference_only_after_company_coverage_and_timeline_persistence_gates",
        },
        "nami_odds": {
            "entitlement": nami_cache.get("entitlement") or NAMI_ODDS_STARTUP_PROBE.get("entitlement") or "unknown",
            "available": nami_available, "decision_use": False,
        },
        "rule": "configured credentials are not decision data; only a fresh persisted market snapshot is decision eligible",
        "missing_action": "PASS" if not decision_route else None,
    }


def fixture_readiness_report(fixture: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    fixture_key = str(fixture)
    store = load_snapshot_store()
    history = get_fixture_snapshots(fixture_key)
    metadata = (store.get("external_prematch") or {}).get(fixture_key) or {}
    freshness = imported_fixture_freshness(metadata, history, now_ts=now_ts)
    timeline_audit = audit_line_movement_timeline(history)
    latest = latest_prematch_snapshot([row for row in history if snapshot_stage_usable(row)])
    consensus = get_nested(latest or {}, ["market_snapshot", "consensus_main_line"], {}) or {}
    required_market_audit = {}
    for market in ("1x2", "asian_handicap", "over_under"):
        row = consensus.get(market) if isinstance(consensus.get(market), dict) else {}
        reasons = []
        if not row:
            reasons.append("data_missing")
        if row and row.get("source") != "complete_company_array":
            reasons.append("complete_company_array_required")
        if row and int(row.get("bookmaker_count") or 0) < MIN_CONSENSUS_BOOKMAKERS:
            reasons.append("insufficient_bookmakers")
        required_market_audit[market] = {"eligible": not reasons, "reasons": reasons, "bookmaker_count": row.get("bookmaker_count"), "source": row.get("source")}
    market_blockers = [f"{market}:{reason}" for market, audit in required_market_audit.items() for reason in audit["reasons"]]
    if not freshness.get("decision_eligible"):
        market_blockers.append("freshness:" + str(freshness.get("reason") or freshness.get("state")))
    if not timeline_audit.get("decision_eligible"):
        market_blockers.append("line_movement:" + str(timeline_audit.get("reason")))
    market_ready = not market_blockers
    versions = get_fundamental_versions(fixture_key)
    latest_version = versions[-1] if versions else None
    chain_audit = audit_fundamental_chain(get_nested(latest_version or {}, ["script"], {}), now_ts=now_ts)
    lineup_audit = audit_lineup_confidence(metadata.get("lineup_history"), 1.0, now_ts=now_ts)
    lineup_ready = (lineup_audit.get("effective_confidence") or 0) >= MIN_LINEUP_CONFIDENCE
    decision_blockers = list(market_blockers)
    if not chain_audit.get("decision_eligible"):
        decision_blockers.append("fundamental_chain_incomplete")
    if not lineup_ready:
        decision_blockers.append("lineup_confidence_insufficient")
    decision_ready = market_ready and chain_audit.get("decision_eligible") is True and lineup_ready
    status = "decision_ready" if decision_ready else ("shadow_ready" if market_ready else "not_ready")
    opening = next((row for row in history if row.get("stage") == "Opening"), None)
    return {
        "version": VERSION, "fixture": fixture_key, "status": status,
        "shadow_ready": market_ready, "decision_ready": decision_ready,
        "market_blockers": market_blockers, "decision_blockers": decision_blockers,
        "freshness": freshness, "line_movement_audit": timeline_audit,
        "required_market_audit": required_market_audit,
        "opening_source_audit": (opening or {}).get("opening_source_audit") or {"status": "data_missing", "verified": False},
        "fundamental_chain_audit": chain_audit, "lineup_confidence_audit": lineup_audit,
        "latest_stage": (latest or {}).get("stage"),
        "policy": "shadow_ready requires fresh two-stage complete-company-array markets; decision_ready also requires verified fundamentals and lineup confidence",
    }


def lock_calibration_prediction(fixture: Any, probabilities: Dict[str, Any], recommendation: Optional[Dict[str, Any]] = None, captured_at: Optional[int] = None) -> Dict[str, Any]:
    fixture_key = str(fixture or "").strip()
    if not fixture_key:
        raise HTTPException(status_code=400, detail="fixture_required")
    captured_at = int(captured_at or time.time())
    parsed = {key: as_float((probabilities or {}).get(key)) for key in ("home", "draw", "away")}
    if any(value is None or not 0 < value < 1 for value in parsed.values()) or abs(sum(parsed.values()) - 1.0) > 0.01:
        raise HTTPException(status_code=400, detail="invalid_1x2_probabilities")
    recommendation = recommendation if isinstance(recommendation, dict) else {"decision": "PASS"}
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        metadata = (store.get("external_prematch") or {}).get(fixture_key) or {}
        kickoff = _parse_timestamp(get_nested(metadata, ["match", "kickoff_utc"]))
        if kickoff is not None and captured_at >= kickoff:
            raise HTTPException(status_code=409, detail="prediction_must_be_locked_before_kickoff")
        predictions = store.setdefault("calibration_predictions", {})
        if fixture_key in predictions:
            existing = predictions[fixture_key]
            if _content_hash({"probabilities": existing.get("probabilities"), "recommendation": existing.get("recommendation")}) == _content_hash({"probabilities": parsed, "recommendation": recommendation}):
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="prediction_already_locked")
        record = {
            "fixture": fixture_key, "captured_at": captured_at, "kickoff_at": kickoff,
            "probabilities": parsed, "recommendation": recommendation,
            "prediction_hash": _content_hash({"fixture": fixture_key, "captured_at": captured_at, "probabilities": parsed, "recommendation": recommendation}),
            "status": "locked", "settlement": None,
        }
        predictions[fixture_key] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "locked"}


def settle_calibration_prediction(fixture: Any, home_goals: Any, away_goals: Any, settled_at: Optional[int] = None) -> Dict[str, Any]:
    fixture_key = str(fixture or "").strip()
    if not fixture_key:
        raise HTTPException(status_code=400, detail="fixture_required")
    try:
        home_goals, away_goals = int(home_goals), int(away_goals)
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="goals_must_be_non_negative_integers")
    if home_goals < 0 or away_goals < 0:
        raise HTTPException(status_code=400, detail="goals_must_be_non_negative_integers")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        record = (store.get("calibration_predictions") or {}).get(fixture_key)
        if not record:
            raise HTTPException(status_code=404, detail="locked_prediction_not_found")
        existing = record.get("settlement")
        if existing:
            if existing.get("home_goals") == home_goals and existing.get("away_goals") == away_goals:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="settlement_already_recorded")
        outcome = "home" if home_goals > away_goals else ("away" if away_goals > home_goals else "draw")
        probabilities = record["probabilities"]
        brier = sum((probabilities[key] - (1.0 if key == outcome else 0.0)) ** 2 for key in ("home", "draw", "away"))
        log_loss = -math.log(max(probabilities[outcome], 1e-15))
        recommendation = record.get("recommendation") or {}
        decision = recommendation.get("decision") or recommendation.get("selection") or "PASS"
        price = as_float(recommendation.get("price"))
        bet_placed = decision in ("home", "draw", "away") and price is not None and price > 1
        unit_return = (price - 1.0 if decision == outcome else -1.0) if bet_placed else None
        settlement = {
            "fixture": fixture_key, "settled_at": int(settled_at or time.time()),
            "home_goals": home_goals, "away_goals": away_goals, "outcome_1x2": outcome,
            "brier_score": round(brier, 8), "log_loss": round(log_loss, 8),
            "bet_placed": bet_placed, "selection": decision if bet_placed else None,
            "price": price if bet_placed else None, "unit_return": round(unit_return, 8) if unit_return is not None else None,
            "prediction_hash": record.get("prediction_hash"), "action": "settled",
        }
        record["status"] = "settled"
        record["settlement"] = settlement
        store["version"] = VERSION
        write_snapshot_store(store)
        return settlement


def calibration_report() -> Dict[str, Any]:
    records = list((load_snapshot_store().get("calibration_predictions") or {}).values())
    settled = [record.get("settlement") for record in records if isinstance(record.get("settlement"), dict)]
    bets = [row for row in settled if row.get("bet_placed") and as_float(row.get("unit_return")) is not None]
    total_return = sum(as_float(row.get("unit_return")) or 0 for row in bets)
    return {
        "version": VERSION, "locked_count": len(records), "settled_count": len(settled),
        "pending_count": len(records) - len(settled),
        "average_brier_score": round(sum(row["brier_score"] for row in settled) / len(settled), 8) if settled else None,
        "average_log_loss": round(sum(row["log_loss"] for row in settled) / len(settled), 8) if settled else None,
        "bet_count": len(bets), "total_unit_return": round(total_return, 8),
        "roi": round(total_return / len(bets), 8) if bets else None,
        "records": settled,
        "policy": "predictions are immutable after locking; PASS is excluded from betting ROI but included in probability calibration",
    }


def learning_major_league_registry_manifest() -> Dict[str, Any]:
    leagues = [
        {"competition_id": competition_id, **LEARNING_MAJOR_LEAGUES[competition_id]}
        for competition_id in sorted(LEARNING_MAJOR_LEAGUES)
    ]
    immutable_content = {
        "version": LEARNING_MAJOR_LEAGUES_VERSION,
        "leagues": leagues,
        "admission_policy": "explicit_user_approved_major_domestic_top_flights_only",
    }
    return {
        **immutable_content,
        "registry_hash": _content_hash(immutable_content),
        "league_count": len(leagues),
        "historical_reclassification_allowed": False,
        "external_evidence_can_self_admit": False,
        "requires_explicit_user_confirmation_for_changes": True,
    }


def audit_learning_scope(scope: Any) -> Dict[str, Any]:
    """Admit only user-approved major men's professional domestic top flights."""
    scope = scope if isinstance(scope, dict) else {}
    reasons = []
    competition_type = str(scope.get("competition_type") or "").strip().lower()
    gender = str(scope.get("gender") or "").strip().lower()
    team_level = str(scope.get("team_level") or "").strip().lower()
    try:
        tier = int(scope.get("tier"))
    except (TypeError, ValueError):
        tier = None
    try:
        competition_id = int(scope.get("competition_id"))
    except (TypeError, ValueError):
        competition_id = None
    registry_manifest = learning_major_league_registry_manifest()
    registry = LEARNING_MAJOR_LEAGUES.get(competition_id)
    verification_refs = scope.get("verification_refs") if isinstance(scope.get("verification_refs"), list) else []
    valid_verification_refs = [
        ref for ref in verification_refs
        if isinstance(ref, dict)
        and str(ref.get("source") or "").strip()
        and str(ref.get("url") or ref.get("id") or ref.get("title") or "").strip()
    ]
    if competition_type != "domestic_league":
        reasons.append("domestic_league_required")
    if tier != 1:
        reasons.append("tier_one_required")
    if gender not in ("men", "male"):
        reasons.append("mens_competition_required")
    if scope.get("professional") is not True:
        reasons.append("professional_competition_required")
    if team_level != "first_team":
        reasons.append("first_team_required")
    if not str(scope.get("competition_name") or "").strip():
        reasons.append("competition_name_required")
    if not str(scope.get("season") or "").strip():
        reasons.append("season_required")
    if registry:
        supplied_name = normalize_fixture_identity_name(scope.get("competition_name"))
        registered_name = normalize_fixture_identity_name(registry.get("name"))
        if supplied_name != registered_name:
            reasons.append("competition_name_registry_mismatch")
        supplied_country = normalize_fixture_identity_name(scope.get("country"))
        registered_country = normalize_fixture_identity_name(registry.get("country"))
        if supplied_country and supplied_country != registered_country:
            reasons.append("competition_country_registry_mismatch")
        verification_method = "curated_major_top_flight_registry"
    else:
        verification_method = "not_in_major_learning_registry"
        reasons.append("competition_not_in_major_learning_registry")
    eligible = not reasons
    return {
        "eligible": eligible,
        "learning_eligibility": "MODEL_LEARNING_ELIGIBLE" if eligible else "MODEL_LEARNING_EXCLUDED",
        "allowed_modes": ["AUTOMATIC_MODEL_LEARNING"] if eligible else ["MARKET_LANGUAGE_ONLY", "SETTLEMENT_ONLY"],
        "reasons": reasons,
        "normalized": {
            "competition_name": str(scope.get("competition_name") or "").strip(),
            "competition_id": competition_id,
            "country": str(scope.get("country") or (registry or {}).get("country") or "").strip(),
            "competition_type": competition_type or None,
            "tier": tier,
            "gender": gender or None,
            "professional": scope.get("professional") is True,
            "team_level": team_level or None,
            "season": str(scope.get("season") or "").strip(),
            "phase": str(scope.get("phase") or "unknown").strip(),
            "format_version": str(scope.get("format_version") or "unknown").strip(),
            "verification_method": verification_method,
            "verification_refs": valid_verification_refs[:10],
            "major_league_registry_version": registry_manifest["version"],
            "major_league_registry_hash": registry_manifest["registry_hash"],
        },
        "policy": "only the explicit user-approved major domestic top-flight registry is model-learning eligible; external tier evidence cannot self-admit a competition",
    }


def discover_learning_fixtures(now_ts: Optional[int] = None, fixture_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Discover only registry-verified domestic top-flight fixtures in the next horizon."""
    now_ts = int(now_ts or time.time())
    horizon_ts = now_ts + LEARNING_DISCOVERY_HORIZON_HOURS * 3600
    source_audit = []
    rows: List[Dict[str, Any]] = []
    if fixture_rows is not None:
        rows = [row for row in fixture_rows if isinstance(row, dict)]
        source_audit.append({"source": "injected_fixture_rows", "ok": True, "row_count": len(rows)})
    else:
        local_now = datetime.fromtimestamp(now_ts, tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai"))
        dates = sorted({local_now.date().isoformat(), (local_now + timedelta(days=1)).date().isoformat()})
        for date_str in dates:
            result = call_api_football("/fixtures", {"date": date_str, "timezone": "Asia/Shanghai"})
            batch = response_list(result)
            rows.extend(row for row in batch if isinstance(row, dict))
            source_audit.append({
                "source": "api_football", "date": date_str, "ok": bool(result.get("ok")),
                "status_code": result.get("status_code"), "row_count": len(batch), "error": result.get("error"),
            })
    registry_manifest = learning_major_league_registry_manifest()
    candidates, observation_candidates, excluded = [], [], []
    seen = set()
    for row in rows:
        summary = dict(row) if row.get("fixture_id") is not None else fixture_summary(row)
        fixture_id = summary.get("fixture_id")
        league_id = summary.get("league_id")
        kickoff = fixture_datetime_utc(summary)
        reason = None
        if league_id not in LEARNING_MAJOR_LEAGUES:
            reason = "competition_not_in_major_learning_registry"
        elif summary.get("status") != "NS":
            reason = "fixture_not_not_started"
        elif not fixture_id or not kickoff:
            reason = "fixture_identity_or_kickoff_missing"
        elif not now_ts < int(kickoff.timestamp()) <= horizon_ts:
            reason = "outside_learning_discovery_horizon"
        elif str(fixture_id) in seen:
            reason = "duplicate_fixture"
        if reason:
            excluded_row = {"fixture_id": fixture_id, "league_id": league_id, "reason": reason}
            if reason == "competition_not_in_major_learning_registry":
                excluded_row.update({
                    "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
                    "allowed_modes": ["MARKET_LANGUAGE_ONLY", "SETTLEMENT_ONLY"],
                })
                if (
                    fixture_id and kickoff and summary.get("status") == "NS"
                    and now_ts < int(kickoff.timestamp()) <= horizon_ts
                    and str(fixture_id) not in seen
                ):
                    seen.add(str(fixture_id))
                    observation_candidates.append({
                        **summary,
                        "scope": {
                            "competition_id": league_id,
                            "competition_name": str(summary.get("league") or "Unknown competition"),
                            "country": str(summary.get("country") or ""),
                            "competition_type": "unclassified_non_learning_competition",
                            "tier": None,
                            "gender": "men",
                            "professional": True,
                            "team_level": "first_team",
                            "season": str(summary.get("season") or ""),
                            "phase": str(summary.get("league_round") or "unknown"),
                            "verification_status": "identification_only",
                        },
                        "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
                        "allowed_modes": ["MARKET_LANGUAGE_ONLY", "SETTLEMENT_ONLY"],
                        "routing_reason": reason,
                    })
            excluded.append(excluded_row)
            continue
        seen.add(str(fixture_id))
        registry = LEARNING_MAJOR_LEAGUES[league_id]
        candidates.append({
            **summary,
            "scope": {
                "competition_id": league_id, "competition_name": registry["name"],
                "country": registry["country"], "competition_type": "domestic_league",
                "tier": 1, "gender": "men", "professional": True,
                "team_level": "first_team", "season": str(summary.get("season") or ""),
                "phase": str(summary.get("league_round") or "unknown"),
                "verification_status": "verified",
            },
            "learning_scope_verified": True,
            "major_league_registry_version": registry_manifest["version"],
            "major_league_registry_hash": registry_manifest["registry_hash"],
        })
    candidates.sort(key=lambda row: (int(row.get("timestamp") or 0), str(row.get("fixture_id"))))
    observation_candidates.sort(key=lambda row: (int(row.get("timestamp") or 0), str(row.get("fixture_id"))))
    excluded_counts = {reason: sum(row["reason"] == reason for row in excluded) for reason in sorted({row["reason"] for row in excluded})}
    return {
        "ok": all(row.get("ok") for row in source_audit) if source_audit else False,
        "generated_at": now_ts, "window_start": now_ts, "window_end": horizon_ts,
        "horizon_hours": LEARNING_DISCOVERY_HORIZON_HOURS,
        "candidate_count": len(candidates), "candidates": candidates,
        "observation_candidate_count": len(observation_candidates),
        "observation_candidates": observation_candidates,
        "excluded_count": len(excluded), "excluded_counts": excluded_counts,
        "excluded_sample": excluded[:50], "excluded_sample_truncated": len(excluded) > 50,
        "source_audit": source_audit,
        "major_league_registry": {
            "version": registry_manifest["version"],
            "registry_hash": registry_manifest["registry_hash"],
            "league_count": registry_manifest["league_count"],
        },
        "policy": "only user-approved major men's professional domestic tier-one first-team leagues are eligible; non-registry competitions are market-language/settlement only",
    }


def scheduled_learning_analysis_node(now_ts: int, kickoff_at: int) -> Optional[str]:
    """Return the latest fixed learning node due by clock; Opening requires real evidence."""
    seconds_before = int(kickoff_at) - int(now_ts)
    if seconds_before <= 0 or seconds_before > LEARNING_DISCOVERY_HORIZON_HOURS * 3600:
        return None
    if seconds_before <= 3600:
        return "T-1h"
    if seconds_before <= 6 * 3600:
        return "T-6h"
    if seconds_before <= 12 * 3600:
        return "T-12h"
    return None


def admit_learning_candidate(candidate: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Persist daily governance admission even when no analysis clock node is due yet."""
    now_ts = int(now_ts or time.time())
    candidate = candidate if isinstance(candidate, dict) else {}
    fixture = str(candidate.get("fixture_id") or "").strip()
    kickoff_at = _parse_timestamp(candidate.get("timestamp"))
    if not fixture or kickoff_at is None:
        raise HTTPException(status_code=422, detail="learning_admission_identity_or_kickoff_missing")
    if str(candidate.get("status") or "").upper() != "NS" or now_ts >= kickoff_at:
        raise HTTPException(status_code=409, detail="learning_admission_requires_not_started_fixture")
    scope_audit = audit_learning_scope(candidate.get("scope"))
    if not scope_audit["eligible"]:
        raise HTTPException(status_code=422, detail={"error": "major_league_scope_not_eligible", "reasons": scope_audit["reasons"]})
    registry = learning_major_league_registry_manifest()
    immutable_content = {
        "fixture": fixture,
        "kickoff_at": kickoff_at,
        "scope": scope_audit["normalized"],
        "major_league_registry_version": registry["version"],
        "major_league_registry_hash": registry["registry_hash"],
        "admission_source": "daily_1430_governance_cycle",
        "learning_eligibility": "MODEL_LEARNING_ELIGIBLE",
    }
    admission_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        admissions = store.setdefault("learning_admissions", {})
        existing = admissions.get(fixture)
        if existing:
            if existing.get("admission_hash") == admission_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="learning_admission_identity_already_recorded")
        record = {
            **immutable_content,
            "admitted_at": now_ts,
            "admission_hash": admission_hash,
            "immutable": True,
            "champion_effect": False,
            "status": "admitted_waiting_for_due_node",
        }
        admissions[fixture] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "admitted"}


def _learning_snapshot_marker(store: Dict[str, Any], fixture: str, now_ts: int) -> Optional[Dict[str, Any]]:
    rows = (store.get("fixtures") or {}).get(str(fixture), []) or []
    available = [
        row for row in complete_prematch_timeline(rows)
        if row.get("timeline_status") == "available"
        and row.get("stage") in LEARNING_PREMATCH_STAGE_ORDER
        and int(row.get("snapshot_at") or 0) <= int(now_ts)
    ]
    latest = latest_prematch_snapshot(available)
    if not latest:
        return None
    evidence_hash = str(latest.get("source_content_hash") or "").strip() or _content_hash({
        "stage": latest.get("stage"),
        "snapshot_at": latest.get("snapshot_at"),
        "market_snapshot": latest.get("market_snapshot"),
        "market_dynamics": latest.get("market_dynamics"),
        "team_news_snapshot": latest.get("team_news_snapshot"),
    })
    return {
        "stage": latest.get("stage"),
        "snapshot_at": int(latest.get("snapshot_at") or 0),
        "evidence_hash": evidence_hash,
    }


def _learning_freeze_analysis_node(row: Dict[str, Any]) -> Optional[str]:
    node = get_nested(row, ["analysis", "analysis_node"])
    if node in LEARNING_PREMATCH_STAGE_ORDER:
        return node
    captured_at = int(row.get("captured_at") or 0)
    kickoff_at = int(row.get("kickoff_at") or 0)
    return scheduled_learning_analysis_node(captured_at, kickoff_at)


def learning_candidate_analysis_state(
    candidate: Dict[str, Any],
    store: Dict[str, Any],
    now_ts: int,
) -> Dict[str, Any]:
    """Decide whether one new PIT analysis version is due, without fetching new facts."""
    fixture = str(candidate.get("fixture_id") or "").strip()
    kickoff_at = _parse_timestamp(candidate.get("timestamp"))
    scheduled_node = scheduled_learning_analysis_node(now_ts, kickoff_at or 0)
    rows = [row for row in ((store.get("learning_frozen") or {}).get(fixture) or []) if isinstance(row, dict)]
    latest_freeze = max(rows, key=lambda row: int(row.get("version_number") or 0), default=None)
    marker = _learning_snapshot_marker(store, fixture, now_ts)
    target_node = scheduled_node
    if marker and marker.get("stage") in LEARNING_PREMATCH_STAGE_ORDER:
        marker_stage = marker["stage"]
        if target_node not in LEARNING_PREMATCH_STAGE_ORDER or (
            LEARNING_PREMATCH_STAGE_ORDER.index(marker_stage) > LEARNING_PREMATCH_STAGE_ORDER.index(target_node)
        ):
            target_node = marker_stage
    latest_node = _learning_freeze_analysis_node(latest_freeze or {})
    due_reason = None
    if target_node is None:
        due_reason = "analysis_clock_node_not_due"
    elif latest_freeze is None:
        due_reason = "initial_analysis_node_due"
    elif latest_node not in LEARNING_PREMATCH_STAGE_ORDER:
        due_reason = "legacy_freeze_missing_analysis_node"
    elif LEARNING_PREMATCH_STAGE_ORDER.index(target_node) > LEARNING_PREMATCH_STAGE_ORDER.index(latest_node):
        due_reason = "analysis_node_advanced"
    else:
        last_source_hash = str(get_nested(latest_freeze, ["analysis", "source_snapshot_hash"]) or "").strip()
        marker_hash = str((marker or {}).get("evidence_hash") or "").strip()
        marker_stage = (marker or {}).get("stage")
        if marker_hash and marker_hash != last_source_hash and marker_stage == target_node:
            due_reason = "new_snapshot_evidence_at_current_node"
        else:
            latest_capture = int(latest_freeze.get("captured_at") or 0)
            pending = [
                task for task in (store.get("fundamental_revalidation_queue") or {}).values()
                if isinstance(task, dict)
                and str(task.get("fixture") or "") == fixture
                and task.get("status") == "pending"
                and int(task.get("created_at") or 0) > latest_capture
                and task.get("stage") in LEARNING_PREMATCH_STAGE_ORDER
                and LEARNING_PREMATCH_STAGE_ORDER.index(task["stage"]) <= LEARNING_PREMATCH_STAGE_ORDER.index(target_node)
            ]
            if pending:
                due_reason = "material_revalidation_trigger"
    analysis_due = due_reason not in {None, "analysis_clock_node_not_due"}
    return {
        "already_frozen": latest_freeze is not None,
        "latest_freeze_id": (latest_freeze or {}).get("freeze_id"),
        "latest_freeze_version": (latest_freeze or {}).get("version_number"),
        "latest_analysis_node": latest_node,
        "scheduled_analysis_node": scheduled_node,
        "target_analysis_node": target_node,
        "source_snapshot_stage": (marker or {}).get("stage"),
        "source_snapshot_at": (marker or {}).get("snapshot_at"),
        "source_snapshot_hash": (marker or {}).get("evidence_hash"),
        "analysis_due": analysis_due,
        "analysis_due_reason": due_reason or "no_new_node_or_material_evidence",
    }


def learning_cycle_plan(
    now_ts: Optional[int] = None,
    fixture_rows: Optional[List[Dict[str, Any]]] = None,
    include_discovery: bool = True,
) -> Dict[str, Any]:
    """Build a non-mutating daily work plan; never invent analysis or process classifications."""
    now_ts = int(now_ts or time.time())
    discovery = discover_learning_fixtures(now_ts=now_ts, fixture_rows=fixture_rows) if include_discovery else {
        "ok": True, "generated_at": now_ts, "window_start": now_ts,
        "window_end": now_ts + LEARNING_DISCOVERY_HORIZON_HOURS * 3600,
        "horizon_hours": LEARNING_DISCOVERY_HORIZON_HOURS,
        "candidate_count": 0, "candidates": [], "excluded_count": 0,
        "excluded_counts": {}, "source_audit": [{"source": "deferred_until_postmatch_phase", "ok": True}],
        "policy": "future fixture discovery is deferred until postmatch processing completes",
    }
    store = load_snapshot_store()
    frozen = store.get("learning_frozen") or {}
    postmatches = store.get("learning_postmatch") or {}
    settlement_due = []
    for fixture, versions in frozen.items():
        latest = max((row for row in versions or [] if isinstance(row, dict)), key=lambda row: int(row.get("version_number") or 0), default=None)
        if not latest or latest.get("freeze_id") in postmatches:
            continue
        kickoff_at = int(latest.get("kickoff_at") or 0)
        age = now_ts - kickoff_at
        if 2 * 3600 <= age <= LEARNING_POSTMATCH_LOOKBACK_HOURS * 3600:
            settlement_due.append({
                "fixture": fixture, "freeze_id": latest.get("freeze_id"),
                "kickoff_at": kickoff_at, "hours_since_kickoff": round(age / 3600, 2),
                "required_action": "verify_result_and_events_then_classify_process_without_result_backfit",
            })
    existing_fact_versions = store.get("learning_postmatch_facts") or {}
    fact_collection_due = [
        row for row in settlement_due
        if not (existing_fact_versions.get(str(row.get("freeze_id"))) or [])
    ]
    existing_draft_versions = store.get("learning_postmatch_drafts") or {}
    review_draft_due = []
    for row in settlement_due:
        freeze_id = str(row.get("freeze_id") or "")
        fact_versions = existing_fact_versions.get(freeze_id) or []
        if not fact_versions:
            continue
        latest_fact = max(fact_versions, key=lambda item: int(item.get("version_number") or 0))
        if get_nested(latest_fact, ["verification", "settlement_eligible"]) is not True:
            continue
        draft_versions = existing_draft_versions.get(freeze_id) or []
        latest_draft = max(draft_versions, key=lambda item: int(item.get("version_number") or 0), default=None)
        if not latest_draft or latest_draft.get("fact_hash") != latest_fact.get("fact_hash"):
            review_draft_due.append({**row, "fact_hash": latest_fact.get("fact_hash")})
    local_date = datetime.fromtimestamp(now_ts, tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).date()
    frozen_today = [
        row for versions in frozen.values() for row in versions or []
        if datetime.fromtimestamp(int(row.get("captured_at") or 0), tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai")).date() == local_date
    ]
    admitted_today = [row for row in frozen_today if int(row.get("version_number") or 0) == 1]
    reanalyses_today = [row for row in frozen_today if int(row.get("version_number") or 0) > 1]
    discovery_candidates = []
    for row in discovery["candidates"]:
        discovery_candidates.append({**row, **learning_candidate_analysis_state(row, store, now_ts)})
    return {
        "version": VERSION, "generated_at": now_ts,
        "settlement_due_count": len(settlement_due), "settlement_due": sorted(settlement_due, key=lambda row: row["kickoff_at"]),
        "postmatch_fact_collection_due_count": len(fact_collection_due),
        "postmatch_fact_collection_due": sorted(fact_collection_due, key=lambda row: row["kickoff_at"]),
        "postmatch_review_draft_due_count": len(review_draft_due),
        "postmatch_review_draft_due": sorted(review_draft_due, key=lambda row: row["kickoff_at"]),
        "discovery": {**discovery, "candidates": discovery_candidates},
        "learning_prematch_stages": LEARNING_PREMATCH_STAGE_ORDER,
        "daily_freeze_cap": None,
        "daily_new_fixture_cap": None,
        "daily_reanalysis_cap": None,
        "match_limit": None,
        "full_historical_odds_sample_limit": None,
        "frozen_today_count": len(frozen_today),
        "admitted_today_count": len(admitted_today),
        "reanalysis_today_count": len(reanalyses_today),
        "remaining_freeze_capacity": None,
        "remaining_new_fixture_capacity": None,
        "remaining_reanalysis_capacity": None,
        "mutation_policy": "plan_only; freezing requires a complete PIT analysis and settlement requires verified facts plus an explicit process classification",
        "automatic_champion_promotion": False,
    }


def build_learning_freeze_payload(candidate: Dict[str, Any], packet: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Convert one PIT prematch packet into the immutable learning schema without inventing missing fields."""
    now_ts = int(now_ts or time.time())
    candidate = candidate if isinstance(candidate, dict) else {}
    packet = packet if isinstance(packet, dict) else {}
    fixture = str(candidate.get("fixture_id") or "").strip()
    kickoff_at = _parse_timestamp(candidate.get("timestamp"))
    if not fixture or kickoff_at is None:
        raise HTTPException(status_code=422, detail="learning_candidate_identity_or_kickoff_missing")
    if str(candidate.get("status") or "").upper() != "NS" or now_ts >= kickoff_at:
        raise HTTPException(status_code=409, detail="automatic_learning_freeze_requires_not_started_fixture")
    if packet.get("ok") is not True:
        raise HTTPException(status_code=422, detail="prematch_packet_not_ready")
    if get_nested(packet, ["analysis_rules", "prematch_only"]) is not True:
        raise HTTPException(status_code=422, detail="prematch_only_packet_required")
    packet_fixture = packet.get("fixture") if isinstance(packet.get("fixture"), dict) else {}
    packet_fixture_id = str(packet_fixture.get("fixture_id") or packet_fixture.get("id") or packet.get("fixture_id") or "").strip()
    if packet_fixture_id and packet_fixture_id != fixture:
        raise HTTPException(status_code=409, detail="prematch_packet_fixture_mismatch")
    generated_at = _parse_timestamp(packet.get("generated_at")) or now_ts
    if generated_at > now_ts + 60:
        raise HTTPException(status_code=409, detail="prematch_packet_timestamp_in_future")
    if generated_at >= kickoff_at:
        raise HTTPException(status_code=409, detail="prematch_packet_generated_after_kickoff")
    decision = copy.deepcopy(packet.get("decision_layer")) if isinstance(packet.get("decision_layer"), dict) else {}
    if not decision:
        decision = {"decision": "PASS", "pass_reasons": ["decision_layer_data_missing"]}
    probability_replay = packet.get("probability_replay") if isinstance(packet.get("probability_replay"), dict) else {
        "schema": "learning_probability_replay_v1",
        "status": "data_missing",
        "decision_eligible": False,
        "blockers": ["learning_probability_replay_inputs_missing"],
    }
    replay_audit = audit_learning_probability_replay(
        probability_replay,
        expected_fixture=fixture,
        data_cutoff_at=generated_at,
        kickoff_at=kickoff_at,
    )
    if replay_audit.get("status") == "invalid":
        raise HTTPException(status_code=422, detail={"error": "learning_probability_replay_invalid", "audit": replay_audit})
    if replay_audit.get("decision_eligible") is not True:
        reasons = decision.get("pass_reasons") if isinstance(decision.get("pass_reasons"), list) else []
        decision["pass_reasons"] = list(dict.fromkeys([*reasons, "learning_probability_replay_inputs_missing"]))
        decision["decision"] = "PASS"
        decision["execution_action"] = "PASS"
        decision["best_market"] = None
    elif decision.get("model_probability") != get_nested(probability_replay, ["model", "probabilities"]):
        raise HTTPException(status_code=422, detail="decision_model_probability_not_bound_to_replay_contract")
    elif decision.get("probability_replay_hash") != probability_replay.get("replay_hash"):
        raise HTTPException(status_code=422, detail="decision_probability_replay_hash_mismatch")
    packet_timeline = get_nested(packet, ["market", "timeline"], []) or []
    available_packet_rows = []
    for row in packet_timeline:
        if not isinstance(row, dict) or row.get("status") != "available" or row.get("stage") not in LEARNING_PREMATCH_STAGE_ORDER:
            continue
        snapshot_at = _parse_timestamp(row.get("snapshot_at"))
        if snapshot_at is None or snapshot_at > generated_at:
            continue
        if audit_stage_timing(row["stage"], snapshot_at, kickoff_at).get("status") == "invalid":
            continue
        available_packet_rows.append(row)
    latest_packet_row = max(
        available_packet_rows,
        key=lambda row: (LEARNING_PREMATCH_STAGE_ORDER.index(row["stage"]), int(row.get("snapshot_at") or 0)),
        default=None,
    )
    actionable_decision = (
        decision.get("decision") != "PASS"
        or decision.get("execution_action") in {"BET", "WAIT", "PENDING_MARKET_LANGUAGE"}
        or isinstance(decision.get("best_market"), dict)
    )
    if replay_audit.get("decision_eligible") is True:
        market_binding = decision.get("market_evidence_binding") if isinstance(decision.get("market_evidence_binding"), dict) else {}
        if market_binding.get("current_unpersisted_quote_used") is not False:
            raise HTTPException(status_code=422, detail="unpersisted_current_quote_cannot_drive_learning_decision")
        if actionable_decision and (
            not latest_packet_row
            or market_binding.get("source") != "persisted_timing_audited_snapshot"
            or market_binding.get("stage") != latest_packet_row.get("stage")
            or _parse_timestamp(market_binding.get("snapshot_at")) != _parse_timestamp(latest_packet_row.get("snapshot_at"))
            or market_binding.get("source_content_hash") != latest_packet_row.get("source_content_hash")
        ):
            raise HTTPException(status_code=422, detail="actionable_decision_market_snapshot_not_hash_bound")
    target_node = candidate.get("target_analysis_node")
    if latest_packet_row and (
        target_node not in LEARNING_PREMATCH_STAGE_ORDER
        or LEARNING_PREMATCH_STAGE_ORDER.index(latest_packet_row["stage"]) > LEARNING_PREMATCH_STAGE_ORDER.index(target_node)
    ):
        target_node = latest_packet_row["stage"]
    if target_node not in LEARNING_PREMATCH_STAGE_ORDER:
        target_node = scheduled_learning_analysis_node(generated_at, kickoff_at)
    if target_node not in LEARNING_PREMATCH_STAGE_ORDER:
        raise HTTPException(status_code=409, detail="learning_analysis_node_not_due")
    if target_node == "Opening" and (not latest_packet_row or latest_packet_row.get("stage") != "Opening"):
        raise HTTPException(status_code=409, detail="verified_opening_snapshot_required_for_learning_node")
    packet_source_hash = str((latest_packet_row or {}).get("source_content_hash") or "").strip()
    if not packet_source_hash and latest_packet_row:
        packet_source_hash = _content_hash(latest_packet_row)
    analysis = {
        "prematch_only": True,
        "packet_generated_at": generated_at,
        "analysis_node": target_node,
        "required_learning_timeline": LEARNING_PREMATCH_STAGE_ORDER,
        "analysis_trigger": candidate.get("analysis_due_reason") or "explicit_prematch_packet",
        "source_snapshot_stage": (latest_packet_row or {}).get("stage") or candidate.get("source_snapshot_stage"),
        "source_snapshot_at": (latest_packet_row or {}).get("snapshot_at") or candidate.get("source_snapshot_at"),
        "source_snapshot_hash": packet_source_hash or candidate.get("source_snapshot_hash"),
        "data_quality": packet.get("data_quality"),
        "coverage": packet.get("coverage"),
        "probability_replay": probability_replay,
        "probability_replay_audit": replay_audit,
        "fundamental_chain": packet.get("pure_fundamental_script") or packet.get("fundamentals") or {"status": "data_missing"},
        "state_tree": get_nested(packet, ["pure_fundamental_script", "chain", "game_state_elasticity"], {"status": "data_missing"}),
        "market_timeline": packet.get("market") or {"status": "data_missing"},
        "market_language": decision.get("market_language") or get_nested(packet, ["market", "latest_dynamics"], {"status": "data_missing"}),
        "expression_optimizer": decision.get("expression_optimizer") or {"action": "PASS", "reason": "data_missing"},
        "analysis_rules": packet.get("analysis_rules") or {},
        "missing_data_policy": "preserve_data_missing; never backfill from post-kickoff information",
    }
    scope = candidate.get("scope") if isinstance(candidate.get("scope"), dict) else {}
    provider_identity = packet.get("provider_identity") if isinstance(packet.get("provider_identity"), dict) else {}
    provider_identities = [row for row in (packet.get("provider_identities") or []) if isinstance(row, dict)]
    if provider_identity and not any(row.get("source") == provider_identity.get("source") for row in provider_identities):
        provider_identities.append(provider_identity)
    frozen_source_refs = [
        {"source": "api_football", "fixture_id": fixture, "captured_at": generated_at},
        {"source": str(packet.get("source") or "shadow_ai_packet"), "packet_version": str(packet.get("version") or VERSION)},
    ]
    for identity in provider_identities:
        if identity.get("source") == "the_odds_api":
            canonical = {
                "source": "the_odds_api",
                "sport_key": str(identity.get("sport_key") or "").strip().lower(),
                "event_id": str(identity.get("event_id") or "").strip().lower(),
                "home_team": str(identity.get("home_team") or "").strip(),
                "away_team": str(identity.get("away_team") or "").strip(),
            }
            required = ("sport_key", "event_id", "home_team", "away_team")
        elif identity.get("source") == "thestats":
            canonical = {
                "source": "thestats",
                "match_id": str(identity.get("match_id") or "").strip(),
                "kickoff_at": _parse_timestamp(identity.get("kickoff_at")),
                "home_team": str(identity.get("home_team") or "").strip(),
                "away_team": str(identity.get("away_team") or "").strip(),
            }
            required = ("match_id", "kickoff_at", "home_team", "away_team")
        else:
            continue
        if all(canonical[key] not in (None, "") for key in required) and identity.get("source_hash") == _content_hash(canonical):
            frozen_source_refs.append({
                **canonical,
                "identity_bound_at": generated_at,
                "identity_source_hash": identity["source_hash"],
            })
    return {
        "fixture": fixture,
        "scope": scope,
        "captured_at": generated_at,
        "data_cutoff_at": generated_at,
        "kickoff_at": kickoff_at,
        "versions": {
            "service": VERSION,
            "model": str(packet.get("version") or VERSION),
            "rules": LEARNING_RULES_VERSION,
            "league_dna": "candidate_only" if league_dna_model_view(scope).get("candidate_tag_count") else "data_missing",
        },
        "analysis": analysis,
        "decision": decision,
        "source_refs": frozen_source_refs,
    }


def learning_node_execution_key(candidate: Dict[str, Any]) -> str:
    """Identify one retryable node job without using mutable wall-clock fields."""
    candidate = candidate if isinstance(candidate, dict) else {}
    identity = {
        "fixture": str(candidate.get("fixture_id") or candidate.get("fixture") or ""),
        "target_analysis_node": candidate.get("target_analysis_node"),
        "latest_freeze_id": candidate.get("latest_freeze_id"),
        "latest_freeze_version": candidate.get("latest_freeze_version"),
        "source_snapshot_hash": candidate.get("source_snapshot_hash"),
        "analysis_due_reason": candidate.get("analysis_due_reason"),
    }
    return "node:" + _content_hash(identity)


def _learning_node_attempt_gate(
    execution_key: str, now_ts: int, store: Dict[str, Any],
) -> Optional[str]:
    attempt = (store.get("learning_node_attempts") or {}).get(execution_key)
    if not isinstance(attempt, dict):
        return None
    status = str(attempt.get("status") or "")
    if status == "completed":
        return "node_attempt_already_completed"
    if status == "running" and int(attempt.get("lease_expires_at") or 0) > now_ts:
        return "node_execution_lease_active"
    if status == "failed" and int(attempt.get("retry_after_at") or 0) > now_ts:
        return "node_retry_backoff_active"
    return None


def claim_learning_node_execution(candidate: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Acquire an in-store lease so one worker owns one node/evidence job at a time."""
    now_ts = int(now_ts or time.time())
    execution_key = str(candidate.get("execution_key") or learning_node_execution_key(candidate))
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        attempts = store.setdefault("learning_node_attempts", {})
        existing = attempts.get(execution_key) if isinstance(attempts.get(execution_key), dict) else {}
        gate = _learning_node_attempt_gate(execution_key, now_ts, store)
        if gate:
            return {
                "claimed": False, "execution_key": execution_key, "reason": gate,
                "status": existing.get("status"), "attempt_count": int(existing.get("attempt_count") or 0),
                "retry_after_at": existing.get("retry_after_at"),
                "lease_expires_at": existing.get("lease_expires_at"),
            }
        attempt_count = int(existing.get("attempt_count") or 0) + 1
        claim_token = _content_hash({
            "execution_key": execution_key, "attempt_count": attempt_count,
            "claimed_at": now_ts, "thread": threading.get_ident(),
        })
        record = {
            "execution_key": execution_key,
            "fixture": str(candidate.get("fixture_id") or candidate.get("fixture") or ""),
            "target_analysis_node": candidate.get("target_analysis_node"),
            "analysis_due_reason": candidate.get("analysis_due_reason"),
            "source_snapshot_hash": candidate.get("source_snapshot_hash"),
            "latest_freeze_id": candidate.get("latest_freeze_id"),
            "attempt_count": attempt_count,
            "first_attempt_at": int(existing.get("first_attempt_at") or now_ts),
            "last_attempt_at": now_ts,
            "status": "running",
            "claim_token": claim_token,
            "lease_expires_at": now_ts + LEARNING_NODE_LEASE_SECONDS,
            "retry_after_at": None,
            "last_error_type": None,
            "last_error": None,
            "completed_at": None,
            "freeze_id": None,
            "immutable_job_identity": True,
            "champion_effect": False,
        }
        attempts[execution_key] = record
        if len(attempts) > LEARNING_NODE_ATTEMPT_RETENTION:
            removable = sorted(
                (
                    (key, row) for key, row in attempts.items()
                    if key != execution_key and isinstance(row, dict) and row.get("status") != "running"
                ),
                key=lambda item: int(item[1].get("last_attempt_at") or 0),
            )
            for key, _ in removable[:max(0, len(attempts) - LEARNING_NODE_ATTEMPT_RETENTION)]:
                attempts.pop(key, None)
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "claimed": True}


def complete_learning_node_execution(
    execution_key: str, claim_token: str, outcome: str,
    now_ts: Optional[int] = None, freeze_id: Optional[str] = None,
    error_type: Optional[str] = None, error: Optional[Any] = None,
) -> Dict[str, Any]:
    """Close a claimed job and persist bounded retry timing without storing exception bodies."""
    now_ts = int(now_ts or time.time())
    if outcome not in {"completed", "failed"}:
        raise ValueError("learning_node_outcome_must_be_completed_or_failed")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        attempts = store.setdefault("learning_node_attempts", {})
        record = attempts.get(str(execution_key))
        if not isinstance(record, dict):
            raise HTTPException(status_code=404, detail="learning_node_attempt_not_found")
        if not hmac.compare_digest(str(record.get("claim_token") or ""), str(claim_token or "")):
            raise HTTPException(status_code=409, detail="learning_node_claim_superseded")
        updated = {**record, "status": outcome, "lease_expires_at": None}
        if outcome == "completed":
            updated.update({
                "completed_at": now_ts, "freeze_id": str(freeze_id or "") or None,
                "retry_after_at": None, "last_error_type": None, "last_error": None,
            })
        else:
            attempt_count = max(1, int(record.get("attempt_count") or 1))
            retry_seconds = min(
                LEARNING_NODE_RETRY_MAX_SECONDS,
                LEARNING_NODE_RETRY_BASE_SECONDS * (2 ** min(attempt_count - 1, 8)),
            )
            updated.update({
                "completed_at": None, "freeze_id": None,
                "retry_after_at": now_ts + retry_seconds,
                "last_error_type": str(error_type or "NodeExecutionError")[:100],
                "last_error": str(error or "node_execution_failed")[:300],
            })
        attempts[str(execution_key)] = updated
        store["version"] = VERSION
        write_snapshot_store(store)
        return updated


def learning_node_execution_plan(now_ts: Optional[int] = None, store_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Plan node updates only for fixtures already admitted by the daily governance cycle."""
    now_ts = int(now_ts or time.time())
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    postmatches = store.get("learning_postmatch") or {}
    frozen = store.get("learning_frozen") or {}
    admissions = dict(store.get("learning_admissions") or {})
    # Compatibility for already frozen fixtures created before the admission
    # ledger existed. They remain eligible for node continuation only.
    for fixture, versions in frozen.items():
        latest = max(
            (row for row in versions or [] if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0),
            default=None,
        )
        if latest and str(fixture) not in admissions:
            admissions[str(fixture)] = {
                "fixture": str(fixture), "kickoff_at": latest.get("kickoff_at"),
                "scope": latest.get("scope"), "admitted_at": latest.get("captured_at"),
                "admission_hash": None, "legacy_from_freeze": True,
            }
    due, skipped = [], []
    for fixture, admission in admissions.items():
        versions = frozen.get(str(fixture)) or []
        latest = max(
            (row for row in versions if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0),
            default=None,
        )
        freeze_id = str((latest or {}).get("freeze_id") or "")
        kickoff_at = int(admission.get("kickoff_at") or (latest or {}).get("kickoff_at") or 0)
        if (freeze_id and freeze_id in postmatches) or now_ts >= kickoff_at:
            skipped.append({"fixture": str(fixture), "reason": "already_settled_or_kicked_off"})
            continue
        scope_audit = audit_learning_scope(admission.get("scope") or (latest or {}).get("scope"))
        if not scope_audit["eligible"]:
            skipped.append({
                "fixture": str(fixture), "reason": "major_league_registry_no_longer_eligible",
                "scope_reasons": scope_audit["reasons"],
            })
            continue
        candidate = {
            "fixture_id": str(fixture),
            "timestamp": kickoff_at,
            "date": datetime.fromtimestamp(kickoff_at, tz=timezone.utc).isoformat(),
            "status": "NS",
            "scope": scope_audit["normalized"],
            "admission_freeze_id": freeze_id or None,
            "admission_hash": admission.get("admission_hash"),
            "admission_registry_version": admission.get("major_league_registry_version")
                or get_nested(admission, ["scope", "major_league_registry_version"]),
        }
        state = learning_candidate_analysis_state(candidate, store, now_ts)
        row = {**candidate, **state}
        row["execution_key"] = learning_node_execution_key(row)
        if state["analysis_due"]:
            attempt_gate = _learning_node_attempt_gate(row["execution_key"], now_ts, store)
            if attempt_gate:
                attempt = (store.get("learning_node_attempts") or {}).get(row["execution_key"]) or {}
                skipped.append({
                    "fixture": str(fixture), "reason": attempt_gate,
                    "target_analysis_node": state["target_analysis_node"],
                    "execution_key": row["execution_key"],
                    "attempt_count": int(attempt.get("attempt_count") or 0),
                    "retry_after_at": attempt.get("retry_after_at"),
                    "lease_expires_at": attempt.get("lease_expires_at"),
                })
            else:
                due.append(row)
        else:
            skipped.append({
                "fixture": str(fixture),
                "reason": state["analysis_due_reason"],
                "latest_analysis_node": state["latest_analysis_node"],
                "target_analysis_node": state["target_analysis_node"],
            })
    due.sort(key=lambda row: (int(row.get("timestamp") or 0), str(row.get("fixture_id"))))
    return {
        "version": VERSION,
        "generated_at": now_ts,
        "due_count": len(due),
        "due": due,
        "skipped_count": len(skipped),
        "skipped": skipped[:100],
        "discovery_performed": False,
        "scope_expansion_allowed": False,
        "allowed_nodes": LEARNING_PREMATCH_STAGE_ORDER,
        "policy": "node executor may only version fixtures previously admitted by the daily governance cycle; it cannot discover or expand scope",
    }


def run_learning_node_executor(
    now_ts: Optional[int] = None,
    apply_changes: bool = True,
    prematch_packet_builder: Optional[Any] = None,
) -> Dict[str, Any]:
    """Advance PIT analysis nodes without rediscovering fixtures or running postmatch learning."""
    now_ts = int(now_ts or time.time())
    if not isinstance(apply_changes, bool):
        raise HTTPException(status_code=400, detail="apply_changes_must_be_boolean")
    plan = learning_node_execution_plan(now_ts=now_ts)
    builder = prematch_packet_builder or build_shadow_ai_packet
    results = []
    for candidate in plan["due"]:
        fixture = str(candidate.get("fixture_id") or "")
        if not apply_changes:
            results.append({
                "fixture": fixture,
                "action": "would_advance_node",
                "target_analysis_node": candidate.get("target_analysis_node"),
                "reason": candidate.get("analysis_due_reason"),
            })
            continue
        claim = claim_learning_node_execution(candidate, now_ts=now_ts)
        if not claim.get("claimed"):
            results.append({
                "fixture": fixture, "action": "skipped", "reason": claim.get("reason"),
                "execution_key": claim.get("execution_key"),
                "attempt_count": claim.get("attempt_count"),
                "retry_after_at": claim.get("retry_after_at"),
                "lease_expires_at": claim.get("lease_expires_at"),
            })
            continue
        execution_key = str(claim.get("execution_key") or "")
        claim_token = str(claim.get("claim_token") or "")
        try:
            packet = builder(int(fixture))
            freeze_payload = build_learning_freeze_payload(candidate, packet, now_ts=now_ts)
            record = freeze_learning_sample(freeze_payload, now_ts=now_ts)
            complete_learning_node_execution(
                execution_key, claim_token, "completed", now_ts=now_ts,
                freeze_id=record.get("freeze_id"),
            )
            results.append({
                "fixture": fixture,
                "freeze_id": record.get("freeze_id"),
                "action": record.get("action"),
                "analysis_node": get_nested(record, ["analysis", "analysis_node"]),
                "reason": candidate.get("analysis_due_reason"),
                "source_snapshot_hash": get_nested(record, ["analysis", "source_snapshot_hash"]),
                "execution_key": execution_key,
                "attempt_count": claim.get("attempt_count"),
            })
        except HTTPException as exc:
            try:
                complete_learning_node_execution(
                    execution_key, claim_token, "failed", now_ts=now_ts,
                    error_type="HTTPException", error=exc.detail,
                )
            except HTTPException:
                pass
            results.append({
                "fixture": fixture, "action": "rejected",
                "status_code": exc.status_code, "reason": exc.detail,
                "execution_key": execution_key, "attempt_count": claim.get("attempt_count"),
            })
        except Exception as exc:
            try:
                complete_learning_node_execution(
                    execution_key, claim_token, "failed", now_ts=now_ts,
                    error_type=type(exc).__name__, error="prematch_node_packet_build_failed",
                )
            except HTTPException:
                pass
            results.append({
                "fixture": fixture, "action": "rejected",
                "reason": "prematch_node_packet_build_failed", "error_type": type(exc).__name__,
                "execution_key": execution_key, "attempt_count": claim.get("attempt_count"),
            })
    node_priority_board = {
        "schema": "learning_prematch_priority_board_v1", "action": "skipped",
        "reason": "no_learning_node_advanced", "outcome_fields_used": False,
        "result_backfit_allowed": False,
    }
    if apply_changes and any(row.get("action") in {"frozen", "unchanged"} for row in results):
        priority_store = load_snapshot_store()
        active_freezes = []
        active_fixture_ids = set((priority_store.get("learning_admissions") or {})) | set((priority_store.get("learning_frozen") or {}))
        for fixture in active_fixture_ids:
            admission = (priority_store.get("learning_admissions") or {}).get(str(fixture)) or {}
            latest = max(
                (row for row in ((priority_store.get("learning_frozen") or {}).get(str(fixture)) or []) if isinstance(row, dict)),
                key=lambda row: int(row.get("version_number") or 0), default=None,
            )
            kickoff_at = int(admission.get("kickoff_at") or (latest or {}).get("kickoff_at") or 0)
            if latest and kickoff_at > now_ts:
                active_freezes.append(latest)
        board_identity = [
            {"fixture": row.get("fixture"), "freeze_id": row.get("freeze_id"), "freeze_hash": row.get("content_hash")}
            for row in sorted(active_freezes, key=lambda row: str(row.get("fixture") or ""))
        ]
        board_id = f"node-{now_ts}-{_content_hash(board_identity)[:16]}"
        candidate_board = build_learning_priority_board(board_id, active_freezes, now_ts)
        if candidate_board.get("match_priority_ranking") or candidate_board.get("non_participating_records"):
            node_priority_board = persist_learning_priority_board(candidate_board)
    return {
        "version": VERSION,
        "generated_at": now_ts,
        "plan": plan,
        "results": results,
        "advanced_count": sum(row.get("action") == "frozen" for row in results),
        "unchanged_count": sum(row.get("action") == "unchanged" for row in results),
        "lease_skipped_count": sum(row.get("action") == "skipped" for row in results),
        "rejected_count": sum(row.get("action") == "rejected" for row in results),
        "learning_priority_board": node_priority_board,
        "discovery_performed": False,
        "postmatch_learning_performed": False,
        "automatic_champion_change": False,
    }


def automatic_learning_data_health(
    now_ts: Optional[int] = None, store_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Audit admitted-fixture progress and retry state without calling providers."""
    now_ts = int(now_ts or time.time())
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    admissions = store.get("learning_admissions") or {}
    frozen = store.get("learning_frozen") or {}
    postmatches = store.get("learning_postmatch") or {}
    attempts = store.get("learning_node_attempts") or {}
    grace_seconds = max(AUTO_SNAPSHOT_POLL_SECONDS * 2, 600)
    records = []
    expired_unfrozen_count = 0
    overdue_node_count = 0
    postmatch_overdue_count = 0
    missing_checkpoint_count = 0
    active_execution_keys = set()
    for fixture, admission in admissions.items():
        if not isinstance(admission, dict):
            continue
        kickoff_at = int(admission.get("kickoff_at") or 0)
        versions = [row for row in (frozen.get(str(fixture)) or []) if isinstance(row, dict)]
        latest = max(versions, key=lambda row: int(row.get("version_number") or 0), default=None)
        latest_node = _learning_freeze_analysis_node(latest or {})
        present_nodes = {
            node for node in (_learning_freeze_analysis_node(row) for row in versions)
            if node in LEARNING_PREMATCH_STAGE_ORDER
        }
        missing_checkpoints = []
        if kickoff_at > 0:
            seconds_before = kickoff_at - now_ts
            for node, threshold in (("T-12h", 12 * 3600), ("T-6h", 6 * 3600), ("T-1h", 3600)):
                if seconds_before <= threshold and node not in present_nodes:
                    missing_checkpoints.append(node)
        missing_checkpoint_count += len(missing_checkpoints)
        state = "waiting_for_first_node"
        execution_key = None
        retry_after_at = None
        if kickoff_at <= 0:
            state = "invalid_admission"
        elif now_ts >= kickoff_at:
            if not latest:
                state = "expired_without_freeze"
                expired_unfrozen_count += 1
            elif latest.get("freeze_id") in postmatches:
                state = "postmatch_completed"
            elif now_ts - kickoff_at > LEARNING_POSTMATCH_LOOKBACK_HOURS * 3600:
                state = "postmatch_window_overdue"
                postmatch_overdue_count += 1
            elif now_ts - kickoff_at >= 2 * 3600:
                state = "postmatch_due"
            else:
                state = "awaiting_postmatch_window"
        else:
            candidate = {
                "fixture_id": str(fixture), "timestamp": kickoff_at, "status": "NS",
                "scope": admission.get("scope") or (latest or {}).get("scope"),
            }
            scope_audit = audit_learning_scope(candidate["scope"])
            if not scope_audit["eligible"]:
                state = "registry_ineligible"
            else:
                analysis_state = learning_candidate_analysis_state(candidate, store, now_ts)
                if analysis_state["analysis_due"]:
                    node_job = {**candidate, **analysis_state}
                    execution_key = learning_node_execution_key(node_job)
                    active_execution_keys.add(execution_key)
                    attempt = attempts.get(execution_key) if isinstance(attempts.get(execution_key), dict) else {}
                    gate = _learning_node_attempt_gate(execution_key, now_ts, store)
                    retry_after_at = attempt.get("retry_after_at")
                    state = {
                        "node_execution_lease_active": "node_running",
                        "node_retry_backoff_active": "node_retry_backoff",
                        "node_attempt_already_completed": "node_completed_pending_state_refresh",
                    }.get(gate, "node_due")
                    target_node = analysis_state.get("target_analysis_node")
                    threshold = {"T-12h": 12 * 3600, "T-6h": 6 * 3600, "T-1h": 3600}.get(target_node)
                    due_at = kickoff_at - threshold if threshold is not None else int(analysis_state.get("source_snapshot_at") or now_ts)
                    if now_ts > due_at + grace_seconds and state in {"node_due", "node_retry_backoff"}:
                        overdue_node_count += 1
                elif latest:
                    state = "node_current"
        records.append({
            "fixture": str(fixture), "kickoff_at": kickoff_at, "state": state,
            "latest_analysis_node": latest_node, "frozen_version_count": len(versions),
            "missing_checkpoints": missing_checkpoints,
            "execution_key": execution_key, "retry_after_at": retry_after_at,
        })
    records.sort(key=lambda row: (row["kickoff_at"], row["fixture"]))
    attempt_rows = [row for row in attempts.values() if isinstance(row, dict)]
    attempt_status_counts = {
        status: sum(row.get("status") == status for row in attempt_rows)
        for status in ("running", "failed", "completed")
    }
    unresolved_failed_attempt_count = sum(
        row.get("status") == "failed" and row.get("execution_key") in active_execution_keys
        for row in attempt_rows
    )
    stale_lease_count = sum(
        row.get("status") == "running" and int(row.get("lease_expires_at") or 0) <= now_ts
        and row.get("execution_key") in active_execution_keys for row in attempt_rows
    )
    priority_boards = [row for row in (store.get("learning_priority_boards") or {}).values() if isinstance(row, dict)]
    invalid_priority_board_count = sum(
        audit_learning_priority_board(board).get("status") != "passed" for board in priority_boards
    )
    unranked_active_freeze_count = 0
    for fixture, admission in admissions.items():
        if not isinstance(admission, dict) or int(admission.get("kickoff_at") or 0) <= now_ts:
            continue
        latest = max(
            (row for row in (frozen.get(str(fixture)) or []) if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0), default=None,
        )
        latest_decision = (latest or {}).get("decision") if isinstance((latest or {}).get("decision"), dict) else {}
        execution_action = latest_decision.get("execution_action") or latest_decision.get("decision")
        if latest and execution_action == "BET" and _learning_priority_context(store, latest) is None:
            unranked_active_freeze_count += 1
    observation_plan = market_language_settlement_plan(now_ts=now_ts, store_override=store)
    observation_due_count = observation_plan["due_count"]
    postmatch_recovery = learning_postmatch_recovery_queue(now_ts=now_ts, store_override=store)
    postmatch_overdue_count = postmatch_recovery["manual_recovery_required_count"]
    critical = expired_unfrozen_count > 0 or invalid_priority_board_count > 0
    warning = overdue_node_count > 0 or postmatch_overdue_count > 0 or unresolved_failed_attempt_count > 0 or stale_lease_count > 0 or unranked_active_freeze_count > 0
    return {
        "version": VERSION, "generated_at": now_ts,
        "status": "critical" if critical else ("warning" if warning else "healthy"),
        "admitted_fixture_count": len(admissions),
        "state_counts": {
            state: sum(row["state"] == state for row in records)
            for state in sorted({row["state"] for row in records})
        },
        "expired_unfrozen_count": expired_unfrozen_count,
        "overdue_node_count": overdue_node_count,
        "postmatch_overdue_count": postmatch_overdue_count,
        "postmatch_recovery_queue_count": postmatch_recovery["queue_count"],
        "postmatch_verified_facts_awaiting_review_count": postmatch_recovery["verified_facts_awaiting_review_count"],
        "missing_checkpoint_count": missing_checkpoint_count,
        "node_attempt_count": len(attempt_rows),
        "node_attempt_status_counts": attempt_status_counts,
        "unresolved_failed_node_attempt_count": unresolved_failed_attempt_count,
        "stale_node_lease_count": stale_lease_count,
        "learning_priority_board_count": len(priority_boards),
        "invalid_learning_priority_board_count": invalid_priority_board_count,
        "unranked_active_freeze_count": unranked_active_freeze_count,
        "market_language_settlement_due_count": observation_due_count,
        "market_language_settlement_ready_count": observation_plan["settlement_ready_count"],
        "records": records[:100], "records_truncated": len(records) > 100,
        "provider_calls_performed": False,
        "result_backfit_used": False,
        "automatic_champion_change": False,
    }


def _thestats_payload_data(response: Dict[str, Any]) -> Any:
    payload = response.get("data")
    return payload.get("data") if isinstance(payload, dict) and "data" in payload else payload


def _provider_team_name(value: Any) -> str:
    if isinstance(value, dict):
        value = value.get("name") or value.get("team_name") or value.get("label")
    return str(value or "").strip()


def _thestats_match_identity(row: Any) -> Dict[str, Any]:
    row = row if isinstance(row, dict) else {}
    home = row.get("home_team") if row.get("home_team") is not None else row.get("home")
    away = row.get("away_team") if row.get("away_team") is not None else row.get("away")
    return {
        "match_id": str(row.get("id") or row.get("match_id") or "").strip(),
        "kickoff_at": _parse_timestamp(row.get("utc_date") or row.get("kickoff_utc") or row.get("date")),
        "home_team": _provider_team_name(home),
        "away_team": _provider_team_name(away),
        "status": str(row.get("status") or "").strip().casefold(),
    }


def _thestats_matches_for_utc_date(date_utc: str) -> Dict[str, Any]:
    """Fetch one complete UTC match day once per short-lived process cache window."""
    credential_fingerprint = hashlib.sha256(THESTATS_API_KEY.encode("utf-8")).hexdigest()[:12] if THESTATS_API_KEY else "missing"
    cache_key = f"{THESTATS_BASE_URL}|{credential_fingerprint}|{date_utc}"
    now_ts = int(time.time())
    with THESTATS_FIXTURE_DAY_CACHE_LOCK:
        cached = THESTATS_FIXTURE_DAY_CACHE.get(cache_key)
        if cached and now_ts - int(cached.get("fetched_at") or 0) <= THESTATS_FIXTURE_DAY_CACHE_TTL_SECONDS:
            return {"ok": True, "rows": copy.deepcopy(cached.get("rows") or []), "cache_hit": True, "page_count": cached.get("page_count")}
        rows = []
        total_pages = 1
        for page in range(1, 11):
            if page > total_pages:
                break
            response = call_thestats("/football/matches", {
                "date_from": date_utc, "date_to": date_utc, "per_page": 100, "page": page,
            })
            if response.get("ok") is not True:
                return {"ok": False, "error": "thestats_fixture_lookup_failed", "status_code": response.get("status_code"), "cache_hit": False}
            payload = response.get("data") if isinstance(response.get("data"), dict) else {}
            page_rows = _thestats_payload_data(response)
            if not isinstance(page_rows, list):
                return {"ok": False, "error": "thestats_fixture_lookup_schema_invalid", "status_code": response.get("status_code"), "cache_hit": False}
            rows.extend(row for row in page_rows if isinstance(row, dict))
            meta = payload.get("meta") if isinstance(payload.get("meta"), dict) else {}
            try:
                total_pages = max(1, int(meta.get("total_pages") or 1))
            except (TypeError, ValueError):
                return {"ok": False, "error": "thestats_fixture_pagination_invalid", "cache_hit": False}
            if total_pages > 10:
                return {"ok": False, "error": "thestats_fixture_pagination_exceeds_safe_bound", "page_count": total_pages, "cache_hit": False}
        record = {"fetched_at": now_ts, "rows": copy.deepcopy(rows), "page_count": total_pages}
        THESTATS_FIXTURE_DAY_CACHE[cache_key] = record
        if len(THESTATS_FIXTURE_DAY_CACHE) > 16:
            evictable = [key for key in THESTATS_FIXTURE_DAY_CACHE if key != cache_key]
            oldest = min(evictable, key=lambda key: int(THESTATS_FIXTURE_DAY_CACHE[key].get("fetched_at") or 0))
            THESTATS_FIXTURE_DAY_CACHE.pop(oldest, None)
        return {"ok": True, "rows": rows, "cache_hit": False, "page_count": total_pages}


def _resolve_thestats_prematch_identity(fixture: Dict[str, Any], observed_at: Any) -> Dict[str, Any]:
    """Bind a unique TheStats match id before kickoff; ambiguity always fails closed."""
    if not THESTATS_API_KEY:
        return {"ok": False, "error": "thestats_not_configured"}
    kickoff_at = _parse_timestamp(fixture.get("timestamp") or fixture.get("date"))
    observed_at = _parse_timestamp(observed_at)
    home_team = str(fixture.get("home") or "").strip()
    away_team = str(fixture.get("away") or "").strip()
    if (
        kickoff_at is None or observed_at is None or observed_at >= kickoff_at
        or not home_team or not away_team
    ):
        return {"ok": False, "error": "thestats_prematch_identity_inputs_invalid"}
    date_utc = datetime.fromtimestamp(kickoff_at, tz=timezone.utc).date().isoformat()
    candidates = []
    day = _thestats_matches_for_utc_date(date_utc)
    if day.get("ok") is not True:
        return day
    for row in day.get("rows") or []:
        identity = _thestats_match_identity(row)
        if (
            identity["match_id"]
            and identity["status"] not in {"live", "finished", "cancelled", "canceled", "abandoned", "forfeit"}
            and identity["kickoff_at"] is not None
            and abs(identity["kickoff_at"] - kickoff_at) <= 900
            and normalize_fixture_identity_name(identity["home_team"]) == normalize_fixture_identity_name(home_team)
            and normalize_fixture_identity_name(identity["away_team"]) == normalize_fixture_identity_name(away_team)
        ):
            candidates.append(identity)
    unique = {row["match_id"]: row for row in candidates}
    if len(unique) != 1:
        return {"ok": False, "error": "thestats_fixture_identity_not_unique", "match_count": len(unique)}
    match = next(iter(unique.values()))
    content = {
        "source": "thestats", "match_id": match["match_id"],
        "kickoff_at": kickoff_at, "home_team": home_team, "away_team": away_team,
    }
    return {
        "ok": True, **content, "source_hash": _content_hash(content),
        "lookup_cache_hit": day.get("cache_hit"), "lookup_page_count": day.get("page_count"),
    }


def _parse_event_minute(value: Any, offset: Any = None) -> Tuple[int, Optional[int]]:
    raw = str(value or "0").strip()
    base, plus = raw, None
    if "+" in raw:
        base, plus = raw.split("+", 1)
    try:
        minute = max(0, int(float(base)))
    except (TypeError, ValueError):
        minute = 0
    if offset is not None:
        plus = offset
    try:
        extra = max(0, int(float(plus))) if plus not in (None, "") else None
    except (TypeError, ValueError):
        extra = None
    return minute, extra


def _normalize_thestats_events(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, dict):
        value = value.get("events") if isinstance(value.get("events"), list) else value.get("timeline")
    rows = value if isinstance(value, list) else []
    normalized = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("type") or row.get("event_type") or "").strip()
        raw_detail = row.get("detail") or row.get("subtype") or row.get("card_type") or row.get("outcome") or ""
        detail = str(raw_detail).strip()
        lowered = f"{event_type} {detail}".casefold().replace("_", " ").replace("-", " ")
        if "goal" in lowered:
            event_type = "Goal"
        elif "card" in lowered:
            event_type = "Card"
        elif "penalty" in lowered:
            event_type = "Penalty"
        elif "substitution" in lowered or "substitute" in lowered:
            event_type = "Substitution"
        elif "var" in lowered:
            event_type = "VAR"
        if row.get("own_goal") is True or "own goal" in lowered:
            detail = "Own Goal"
        elif row.get("penalty") is True or "penalty" in lowered:
            detail = "Penalty"
        elif "red" in lowered and "card" in lowered:
            detail = "Red Card"
        minute, extra = _parse_event_minute(row.get("minute") or row.get("elapsed"), row.get("offset") or row.get("extra"))
        team = _provider_team_name(row.get("team") or row.get("team_name"))
        player = _provider_team_name(row.get("player") or row.get("scorer") or row.get("shooter"))
        normalized.append({
            "elapsed": minute, "extra": extra, "team": team or None,
            "player": player or None, "type": event_type or None, "detail": detail or None,
        })
    return normalized


def _material_event_signature(events: Any, home_team: Any = None, away_team: Any = None) -> List[Dict[str, Any]]:
    home_key = normalize_fixture_identity_name(home_team)
    away_key = normalize_fixture_identity_name(away_team)
    signature = []
    for row in events if isinstance(events, list) else []:
        if not isinstance(row, dict):
            continue
        event_type = str(row.get("type") or "").strip().casefold()
        detail = str(row.get("detail") or "").strip().casefold()
        minute, extra = _parse_event_minute(row.get("elapsed") or row.get("minute"), row.get("extra") or row.get("offset"))
        team_key = normalize_fixture_identity_name(row.get("team"))
        side = "home" if home_key and team_key == home_key else ("away" if away_key and team_key == away_key else team_key or "unknown")
        categories = []
        if event_type == "goal":
            categories.append("goal")
        if event_type == "card" and ("red" in detail or "second yellow" in detail):
            categories.append("red_card")
        if "penalty" in event_type or "penalty" in detail:
            categories.append("penalty_event")
        if "own goal" in detail:
            categories.append("own_goal")
        for category in categories:
            signature.append({"category": category, "side": side, "minute": minute, "extra": extra})
    return sorted(signature, key=lambda row: (row["category"], row["side"], row["minute"], row.get("extra") or 0))


def _material_event_sequences_match(left: List[Dict[str, Any]], right: List[Dict[str, Any]], total_goals: int) -> bool:
    if sum(row["category"] == "goal" for row in left) != total_goals or sum(row["category"] == "goal" for row in right) != total_goals:
        return False
    if len(left) != len(right):
        return False
    return all(
        a["category"] == b["category"] and a["side"] == b["side"]
        and abs((a["minute"] + (a.get("extra") or 0)) - (b["minute"] + (b.get("extra") or 0))) <= 2
        for a, b in zip(left, right)
    )


def _the_odds_api_frozen_result_evidence(freeze: Dict[str, Any]) -> Dict[str, Any]:
    """Corroborate a result only through a The Odds API identity frozen before kickoff."""
    identity = next((
        row for row in (freeze.get("source_refs") or [])
        if isinstance(row, dict) and row.get("source") == "the_odds_api"
    ), None)
    if not identity:
        return {"ok": False, "error": "frozen_the_odds_api_identity_missing"}
    sport_key = str(identity.get("sport_key") or "").strip().lower()
    event_id = str(identity.get("event_id") or "").strip().lower()
    home_team = str(identity.get("home_team") or "").strip()
    away_team = str(identity.get("away_team") or "").strip()
    bound_at = _parse_timestamp(identity.get("identity_bound_at"))
    identity_content = {
        "source": "the_odds_api", "sport_key": sport_key, "event_id": event_id,
        "home_team": home_team, "away_team": away_team,
    }
    if (
        not sport_key or any(char not in "abcdefghijklmnopqrstuvwxyz0123456789_" for char in sport_key)
        or len(event_id) != 32 or any(char not in "0123456789abcdef" for char in event_id)
        or not home_team or not away_team
        or bound_at is None or bound_at >= int(freeze.get("kickoff_at") or 0)
        or identity.get("identity_source_hash") != _content_hash(identity_content)
    ):
        return {"ok": False, "error": "frozen_the_odds_api_identity_invalid"}
    response = call_the_odds_api(
        f"/sports/{sport_key}/scores", {"daysFrom": 3, "dateFormat": "iso", "eventIds": event_id},
    )
    if response.get("ok") is not True:
        return {
            "ok": False, "error": "the_odds_api_score_request_failed",
            "status_code": response.get("status_code"),
        }
    rows = response.get("data") if isinstance(response.get("data"), list) else []
    matches = [row for row in rows if isinstance(row, dict) and str(row.get("id") or "").strip().lower() == event_id]
    if len(matches) != 1:
        return {"ok": False, "error": "the_odds_api_bound_event_score_missing", "status_code": response.get("status_code")}
    event = matches[0]
    if event.get("completed") is not True:
        return {"ok": False, "error": "the_odds_api_bound_event_not_completed", "status_code": response.get("status_code")}
    if str(event.get("sport_key") or "").strip().lower() != sport_key:
        return {"ok": False, "error": "the_odds_api_bound_event_sport_mismatch", "status_code": response.get("status_code")}
    if (
        normalize_fixture_identity_name(event.get("home_team")) != normalize_fixture_identity_name(home_team)
        or normalize_fixture_identity_name(event.get("away_team")) != normalize_fixture_identity_name(away_team)
    ):
        return {"ok": False, "error": "the_odds_api_bound_event_team_mismatch", "status_code": response.get("status_code")}
    scores = event.get("scores") if isinstance(event.get("scores"), list) else []
    normalized_score_rows = [
        (normalize_fixture_identity_name(row.get("name")), row.get("score"))
        for row in scores if isinstance(row, dict) and str(row.get("name") or "").strip()
    ]
    expected_score_names = {
        normalize_fixture_identity_name(home_team), normalize_fixture_identity_name(away_team),
    }
    if (
        len(normalized_score_rows) != 2
        or len({name for name, _ in normalized_score_rows}) != 2
        or {name for name, _ in normalized_score_rows} != expected_score_names
    ):
        return {"ok": False, "error": "the_odds_api_bound_event_score_incomplete", "status_code": response.get("status_code")}
    score_by_team = dict(normalized_score_rows)
    try:
        home_goals = int(score_by_team[normalize_fixture_identity_name(home_team)])
        away_goals = int(score_by_team[normalize_fixture_identity_name(away_team)])
    except (KeyError, TypeError, ValueError):
        return {"ok": False, "error": "the_odds_api_bound_event_score_incomplete", "status_code": response.get("status_code")}
    if min(home_goals, away_goals) < 0:
        return {"ok": False, "error": "the_odds_api_bound_event_score_invalid", "status_code": response.get("status_code")}
    return {
        "ok": True, "source": "the_odds_api", "component": "result",
        "home_goals": home_goals, "away_goals": away_goals,
        "evidence_ref": f"https://api.the-odds-api.com/v4/sports/{sport_key}/scores?eventIds={event_id}",
        "status_code": response.get("status_code"), "event_id": event_id,
        "identity_source_hash": identity.get("identity_source_hash"),
    }


def _thestats_frozen_postmatch_evidence(
    freeze: Dict[str, Any], api_events: List[Dict[str, Any]], expected_result: Dict[str, Any],
) -> Dict[str, Any]:
    """Verify a frozen TheStats result and material event sequence against API-Football."""
    identity = next((
        row for row in (freeze.get("source_refs") or [])
        if isinstance(row, dict) and row.get("source") == "thestats"
    ), None)
    if not identity:
        return {"ok": False, "error": "frozen_thestats_identity_missing"}
    match_id = str(identity.get("match_id") or "").strip()
    home_team = str(identity.get("home_team") or "").strip()
    away_team = str(identity.get("away_team") or "").strip()
    kickoff_at = _parse_timestamp(identity.get("kickoff_at"))
    bound_at = _parse_timestamp(identity.get("identity_bound_at"))
    identity_content = {
        "source": "thestats", "match_id": match_id, "kickoff_at": kickoff_at,
        "home_team": home_team, "away_team": away_team,
    }
    if (
        not match_id or len(match_id) > 128 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in match_id)
        or not home_team or not away_team or kickoff_at is None or bound_at is None
        or bound_at >= int(freeze.get("kickoff_at") or 0)
        or identity.get("identity_source_hash") != _content_hash(identity_content)
    ):
        return {"ok": False, "error": "frozen_thestats_identity_invalid"}
    detail_response = call_thestats(f"/football/matches/{match_id}")
    if detail_response.get("ok") is not True:
        return {"ok": False, "error": "thestats_match_detail_failed", "status_code": detail_response.get("status_code")}
    match = _thestats_payload_data(detail_response)
    match = match if isinstance(match, dict) else {}
    parsed = _thestats_match_identity(match)
    if (
        parsed["match_id"] != match_id
        or parsed["status"] not in {"finished", "ft", "aet", "pen"}
        or parsed["kickoff_at"] is None or abs(parsed["kickoff_at"] - kickoff_at) > 900
        or normalize_fixture_identity_name(parsed["home_team"]) != normalize_fixture_identity_name(home_team)
        or normalize_fixture_identity_name(parsed["away_team"]) != normalize_fixture_identity_name(away_team)
    ):
        return {"ok": False, "error": "thestats_bound_match_identity_mismatch", "status_code": detail_response.get("status_code")}
    score = match.get("score") if isinstance(match.get("score"), dict) else {}
    home_score_value = score.get("home")
    away_score_value = score.get("away")
    if home_score_value is None and isinstance(match.get("home_team"), dict):
        home_score_value = match["home_team"].get("score")
    if away_score_value is None and isinstance(match.get("away_team"), dict):
        away_score_value = match["away_team"].get("score")
    try:
        home_goals, away_goals = int(home_score_value), int(away_score_value)
        expected_home = int(expected_result.get("home_goals"))
        expected_away = int(expected_result.get("away_goals"))
    except (TypeError, ValueError):
        return {"ok": False, "error": "thestats_bound_match_score_incomplete", "status_code": detail_response.get("status_code")}
    if min(home_goals, away_goals) < 0 or (home_goals, away_goals) != (expected_home, expected_away):
        return {"ok": False, "error": "thestats_bound_match_score_mismatch", "status_code": detail_response.get("status_code")}
    timeline_response = call_thestats(f"/football/matches/{match_id}/timeline")
    timeline_data = _thestats_payload_data(timeline_response) if timeline_response.get("ok") is True else None
    thestats_events = _normalize_thestats_events(timeline_data)
    api_signature = _material_event_signature(api_events, home_team, away_team)
    thestats_signature = _material_event_signature(thestats_events, home_team, away_team)
    sequence_matches = bool(timeline_response.get("ok") is True) and _material_event_sequences_match(
        api_signature, thestats_signature, home_goals + away_goals,
    )
    signature_hash = _content_hash(api_signature) if sequence_matches else None
    return {
        "ok": True,
        "result_audit": {
            "source": "thestats", "component": "result", "ok": True,
            "home_goals": home_goals, "away_goals": away_goals,
            "evidence_ref": f"https://api.thestatsapi.com/api/football/matches/{match_id}",
            "status_code": detail_response.get("status_code"), "match_id": match_id,
        },
        "event_audit": {
            "source": "thestats", "component": "events", "ok": sequence_matches,
            "evidence_ref": f"https://api.thestatsapi.com/api/football/matches/{match_id}/timeline",
            "status_code": timeline_response.get("status_code"),
            "event_signature_hash": signature_hash,
            "error": None if sequence_matches else "thestats_material_event_sequence_mismatch_or_unavailable",
        },
        "events": thestats_events,
    }


def _learning_postmatch_fact_fetch(freeze: Dict[str, Any]) -> Dict[str, Any]:
    """Fetch API-Football facts and independently corroborate the frozen result when possible."""
    fixture_id = int(freeze.get("fixture"))
    detail = call_api_football("/fixtures", {"id": fixture_id})
    row = response_first(detail)
    if not row:
        return {
            "ok": False,
            "error": "fixture_result_unavailable",
            "source_audit": [{"source": "api_football", "ok": False, "status_code": detail.get("status_code")}],
        }
    summary = fixture_summary(row)
    status = str(summary.get("status") or "").upper()
    goals = summary.get("goals") if isinstance(summary.get("goals"), dict) else {}
    if status not in {"FT", "AET", "PEN"} or goals.get("home") is None or goals.get("away") is None:
        return {
            "ok": False, "error": "verified_final_result_not_available", "fixture": summary,
            "source_audit": [{"source": "api_football", "ok": bool(detail.get("ok")), "status_code": detail.get("status_code")}],
        }
    events_response = call_api_football("/fixtures/events", {"fixture": fixture_id})
    statistics_response = call_api_football("/fixtures/statistics", {"fixture": fixture_id})
    events = []
    for event in response_list(events_response)[:200]:
        if not isinstance(event, dict):
            continue
        events.append({
            "elapsed": get_nested(event, ["time", "elapsed"]), "extra": get_nested(event, ["time", "extra"]),
            "team_id": get_nested(event, ["team", "id"]), "team": get_nested(event, ["team", "name"]),
            "player": get_nested(event, ["player", "name"]), "assist": get_nested(event, ["assist", "name"]),
            "type": event.get("type"), "detail": event.get("detail"), "comments": event.get("comments"),
        })
    allowed_statistics = {
        "Shots on Goal", "Shots off Goal", "Total Shots", "Blocked Shots", "Shots insidebox", "Shots outsidebox",
        "Fouls", "Corner Kicks", "Offsides", "Ball Possession", "Yellow Cards", "Red Cards", "Goalkeeper Saves",
        "Total passes", "Passes accurate", "Passes %", "expected_goals", "goals_prevented",
    }
    statistics = []
    for team_row in response_list(statistics_response):
        if not isinstance(team_row, dict):
            continue
        values = {
            str(item.get("type")): item.get("value")
            for item in (team_row.get("statistics") or [])
            if isinstance(item, dict) and str(item.get("type")) in allowed_statistics
        }
        statistics.append({"team_id": get_nested(team_row, ["team", "id"]), "team": get_nested(team_row, ["team", "name"]), "statistics": values})
    home_team_name = str(get_nested(row, ["teams", "home", "name"]) or "").strip()
    away_team_name = str(get_nested(row, ["teams", "away", "name"]) or "").strip()
    api_event_signature = _material_event_signature(events, home_team_name, away_team_name)
    api_event_signature_hash = _content_hash(api_event_signature)
    source_audit = [
        {"source": "api_football", "component": "result", "ok": bool(detail.get("ok")), "status_code": detail.get("status_code"), "evidence_ref": f"api_football:/fixtures?id={fixture_id}", "home_goals": int(goals["home"]), "away_goals": int(goals["away"])},
        {"source": "api_football", "component": "events", "ok": bool(events_response.get("ok")), "status_code": events_response.get("status_code"), "evidence_ref": f"api_football:/fixtures/events?fixture={fixture_id}", "event_signature_hash": api_event_signature_hash if events_response.get("ok") else None},
        {"source": "api_football", "component": "statistics", "ok": bool(statistics_response.get("ok")), "status_code": statistics_response.get("status_code"), "evidence_ref": f"api_football:/fixtures/statistics?fixture={fixture_id}"},
    ]
    odds_result = _the_odds_api_frozen_result_evidence(freeze)
    source_audit.append(odds_result if odds_result.get("ok") else {
        "source": "the_odds_api", "component": "result", "ok": False,
        "error": odds_result.get("error"), "status_code": odds_result.get("status_code"),
    })
    thestats = _thestats_frozen_postmatch_evidence(
        freeze, events, {"home_goals": int(goals["home"]), "away_goals": int(goals["away"])},
    )
    if thestats.get("ok") is True:
        source_audit.extend([thestats["result_audit"], thestats["event_audit"]])
    else:
        source_audit.extend([
            {"source": "thestats", "component": "result", "ok": False, "error": thestats.get("error"), "status_code": thestats.get("status_code")},
            {"source": "thestats", "component": "events", "ok": False, "error": thestats.get("error"), "status_code": thestats.get("status_code")},
        ])
    return {
        "ok": True,
        "result": {"status": status, "home_goals": int(goals["home"]), "away_goals": int(goals["away"])},
        "fixture": summary,
        "events": events,
        "statistics": statistics,
        "source_audit": source_audit,
    }


def collect_learning_postmatch_facts(freeze_id: Any, now_ts: Optional[int] = None, fact_fetcher: Optional[Any] = None) -> Dict[str, Any]:
    """Version postmatch facts without assigning process correctness or settling the sample."""
    freeze_id = str(freeze_id or "").strip()
    if not freeze_id:
        raise HTTPException(status_code=400, detail="freeze_id_required")
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    freeze = _learning_freeze_by_id(store, freeze_id)
    if not freeze:
        raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
    if now_ts < int(freeze.get("kickoff_at") or 0) + 2 * 3600:
        raise HTTPException(status_code=409, detail="postmatch_fact_collection_not_due")
    raw_fixture_id = freeze.get("fixture")
    if fact_fetcher is None:
        try:
            int(raw_fixture_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="provider_fixture_id_required_for_automatic_fact_collection") from exc
        fetched = _learning_postmatch_fact_fetch(freeze)
    else:
        fetched = fact_fetcher(raw_fixture_id)
    if not isinstance(fetched, dict) or fetched.get("ok") is not True:
        raise HTTPException(status_code=422, detail={"error": "postmatch_fact_fetch_incomplete", "reason": (fetched or {}).get("error") if isinstance(fetched, dict) else "invalid_fetcher_response"})
    result = fetched.get("result") if isinstance(fetched.get("result"), dict) else {}
    try:
        home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="postmatch_fact_result_incomplete") from exc
    if str(result.get("status") or "").upper() not in {"FT", "AET", "PEN"} or min(home_goals, away_goals) < 0:
        raise HTTPException(status_code=422, detail="postmatch_fact_result_not_final")
    verified_result_sources = []
    verified_result_authorities = []
    verified_evidence_refs = set()
    for row in fetched.get("source_audit") or []:
        if not isinstance(row, dict) or row.get("ok") is not True or str(row.get("component") or "").strip().lower() != "result":
            continue
        source = str(row.get("source") or "").strip().casefold()
        evidence_ref = str(row.get("evidence_ref") or row.get("url") or row.get("id") or "").strip()
        try:
            source_home, source_away = int(row.get("home_goals")), int(row.get("away_goals"))
        except (TypeError, ValueError):
            continue
        if source and evidence_ref and source_home == home_goals and source_away == away_goals:
            parsed = urlparse(evidence_ref)
            authority = str(parsed.hostname or source).strip().casefold()
            normalized_ref = evidence_ref.strip().casefold()
            if normalized_ref in verified_evidence_refs:
                continue
            verified_evidence_refs.add(normalized_ref)
            verified_result_sources.append(source)
            verified_result_authorities.append(authority)
    source_names = set(verified_result_sources)
    independent_authorities = set(verified_result_authorities)
    immutable_content = {
        "freeze_id": freeze_id,
        "fixture": freeze.get("fixture"),
        "freeze_hash": freeze.get("content_hash"),
        "result": {**result, "home_goals": home_goals, "away_goals": away_goals},
        "events": fetched.get("events") if isinstance(fetched.get("events"), list) else [],
        "statistics": fetched.get("statistics") if isinstance(fetched.get("statistics"), list) else [],
        "source_audit": fetched.get("source_audit") if isinstance(fetched.get("source_audit"), list) else [],
        "verification": {
            "independent_source_count": len(independent_authorities),
            "verified_source_names": sorted(source_names),
            "verified_source_authorities": sorted(independent_authorities),
            "unique_evidence_ref_count": len(verified_evidence_refs),
            "status": "verified" if len(independent_authorities) >= 2 else "single_source_pending",
            "settlement_eligible": len(independent_authorities) >= 2,
            "required_independent_sources": 2,
        },
        "process_classification": None,
        "result_backfit_used": False,
        "champion_effect": False,
    }
    fact_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        latest_store = load_snapshot_store()
        rows = latest_store.setdefault("learning_postmatch_facts", {}).setdefault(freeze_id, [])
        duplicate = next((row for row in rows if row.get("fact_hash") == fact_hash), None)
        if duplicate:
            return {**duplicate, "action": "unchanged"}
        version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
        record = {
            **immutable_content, "fact_hash": fact_hash, "version_number": version_number,
            "collected_at": now_ts, "action": "facts_collected", "immutable": True,
        }
        rows.append(record)
        latest_store["version"] = VERSION
        write_snapshot_store(latest_store)
        return record


def learning_postmatch_recovery_queue(
    now_ts: Optional[int] = None, store_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Expose unresolved frozen samples, including those beyond the automatic 36-hour window."""
    now_ts = int(now_ts or time.time())
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    postmatches = store.get("learning_postmatch") or {}
    facts = store.get("learning_postmatch_facts") or {}
    rows = []
    for fixture, versions in (store.get("learning_frozen") or {}).items():
        latest = max((row for row in versions or [] if isinstance(row, dict)), key=lambda row: int(row.get("version_number") or 0), default=None)
        if not latest or latest.get("freeze_id") in postmatches:
            continue
        kickoff_at = int(latest.get("kickoff_at") or 0)
        age_seconds = now_ts - kickoff_at
        if age_seconds < 2 * 3600:
            continue
        freeze_id = str(latest.get("freeze_id") or "")
        fact_versions = [row for row in (facts.get(freeze_id) or []) if isinstance(row, dict)]
        latest_fact = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
        verified = get_nested(latest_fact or {}, ["verification", "settlement_eligible"]) is True
        in_automatic_window = age_seconds <= LEARNING_POSTMATCH_LOOKBACK_HOURS * 3600
        if verified:
            state, required_action = "verified_facts_awaiting_review", "build_or_complete_hash_bound_review"
        elif in_automatic_window:
            state, required_action = "automatic_fact_collection_pending", "await_next_1430_cycle_or_collect_facts_now"
        else:
            state, required_action = "manual_fact_recovery_required", "invoke_protected_fact_collection_without_rebuilding_prematch"
        rows.append({
            "fixture": str(fixture), "freeze_id": freeze_id, "kickoff_at": kickoff_at,
            "hours_since_kickoff": round(age_seconds / 3600, 2),
            "state": state, "required_action": required_action,
            "automatic_window": in_automatic_window,
            "fact_hash": (latest_fact or {}).get("fact_hash"),
            "fact_verification_status": get_nested(latest_fact or {}, ["verification", "status"]) or "data_missing",
            "prematch_rebuild_allowed": False, "result_backfit_allowed": False,
        })
    rows.sort(key=lambda row: (row["kickoff_at"], row["fixture"]))
    return {
        "version": VERSION, "generated_at": now_ts,
        "queue_count": len(rows),
        "manual_recovery_required_count": sum(row["state"] == "manual_fact_recovery_required" for row in rows),
        "automatic_fact_collection_pending_count": sum(row["state"] == "automatic_fact_collection_pending" for row in rows),
        "verified_facts_awaiting_review_count": sum(row["state"] == "verified_facts_awaiting_review" for row in rows),
        "items": rows, "prematch_rebuild_allowed": False,
        "result_backfit_allowed": False, "automatic_champion_change": False,
    }


def _postmatch_event_evidence(facts: Dict[str, Any]) -> Dict[str, Any]:
    """Describe the realised state path and pollution flags without grading the prematch model."""
    events = [row for row in (facts.get("events") or []) if isinstance(row, dict)]
    goals, red_cards, penalties, own_goals = [], [], [], []
    for row in events:
        event_type = str(row.get("type") or "").strip().casefold()
        detail = str(row.get("detail") or "").strip().casefold()
        minute = int(as_float(row.get("elapsed")) or 0)
        normalized = {
            "minute": minute, "extra": row.get("extra"),
            "team_id": row.get("team_id"), "team": row.get("team"),
            "player": row.get("player"), "type": row.get("type"), "detail": row.get("detail"),
        }
        if event_type == "goal":
            goals.append(normalized)
        if event_type == "card" and ("red" in detail or "second yellow" in detail):
            red_cards.append(normalized)
        if "penalty" in detail:
            penalties.append(normalized)
        if "own goal" in detail:
            own_goals.append(normalized)
    fixture = facts.get("fixture") if isinstance(facts.get("fixture"), dict) else {}
    expected_event_signature_hash = _content_hash(_material_event_signature(
        events, fixture.get("home") or fixture.get("home_team_name"), fixture.get("away") or fixture.get("away_team_name"),
    ))
    event_sources, event_authorities, event_evidence_refs = set(), set(), set()
    for row in facts.get("source_audit") or []:
        if (
            not isinstance(row, dict) or row.get("ok") is not True
            or str(row.get("component") or "").strip().casefold() != "events"
        ):
            continue
        source = str(row.get("source") or "").strip().casefold()
        evidence_ref = str(row.get("evidence_ref") or "").strip()
        if not source or not evidence_ref or row.get("event_signature_hash") != expected_event_signature_hash:
            continue
        normalized_ref = evidence_ref.casefold()
        if normalized_ref in event_evidence_refs:
            continue
        parsed = urlparse(evidence_ref)
        event_evidence_refs.add(normalized_ref)
        event_sources.add(source)
        event_authorities.add(str(parsed.hostname or source).strip().casefold())
    pollution_flags = []
    if red_cards:
        pollution_flags.append("red_card")
    if penalties:
        pollution_flags.append("penalty_event")
    if own_goals:
        pollution_flags.append("own_goal")
    first_goal = min(goals, key=lambda row: (row["minute"], int(row.get("extra") or 0)), default=None)
    result = facts.get("result") if isinstance(facts.get("result"), dict) else {}
    return {
        "event_count": len(events),
        "event_source_count": len(event_authorities),
        "event_sources": sorted(event_sources),
        "event_source_authorities": sorted(event_authorities),
        "event_unique_evidence_ref_count": len(event_evidence_refs),
        "event_signature_hash": expected_event_signature_hash,
        "event_verification": "verified" if len(event_authorities) >= 2 else ("single_source" if event_authorities else "data_missing"),
        "first_goal": first_goal,
        "goal_count_in_event_feed": len(goals),
        "red_cards": red_cards,
        "penalties": penalties,
        "own_goals": own_goals,
        "pollution_flags": pollution_flags,
        "pollution_status": "requires_human_review" if pollution_flags else ("clean_verified" if len(event_authorities) >= 2 else "clean_unverified"),
        "actual_state_path": {
            "first_goal_side_or_team": (first_goal or {}).get("team"),
            "first_goal_minute": (first_goal or {}).get("minute"),
            "final_home_goals": result.get("home_goals"),
            "final_away_goals": result.get("away_goals"),
            "red_card_count": len(red_cards),
            "penalty_event_count": len(penalties),
            "own_goal_count": len(own_goals),
        },
        "policy": "events describe the realised path only; final score and incidents do not grade the frozen process",
    }


def build_learning_postmatch_review_draft(freeze_id: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Create an immutable evidence draft; never settle, classify correctness, or create theory."""
    freeze_id = str(freeze_id or "").strip()
    if not freeze_id:
        raise HTTPException(status_code=400, detail="freeze_id_required")
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    freeze = _learning_freeze_by_id(store, freeze_id)
    if not freeze:
        raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
    if freeze_id in (store.get("learning_postmatch") or {}):
        raise HTTPException(status_code=409, detail="learning_sample_already_settled")
    fact_versions = (store.get("learning_postmatch_facts") or {}).get(freeze_id) or []
    if not fact_versions:
        raise HTTPException(status_code=409, detail="verified_postmatch_fact_packet_required")
    facts = max(fact_versions, key=lambda row: int(row.get("version_number") or 0))
    if get_nested(facts, ["verification", "settlement_eligible"]) is not True:
        raise HTTPException(status_code=409, detail="independent_result_verification_required_for_review_draft")

    analysis = freeze.get("analysis") if isinstance(freeze.get("analysis"), dict) else {}
    decision = freeze.get("decision") if isinstance(freeze.get("decision"), dict) else {}
    is_pass = str(decision.get("decision") or "").strip().upper() == "PASS"
    fundamental = analysis.get("fundamental_chain") if isinstance(analysis.get("fundamental_chain"), dict) else {}
    chain_audit = audit_fundamental_chain(fundamental)
    if chain_audit.get("decision_eligible") is True:
        fundamental_status, fundamental_reason = "passed", "frozen_fundamental_chain_was_decision_eligible"
    elif is_pass:
        fundamental_status, fundamental_reason = "passed", "incomplete_chain_was_preserved_as_safe_pass"
    else:
        fundamental_status, fundamental_reason = "failed", "non_pass_decision_used_an_ineligible_fundamental_chain"

    market = analysis.get("market_timeline") if isinstance(analysis.get("market_timeline"), dict) else {}
    available_stage_count = int(market.get("available_prematch_stage_count") or 0)
    latest_dynamics = market.get("latest_dynamics") if isinstance(market.get("latest_dynamics"), dict) else {}
    line_path_ready = available_stage_count >= 2 and latest_dynamics.get("comparison_status") == "compared"
    frozen_market_language = analysis.get("market_language") if isinstance(analysis.get("market_language"), dict) else {}
    proxy_labeled = frozen_market_language.get("capital_pressure_is_proxy_only") is True
    real_money_status = get_nested(frozen_market_language, ["real_money_data", "status"]) or "data_missing"
    real_money_contract_ready = (
        (real_money_status == "data_missing" and proxy_labeled)
        or (
            real_money_status == "available"
            and get_nested(frozen_market_language, ["real_money_data", "decision_eligible"]) is True
            and bool(get_nested(frozen_market_language, ["real_money_data", "evidence_hash"]))
        )
    )
    four_axes_present = all(axis in (frozen_market_language.get("axes") or {}) for axis in ("Home", "Away", "Over", "Under"))
    market_language_ready = real_money_contract_ready and four_axes_present
    market_language_contract_applicable = version_at_least(get_nested(freeze, ["versions", "service"]), (1, 95, 0))
    if line_path_ready and (market_language_ready or not market_language_contract_applicable):
        market_status = "passed"
        market_reason = "frozen_market_path_and_v1_95_language_contract_were_complete" if market_language_contract_applicable else "legacy_freeze_market_path_was_complete; v1_95_contract_not_applied_retroactively"
    elif is_pass:
        market_status, market_reason = "passed", "insufficient_market_path_was_preserved_as_safe_pass"
    else:
        market_status, market_reason = "failed", "non_pass_decision_lacked_a_comparable_market_path"

    state_tree = analysis.get("state_tree") if isinstance(analysis.get("state_tree"), dict) else {}
    states = state_tree.get("states") if isinstance(state_tree.get("states"), dict) else {}
    populated_states = sorted(key for key, value in states.items() if value not in (None, "", [], {}))
    event_evidence = _postmatch_event_evidence(facts)
    if not populated_states:
        state_status, state_reason = "data_missing", "frozen_state_tree_had_no_testable_state_predictions"
    elif event_evidence["event_verification"] == "data_missing":
        state_status, state_reason = "data_missing", "event_sequence_evidence_is_missing"
    else:
        state_status, state_reason = "inconclusive", "state_predictions_exist_but_tactical_response_requires_human_event_review"

    selected_expression = _learning_selected_expression(decision)
    decision_contract = audit_decision_output(decision)
    if decision_contract.get("decision_eligible") is True:
        expression_status, expression_reason = "passed", "frozen_decision_output_contract_was_complete"
    elif is_pass and decision.get("pass_reasons"):
        expression_status, expression_reason = "passed", "safe_pass_has_explicit_reasons_and_no_actionable_expression"
    else:
        expression_status, expression_reason = "data_missing", "frozen_expression_contract_is_not_complete_enough_for_automatic_audit"
    entry_price = as_float(selected_expression.get("price"))
    price_status = "inconclusive" if entry_price is not None else "data_missing"
    price_reason = "entry_price_present_pending_server_verified_t1h_reference" if entry_price is not None else "entry_or_t1h_price_evidence_missing"
    match_rating = decision.get("match_rating")
    match_status = "inconclusive" if match_rating is not None else "data_missing"
    match_reason = "frozen_match_rating_present_but_selection_quality_requires_cross_match_comparison" if match_rating is not None else "frozen_match_rating_missing"

    draft_review = {
        "match_selection_quality": {"status": match_status, "reason": match_reason, "frozen_match_rating": match_rating},
        "fundamental_chain_audit": {"status": fundamental_status, "reason": fundamental_reason, "audit": chain_audit},
        "state_tree_coverage": {
            "status": state_status, "reason": state_reason,
            "populated_frozen_states": populated_states,
            "actual_state_path": event_evidence["actual_state_path"],
        },
        "market_language_audit": {
            "status": market_status, "reason": market_reason,
            "available_stage_count": available_stage_count,
            "latest_comparison_status": latest_dynamics.get("comparison_status"),
            "frozen_market_classification": latest_dynamics.get("classification"),
            "capital_pressure_proxy_only": proxy_labeled,
            "real_money_data_status": real_money_status,
            "real_money_contract_ready": real_money_contract_ready,
            "four_axis_language_present": four_axes_present,
            "selected_acceptance": frozen_market_language.get("selected_acceptance"),
            "v1_95_contract_applicable": market_language_contract_applicable,
        },
        "expression_audit": {
            "status": expression_status, "reason": expression_reason,
            "selected_expression": selected_expression,
            "decision_contract": decision_contract,
        },
        "price_execution_audit": {"status": price_status, "reason": price_reason, "entry_price": entry_price},
        "process_reasoning": "System draft audits only frozen PIT completeness, decision-rule execution and verified event evidence; it does not infer process correctness from the final score.",
        "learning_disposition": {
            "result_backfit_used": False,
            "champion_change_requested": False,
            "new_theory_status": "none",
            "existing_rule_implementation_gap": False,
        },
    }
    review_audit = audit_learning_postmatch_review(draft_review)
    immutable_content = {
        "freeze_id": freeze_id,
        "fixture": freeze.get("fixture"),
        "freeze_hash": freeze.get("content_hash"),
        "fact_hash": facts.get("fact_hash"),
        "fact_version_number": facts.get("version_number"),
        "event_evidence": event_evidence,
        "review": review_audit["normalized"],
        "review_audit": {"eligible": review_audit["eligible"], "reasons": review_audit["reasons"]},
        "suggested_process_classification": "DATA_INSUFFICIENT",
        "manual_review_required": True,
        "automatic_settlement_eligible": False,
        "result_outcome_used_to_grade_process": False,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
    }
    draft_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        latest_store = load_snapshot_store()
        latest_fact_versions = (latest_store.get("learning_postmatch_facts") or {}).get(freeze_id) or []
        if not latest_fact_versions:
            raise HTTPException(status_code=409, detail="verified_postmatch_fact_packet_required")
        latest_facts = max(latest_fact_versions, key=lambda row: int(row.get("version_number") or 0))
        if latest_facts.get("fact_hash") != facts.get("fact_hash"):
            raise HTTPException(status_code=409, detail="postmatch_facts_changed_during_review_draft")
        rows = latest_store.setdefault("learning_postmatch_drafts", {}).setdefault(freeze_id, [])
        duplicate = next((row for row in rows if row.get("draft_hash") == draft_hash), None)
        if duplicate:
            return {**duplicate, "action": "unchanged"}
        version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
        record = {
            **immutable_content,
            "version_number": version_number,
            "draft_hash": draft_hash,
            "drafted_at": now_ts,
            "immutable": True,
            "action": "drafted",
        }
        rows.append(record)
        latest_store["version"] = VERSION
        write_snapshot_store(latest_store)
        return record


def _derive_frozen_selection_outcome(freeze: Dict[str, Any], facts: Dict[str, Any]) -> Dict[str, Any]:
    """Derive only unambiguous settlement outcomes after process quality has been graded."""
    frozen_decision = freeze.get("decision") if isinstance(freeze.get("decision"), dict) else {}
    execution_action = str(frozen_decision.get("execution_action") or ("PASS" if str(frozen_decision.get("decision") or "").upper() == "PASS" else "BET")).upper()
    if execution_action in {"WAIT", "PASS"}:
        return {
            "status": "not_executed", "outcome": None,
            "reason": "frozen_execution_action_was_not_bet",
            "execution_action": execution_action,
            "result_used_only_after_process_grade": True,
        }
    expression = _learning_selected_expression(frozen_decision)
    market = str(expression.get("market") or "").strip().casefold()
    selection = str(expression.get("selection") or "").strip().casefold()
    line = as_float(expression.get("line"))
    result = facts.get("result") if isinstance(facts.get("result"), dict) else {}
    try:
        home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    except (TypeError, ValueError):
        return {"status": "data_missing", "outcome": None, "reason": "verified_goals_missing"}
    outcome = None
    method = None
    if market in {"1x2", "match_winner", "moneyline"}:
        actual = "home" if home_goals > away_goals else ("away" if away_goals > home_goals else "draw")
        aliases = {"主胜": "home", "home": "home", "draw": "draw", "平": "draw", "客胜": "away", "away": "away"}
        normalized = aliases.get(selection)
        if normalized:
            outcome, method = ("win" if normalized == actual else "loss"), "verified_1x2_result"
    elif market in {"over_under", "total", "goals"} and line is not None:
        total = home_goals + away_goals
        if total == line:
            return {"status": "push", "outcome": None, "reason": "total_goals_equal_line", "market": market, "line": line}
        if selection in {"over", "大", "o"}:
            outcome, method = ("win" if total > line else "loss"), "verified_total_goals_vs_line"
        elif selection in {"under", "小", "u"}:
            outcome, method = ("win" if total < line else "loss"), "verified_total_goals_vs_line"
    elif market in {"btts", "both_teams_to_score"}:
        actual_yes = home_goals > 0 and away_goals > 0
        if selection in {"yes", "是", "btts yes"}:
            outcome, method = ("win" if actual_yes else "loss"), "verified_btts_result"
        elif selection in {"no", "否", "btts no"}:
            outcome, method = ("win" if not actual_yes else "loss"), "verified_btts_result"
    elif market in {"home_team_total", "away_team_total"} and line is not None:
        goals = home_goals if market == "home_team_total" else away_goals
        if goals == line:
            return {"status": "push", "outcome": None, "reason": "team_goals_equal_line", "market": market, "line": line}
        if selection in {"over", "大", "o"}:
            outcome, method = ("win" if goals > line else "loss"), "verified_team_goals_vs_line"
        elif selection in {"under", "小", "u"}:
            outcome, method = ("win" if goals < line else "loss"), "verified_team_goals_vs_line"
    if outcome is None:
        return {
            "status": "unsupported", "outcome": None,
            "reason": "frozen_expression_not_unambiguously_settleable",
            "market": market or None, "selection": selection or None, "line": line,
        }
    return {
        "status": "derived", "outcome": outcome, "method": method,
        "market": market, "selection": selection, "line": line,
        "result_used_only_after_process_grade": True,
    }


def complete_learning_postmatch_review(payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Settle from a hash-bound evidence review while deriving, never accepting, Process classification."""
    payload = payload if isinstance(payload, dict) else {}
    freeze_id = str(payload.get("freeze_id") or "").strip()
    draft_hash = str(payload.get("draft_hash") or "").strip()
    if not freeze_id or not draft_hash:
        raise HTTPException(status_code=400, detail="freeze_id_and_draft_hash_required")
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    freeze = _learning_freeze_by_id(store, freeze_id)
    if not freeze:
        raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
    fact_versions = (store.get("learning_postmatch_facts") or {}).get(freeze_id) or []
    draft_versions = (store.get("learning_postmatch_drafts") or {}).get(freeze_id) or []
    facts = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
    draft = max(draft_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
    if not facts or get_nested(facts, ["verification", "settlement_eligible"]) is not True:
        raise HTTPException(status_code=409, detail="latest_independently_verified_facts_required")
    if not draft or draft.get("draft_hash") != draft_hash or draft.get("fact_hash") != facts.get("fact_hash"):
        raise HTTPException(status_code=409, detail="latest_matching_review_draft_hash_required")
    review = payload.get("review") if isinstance(payload.get("review"), dict) else {}
    review_audit = audit_learning_postmatch_review(review)
    if not review_audit["eligible"]:
        raise HTTPException(status_code=422, detail={"error": "postmatch_review_incomplete_or_backfit_risk", "reasons": review_audit["reasons"]})
    normalized = review_audit["normalized"]
    if normalized.get("review_mode") not in {"scheduled_agent", "automatic_evidence_review"}:
        raise HTTPException(status_code=422, detail="evidence_review_mode_required")
    if normalized.get("outcome_not_used_for_process_grade") is not True:
        raise HTTPException(status_code=422, detail="outcome_non_use_attestation_required")
    freeze_hash = str(freeze.get("content_hash") or "")
    fact_hash = str(facts.get("fact_hash") or "")
    evidence_failures = []
    for section in LEARNING_REVIEW_SECTIONS:
        row = normalized.get(section) if isinstance(normalized.get(section), dict) else {}
        if row.get("status") not in {"passed", "failed"}:
            continue
        refs = [str(value) for value in (row.get("evidence_refs") or []) if str(value)] if isinstance(row.get("evidence_refs"), list) else []
        required_hashes = {freeze_hash}
        if section == "state_tree_coverage":
            required_hashes.add(fact_hash)
        if not refs or not any(any(hash_value and hash_value in ref for hash_value in required_hashes) for ref in refs):
            evidence_failures.append(f"{section}_hash_bound_evidence_required")
        if len(str(row.get("reason") or "").strip()) < 20:
            evidence_failures.append(f"{section}_reason_too_short")
    if evidence_failures:
        raise HTTPException(status_code=422, detail={"error": "automatic_review_evidence_incomplete", "reasons": evidence_failures})
    event_audit = payload.get("event_audit") if isinstance(payload.get("event_audit"), dict) else {}
    event_status = str(event_audit.get("status") or "data_missing").strip().lower()
    if event_status not in {"clean", "contaminated", "data_missing"}:
        raise HTTPException(status_code=422, detail="invalid_automatic_event_audit_status")
    event_refs = [str(value) for value in (event_audit.get("evidence_refs") or []) if str(value)] if isinstance(event_audit.get("evidence_refs"), list) else []
    if event_status in {"clean", "contaminated"} and not any(fact_hash in ref or draft_hash in ref for ref in event_refs):
        raise HTTPException(status_code=422, detail="event_audit_hash_bound_evidence_required")
    event_evidence = draft.get("event_evidence") if isinstance(draft.get("event_evidence"), dict) else {}
    if event_evidence.get("pollution_flags") and event_status == "clean" and len(str(event_audit.get("pollution_reasoning") or "").strip()) < 20:
        raise HTTPException(status_code=422, detail="pollution_flags_require_explicit_reasoning")

    statuses = [get_nested(normalized, [section, "status"]) for section in LEARNING_REVIEW_SECTIONS]
    if event_status == "contaminated":
        classification = "EVENT_CONTAMINATED"
        process_grade = "not_graded_event_contaminated"
        outcome_audit = {"status": "not_used", "reason": "event_contamination_precedes_process_grade"}
    elif event_status == "data_missing":
        classification = "DATA_INSUFFICIENT"
        process_grade = "not_graded_event_pollution_unknown"
        outcome_audit = {"status": "not_used", "reason": "event_pollution_could_not_be_verified"}
    elif any(status in {"inconclusive", "data_missing", None} for status in statuses):
        classification = "DATA_INSUFFICIENT"
        process_grade = "not_graded_incomplete_review"
        outcome_audit = {"status": "not_used", "reason": "review_evidence_incomplete"}
    elif get_nested(event_evidence, ["event_verification"]) != "verified" and get_nested(normalized, ["state_tree_coverage", "status"]) in {"passed", "failed"}:
        classification = "DATA_INSUFFICIENT"
        process_grade = "not_graded_event_sequence_unverified"
        outcome_audit = {"status": "not_used", "reason": "event_sequence_not_independently_verified"}
    else:
        process_grade = "error" if "failed" in statuses else "correct"
        outcome_audit = _derive_frozen_selection_outcome(freeze, facts)
        if outcome_audit.get("outcome") not in {"win", "loss"}:
            classification = "DATA_INSUFFICIENT"
        else:
            classification = f"PROCESS_{process_grade.upper()}_RESULT_{str(outcome_audit['outcome']).upper()}"
    derived_audit = {
        "classification": classification,
        "process_grade": process_grade,
        "process_grade_derived_before_outcome": True,
        "section_statuses": dict(zip(LEARNING_REVIEW_SECTIONS, statuses)),
        "selection_outcome_audit": outcome_audit,
        "freeze_hash": freeze_hash, "fact_hash": fact_hash, "draft_hash": draft_hash,
        "caller_supplied_process_classification_used": False,
        "result_backfit_used": False,
    }
    enriched_event_audit = {**event_audit, "automatic_review_derivation": derived_audit}
    return settle_learning_sample(
        freeze_id, facts.get("result"), classification, enriched_event_audit,
        payload.get("settled_at") or now_ts, review, fact_hash,
    )


def _learning_match_rating_at_least_b(value: Any) -> Optional[bool]:
    """Grade only an explicitly frozen rating; never infer one from the result."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return None
    normalized = str(value).strip().upper().replace("级", "")
    if not normalized:
        return None
    grade = normalized[0]
    if grade not in {"A", "B", "C", "D", "E", "F"}:
        return None
    return grade in {"A", "B"}


def complete_automatic_learning_postmatch_review(freeze_id: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Complete a deterministic evidence-contract review without using the realised score to grade process."""
    freeze_id = str(freeze_id or "").strip()
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    invalid_priority_board_ids = sorted(
        str(board_id) for board_id, board in (store.get("learning_priority_boards") or {}).items()
        if audit_learning_priority_board(board).get("status") != "passed"
    )
    if invalid_priority_board_ids:
        raise HTTPException(status_code=409, detail={
            "error": "learning_priority_board_integrity_invalid",
            "board_ids": invalid_priority_board_ids[:20],
            "board_ids_truncated": len(invalid_priority_board_ids) > 20,
            "automatic_review_allowed": False,
        })
    freeze = _learning_freeze_by_id(store, freeze_id)
    facts = max(
        ((store.get("learning_postmatch_facts") or {}).get(freeze_id) or []),
        key=lambda row: int(row.get("version_number") or 0), default=None,
    )
    draft = max(
        ((store.get("learning_postmatch_drafts") or {}).get(freeze_id) or []),
        key=lambda row: int(row.get("version_number") or 0), default=None,
    )
    if not freeze or not facts or get_nested(facts, ["verification", "settlement_eligible"]) is not True:
        raise HTTPException(status_code=409, detail="latest_independently_verified_facts_required")
    if not draft or draft.get("fact_hash") != facts.get("fact_hash"):
        raise HTTPException(status_code=409, detail="latest_matching_review_draft_hash_required")
    freeze_ref = f"freeze:{freeze.get('content_hash')}"
    fact_ref = f"fact:{facts.get('fact_hash')}"
    draft_ref = f"draft:{draft.get('draft_hash')}"
    frozen_decision = freeze.get("decision") if isinstance(freeze.get("decision"), dict) else {}
    decision_contract = audit_decision_output(frozen_decision)
    is_pass = str(frozen_decision.get("decision") or "").upper() == "PASS"
    frozen_match_rating = _learning_frozen_rating(frozen_decision, "match_rating")
    rating_at_least_b = _learning_match_rating_at_least_b(frozen_match_rating)
    priority_context = _learning_priority_context(store, freeze)
    priority_board_required = version_at_least(get_nested(freeze, ["versions", "service"]), (2, 19, 8))
    priority_rank = int((priority_context or {}).get("rank") or 0)
    priority_core_count = int((priority_context or {}).get("core_priority_count") or 0)
    if is_pass and bool(frozen_decision.get("pass_reasons")):
        match_status = "passed"
        match_reason = "The frozen safe PASS has explicit reasons and is auditable without consulting the final score."
    elif priority_context and decision_contract.get("decision_eligible") is True and rating_at_least_b is True and 0 < priority_rank <= priority_core_count:
        match_status = "passed"
        match_reason = "The actionable match was frozen inside the preregistered core priority set using prematch-only fundamentals-first ordering."
    elif priority_context and decision_contract.get("decision_eligible") is True and priority_rank > priority_core_count:
        match_status = "failed"
        match_reason = "The actionable match was outside the frozen core priority set; settlement was not consulted."
    elif priority_context and decision_contract.get("decision_eligible") is True and rating_at_least_b is False:
        match_status = "failed"
        match_reason = "The actionable match was below the preregistered B threshold on the frozen priority board."
    elif priority_context and decision_contract.get("decision_eligible") is True:
        match_status = "data_missing"
        match_reason = "The frozen priority board is valid, but the actionable match has no parseable frozen B-or-better rating."
    elif priority_board_required and decision_contract.get("decision_eligible") is True:
        match_status = "data_missing"
        match_reason = "This v2.19.8-or-later actionable freeze has no valid prematch priority-board binding; legacy rating fallback is forbidden."
    elif decision_contract.get("decision_eligible") is True and rating_at_least_b is True:
        match_status = "passed"
        match_reason = "The actionable decision satisfied the frozen B-or-better threshold; no cycle priority board exists for this legacy or manual freeze."
    elif decision_contract.get("decision_eligible") is True and rating_at_least_b is False:
        match_status = "failed"
        match_reason = "The actionable decision was taken below the preregistered B match threshold."
    elif decision_contract.get("decision_eligible") is True:
        match_status = "data_missing"
        match_reason = "The actionable decision has no parseable frozen match rating."
    else:
        match_status = "failed"
        match_reason = "The frozen actionable decision did not satisfy the prematch decision contract."
    event_evidence = draft.get("event_evidence") if isinstance(draft.get("event_evidence"), dict) else {}
    event_verified = event_evidence.get("event_verification") == "verified"
    populated_states = get_nested(draft, ["review", "state_tree_coverage", "populated_frozen_states"], []) or []
    if not populated_states:
        state_status, state_reason = "not_applicable", "No frozen state prediction was available, so state coverage is not graded."
    elif event_verified:
        state_status, state_reason = "inconclusive", "Frozen state predictions and independently verified events are present, but no deterministic state-path comparison proves coverage automatically."
    else:
        state_status, state_reason = "data_missing", "Frozen state predictions exist but the event sequence lacks independent verification."
    frozen_expression = _learning_selected_expression(frozen_decision)
    entry_price = as_float(frozen_expression.get("price"))
    if is_pass:
        price_status = "not_applicable"
        price_reason = "The frozen decision was PASS, so no price execution was attempted."
        price_evidence_refs = []
        price_details = {"entry_price": entry_price, "reference_stage": "T-1h"}
    elif entry_price is None:
        price_status = "failed"
        price_reason = "The actionable frozen expression omitted its entry price."
        price_evidence_refs = [freeze_ref]
        price_details = {"entry_price": None, "reference_stage": "T-1h"}
    else:
        price_reference = _learning_t1h_price_evidence_for_expression(freeze, frozen_expression, store)
        if price_reference.get("ok") is not True:
            price_status = "data_missing"
            price_reason = str(price_reference.get("reason") or "verified_learning_t1h_snapshot_required")
            price_evidence_refs = [freeze_ref]
            price_details = {"entry_price": entry_price, "reference_stage": "T-1h"}
        else:
            reference_price = float(price_reference["reference_decimal_price"])
            price_probability_delta = round(1.0 / reference_price - 1.0 / entry_price, 8)
            price_status = "passed" if price_probability_delta >= 0.0 else "failed"
            price_reason = (
                "The frozen entry held or improved versus the exact-line T-1h consensus."
                if price_status == "passed"
                else "The frozen entry deteriorated versus the exact-line T-1h consensus."
            )
            price_evidence_refs = [freeze_ref, str(price_reference["price_reference_evidence_ref"])]
            price_details = {
                "entry_price": entry_price, "reference_stage": "T-1h",
                "reference_price": reference_price,
                "t1h_price_probability_delta": price_probability_delta,
                "reference_snapshot_at": price_reference.get("snapshot_at"),
                "caller_supplied_price_reference_used": False,
            }
    review = copy.deepcopy(draft.get("review") if isinstance(draft.get("review"), dict) else {})
    section_overrides = {
        "match_selection_quality": {
            "status": match_status,
            "reason": match_reason,
            "frozen_match_rating": frozen_match_rating,
            "required_minimum_rating": "B",
            "priority_board_id": (priority_context or {}).get("board_id"),
            "priority_board_hash": (priority_context or {}).get("board_hash"),
            "frozen_priority_rank": (priority_context or {}).get("rank"),
            "frozen_priority_role": (priority_context or {}).get("role"),
            "core_priority_count": (priority_context or {}).get("core_priority_count"),
            "priority_board_used": priority_context is not None,
            "priority_board_required": priority_board_required,
            "rating_derived_from_result": False,
            "evidence_refs": [freeze_ref, f"priority-board:{priority_context.get('board_hash')}"] if priority_context and match_status in {"passed", "failed"} else ([freeze_ref] if match_status in {"passed", "failed"} else []),
        },
        "state_tree_coverage": {
            "status": state_status, "reason": state_reason,
            "evidence_refs": [freeze_ref, fact_ref] if state_status in {"passed", "failed"} else [],
        },
        "price_execution_audit": {
            "status": price_status,
            "reason": price_reason,
            **price_details,
            "evidence_refs": price_evidence_refs,
        },
    }
    for section in LEARNING_REVIEW_SECTIONS:
        if section in section_overrides:
            review[section] = section_overrides[section]
            continue
        row = review.get(section) if isinstance(review.get(section), dict) else {}
        status = str(row.get("status") or "data_missing").lower()
        reason = str(row.get("reason") or "Frozen evidence was insufficient for this process section.")
        review[section] = {
            **row, "status": status, "reason": reason,
            "evidence_refs": [freeze_ref] if status in {"passed", "failed"} else [],
        }
    review.update({
        "review_mode": "automatic_evidence_review",
        "outcome_not_used_for_process_grade": True,
        "process_reasoning": "The automatic process grade uses only the immutable prematch contract and hash-bound event evidence; the verified final score is consulted afterward solely to settle the frozen selection.",
        "learning_disposition": {
            "result_backfit_used": False, "champion_change_requested": False,
            "new_theory_status": "none", "existing_rule_implementation_gap": False,
        },
    })
    pollution_flags = list(event_evidence.get("pollution_flags") or [])
    if not event_verified:
        event_status = "data_missing"
    elif pollution_flags:
        event_status = "contaminated"
    else:
        event_status = "clean"
    event_audit = {
        "status": event_status,
        "evidence_refs": [fact_ref, draft_ref] if event_status != "data_missing" else [],
        "pollution_flags": pollution_flags,
        "pollution_reasoning": (
            "Independently verified event evidence contains a listed material incident."
            if event_status == "contaminated"
            else "Independently verified event evidence contains no listed pollution incident."
            if event_status == "clean"
            else "Event pollution remains unknown because the event sequence lacks two-source verification."
        ),
    }
    return complete_learning_postmatch_review({
        "freeze_id": freeze_id, "draft_hash": draft.get("draft_hash"),
        "review": review, "event_audit": event_audit, "settled_at": now_ts,
    }, now_ts=now_ts)


def learning_review_queue() -> Dict[str, Any]:
    """Expose frozen context plus versioned facts for evidence-led postmatch review."""
    store = load_snapshot_store()
    postmatches = store.get("learning_postmatch") or {}
    rows = []
    for freeze_id, fact_versions in (store.get("learning_postmatch_facts") or {}).items():
        if freeze_id in postmatches or not fact_versions:
            continue
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze:
            continue
        latest = max(fact_versions, key=lambda row: int(row.get("version_number") or 0))
        verification = latest.get("verification") if isinstance(latest.get("verification"), dict) else {}
        draft_versions = (store.get("learning_postmatch_drafts") or {}).get(freeze_id) or []
        latest_draft = max(draft_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
        draft_matches_facts = bool(latest_draft and latest_draft.get("fact_hash") == latest.get("fact_hash"))
        rows.append({
            "freeze_id": freeze_id,
            "fixture": freeze.get("fixture"),
            "kickoff_at": freeze.get("kickoff_at"),
            "freeze_hash": freeze.get("content_hash"),
            "frozen_analysis": freeze.get("analysis"),
            "frozen_decision": freeze.get("decision"),
            "postmatch_facts": {
                "fact_hash": latest.get("fact_hash"), "version_number": latest.get("version_number"),
                "result": latest.get("result"), "events": latest.get("events"), "statistics": latest.get("statistics"),
                "source_audit": latest.get("source_audit"), "verification": verification,
            },
            "review_ready": verification.get("settlement_eligible") is True,
            "automatic_review_draft": latest_draft if draft_matches_facts else None,
            "automatic_review_draft_ready": draft_matches_facts,
            "required_next_action": "compare_process_to_frozen_prematch_without_result_backfit" if verification.get("settlement_eligible") is True else "obtain_independent_result_verification",
            "allowed_process_classes": sorted(LEARNING_PROCESS_CLASSES),
            "automatic_champion_change": False,
        })
    rows.sort(key=lambda row: (not row["review_ready"], int(row.get("kickoff_at") or 0), str(row.get("fixture"))))
    return {
        "version": VERSION,
        "queue_count": len(rows),
        "review_ready_count": sum(row["review_ready"] for row in rows),
        "automatic_review_draft_ready_count": sum(row["automatic_review_draft_ready"] for row in rows),
        "waiting_for_verification_count": sum(not row["review_ready"] for row in rows),
        "items": rows,
        "result_backfit_allowed": False,
        "automatic_process_classification": False,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
    }


def run_learning_cycle(
    payload: Optional[Dict[str, Any]] = None,
    now_ts: Optional[int] = None,
    fixture_rows: Optional[List[Dict[str, Any]]] = None,
    prematch_packet_builder: Optional[Any] = None,
    postmatch_fact_fetcher: Optional[Any] = None,
    market_language_fact_fetcher: Optional[Any] = None,
    shadow_model_runner: Optional[Any] = None,
) -> Dict[str, Any]:
    """Execute one bounded cycle; only preregistered templates may create Hypotheses, never Champion changes."""
    payload = payload if isinstance(payload, dict) else {}
    now_ts = int(now_ts or time.time())
    apply_changes = payload.get("apply") is True
    auto_prepare = payload.get("auto_prepare_prematch", True)
    if not isinstance(auto_prepare, bool):
        raise HTTPException(status_code=400, detail="auto_prepare_prematch_must_be_boolean")
    auto_collect_facts = payload.get("auto_collect_postmatch_facts", True)
    if not isinstance(auto_collect_facts, bool):
        raise HTTPException(status_code=400, detail="auto_collect_postmatch_facts_must_be_boolean")
    auto_collect_observation_facts = payload.get("auto_collect_market_language_facts", True)
    if not isinstance(auto_collect_observation_facts, bool):
        raise HTTPException(status_code=400, detail="auto_collect_market_language_facts_must_be_boolean")
    auto_build_review_drafts = payload.get("auto_build_postmatch_review_drafts", True)
    if not isinstance(auto_build_review_drafts, bool):
        raise HTTPException(status_code=400, detail="auto_build_postmatch_review_drafts_must_be_boolean")
    auto_complete_reviews = payload.get("auto_complete_postmatch_reviews", True)
    if not isinstance(auto_complete_reviews, bool):
        raise HTTPException(status_code=400, detail="auto_complete_postmatch_reviews_must_be_boolean")
    auto_refresh_research_proposals = payload.get("auto_refresh_research_proposals", True)
    if not isinstance(auto_refresh_research_proposals, bool):
        raise HTTPException(status_code=400, detail="auto_refresh_research_proposals_must_be_boolean")
    auto_register_preregistered_hypotheses = payload.get("auto_register_preregistered_hypotheses", True)
    if not isinstance(auto_register_preregistered_hypotheses, bool):
        raise HTTPException(status_code=400, detail="auto_register_preregistered_hypotheses_must_be_boolean")
    auto_refresh_quality_cards = payload.get("auto_refresh_quality_cards", True)
    if not isinstance(auto_refresh_quality_cards, bool):
        raise HTTPException(status_code=400, detail="auto_refresh_quality_cards_must_be_boolean")
    auto_lock_forward_validation = payload.get("auto_lock_forward_validation", True)
    if not isinstance(auto_lock_forward_validation, bool):
        raise HTTPException(status_code=400, detail="auto_lock_forward_validation_must_be_boolean")
    auto_record_forward_validation = payload.get("auto_record_forward_validation", True)
    if not isinstance(auto_record_forward_validation, bool):
        raise HTTPException(status_code=400, detail="auto_record_forward_validation_must_be_boolean")
    auto_create_promotion_candidates = payload.get("auto_create_promotion_candidates", True)
    if not isinstance(auto_create_promotion_candidates, bool):
        raise HTTPException(status_code=400, detail="auto_create_promotion_candidates_must_be_boolean")
    supplied_run_id = str(payload.get("run_id") or "").strip()
    safe_run_id = supplied_run_id and len(supplied_run_id) <= 100 and all(char in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in supplied_run_id)
    if apply_changes and not safe_run_id:
        raise HTTPException(status_code=400, detail="safe_run_id_required_when_apply_is_true")
    postmatch_plan = learning_cycle_plan(now_ts=now_ts, fixture_rows=[], include_discovery=False)
    observation_postmatch_plan = market_language_settlement_plan(now_ts=now_ts)
    if apply_changes:
        existing = (load_snapshot_store().get("learning_runs") or {}).get(supplied_run_id)
        if existing:
            if learning_run_record_integrity(existing).get("valid") is not True:
                raise HTTPException(status_code=409, detail="learning_run_integrity_invalid")
            return {**existing, "action": "unchanged"}
    prematch_packets = payload.get("prematch_packets") if isinstance(payload.get("prematch_packets"), dict) else {}
    postmatch_fact_packets = payload.get("postmatch_fact_packets") if isinstance(payload.get("postmatch_fact_packets"), dict) else {}
    settlement_packets = payload.get("settlement_packets") if isinstance(payload.get("settlement_packets"), list) else []
    review_completion_packets = payload.get("review_completion_packets") if isinstance(payload.get("review_completion_packets"), list) else []
    due_ids = {str(row.get("freeze_id")) for row in postmatch_plan.get("settlement_due") or []}
    settlement_results = []
    for row in settlement_packets:
        row = row if isinstance(row, dict) else {}
        freeze_id = str(row.get("freeze_id") or "").strip()
        if freeze_id not in due_ids:
            settlement_results.append({"freeze_id": freeze_id or None, "action": "skipped", "reason": "freeze_not_due_in_current_cycle"})
            continue
        if not apply_changes:
            review_audit = audit_learning_postmatch_review(row.get("review"))
            settlement_results.append({
                "freeze_id": freeze_id, "action": "would_settle" if review_audit["eligible"] else "would_reject",
                "review_eligible": review_audit["eligible"], "reasons": review_audit["reasons"],
            })
            continue
        try:
            record = settle_learning_sample(
                freeze_id, row.get("result"), row.get("process_classification"), row.get("event_audit"),
                row.get("settled_at") or now_ts, row.get("review"), row.get("fact_hash"),
            )
            settlement_results.append({"freeze_id": freeze_id, "action": record.get("action"), "postmatch_hash": record.get("postmatch_hash")})
        except HTTPException as exc:
            settlement_results.append({"freeze_id": freeze_id, "action": "rejected", "status_code": exc.status_code, "reason": exc.detail})
    settled_or_existing = {
        str(row.get("freeze_id")) for row in settlement_results if row.get("action") in {"settled", "unchanged"}
    }
    fact_results = []
    if auto_collect_facts:
        current_store = load_snapshot_store()
        existing_facts = current_store.get("learning_postmatch_facts") or {}
        existing_postmatches = current_store.get("learning_postmatch") or {}
        for freeze_id in sorted(due_ids):
            if freeze_id in settled_or_existing or freeze_id in existing_postmatches:
                fact_results.append({"freeze_id": freeze_id, "action": "skipped", "reason": "postmatch_already_settled"})
                continue
            supplied_facts = postmatch_fact_packets.get(freeze_id)
            versions = existing_facts.get(freeze_id) or []
            if versions and supplied_facts is None:
                latest = max(versions, key=lambda row: int(row.get("version_number") or 0))
                verified = get_nested(latest, ["verification", "settlement_eligible"]) is True
                if verified or postmatch_fact_fetcher is not None:
                    fact_results.append({
                        "freeze_id": freeze_id, "action": "facts_verified_awaiting_review" if verified else "awaiting_independent_verification",
                        "verification": latest.get("verification"), "fact_hash": latest.get("fact_hash"),
                    })
                    continue
            if not apply_changes:
                fact_results.append({"freeze_id": freeze_id, "action": "would_collect_facts", "supplied_fact_packet": supplied_facts is not None})
                continue
            try:
                active_fact_fetcher = (lambda fixture_id, packet=supplied_facts: packet) if supplied_facts is not None else postmatch_fact_fetcher
                fact_record = collect_learning_postmatch_facts(freeze_id, now_ts=now_ts, fact_fetcher=active_fact_fetcher)
                fact_results.append({
                    "freeze_id": freeze_id, "action": fact_record.get("action"),
                    "verification": fact_record.get("verification"), "fact_hash": fact_record.get("fact_hash"),
                })
            except HTTPException as exc:
                fact_results.append({"freeze_id": freeze_id, "action": "rejected", "status_code": exc.status_code, "reason": exc.detail})
    observation_fact_packets = payload.get("market_language_postmatch_fact_packets") if isinstance(payload.get("market_language_postmatch_fact_packets"), dict) else {}
    observation_fact_results = []
    if auto_collect_observation_facts:
        for item in observation_postmatch_plan.get("items") or []:
            observation_id = str(item.get("observation_id") or "")
            if item.get("settlement_ready"):
                observation_fact_results.append({
                    "observation_id": observation_id, "action": "facts_verified_awaiting_settlement",
                    "fact_hash": item.get("fact_hash"),
                })
                continue
            if item.get("automatic_fact_collection_eligible") is not True:
                observation_fact_results.append({
                    "observation_id": observation_id, "action": "skipped",
                    "reason": "outside_automatic_observation_fact_window",
                })
                continue
            supplied_facts = observation_fact_packets.get(observation_id)
            if not apply_changes:
                observation_fact_results.append({
                    "observation_id": observation_id, "action": "would_collect_facts",
                    "supplied_fact_packet": supplied_facts is not None,
                })
                continue
            try:
                active_fetcher = (
                    (lambda fixture_id, packet=supplied_facts: packet)
                    if supplied_facts is not None else market_language_fact_fetcher
                )
                record = collect_market_language_postmatch_facts(
                    observation_id, now_ts=now_ts, fact_fetcher=active_fetcher,
                )
                observation_fact_results.append({
                    "observation_id": observation_id, "action": record.get("action"),
                    "fact_hash": record.get("fact_hash"), "verification": record.get("verification"),
                })
            except HTTPException as exc:
                observation_fact_results.append({
                    "observation_id": observation_id, "action": "rejected",
                    "status_code": exc.status_code, "reason": exc.detail,
                })
    draft_results = []
    if auto_build_review_drafts:
        current_store = load_snapshot_store()
        for freeze_id in sorted(due_ids):
            if freeze_id in settled_or_existing or freeze_id in (current_store.get("learning_postmatch") or {}):
                draft_results.append({"freeze_id": freeze_id, "action": "skipped", "reason": "postmatch_already_settled"})
                continue
            fact_versions = (current_store.get("learning_postmatch_facts") or {}).get(freeze_id) or []
            latest_fact = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
            if not latest_fact or get_nested(latest_fact, ["verification", "settlement_eligible"]) is not True:
                draft_results.append({"freeze_id": freeze_id, "action": "skipped", "reason": "independent_result_verification_required"})
                continue
            if not apply_changes:
                draft_results.append({"freeze_id": freeze_id, "action": "would_build_review_draft", "fact_hash": latest_fact.get("fact_hash")})
                continue
            try:
                draft = build_learning_postmatch_review_draft(freeze_id, now_ts=now_ts)
                draft_results.append({
                    "freeze_id": freeze_id, "action": draft.get("action"),
                    "draft_hash": draft.get("draft_hash"),
                    "suggested_process_classification": draft.get("suggested_process_classification"),
                    "manual_review_required": draft.get("manual_review_required"),
                })
            except HTTPException as exc:
                draft_results.append({"freeze_id": freeze_id, "action": "rejected", "status_code": exc.status_code, "reason": exc.detail})
    review_completion_results = []
    explicit_review_freeze_ids = set()
    for row in review_completion_packets:
        row = row if isinstance(row, dict) else {}
        freeze_id = str(row.get("freeze_id") or "").strip()
        if freeze_id:
            explicit_review_freeze_ids.add(freeze_id)
        if freeze_id not in due_ids:
            review_completion_results.append({"freeze_id": freeze_id or None, "action": "skipped", "reason": "freeze_not_due_in_current_cycle"})
            continue
        if not apply_changes:
            review_completion_results.append({"freeze_id": freeze_id, "action": "would_complete_evidence_review"})
            continue
        try:
            completed = complete_learning_postmatch_review(row, now_ts=now_ts)
            review_completion_results.append({
                "freeze_id": freeze_id, "action": completed.get("action"),
                "postmatch_hash": completed.get("postmatch_hash"),
                "process_classification": completed.get("process_classification"),
                "caller_supplied_process_classification_used": False,
            })
        except HTTPException as exc:
            review_completion_results.append({
                "freeze_id": freeze_id, "action": "rejected",
                "status_code": exc.status_code, "reason": exc.detail,
            })
    if auto_complete_reviews:
        current_store = load_snapshot_store()
        current_postmatches = current_store.get("learning_postmatch") or {}
        for freeze_id in sorted(due_ids - explicit_review_freeze_ids):
            if freeze_id in current_postmatches:
                continue
            facts = max(
                ((current_store.get("learning_postmatch_facts") or {}).get(freeze_id) or []),
                key=lambda row: int(row.get("version_number") or 0), default=None,
            )
            draft = max(
                ((current_store.get("learning_postmatch_drafts") or {}).get(freeze_id) or []),
                key=lambda row: int(row.get("version_number") or 0), default=None,
            )
            if (
                not facts or get_nested(facts, ["verification", "settlement_eligible"]) is not True
                or not draft or draft.get("fact_hash") != facts.get("fact_hash")
            ):
                continue
            if not apply_changes:
                review_completion_results.append({
                    "freeze_id": freeze_id, "action": "would_complete_automatic_evidence_review",
                    "draft_hash": draft.get("draft_hash"), "fact_hash": facts.get("fact_hash"),
                })
                continue
            try:
                completed = complete_automatic_learning_postmatch_review(freeze_id, now_ts=now_ts)
                review_completion_results.append({
                    "freeze_id": freeze_id, "action": completed.get("action"),
                    "postmatch_hash": completed.get("postmatch_hash"),
                    "process_classification": completed.get("process_classification"),
                    "review_mode": "automatic_evidence_review",
                    "caller_supplied_process_classification_used": False,
                })
            except HTTPException as exc:
                review_completion_results.append({
                    "freeze_id": freeze_id, "action": "rejected",
                    "status_code": exc.status_code, "reason": exc.detail,
                    "review_mode": "automatic_evidence_review",
                })
    quality_card_results = []
    if auto_refresh_quality_cards:
        if apply_changes:
            quality_refresh = refresh_learning_quality_cards(now_ts=now_ts)
            quality_card_results = quality_refresh.get("results") or []
        else:
            quality_preview = learning_quality_card_candidates()
            quality_card_results = [
                {
                    "card_id": row.get("card_id"), "freeze_id": row.get("freeze_id"),
                    "action": "would_create", "card_hash": row.get("card_hash"),
                }
                for row in quality_preview.get("candidates") or []
            ]
    forward_validation_evidence_results = []
    if auto_record_forward_validation:
        evidence_refresh = refresh_learning_forward_validation_evidence(
            now_ts=now_ts, apply_changes=apply_changes,
        )
        forward_validation_evidence_results = evidence_refresh.get("results") or []
    promotion_candidate_results = []
    if auto_create_promotion_candidates:
        promotion_refresh = refresh_learning_promotion_candidates(
            now_ts=now_ts, apply_changes=apply_changes,
        )
        promotion_candidate_results = promotion_refresh.get("results") or []
    research_proposal_results = []
    if auto_refresh_research_proposals:
        if apply_changes:
            proposal_refresh = refresh_learning_research_proposals(now_ts=now_ts)
            research_proposal_results = proposal_refresh.get("results") or []
        else:
            proposal_preview = learning_research_proposal_candidates()
            research_proposal_results = [
                {
                    "proposal_id": row.get("proposal_id"), "action": "would_propose",
                    "proposal_hash": row.get("proposal_hash"),
                    "scope": row.get("scope"), "signal_dimension": row.get("signal_dimension"),
                }
                for row in proposal_preview.get("candidates") or []
            ]
    hypothesis_registration_results = []
    if auto_register_preregistered_hypotheses:
        hypothesis_refresh = instantiate_preregistered_learning_hypotheses(
            now_ts=now_ts, apply_changes=apply_changes,
        )
        hypothesis_registration_results = hypothesis_refresh.get("results") or []
    prematch_plan = learning_cycle_plan(now_ts=now_ts, fixture_rows=fixture_rows, include_discovery=True)
    admission_results = []
    for candidate in prematch_plan.get("discovery", {}).get("candidates") or []:
        fixture = str(candidate.get("fixture_id") or "")
        if not apply_changes:
            admission_results.append({
                "fixture": fixture, "action": "would_admit",
                "major_league_registry_version": candidate.get("major_league_registry_version"),
                "major_league_registry_hash": candidate.get("major_league_registry_hash"),
            })
            continue
        try:
            admission = admit_learning_candidate(candidate, now_ts=now_ts)
            admission_results.append({
                "fixture": fixture, "action": admission.get("action"),
                "admission_hash": admission.get("admission_hash"),
                "major_league_registry_version": admission.get("major_league_registry_version"),
            })
        except HTTPException as exc:
            admission_results.append({
                "fixture": fixture, "action": "rejected",
                "status_code": exc.status_code, "reason": exc.detail,
            })
    freeze_results = []
    priority_freezes = []
    builder = prematch_packet_builder or build_shadow_ai_packet
    for candidate in prematch_plan.get("discovery", {}).get("candidates") or []:
        fixture = str(candidate.get("fixture_id") or "")
        if not candidate.get("analysis_due"):
            freeze_results.append({
                "fixture": fixture, "action": "skipped", "reason": candidate.get("analysis_due_reason") or "analysis_not_due",
                "latest_analysis_node": candidate.get("latest_analysis_node"),
                "target_analysis_node": candidate.get("target_analysis_node"),
            })
            continue
        packet = prematch_packets.get(fixture)
        if packet is None and auto_prepare and apply_changes:
            try:
                packet = builder(int(fixture))
            except Exception as exc:
                freeze_results.append({"fixture": fixture, "action": "rejected", "reason": "prematch_packet_build_failed", "error_type": type(exc).__name__})
                continue
        if packet is None:
            freeze_results.append({"fixture": fixture, "action": "would_prepare" if auto_prepare else "skipped", "reason": "prematch_packet_not_supplied"})
            continue
        try:
            freeze_payload = build_learning_freeze_payload(candidate, packet, now_ts=now_ts)
            if not apply_changes:
                priority_freezes.append(freeze_payload)
                freeze_results.append({
                    "fixture": fixture, "action": "would_reanalyze" if candidate.get("already_frozen") else "would_freeze",
                    "analysis_node": get_nested(freeze_payload, ["analysis", "analysis_node"]),
                    "reason": candidate.get("analysis_due_reason"),
                    "decision": get_nested(freeze_payload, ["decision", "decision"]),
                    "probability_replay_status": get_nested(freeze_payload, ["analysis", "probability_replay_audit", "status"]),
                    "probability_replay_decision_eligible": get_nested(freeze_payload, ["analysis", "probability_replay_audit", "decision_eligible"]) is True,
                })
                continue
            record = freeze_learning_sample(freeze_payload, now_ts=now_ts)
            priority_freezes.append(record)
            freeze_results.append({
                "fixture": fixture, "freeze_id": record.get("freeze_id"), "action": record.get("action"),
                "analysis_node": get_nested(record, ["analysis", "analysis_node"]),
                "reason": candidate.get("analysis_due_reason"),
                "decision": get_nested(record, ["decision", "decision"]),
                "probability_replay_status": get_nested(record, ["analysis", "probability_replay_audit", "status"]),
                "probability_replay_decision_eligible": get_nested(record, ["analysis", "probability_replay_audit", "decision_eligible"]) is True,
                "probability_replay_hash": get_nested(record, ["analysis", "probability_replay", "replay_hash"]),
            })
        except HTTPException as exc:
            freeze_results.append({"fixture": fixture, "action": "rejected", "status_code": exc.status_code, "reason": exc.detail})
    candidate_fixtures = {
        str(row.get("fixture_id") or "")
        for row in (prematch_plan.get("discovery", {}).get("candidates") or [])
        if str(row.get("fixture_id") or "")
    }
    priority_store = load_snapshot_store()
    for fixture in sorted(candidate_fixtures):
        latest = max(
            (row for row in ((priority_store.get("learning_frozen") or {}).get(fixture) or []) if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0), default=None,
        )
        if latest:
            priority_freezes.append(latest)
    priority_board_id = supplied_run_id or f"preview-{now_ts}"
    learning_priority_board = build_learning_priority_board(priority_board_id, priority_freezes, now_ts)
    existing_priority_board = (priority_store.get("learning_priority_boards") or {}).get(priority_board_id) if apply_changes else None
    if isinstance(existing_priority_board, dict):
        existing_priority_audit = audit_learning_priority_board(existing_priority_board)
        if existing_priority_audit.get("status") != "passed":
            raise HTTPException(status_code=409, detail="existing_learning_priority_board_integrity_invalid")
        learning_priority_board_result = {**existing_priority_board, "action": "unchanged"}
    elif learning_priority_board.get("match_priority_ranking") or learning_priority_board.get("non_participating_records"):
        if apply_changes:
            learning_priority_board_result = persist_learning_priority_board(learning_priority_board)
        else:
            learning_priority_board_result = {**learning_priority_board, "action": "would_freeze"}
    else:
        learning_priority_board_result = {
            "schema": "learning_prematch_priority_board_v1", "board_id": priority_board_id,
            "created_at": now_ts, "action": "skipped", "reason": "no_active_prematch_learning_freezes",
            "outcome_fields_used": False, "result_backfit_allowed": False,
        }
    learning_priority_board_contract = {
        key: value for key, value in learning_priority_board_result.items() if key != "action"
    }
    if auto_prepare or not apply_changes:
        observation_routing = route_market_language_observations(
            prematch_plan.get("discovery") or {},
            now_ts=now_ts,
            apply_changes=apply_changes,
            prematch_packet_builder=builder,
        )
    else:
        observation_routing = {
            "version": VERSION, "generated_at": now_ts,
            "results": [
                {"fixture": str(row.get("fixture_id") or ""), "action": "skipped", "reason": "auto_prepare_prematch_disabled"}
                for row in (prematch_plan.get("discovery", {}).get("observation_candidates") or [])
            ],
            "frozen_count": 0, "rejected_count": 0,
            "model_learning_effect": False, "automatic_scope_expansion": False,
        }
    forward_validation_queue_before_lock = learning_forward_validation_queue(now_ts=now_ts)
    shadow_lock_results = []
    if auto_lock_forward_validation:
        active_shadow_runner = shadow_model_runner or LEARNING_SHADOW_MODEL_RUNNER
        for item in forward_validation_queue_before_lock.get("items") or []:
            if not callable(active_shadow_runner):
                shadow_lock_results.append({
                    "hypothesis_id": item.get("hypothesis_id"), "freeze_id": item.get("freeze_id"),
                    "action": "blocked", "reason": "internal_shadow_model_runner_not_configured",
                })
                continue
            if not apply_changes:
                shadow_lock_results.append({
                    "hypothesis_id": item.get("hypothesis_id"), "freeze_id": item.get("freeze_id"),
                    "action": "would_compute_and_lock",
                })
                continue
            try:
                locked = generate_internal_shadow_lock(
                    item.get("hypothesis_id"), item.get("freeze_id"),
                    model_runner=active_shadow_runner, now_ts=now_ts,
                )
                shadow_lock_results.append({
                    "hypothesis_id": item.get("hypothesis_id"), "freeze_id": item.get("freeze_id"),
                    "action": locked.get("action"), "lock_hash": locked.get("lock_hash"),
                    "calculator_provenance": locked.get("calculator_provenance"),
                })
            except HTTPException as exc:
                shadow_lock_results.append({
                    "hypothesis_id": item.get("hypothesis_id"), "freeze_id": item.get("freeze_id"),
                    "action": "rejected", "status_code": exc.status_code, "reason": exc.detail,
                })
    forward_validation_queue = learning_forward_validation_queue(now_ts=now_ts) if apply_changes else forward_validation_queue_before_lock
    immutable_summary = {
        "run_id": supplied_run_id or None,
        "mode": "apply" if apply_changes else "dry_run",
        "started_at": now_ts,
        "postmatch_plan_generated_at": postmatch_plan.get("generated_at"),
        "prematch_plan_generated_at": prematch_plan.get("generated_at"),
        "execution_order": ["past_36h_postmatch", "future_24h_prematch"],
        "settlement_results": settlement_results,
        "postmatch_fact_results": fact_results,
        "market_language_postmatch_fact_results": observation_fact_results,
        "postmatch_review_draft_results": draft_results,
        "review_completion_results": review_completion_results,
        "quality_card_results": quality_card_results,
        "forward_validation_evidence_results": forward_validation_evidence_results,
        "promotion_candidate_results": promotion_candidate_results,
        "research_proposal_results": research_proposal_results,
        "hypothesis_registration_results": hypothesis_registration_results,
        "learning_admission_results": admission_results,
        "freeze_results": freeze_results,
        "learning_priority_board": learning_priority_board_contract,
        "market_language_observation_results": observation_routing.get("results") or [],
        "shadow_lock_results": shadow_lock_results,
        "forward_validation_queue": forward_validation_queue,
        "automatic_hypothesis_registration": any(
            row.get("action") in {"registered", "would_register"} for row in hypothesis_registration_results
        ),
        "automatic_hypothesis_registration_policy": "preregistered_templates_only",
        "automatic_champion_change": False,
        "automatic_promotion_candidate_creation": True,
        "explicit_user_confirmation_required_for_champion": True,
        "result_backfit_allowed": False,
    }
    result = {
        **immutable_summary,
        "settled_count": sum(row.get("action") == "settled" for row in settlement_results + review_completion_results),
        "admitted_count": sum(row.get("action") == "admitted" for row in admission_results),
        "frozen_count": sum(row.get("action") == "frozen" for row in freeze_results),
        "priority_board_frozen_count": int(learning_priority_board_result.get("action") == "frozen"),
        "priority_board_action": learning_priority_board_result.get("action"),
        "market_language_observation_frozen_count": observation_routing.get("frozen_count", 0),
        "market_language_postmatch_fact_collected_count": sum(
            row.get("action") == "facts_collected" for row in observation_fact_results
        ),
        "probability_replay_ready_count": sum(row.get("probability_replay_status") == "ready" for row in freeze_results),
        "probability_replay_missing_count": sum(row.get("probability_replay_status") == "data_missing" for row in freeze_results),
        "review_draft_count": sum(row.get("action") == "drafted" for row in draft_results),
        "quality_card_created_count": sum(row.get("action") == "created" for row in quality_card_results),
        "forward_validation_evidence_recorded_count": sum(row.get("action") == "recorded" for row in forward_validation_evidence_results),
        "promotion_candidate_created_count": sum(row.get("action") == "candidate_created" for row in promotion_candidate_results),
        "shadow_lock_created_count": sum(row.get("action") == "locked" for row in shadow_lock_results),
        "research_proposal_version_count": sum(row.get("action") == "proposed" for row in research_proposal_results),
        "hypothesis_registered_count": sum(row.get("action") == "registered" for row in hypothesis_registration_results),
        "rejected_count": sum(row.get("action") in {"rejected", "would_reject"} for row in settlement_results + fact_results + observation_fact_results + draft_results + review_completion_results + admission_results + freeze_results + (observation_routing.get("results") or []) + shadow_lock_results + forward_validation_evidence_results + promotion_candidate_results),
        "action": "completed" if apply_changes else "previewed",
    }
    if apply_changes:
        with SNAPSHOT_STORE_LOCK:
            store = load_snapshot_store()
            runs = store.setdefault("learning_runs", {})
            if supplied_run_id in runs:
                if learning_run_record_integrity(runs[supplied_run_id]).get("valid") is not True:
                    raise HTTPException(status_code=409, detail="learning_run_integrity_invalid")
                return {**runs[supplied_run_id], "action": "unchanged"}
            record = {**result, "run_hash": _content_hash(immutable_summary), "immutable": True}
            runs[supplied_run_id] = record
            store["version"] = VERSION
            write_snapshot_store(store)
            result = record
    return result


def _learning_freeze_by_id(store: Dict[str, Any], freeze_id: str) -> Optional[Dict[str, Any]]:
    for rows in (store.get("learning_frozen") or {}).values():
        for row in rows or []:
            if row.get("freeze_id") == freeze_id:
                return row
    return None


def _market_language_observation_by_id(store: Dict[str, Any], observation_id: str) -> Optional[Dict[str, Any]]:
    for rows in (store.get("market_language_frozen") or {}).values():
        for row in rows or []:
            if row.get("observation_id") == observation_id:
                return row
    return None


def build_market_language_observation_payload(
    candidate: Dict[str, Any], packet: Dict[str, Any], now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Build an observation-only prematch record without any learning contract."""
    now_ts = int(now_ts or time.time())
    candidate = candidate if isinstance(candidate, dict) else {}
    packet = packet if isinstance(packet, dict) else {}
    fixture = str(candidate.get("fixture_id") or "").strip()
    kickoff_at = _parse_timestamp(candidate.get("timestamp"))
    if not fixture or kickoff_at is None:
        raise HTTPException(status_code=422, detail="observation_candidate_identity_or_kickoff_missing")
    if str(candidate.get("status") or "").upper() != "NS" or now_ts >= kickoff_at:
        raise HTTPException(status_code=409, detail="observation_freeze_requires_not_started_fixture")
    if packet.get("ok") is not True or get_nested(packet, ["analysis_rules", "prematch_only"]) is not True:
        raise HTTPException(status_code=422, detail="prematch_only_observation_packet_required")
    generated_at = _parse_timestamp(packet.get("generated_at")) or now_ts
    if generated_at > now_ts + 60 or generated_at >= kickoff_at:
        raise HTTPException(status_code=409, detail="observation_packet_timestamp_invalid")
    decision = copy.deepcopy(packet.get("decision_layer")) if isinstance(packet.get("decision_layer"), dict) else {
        "decision": "PASS", "execution_action": "PASS", "pass_reasons": ["decision_layer_data_missing"],
    }
    market = packet.get("market") if isinstance(packet.get("market"), dict) else {"status": "data_missing"}
    analysis = {
        "prematch_only": True,
        "observation_mode": "MARKET_LANGUAGE_ONLY",
        "packet_generated_at": generated_at,
        "market_timeline": market,
        "market_language": decision.get("market_language") or market.get("latest_dynamics") or {"status": "data_missing"},
        "expression_optimizer": decision.get("expression_optimizer") or {"action": "PASS", "reason": "data_missing"},
        "fundamental_context": packet.get("pure_fundamental_script") or packet.get("fundamentals") or {"status": "data_missing"},
        "result_backfit_allowed": False,
        "model_learning_allowed": False,
    }
    provider_identity = packet.get("provider_identity") if isinstance(packet.get("provider_identity"), dict) else {}
    provider_identities = [row for row in (packet.get("provider_identities") or []) if isinstance(row, dict)]
    if provider_identity and not any(row.get("source") == provider_identity.get("source") for row in provider_identities):
        provider_identities.append(provider_identity)
    source_refs = [
        {"source": "api_football", "fixture_id": fixture, "captured_at": generated_at},
        {"source": str(packet.get("source") or "shadow_ai_packet"), "packet_version": str(packet.get("version") or VERSION)},
    ]
    for identity in provider_identities:
        if identity.get("source") == "the_odds_api":
            canonical = {
                "source": "the_odds_api",
                "sport_key": str(identity.get("sport_key") or "").strip().lower(),
                "event_id": str(identity.get("event_id") or "").strip().lower(),
                "home_team": str(identity.get("home_team") or "").strip(),
                "away_team": str(identity.get("away_team") or "").strip(),
            }
            required = ("sport_key", "event_id", "home_team", "away_team")
        elif identity.get("source") == "thestats":
            canonical = {
                "source": "thestats",
                "match_id": str(identity.get("match_id") or "").strip(),
                "kickoff_at": _parse_timestamp(identity.get("kickoff_at")),
                "home_team": str(identity.get("home_team") or "").strip(),
                "away_team": str(identity.get("away_team") or "").strip(),
            }
            required = ("match_id", "kickoff_at", "home_team", "away_team")
        else:
            continue
        if all(canonical[key] not in (None, "") for key in required) and identity.get("source_hash") == _content_hash(canonical):
            source_refs.append({
                **canonical, "identity_bound_at": generated_at,
                "identity_source_hash": identity["source_hash"],
            })
    return {
        "fixture": fixture,
        "captured_at": generated_at,
        "data_cutoff_at": generated_at,
        "kickoff_at": kickoff_at,
        "scope": candidate.get("scope") if isinstance(candidate.get("scope"), dict) else {},
        "versions": {"service": VERSION, "model": str(packet.get("version") or VERSION), "rules": LEARNING_RULES_VERSION},
        "analysis": analysis,
        "decision": decision,
        "source_refs": source_refs,
    }


def freeze_market_language_observation(payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Freeze a non-learning prematch record for market-language audit and later settlement only."""
    payload = payload if isinstance(payload, dict) else {}
    fixture = str(payload.get("fixture") or "").strip()
    if not fixture:
        raise HTTPException(status_code=400, detail="fixture_required")
    scope_audit = audit_learning_scope(payload.get("scope"))
    if scope_audit["eligible"]:
        raise HTTPException(status_code=409, detail="major_league_sample_requires_learning_pipeline")
    captured_at = _parse_timestamp(payload.get("captured_at")) or int(now_ts or time.time())
    data_cutoff_at = _parse_timestamp(payload.get("data_cutoff_at")) or captured_at
    kickoff_at = _parse_timestamp(payload.get("kickoff_at"))
    if kickoff_at is None:
        raise HTTPException(status_code=400, detail="valid_kickoff_at_required")
    if data_cutoff_at > captured_at:
        raise HTTPException(status_code=409, detail="data_cutoff_cannot_follow_capture")
    if captured_at >= kickoff_at or data_cutoff_at >= kickoff_at:
        raise HTTPException(status_code=409, detail="market_language_observation_must_be_frozen_before_kickoff")
    analysis = payload.get("analysis")
    decision = payload.get("decision")
    if not isinstance(analysis, dict) or not isinstance(decision, dict):
        raise HTTPException(status_code=400, detail="analysis_and_decision_objects_required")
    versions = payload.get("versions") if isinstance(payload.get("versions"), dict) else {}
    if not str(versions.get("rules") or "").strip():
        raise HTTPException(status_code=400, detail="rules_version_required")
    immutable_content = {
        "fixture": fixture,
        "kickoff_at": kickoff_at,
        "data_cutoff_at": data_cutoff_at,
        "scope": scope_audit["normalized"],
        "versions": {
            "service": str(versions.get("service") or VERSION),
            "model": str(versions.get("model") or VERSION),
            "rules": str(versions.get("rules")),
        },
        "analysis": analysis,
        "decision": decision,
        "source_refs": payload.get("source_refs") if isinstance(payload.get("source_refs"), list) else [],
        "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
        "allowed_modes": ["MARKET_LANGUAGE_ONLY", "SETTLEMENT_ONLY"],
        "model_learning_effect": False,
        "hypothesis_effect": False,
        "league_dna_effect": False,
    }
    content_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        rows = store.setdefault("market_language_frozen", {}).setdefault(fixture, [])
        duplicate = next((row for row in rows if row.get("content_hash") == content_hash), None)
        if duplicate:
            return {**duplicate, "action": "unchanged"}
        if rows and captured_at <= max(int(row.get("captured_at") or 0) for row in rows):
            raise HTTPException(status_code=409, detail="market_language_freeze_time_must_increase")
        version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
        observation_id = f"market:{fixture}:v{version_number}"
        record = {
            **immutable_content,
            "observation_id": observation_id,
            "version_number": version_number,
            "captured_at": captured_at,
            "content_hash": content_hash,
            "scope_audit": scope_audit,
            "status": "frozen",
            "immutable": True,
            "champion_effect": False,
        }
        rows.append(record)
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "frozen"}


def collect_market_language_postmatch_facts(
    observation_id: Any, now_ts: Optional[int] = None, fact_fetcher: Optional[Any] = None,
) -> Dict[str, Any]:
    """Collect independently verifiable result facts without grading or learning from the observation."""
    observation_id = str(observation_id or "").strip()
    if not observation_id:
        raise HTTPException(status_code=400, detail="observation_id_required")
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    freeze = _market_language_observation_by_id(store, observation_id)
    if not freeze:
        raise HTTPException(status_code=404, detail="market_language_observation_not_found")
    if now_ts < int(freeze.get("kickoff_at") or 0) + 2 * 3600:
        raise HTTPException(status_code=409, detail="market_language_fact_collection_not_due")
    raw_fixture_id = freeze.get("fixture")
    if fact_fetcher is None:
        try:
            int(raw_fixture_id)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail="provider_fixture_id_required_for_observation_fact_collection") from exc
        fetched = _learning_postmatch_fact_fetch(freeze)
    else:
        fetched = fact_fetcher(raw_fixture_id)
    if not isinstance(fetched, dict) or fetched.get("ok") is not True:
        raise HTTPException(status_code=422, detail={
            "error": "market_language_postmatch_fact_fetch_incomplete",
            "reason": (fetched or {}).get("error") if isinstance(fetched, dict) else "invalid_fetcher_response",
        })
    result = fetched.get("result") if isinstance(fetched.get("result"), dict) else {}
    try:
        home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="market_language_postmatch_result_incomplete") from exc
    if str(result.get("status") or "").upper() not in {"FT", "AET", "PEN"} or min(home_goals, away_goals) < 0:
        raise HTTPException(status_code=422, detail="market_language_postmatch_result_not_final")
    verified_sources, verified_authorities, evidence_refs = set(), set(), set()
    for row in fetched.get("source_audit") or []:
        if not isinstance(row, dict) or row.get("ok") is not True or str(row.get("component") or "").strip().lower() != "result":
            continue
        source = str(row.get("source") or "").strip().casefold()
        evidence_ref = str(row.get("evidence_ref") or row.get("url") or row.get("id") or "").strip()
        try:
            source_home, source_away = int(row.get("home_goals")), int(row.get("away_goals"))
        except (TypeError, ValueError):
            continue
        if not source or not evidence_ref or source_home != home_goals or source_away != away_goals:
            continue
        normalized_ref = evidence_ref.casefold()
        if normalized_ref in evidence_refs:
            continue
        evidence_refs.add(normalized_ref)
        parsed = urlparse(evidence_ref)
        verified_sources.add(source)
        verified_authorities.add(str(parsed.hostname or source).strip().casefold())
    immutable_content = {
        "observation_id": observation_id,
        "fixture": freeze.get("fixture"),
        "freeze_hash": freeze.get("content_hash"),
        "result": {**result, "home_goals": home_goals, "away_goals": away_goals},
        "events": fetched.get("events") if isinstance(fetched.get("events"), list) else [],
        "statistics": fetched.get("statistics") if isinstance(fetched.get("statistics"), list) else [],
        "source_audit": fetched.get("source_audit") if isinstance(fetched.get("source_audit"), list) else [],
        "verification": {
            "independent_source_count": len(verified_authorities),
            "verified_source_names": sorted(verified_sources),
            "verified_source_authorities": sorted(verified_authorities),
            "unique_evidence_ref_count": len(evidence_refs),
            "status": "verified" if len(verified_authorities) >= 2 else "single_source_pending",
            "settlement_eligible": len(verified_authorities) >= 2,
            "required_independent_sources": 2,
        },
        "market_read_evaluation": None,
        "model_learning_effect": False,
        "hypothesis_effect": False,
        "league_dna_effect": False,
        "champion_effect": False,
    }
    fact_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        latest_store = load_snapshot_store()
        rows = latest_store.setdefault("market_language_postmatch_facts", {}).setdefault(observation_id, [])
        duplicate = next((row for row in rows if row.get("fact_hash") == fact_hash), None)
        if duplicate:
            return {**duplicate, "action": "unchanged"}
        version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
        record = {
            **immutable_content, "fact_hash": fact_hash, "version_number": version_number,
            "collected_at": now_ts, "immutable": True, "action": "facts_collected",
        }
        rows.append(record)
        latest_store["version"] = VERSION
        write_snapshot_store(latest_store)
        return record


def market_language_settlement_plan(
    now_ts: Optional[int] = None, store_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now_ts = int(now_ts or time.time())
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    settlements = store.get("market_language_settlements") or {}
    fact_store = store.get("market_language_postmatch_facts") or {}
    rows = []
    for fixture, versions in (store.get("market_language_frozen") or {}).items():
        latest = max((row for row in versions or [] if isinstance(row, dict)), key=lambda row: int(row.get("version_number") or 0), default=None)
        if not latest:
            continue
        observation_id = str(latest.get("observation_id") or "")
        kickoff_at = int(latest.get("kickoff_at") or 0)
        if observation_id in settlements or now_ts < kickoff_at + 2 * 3600:
            continue
        fact_versions = [row for row in (fact_store.get(observation_id) or []) if isinstance(row, dict)]
        latest_fact = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
        verified = get_nested(latest_fact or {}, ["verification", "settlement_eligible"]) is True
        age_seconds = now_ts - kickoff_at
        rows.append({
            "fixture": str(fixture), "observation_id": observation_id,
            "kickoff_at": kickoff_at, "hours_since_kickoff": round(age_seconds / 3600, 2),
            "fact_hash": (latest_fact or {}).get("fact_hash"),
            "fact_verification_status": get_nested(latest_fact or {}, ["verification", "status"]) or "data_missing",
            "required_action": "review_market_language_against_verified_facts" if verified else "collect_or_verify_result_facts",
            "settlement_ready": verified,
            "automatic_fact_collection_eligible": (
                not verified and age_seconds <= LEARNING_POSTMATCH_LOOKBACK_HOURS * 3600
            ),
            "model_learning_effect": False,
        })
    rows.sort(key=lambda row: (row["kickoff_at"], row["fixture"]))
    return {
        "version": VERSION, "generated_at": now_ts,
        "due_count": len(rows), "settlement_ready_count": sum(row["settlement_ready"] for row in rows),
        "fact_collection_due_count": sum(row["fact_verification_status"] != "verified" for row in rows),
        "automatic_fact_collection_due_count": sum(row["automatic_fact_collection_eligible"] for row in rows),
        "items": rows, "automatic_market_read_grade": False,
        "model_learning_effect": False, "automatic_champion_change": False,
    }


def settle_market_language_observation(observation_id: Any, payload: Any, now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Settle an observation without creating any model-learning or league-DNA evidence."""
    observation_id = str(observation_id or "").strip()
    if not observation_id:
        raise HTTPException(status_code=400, detail="observation_id_required")
    payload = payload if isinstance(payload, dict) else {}
    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    try:
        home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="final_goals_required")
    if home_goals < 0 or away_goals < 0 or str(result.get("status") or "").upper() not in ("FT", "AET", "PEN"):
        raise HTTPException(status_code=400, detail="verified_final_result_required")
    pre_store = load_snapshot_store()
    pre_freeze = _market_language_observation_by_id(pre_store, observation_id)
    if not pre_freeze:
        raise HTTPException(status_code=404, detail="market_language_observation_not_found")
    fact_versions = [
        row for row in ((pre_store.get("market_language_postmatch_facts") or {}).get(observation_id) or [])
        if isinstance(row, dict)
    ]
    latest_fact = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
    verification_refs = payload.get("verification_refs") if isinstance(payload.get("verification_refs"), list) else []
    fact_hash = str(payload.get("fact_hash") or "").strip()
    if latest_fact:
        if get_nested(latest_fact, ["verification", "settlement_eligible"]) is not True:
            raise HTTPException(status_code=409, detail="verified_market_language_fact_packet_required")
        if not fact_hash or fact_hash != latest_fact.get("fact_hash"):
            raise HTTPException(status_code=409, detail="latest_market_language_fact_hash_required")
        fact_result = latest_fact.get("result") if isinstance(latest_fact.get("result"), dict) else {}
        if (
            int(fact_result.get("home_goals", -1)) != home_goals
            or int(fact_result.get("away_goals", -1)) != away_goals
            or str(fact_result.get("status") or "").upper() != str(result.get("status") or "").upper()
        ):
            raise HTTPException(status_code=409, detail="market_language_result_must_match_verified_facts")
        verification_refs = [
            {
                "source": str(row.get("source") or ""),
                "id": str(row.get("evidence_ref") or row.get("url") or row.get("id") or ""),
            }
            for row in (latest_fact.get("source_audit") or [])
            if isinstance(row, dict) and row.get("ok") is True
            and str(row.get("component") or "").strip().lower() == "result"
        ]
    valid_refs = [
        ref for ref in verification_refs
        if isinstance(ref, dict)
        and str(ref.get("source") or "").strip()
        and str(ref.get("url") or ref.get("id") or ref.get("title") or "").strip()
    ]
    unique_refs, independent_authorities = set(), set()
    for ref in valid_refs:
        evidence_ref = str(ref.get("url") or ref.get("id") or ref.get("title") or "").strip()
        normalized_ref = evidence_ref.casefold()
        if normalized_ref in unique_refs:
            continue
        unique_refs.add(normalized_ref)
        parsed = urlparse(evidence_ref)
        independent_authorities.add(str(parsed.hostname or ref.get("source") or "").strip().casefold())
    if len(independent_authorities) < 2:
        raise HTTPException(status_code=409, detail="two_independent_result_sources_required")
    settlement = payload.get("settlement") if isinstance(payload.get("settlement"), dict) else {}
    payout_outcome = str(settlement.get("payout_outcome") or "").strip().upper()
    if payout_outcome not in {"WIN", "HALF_WIN", "PUSH", "HALF_LOSS", "LOSS", "VOID", "PASS"}:
        raise HTTPException(status_code=400, detail="valid_payout_outcome_required")
    market_read = str(payload.get("market_read_evaluation") or "").strip().upper()
    if market_read not in {"CONFIRMED", "PARTIAL", "REJECTED", "INCONCLUSIVE"}:
        raise HTTPException(status_code=400, detail="valid_market_read_evaluation_required")
    settled_at = _parse_timestamp(payload.get("settled_at")) or int(now_ts or time.time())
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        freeze = _market_language_observation_by_id(store, observation_id)
        if not freeze:
            raise HTTPException(status_code=404, detail="market_language_observation_not_found")
        if settled_at < int(freeze.get("kickoff_at") or 0):
            raise HTTPException(status_code=409, detail="settlement_cannot_precede_kickoff")
        immutable_content = {
            "observation_id": observation_id,
            "fixture": freeze.get("fixture"),
            "freeze_hash": freeze.get("content_hash"),
            "fact_hash": latest_fact.get("fact_hash") if latest_fact else None,
            "result": {**result, "home_goals": home_goals, "away_goals": away_goals},
            "verification_refs": valid_refs[:10],
            "result_verification": {
                "independent_authority_count": len(independent_authorities),
                "independent_authorities": sorted(independent_authorities),
                "unique_evidence_ref_count": len(unique_refs),
                "required_independent_sources": 2,
                "status": "verified",
            },
            "market_read_evaluation": market_read,
            "settlement": {**settlement, "payout_outcome": payout_outcome},
            "event_audit": payload.get("event_audit") if isinstance(payload.get("event_audit"), dict) else {"status": "data_missing"},
            "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
            "model_learning_effect": False,
            "hypothesis_effect": False,
            "league_dna_effect": False,
        }
        settlement_hash = _content_hash(immutable_content)
        settlements = store.setdefault("market_language_settlements", {})
        existing = settlements.get(observation_id)
        if existing:
            if existing.get("settlement_hash") == settlement_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="market_language_settlement_already_recorded")
        record = {
            **immutable_content,
            "settled_at": settled_at,
            "settlement_hash": settlement_hash,
            "immutable": True,
            "champion_effect": False,
            "action": "settled",
        }
        settlements[observation_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def market_language_observation_status_report(now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    settlements = store.get("market_language_settlements") or {}
    facts = store.get("market_language_postmatch_facts") or {}
    latest_rows = []
    for fixture, versions in (store.get("market_language_frozen") or {}).items():
        latest = max(
            (row for row in versions or [] if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0),
            default=None,
        )
        if not latest:
            continue
        observation_id = str(latest.get("observation_id") or "")
        settled = observation_id in settlements
        kickoff_at = int(latest.get("kickoff_at") or 0)
        fact_versions = [row for row in (facts.get(observation_id) or []) if isinstance(row, dict)]
        latest_fact = max(fact_versions, key=lambda row: int(row.get("version_number") or 0), default=None)
        facts_verified = get_nested(latest_fact or {}, ["verification", "settlement_eligible"]) is True
        if settled:
            state = "settled"
        elif now_ts < kickoff_at:
            state = "prematch_frozen"
        elif now_ts < kickoff_at + 2 * 3600:
            state = "awaiting_result_window"
        elif not latest_fact:
            state = "fact_collection_due"
        elif not facts_verified:
            state = "result_verification_pending"
        else:
            state = "settlement_ready"
        latest_rows.append({
            "fixture": str(fixture),
            "observation_id": observation_id,
            "kickoff_at": kickoff_at,
            "state": state,
            "competition_id": get_nested(latest, ["scope", "competition_id"]),
            "competition_name": get_nested(latest, ["scope", "competition_name"]),
            "fact_hash": (latest_fact or {}).get("fact_hash"),
            "fact_verification_status": get_nested(latest_fact or {}, ["verification", "status"]) or "data_missing",
            "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
            "allowed_modes": ["MARKET_LANGUAGE_ONLY", "SETTLEMENT_ONLY"],
        })
    latest_rows.sort(key=lambda row: (row["kickoff_at"], row["fixture"]))
    state_counts = {
        state: sum(row["state"] == state for row in latest_rows)
        for state in (
            "prematch_frozen", "awaiting_result_window", "fact_collection_due",
            "result_verification_pending", "settlement_ready", "settled",
        )
    }
    return {
        "version": VERSION,
        "generated_at": now_ts,
        "fixture_count": len(latest_rows),
        "frozen_version_count": sum(len(rows or []) for rows in (store.get("market_language_frozen") or {}).values()),
        "postmatch_fact_version_count": sum(len(rows or []) for rows in facts.values()),
        "settlement_count": len(settlements),
        "state_counts": state_counts,
        "fact_collection_due": [row for row in latest_rows if row["state"] == "fact_collection_due"],
        "settlement_ready": [row for row in latest_rows if row["state"] == "settlement_ready"],
        "records": latest_rows[:100],
        "records_truncated": len(latest_rows) > 100,
        "model_learning_effect": False,
        "hypothesis_effect": False,
        "league_dna_effect": False,
        "champion_effect": False,
        "policy": "observation records are isolated from every model-learning and promotion namespace",
    }


def route_market_language_observations(
    discovery: Dict[str, Any], now_ts: Optional[int] = None, apply_changes: bool = True,
    prematch_packet_builder: Optional[Any] = None,
) -> Dict[str, Any]:
    """Route non-registry prematch fixtures into the isolated observation namespace."""
    now_ts = int(now_ts or time.time())
    builder = prematch_packet_builder or build_shadow_ai_packet
    results = []
    candidates = discovery.get("observation_candidates") if isinstance(discovery, dict) else []
    for candidate in candidates or []:
        fixture = str(candidate.get("fixture_id") or "")
        if not apply_changes:
            results.append({"fixture": fixture, "action": "would_observe", "reason": candidate.get("routing_reason")})
            continue
        try:
            packet = builder(int(fixture))
            payload = build_market_language_observation_payload(candidate, packet, now_ts=now_ts)
            record = freeze_market_language_observation(payload, now_ts=now_ts)
            results.append({
                "fixture": fixture,
                "observation_id": record.get("observation_id"),
                "action": record.get("action"),
                "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
            })
        except HTTPException as exc:
            results.append({"fixture": fixture, "action": "rejected", "status_code": exc.status_code, "reason": exc.detail})
        except Exception as exc:
            results.append({
                "fixture": fixture, "action": "rejected",
                "reason": "observation_packet_build_failed", "error_type": type(exc).__name__,
            })
    return {
        "version": VERSION,
        "generated_at": now_ts,
        "results": results,
        "frozen_count": sum(row.get("action") == "frozen" for row in results),
        "rejected_count": sum(row.get("action") == "rejected" for row in results),
        "model_learning_effect": False,
        "automatic_scope_expansion": False,
    }


def freeze_learning_sample(payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    fixture = str(payload.get("fixture") or "").strip()
    if not fixture:
        raise HTTPException(status_code=400, detail="fixture_required")
    scope_audit = audit_learning_scope(payload.get("scope"))
    if not scope_audit["eligible"]:
        raise HTTPException(status_code=422, detail={"error": "top_flight_scope_not_verified", "reasons": scope_audit["reasons"]})
    captured_at = _parse_timestamp(payload.get("captured_at")) or int(now_ts or time.time())
    data_cutoff_at = _parse_timestamp(payload.get("data_cutoff_at")) or captured_at
    kickoff_at = _parse_timestamp(payload.get("kickoff_at"))
    if kickoff_at is None:
        raise HTTPException(status_code=400, detail="valid_kickoff_at_required")
    if data_cutoff_at > captured_at:
        raise HTTPException(status_code=409, detail="data_cutoff_cannot_follow_capture")
    if captured_at >= kickoff_at or data_cutoff_at >= kickoff_at:
        raise HTTPException(status_code=409, detail="learning_sample_must_be_frozen_before_kickoff")
    analysis = payload.get("analysis")
    decision = payload.get("decision")
    if not isinstance(analysis, dict) or not isinstance(decision, dict):
        raise HTTPException(status_code=400, detail="analysis_and_decision_objects_required")
    versions = payload.get("versions") if isinstance(payload.get("versions"), dict) else {}
    if not str(versions.get("rules") or "").strip():
        raise HTTPException(status_code=400, detail="rules_version_required")
    league_dna_view = league_dna_model_view(scope_audit["normalized"])
    requested_league_dna = str(versions.get("league_dna") or "data_missing").strip()
    if league_dna_view["status"] == "VERIFIED_ACTIVE":
        if requested_league_dna != league_dna_view["version"]:
            raise HTTPException(status_code=409, detail={"error": "league_dna_version_mismatch", "required_version": league_dna_view["version"]})
    elif requested_league_dna not in {"data_missing", "candidate_only"}:
        raise HTTPException(status_code=409, detail="unverified_league_dna_cannot_enter_prematch_freeze")
    immutable_content = {
        "fixture": fixture,
        "kickoff_at": kickoff_at,
        "data_cutoff_at": data_cutoff_at,
        "scope": scope_audit["normalized"],
        "versions": {
            "service": str(versions.get("service") or VERSION),
            "model": str(versions.get("model") or VERSION),
            "rules": str(versions.get("rules")),
            "league_dna": league_dna_view["version"] or league_dna_view["status"],
        },
        "league_dna_audit": {
            "status": league_dna_view["status"], "version": league_dna_view["version"],
            "active_tag_count": league_dna_view["active_tag_count"],
            "candidate_tag_count": league_dna_view["candidate_tag_count"],
            "champion_effect": league_dna_view["champion_effect"],
        },
        "analysis": analysis,
        "decision": decision,
        "source_refs": payload.get("source_refs") if isinstance(payload.get("source_refs"), list) else [],
    }
    content_hash = _content_hash(immutable_content)
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        rows = store.setdefault("learning_frozen", {}).setdefault(fixture, [])
        duplicate = next((row for row in rows if row.get("content_hash") == content_hash), None)
        if duplicate:
            return {**duplicate, "action": "unchanged"}
        if rows and captured_at <= max(int(row.get("captured_at") or 0) for row in rows):
            raise HTTPException(status_code=409, detail="learning_freeze_time_must_increase")
        version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
        freeze_id = f"{fixture}:v{version_number}"
        record = {
            **immutable_content,
            "freeze_id": freeze_id,
            "version_number": version_number,
            "captured_at": captured_at,
            "content_hash": content_hash,
            "scope_audit": scope_audit,
            "status": "frozen",
            "immutable": True,
            "champion_effect": False,
        }
        rows.append(record)
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "frozen"}


def audit_learning_postmatch_review(review: Any) -> Dict[str, Any]:
    review = review if isinstance(review, dict) else {}
    reasons = []
    normalized_sections = {}
    for section in LEARNING_REVIEW_SECTIONS:
        row = review.get(section) if isinstance(review.get(section), dict) else {}
        status = str(row.get("status") or "").strip().lower()
        if status not in LEARNING_REVIEW_STATUSES:
            reasons.append(f"{section}_status_required")
            status = "data_missing"
        normalized_sections[section] = {**row, "status": status}
    process_reasoning = str(review.get("process_reasoning") or "").strip()
    if len(process_reasoning) < 20:
        reasons.append("process_reasoning_too_short")
    disposition = review.get("learning_disposition") if isinstance(review.get("learning_disposition"), dict) else {}
    result_backfit_used = disposition.get("result_backfit_used")
    champion_change_requested = disposition.get("champion_change_requested")
    new_theory_status = str(disposition.get("new_theory_status") or "none").strip()
    if result_backfit_used is not False:
        reasons.append("result_backfit_must_be_explicitly_false")
    if champion_change_requested is not False:
        reasons.append("single_match_champion_change_forbidden")
    if new_theory_status not in {"none", *LEARNING_HYPOTHESIS_TYPES}:
        reasons.append("invalid_single_match_theory_disposition")
    implementation_gap = disposition.get("existing_rule_implementation_gap") is True
    existing_rule_ref = str(disposition.get("existing_rule_ref") or "").strip()
    if implementation_gap and not existing_rule_ref:
        reasons.append("implementation_gap_requires_existing_rule_reference")
    normalized_disposition = {
        "result_backfit_used": result_backfit_used,
        "champion_change_requested": champion_change_requested,
        "new_theory_status": new_theory_status,
        "existing_rule_implementation_gap": implementation_gap,
        "existing_rule_ref": existing_rule_ref or None,
        "regression_test_required": bool(disposition.get("regression_test_required")) if implementation_gap else False,
    }
    if implementation_gap and not normalized_disposition["regression_test_required"]:
        reasons.append("implementation_gap_requires_regression_test")
    review_mode = str(review.get("review_mode") or "manual").strip().lower()
    outcome_not_used_for_process_grade = review.get("outcome_not_used_for_process_grade")
    return {
        "eligible": not reasons,
        "reasons": reasons,
        "normalized": {
            **normalized_sections,
            "process_reasoning": process_reasoning,
            "review_mode": review_mode,
            "outcome_not_used_for_process_grade": outcome_not_used_for_process_grade,
            "learning_disposition": normalized_disposition,
        },
        "policy": "review process and expression against the immutable prematch freeze; never infer a rule from the final score",
    }


def settle_learning_sample(freeze_id: Any, result: Any, process_classification: Any, event_audit: Any = None, settled_at: Any = None, review: Any = None, fact_hash: Any = None) -> Dict[str, Any]:
    freeze_id = str(freeze_id or "").strip()
    if not freeze_id:
        raise HTTPException(status_code=400, detail="freeze_id_required")
    if process_classification not in LEARNING_PROCESS_CLASSES:
        raise HTTPException(status_code=400, detail="invalid_process_classification")
    result = result if isinstance(result, dict) else {}
    try:
        home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    except (TypeError, ValueError):
        raise HTTPException(status_code=400, detail="final_goals_required")
    if home_goals < 0 or away_goals < 0 or str(result.get("status") or "").upper() not in ("FT", "AET", "PEN"):
        raise HTTPException(status_code=400, detail="verified_final_result_required")
    event_audit = event_audit if isinstance(event_audit, dict) else {"status": "data_missing"}
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze:
            raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
        review_audit = audit_learning_postmatch_review(review)
        if not review_audit["eligible"]:
            raise HTTPException(status_code=422, detail={"error": "postmatch_review_incomplete_or_backfit_risk", "reasons": review_audit["reasons"]})
        fact_versions = (store.get("learning_postmatch_facts") or {}).get(freeze_id) or []
        if not fact_versions:
            raise HTTPException(status_code=409, detail="verified_postmatch_fact_packet_required")
        verified_facts = max(fact_versions, key=lambda row: int(row.get("version_number") or 0))
        if get_nested(verified_facts, ["verification", "settlement_eligible"]) is not True:
            raise HTTPException(status_code=409, detail="independent_result_verification_required")
        supplied_fact_hash = str(fact_hash or "").strip()
        if not supplied_fact_hash or supplied_fact_hash != str(verified_facts.get("fact_hash") or ""):
            raise HTTPException(status_code=409, detail="latest_verified_fact_hash_required")
        verified_result = verified_facts.get("result") if isinstance(verified_facts.get("result"), dict) else {}
        if (
            str(verified_result.get("status") or "").upper() != str(result.get("status") or "").upper()
            or int(verified_result.get("home_goals")) != home_goals
            or int(verified_result.get("away_goals")) != away_goals
        ):
            raise HTTPException(status_code=409, detail="submitted_result_does_not_match_verified_facts")
        settled_ts = _parse_timestamp(settled_at) or int(time.time())
        if settled_ts < int(freeze.get("kickoff_at") or 0):
            raise HTTPException(status_code=409, detail="postmatch_settlement_cannot_precede_kickoff")
        immutable_content = {
            "freeze_id": freeze_id,
            "fixture": freeze.get("fixture"),
            "freeze_hash": freeze.get("content_hash"),
            "fact_hash": verified_facts.get("fact_hash"),
            "fact_version_number": verified_facts.get("version_number"),
            "result": {**result, "home_goals": home_goals, "away_goals": away_goals},
            "process_classification": process_classification,
            "event_audit": event_audit,
            "review": review_audit["normalized"],
        }
        postmatch_hash = _content_hash(immutable_content)
        postmatches = store.setdefault("learning_postmatch", {})
        existing = postmatches.get(freeze_id)
        if existing:
            if existing.get("postmatch_hash") == postmatch_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="postmatch_already_recorded")
        record = {
            **immutable_content,
            "settled_at": settled_ts,
            "postmatch_hash": postmatch_hash,
            "review_audit": {"eligible": True, "policy": review_audit["policy"]},
            "immutable": True,
            "champion_effect": False,
            "action": "settled",
        }
        postmatches[freeze_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def _normalize_learning_ablation_plan(value: Any) -> Dict[str, Any]:
    value = value if isinstance(value, dict) else {}
    primary_module = str(value.get("primary_module") or "").strip().upper()
    raw_modules = value.get("required_modules") if isinstance(value.get("required_modules"), list) else []
    required_modules = list(dict.fromkeys(str(module).strip().upper() for module in raw_modules if str(module).strip()))
    if not required_modules:
        raise HTTPException(status_code=422, detail="ablation_plan_required_modules_required")
    invalid = [module for module in required_modules if module not in LEARNING_ABLATION_INTERVENTIONS]
    if invalid:
        raise HTTPException(status_code=422, detail={"error": "unsupported_ablation_module", "modules": invalid})
    if primary_module not in required_modules:
        raise HTTPException(status_code=422, detail="ablation_primary_module_must_be_required")
    raw_interventions = value.get("module_interventions") if isinstance(value.get("module_interventions"), dict) else {}
    module_interventions = {}
    for module in required_modules:
        row = raw_interventions.get(module) if isinstance(raw_interventions.get(module), dict) else {}
        expected = LEARNING_ABLATION_INTERVENTIONS[module]
        if str(row.get("intervention") or "").strip() != expected:
            raise HTTPException(status_code=422, detail={
                "error": "exact_module_ablation_intervention_required",
                "module": module, "required_intervention": expected,
            })
        minimum_gain = as_float(row.get("minimum_brier_gain"))
        if minimum_gain is None or not 0 <= minimum_gain <= 1:
            raise HTTPException(status_code=422, detail={"error": "valid_module_minimum_brier_gain_required", "module": module})
        module_interventions[module] = {
            "intervention": expected,
            "minimum_brier_gain": minimum_gain,
            "expected_direction": "challenger_brier_lower_than_module_ablated_brier",
        }
    minimum_champion_gain = as_float(value.get("minimum_challenger_brier_gain_over_champion"))
    if minimum_champion_gain is None or not 0 <= minimum_champion_gain <= 1:
        raise HTTPException(status_code=422, detail="valid_minimum_challenger_brier_gain_over_champion_required")
    return {
        "schema_version": "module_ablation_v1",
        "primary_module": primary_module,
        "required_modules": required_modules,
        "module_interventions": module_interventions,
        "minimum_challenger_brier_gain_over_champion": minimum_champion_gain,
        "caller_supplied_validation_outcome_allowed": False,
    }


def _normalize_learning_challenger_spec(value: Any, required_modules: List[str]) -> Dict[str, Any]:
    """Validate a bounded, executable and entirely preregistered Poisson intervention."""
    value = value if isinstance(value, dict) else {}
    if value.get("schema_version") != "poisson_log_rate_adjustment_v1":
        raise HTTPException(status_code=422, detail="executable_challenger_spec_required")
    if value.get("uses_market_odds") is not False:
        raise HTTPException(status_code=422, detail="challenger_spec_must_be_odds_independent")
    raw = value.get("module_log_rate_deltas") if isinstance(value.get("module_log_rate_deltas"), dict) else {}
    if set(raw) != set(required_modules):
        raise HTTPException(status_code=422, detail={
            "error": "challenger_spec_modules_must_match_ablation_plan",
            "required_modules": required_modules,
            "submitted_modules": sorted(str(module) for module in raw),
        })
    normalized = {}
    for module in required_modules:
        row = raw.get(module) if isinstance(raw.get(module), dict) else {}
        home_delta, away_delta = as_float(row.get("home")), as_float(row.get("away"))
        if any(delta is None or abs(delta) > LEARNING_CHALLENGER_MAX_MODULE_LOG_RATE_DELTA for delta in (home_delta, away_delta)):
            raise HTTPException(status_code=422, detail={
                "error": "challenger_module_log_rate_delta_out_of_range", "module": module,
                "maximum_absolute_delta": LEARNING_CHALLENGER_MAX_MODULE_LOG_RATE_DELTA,
            })
        if abs(home_delta) < 1e-12 and abs(away_delta) < 1e-12:
            raise HTTPException(status_code=422, detail={"error": "challenger_module_intervention_cannot_be_zero", "module": module})
        normalized[module] = {"home": round(home_delta, 8), "away": round(away_delta, 8)}
    combined_home = round(sum(row["home"] for row in normalized.values()), 8)
    combined_away = round(sum(row["away"] for row in normalized.values()), 8)
    if any(abs(delta) > LEARNING_CHALLENGER_MAX_COMBINED_LOG_RATE_DELTA for delta in (combined_home, combined_away)):
        raise HTTPException(status_code=422, detail={
            "error": "challenger_combined_log_rate_delta_out_of_range",
            "maximum_absolute_delta": LEARNING_CHALLENGER_MAX_COMBINED_LOG_RATE_DELTA,
        })
    normalized_spec = {
        "schema_version": "poisson_log_rate_adjustment_v1",
        "runner_id": "builtin_preregistered_poisson_challenger",
        "runner_version": "1",
        "uses_market_odds": False,
        "expression_policy": "preserve_frozen_market_selection_and_line",
        "module_log_rate_deltas": normalized,
        "combined_log_rate_delta": {"home": combined_home, "away": combined_away},
    }
    return {**normalized_spec, "spec_hash": _content_hash(normalized_spec)}


def _normalize_learning_validation_plan(value: Any) -> Dict[str, Any]:
    value = value if isinstance(value, dict) else {}
    normalized_ablation_plan = _normalize_learning_ablation_plan(value.get("ablation_plan"))
    structured_scope = value.get("structured_scope") if isinstance(value.get("structured_scope"), dict) else {}
    raw_competition_ids = structured_scope.get("competition_ids") if isinstance(structured_scope.get("competition_ids"), list) else []
    try:
        competition_ids = sorted(set(int(item) for item in raw_competition_ids if not isinstance(item, bool)))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="structured_scope_competition_ids_invalid") from exc
    markets = sorted(set(
        str(item).strip() for item in (structured_scope.get("markets") or []) if str(item).strip()
    )) if isinstance(structured_scope.get("markets"), list) else []
    if not competition_ids or not markets:
        raise HTTPException(status_code=422, detail="structured_validation_scope_required")
    invalid_markets = [market for market in markets if market not in LEAGUE_DNA_MARKETS]
    if invalid_markets:
        raise HTTPException(status_code=422, detail={"error": "unsupported_structured_scope_market", "markets": invalid_markets})
    challenger_spec = _normalize_learning_challenger_spec(
        value.get("challenger_spec"), normalized_ablation_plan["required_modules"],
    )
    return {
        **value,
        "structured_scope": {"competition_ids": competition_ids, "markets": markets},
        "ablation_plan": normalized_ablation_plan,
        "challenger_spec": challenger_spec,
        "calculator_policy": {
            "required_origin": "internal_shadow_runner",
            "runner_schema": "learning_shadow_model_run_v1",
            "required_runner_id": "builtin_preregistered_poisson_challenger",
            "required_runner_version": "1",
            "external_submitted_outputs_promotion_eligible": False,
        },
    }


def register_learning_hypothesis(payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    hypothesis_type = str(payload.get("type") or "").strip()
    if hypothesis_type not in LEARNING_HYPOTHESIS_TYPES:
        raise HTTPException(status_code=400, detail="invalid_hypothesis_type")
    required_text = ("title", "definition", "applicable_scope", "expected_direction", "failure_conditions", "falsification_criteria")
    missing = [key for key in required_text if not str(payload.get(key) or "").strip()]
    if missing:
        raise HTTPException(status_code=400, detail={"error": "hypothesis_fields_required", "missing": missing})
    discovery = [str(value).strip() for value in (payload.get("discovery_freeze_ids") or []) if str(value).strip()]
    if not discovery:
        raise HTTPException(status_code=400, detail="discovery_freeze_ids_required")
    base = {key: str(payload.get(key)).strip() for key in required_text}
    hypothesis_id = str(payload.get("hypothesis_id") or f"hyp-{_content_hash({**base, 'type': hypothesis_type})[:16]}").strip()
    source_proposal_id = str(payload.get("source_proposal_id") or "").strip()
    source_proposal_hash = str(payload.get("source_proposal_hash") or "").strip()
    validation_plan = _normalize_learning_validation_plan(payload.get("validation_plan"))
    registration_provenance = payload.get("registration_provenance") if isinstance(payload.get("registration_provenance"), dict) else None
    if registration_provenance is not None:
        if registration_provenance.get("mode") != "automatic_from_preregistered_template":
            raise HTTPException(status_code=422, detail="unsupported_hypothesis_registration_provenance")
        if not all(str(registration_provenance.get(key) or "").strip() for key in ("template_id", "template_hash", "proposal_id", "proposal_hash")):
            raise HTTPException(status_code=422, detail="complete_hypothesis_registration_provenance_required")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        unknown = [freeze_id for freeze_id in discovery if not _learning_freeze_by_id(store, freeze_id)]
        unsettled = [freeze_id for freeze_id in discovery if freeze_id not in (store.get("learning_postmatch") or {})]
        if unknown:
            raise HTTPException(status_code=404, detail={"error": "discovery_freeze_not_found", "freeze_ids": unknown})
        if unsettled:
            raise HTTPException(status_code=409, detail={"error": "discovery_sample_not_settled", "freeze_ids": unsettled})
        source_proposal = None
        if source_proposal_id:
            proposal_versions = (store.get("learning_research_proposals") or {}).get(source_proposal_id) or []
            source_proposal = max(
                (row for row in proposal_versions if isinstance(row, dict)),
                key=lambda row: int(row.get("version_number") or 0), default=None,
            )
            if not source_proposal:
                raise HTTPException(status_code=404, detail="source_research_proposal_not_found")
            if not source_proposal_hash or source_proposal_hash != source_proposal.get("proposal_hash"):
                raise HTTPException(status_code=409, detail="latest_source_research_proposal_hash_required")
            required_discovery = set(get_nested(source_proposal, ["evidence", "supporting_freeze_ids"], []) or [])
            if set(discovery) != required_discovery:
                raise HTTPException(status_code=409, detail="hypothesis_must_bind_all_and_only_supporting_proposal_samples")
            try:
                minimum_samples = int(validation_plan.get("minimum_samples") or 0)
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=422, detail="proposal_hypothesis_validation_minimum_invalid") from exc
            if minimum_samples < LEARNING_MIN_VALIDATION_SAMPLES:
                raise HTTPException(status_code=422, detail="proposal_hypothesis_validation_minimum_below_governance_floor")
            structured_scope = validation_plan.get("structured_scope") if isinstance(validation_plan.get("structured_scope"), dict) else {}
            competition_ids = structured_scope.get("competition_ids") if isinstance(structured_scope.get("competition_ids"), list) else []
            markets = structured_scope.get("markets") if isinstance(structured_scope.get("markets"), list) else []
            proposal_competition_id = get_nested(source_proposal, ["scope", "competition_id"])
            proposal_market = str(get_nested(source_proposal, ["scope", "market"]) or "")
            if proposal_competition_id not in competition_ids or proposal_market not in {str(value) for value in markets}:
                raise HTTPException(status_code=422, detail="proposal_hypothesis_structured_scope_must_cover_source_signal")
        if registration_provenance is not None:
            if not source_proposal:
                raise HTTPException(status_code=422, detail="automatic_template_registration_requires_source_proposal")
            template_id = str(registration_provenance.get("template_id") or "")
            template = (store.get("learning_hypothesis_templates") or {}).get(template_id)
            if not isinstance(template, dict):
                raise HTTPException(status_code=404, detail="preregistered_hypothesis_template_not_found")
            if str(registration_provenance.get("template_hash") or "") != str(template.get("template_hash") or ""):
                raise HTTPException(status_code=409, detail="preregistered_hypothesis_template_hash_mismatch")
            if (
                str(registration_provenance.get("proposal_id") or "") != source_proposal_id
                or str(registration_provenance.get("proposal_hash") or "") != source_proposal_hash
            ):
                raise HTTPException(status_code=409, detail="automatic_registration_proposal_provenance_mismatch")
            postmatches = store.get("learning_postmatch") or {}
            support_settled_at = [
                int(get_nested(postmatches.get(str(freeze_id)) or {}, ["settled_at"]) or 0)
                for freeze_id in discovery
            ]
            if not support_settled_at or any(value <= 0 for value in support_settled_at):
                raise HTTPException(status_code=409, detail="automatic_registration_settled_discovery_evidence_required")
            if int(template.get("registered_at") or 0) >= min(support_settled_at):
                raise HTTPException(status_code=409, detail="hypothesis_template_must_predate_all_discovery_samples")
            if hypothesis_type != template.get("type") or any(base[key] != str(template.get(key) or "") for key in required_text):
                raise HTTPException(status_code=409, detail="hypothesis_must_exactly_instantiate_preregistered_template")
            if _content_hash(validation_plan) != _content_hash(template.get("pre_registered_validation_plan") or {}):
                raise HTTPException(status_code=409, detail="hypothesis_validation_plan_must_match_preregistered_template")
            proposal_dimension = str(source_proposal.get("signal_dimension") or "")
            if proposal_dimension not in (template.get("signal_dimensions") or []):
                raise HTTPException(status_code=409, detail="hypothesis_template_signal_dimension_mismatch")
            registration_provenance = {
                "mode": "automatic_from_preregistered_template",
                "template_id": template_id, "template_hash": template.get("template_hash"),
                "template_registered_at": template.get("registered_at"),
                "proposal_id": source_proposal_id, "proposal_hash": source_proposal_hash,
                "all_supporting_samples_postdate_template": True,
                "result_outcome_used": False,
            }
        content = {
            "hypothesis_id": hypothesis_id,
            "type": hypothesis_type,
            **base,
            "discovery_freeze_ids": sorted(set(discovery)),
            "pre_registered_validation_plan": validation_plan,
        }
        if registration_provenance is not None:
            content["registration_provenance"] = copy.deepcopy(registration_provenance)
        if source_proposal:
            content["source_research_proposal"] = {
                "proposal_id": source_proposal_id,
                "proposal_hash": source_proposal_hash,
            }
        content_hash = _content_hash(content)
        hypotheses = store.setdefault("learning_hypotheses", {})
        existing = hypotheses.get(hypothesis_id)
        if existing:
            if existing.get("content_hash") == content_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="hypothesis_id_already_registered")
        record = {
            **content,
            "registered_at": int(now_ts or time.time()),
            "content_hash": content_hash,
            "status": hypothesis_type,
            "validation_evidence": [],
            "champion_effect": False,
            "action": "registered",
        }
        hypotheses[hypothesis_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def register_learning_hypothesis_template(payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Register an immutable causal/Challenger template before its discovery samples exist."""
    payload = payload if isinstance(payload, dict) else {}
    hypothesis_type = str(payload.get("type") or "HYPOTHESIS_ONLY").strip()
    if hypothesis_type not in LEARNING_HYPOTHESIS_TYPES:
        raise HTTPException(status_code=400, detail="invalid_hypothesis_type")
    required_text = ("title", "definition", "applicable_scope", "expected_direction", "failure_conditions", "falsification_criteria")
    missing = [key for key in required_text if not str(payload.get(key) or "").strip()]
    if missing:
        raise HTTPException(status_code=400, detail={"error": "hypothesis_template_fields_required", "missing": missing})
    allowed_dimensions = {"process_error", "match_selection", "expression", "price_execution"}
    signal_dimensions = sorted(set(
        str(value).strip() for value in (payload.get("signal_dimensions") or []) if str(value).strip()
    )) if isinstance(payload.get("signal_dimensions"), list) else []
    if not signal_dimensions or any(value not in allowed_dimensions for value in signal_dimensions):
        raise HTTPException(status_code=422, detail="valid_hypothesis_template_signal_dimensions_required")
    validation_plan = _normalize_learning_validation_plan(payload.get("validation_plan"))
    try:
        minimum_samples = int(validation_plan.get("minimum_samples") or 0)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="hypothesis_template_validation_minimum_invalid") from exc
    if minimum_samples < LEARNING_MIN_VALIDATION_SAMPLES:
        raise HTTPException(status_code=422, detail="hypothesis_template_validation_minimum_below_governance_floor")
    base = {key: str(payload.get(key)).strip() for key in required_text}
    immutable = {
        "type": hypothesis_type, **base,
        "signal_dimensions": signal_dimensions,
        "pre_registered_validation_plan": validation_plan,
        "automatic_instantiation_allowed": True,
        "result_outcome_used": False,
        "champion_effect": False,
    }
    template_id = str(payload.get("template_id") or f"hyp-template-{_content_hash(immutable)[:16]}").strip()
    if not template_id or len(template_id) > 100 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in template_id):
        raise HTTPException(status_code=400, detail="safe_hypothesis_template_id_required")
    content = {"template_id": template_id, **immutable}
    template_hash = _content_hash(content)
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        templates = store.setdefault("learning_hypothesis_templates", {})
        existing = templates.get(template_id)
        if existing:
            if existing.get("template_hash") == template_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="hypothesis_template_id_already_registered")
        record = {
            **content, "template_hash": template_hash,
            "registered_at": int(now_ts or time.time()),
            "immutable": True, "action": "registered",
        }
        templates[template_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def learning_hypothesis_template_report() -> Dict[str, Any]:
    templates = [
        row for row in (load_snapshot_store().get("learning_hypothesis_templates") or {}).values()
        if isinstance(row, dict)
    ]
    templates.sort(key=lambda row: str(row.get("template_id")))
    return {
        "version": VERSION, "template_count": len(templates), "templates": templates,
        "automatic_instantiation_policy": "template_must_preexist_every_supporting_discovery_sample",
        "automatic_champion_change": False,
    }


def instantiate_preregistered_learning_hypotheses(now_ts: Optional[int] = None, apply_changes: bool = True) -> Dict[str, Any]:
    """Turn a repeated proposal into a Hypothesis only when one unique causal template predates all evidence."""
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    templates = [row for row in (store.get("learning_hypothesis_templates") or {}).values() if isinstance(row, dict)]
    existing_proposals = {
        str(get_nested(row, ["source_research_proposal", "proposal_id"]) or "")
        for row in (store.get("learning_hypotheses") or {}).values() if isinstance(row, dict)
    }
    proposals = []
    for versions in (store.get("learning_research_proposals") or {}).values():
        latest = max((row for row in versions or [] if isinstance(row, dict)), key=lambda row: int(row.get("version_number") or 0), default=None)
        if latest:
            proposals.append(latest)
    results = []
    for proposal in proposals:
        proposal_id = str(proposal.get("proposal_id") or "")
        if proposal_id in existing_proposals:
            results.append({"proposal_id": proposal_id, "action": "skipped", "reason": "proposal_already_has_registered_hypothesis"})
            continue
        support_ids = sorted(set(get_nested(proposal, ["evidence", "supporting_freeze_ids"], []) or []))
        postmatches = store.get("learning_postmatch") or {}
        settled_times = [int(get_nested(postmatches.get(str(freeze_id)) or {}, ["settled_at"]) or 0) for freeze_id in support_ids]
        if not support_ids or any(value <= 0 for value in settled_times):
            results.append({"proposal_id": proposal_id, "action": "blocked", "reason": "complete_settled_discovery_evidence_required"})
            continue
        competition_id = get_nested(proposal, ["scope", "competition_id"])
        market = str(get_nested(proposal, ["scope", "market"]) or "")
        dimension = str(proposal.get("signal_dimension") or "")
        matching = []
        for template in templates:
            scope = get_nested(template, ["pre_registered_validation_plan", "structured_scope"], {}) or {}
            if (
                dimension in (template.get("signal_dimensions") or [])
                and competition_id in (scope.get("competition_ids") or [])
                and market in (scope.get("markets") or [])
                and int(template.get("registered_at") or 0) < min(settled_times)
            ):
                matching.append(template)
        if len(matching) != 1:
            results.append({
                "proposal_id": proposal_id, "action": "blocked",
                "reason": "no_preregistered_template" if not matching else "ambiguous_preregistered_templates",
                "matching_template_count": len(matching),
            })
            continue
        template = matching[0]
        hypothesis_id = "auto-hyp-" + _content_hash({
            "template_hash": template.get("template_hash"), "proposal_id": proposal_id,
        })[:16]
        payload = {
            "hypothesis_id": hypothesis_id,
            "type": template.get("type"),
            **{key: template.get(key) for key in ("title", "definition", "applicable_scope", "expected_direction", "failure_conditions", "falsification_criteria")},
            "discovery_freeze_ids": support_ids,
            "source_proposal_id": proposal_id,
            "source_proposal_hash": proposal.get("proposal_hash"),
            "validation_plan": copy.deepcopy(template.get("pre_registered_validation_plan") or {}),
            "registration_provenance": {
                "mode": "automatic_from_preregistered_template",
                "template_id": template.get("template_id"), "template_hash": template.get("template_hash"),
                "template_registered_at": template.get("registered_at"),
                "proposal_id": proposal_id, "proposal_hash": proposal.get("proposal_hash"),
                "all_supporting_samples_postdate_template": True,
                "result_outcome_used": False,
            },
        }
        if not apply_changes:
            results.append({
                "proposal_id": proposal_id, "hypothesis_id": hypothesis_id,
                "template_id": template.get("template_id"), "action": "would_register",
            })
            continue
        try:
            hypothesis = register_learning_hypothesis(payload, now_ts=now_ts)
            results.append({
                "proposal_id": proposal_id, "hypothesis_id": hypothesis.get("hypothesis_id"),
                "template_id": template.get("template_id"), "action": hypothesis.get("action"),
                "champion_effect": False,
            })
        except HTTPException as exc:
            results.append({
                "proposal_id": proposal_id, "hypothesis_id": hypothesis_id,
                "template_id": template.get("template_id"), "action": "rejected",
                "status_code": exc.status_code, "reason": exc.detail,
            })
    return {
        "version": VERSION, "result_count": len(results), "results": results,
        "registered_count": sum(row.get("action") == "registered" for row in results),
        "automatic_registration_scope": "preregistered_templates_only",
        "single_match_registration_allowed": False,
        "automatic_champion_change": False,
    }


def learning_forward_validation_queue(now_ts: Optional[int] = None) -> Dict[str, Any]:
    """List only independent post-registration frozen matches eligible for a future Shadow lock."""
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    items = []
    blocked_hypotheses = []
    latest_freezes = []
    for fixture, versions in (store.get("learning_frozen") or {}).items():
        latest = max(
            (row for row in versions or [] if isinstance(row, dict)),
            key=lambda row: int(row.get("version_number") or 0), default=None,
        )
        if latest:
            latest_freezes.append(latest)
    for hypothesis_id, hypothesis in (store.get("learning_hypotheses") or {}).items():
        if hypothesis.get("status") not in (*LEARNING_HYPOTHESIS_TYPES, "SHADOW_VALIDATION"):
            continue
        plan = hypothesis.get("pre_registered_validation_plan") if isinstance(hypothesis.get("pre_registered_validation_plan"), dict) else {}
        structured_scope = plan.get("structured_scope") if isinstance(plan.get("structured_scope"), dict) else {}
        ablation_plan = plan.get("ablation_plan") if isinstance(plan.get("ablation_plan"), dict) else {}
        challenger_spec = plan.get("challenger_spec") if isinstance(plan.get("challenger_spec"), dict) else {}
        competition_ids = set(structured_scope.get("competition_ids") or []) if isinstance(structured_scope.get("competition_ids"), list) else set()
        markets = {str(value) for value in (structured_scope.get("markets") or [])} if isinstance(structured_scope.get("markets"), list) else set()
        if not competition_ids or not markets:
            blocked_hypotheses.append({
                "hypothesis_id": hypothesis_id,
                "reason": "structured_validation_scope_missing",
                "required_fields": ["validation_plan.structured_scope.competition_ids", "validation_plan.structured_scope.markets"],
            })
            continue
        discovery_ids = set(hypothesis.get("discovery_freeze_ids") or [])
        locks = (store.get("learning_shadow_locks") or {}).get(hypothesis_id) or {}
        locked_fixture_ids = {
            str(((_learning_freeze_by_id(store, freeze_id) or {}).get("fixture")) or "")
            for freeze_id in locks
        }
        for freeze in latest_freezes:
            freeze_id = str(freeze.get("freeze_id") or "")
            fixture = str(freeze.get("fixture") or "")
            if freeze_id in discovery_ids or fixture in locked_fixture_ids:
                continue
            if freeze_id in (store.get("learning_postmatch") or {}):
                continue
            if int(freeze.get("captured_at") or 0) < int(hypothesis.get("registered_at") or 0):
                continue
            if int(freeze.get("kickoff_at") or 0) <= now_ts:
                continue
            competition_id = get_nested(freeze, ["scope", "competition_id"])
            expression = _learning_selected_expression(freeze.get("decision"))
            if competition_id not in competition_ids or expression.get("market") not in markets:
                continue
            items.append({
                "hypothesis_id": hypothesis_id,
                "hypothesis_hash": hypothesis.get("content_hash"),
                "freeze_id": freeze_id,
                "freeze_hash": freeze.get("content_hash"),
                "fixture": freeze.get("fixture"),
                "kickoff_at": freeze.get("kickoff_at"),
                "competition_id": competition_id,
                "market": expression.get("market"),
                "required_action": "compute_champion_challenger_and_ablation_outputs_then_lock_before_kickoff",
                "required_ablation_modules": ablation_plan.get("required_modules") or [],
                "challenger_spec_hash": challenger_spec.get("spec_hash"),
                "automatic_shadow_lock": bool(challenger_spec.get("spec_hash")) and callable(LEARNING_SHADOW_MODEL_RUNNER),
                "champion_effect": False,
            })
    items.sort(key=lambda row: (int(row.get("kickoff_at") or 0), str(row.get("hypothesis_id")), str(row.get("fixture"))))
    return {
        "version": VERSION, "generated_at": now_ts,
        "queue_count": len(items), "items": items,
        "blocked_hypothesis_count": len(blocked_hypotheses),
        "blocked_hypotheses": blocked_hypotheses,
        "automatic_shadow_lock": callable(LEARNING_SHADOW_MODEL_RUNNER),
        "automatic_champion_change": False,
        "policy": "only post-registration, non-discovery, distinct future matches enter the forward queue; model outputs must be computed and locked before kickoff",
    }


def _normalize_1x2_probabilities(value: Any, field_name: str) -> Dict[str, float]:
    value = value if isinstance(value, dict) else {}
    probabilities = {key: as_float(value.get(key)) for key in ("home", "draw", "away")}
    if any(probability is None or not 0 <= probability <= 1 for probability in probabilities.values()):
        raise HTTPException(status_code=422, detail=f"{field_name}_must_contain_valid_1x2_probabilities")
    total = sum(probabilities.values())
    if abs(total - 1.0) > 0.01:
        raise HTTPException(status_code=422, detail=f"{field_name}_probabilities_must_sum_to_one")
    return {key: round(probability / total, 8) for key, probability in probabilities.items()}


LINE_SETTLEMENT_CATEGORIES = ("full_win", "half_win", "push", "half_loss", "full_loss")


def _learning_forecast_categories(market: str) -> Tuple[str, ...]:
    if market == "1x2":
        return ("home", "draw", "away")
    if market == "btts":
        return ("yes", "no")
    if market in {"asian_handicap", "over_under", "home_team_total", "away_team_total"}:
        return LINE_SETTLEMENT_CATEGORIES
    raise HTTPException(status_code=422, detail="unsupported_learning_forecast_market")


def _line_settlement_category(outcome: float, line: float, selection: str, market: str) -> Optional[str]:
    split = _split_asian_line(line)
    if not split:
        return None
    signs = []
    for component in split:
        if market == "asian_handicap":
            settled = outcome + component if selection == "home" else -outcome - component
        else:
            settled = outcome - component if selection == "over" else component - outcome
        signs.append(1 if settled > 1e-9 else (-1 if settled < -1e-9 else 0))
    if all(sign > 0 for sign in signs):
        return "full_win"
    if all(sign < 0 for sign in signs):
        return "full_loss"
    if all(sign == 0 for sign in signs):
        return "push"
    if all(sign >= 0 for sign in signs) and any(sign > 0 for sign in signs):
        return "half_win"
    if all(sign <= 0 for sign in signs) and any(sign < 0 for sign in signs):
        return "half_loss"
    return None


def market_forecast_from_probability_replay(replay: Dict[str, Any], expression: Dict[str, Any]) -> Dict[str, Any]:
    """Project the frozen Poisson output onto the exact selected market settlement space."""
    market = str(expression.get("market") or "").strip()
    selection = str(expression.get("selection") or "").strip()
    line = as_float(expression.get("line"))
    probabilities = get_nested(replay, ["model", "probabilities"], {}) or {}
    categories = _learning_forecast_categories(market)
    if market == "1x2":
        raw = probabilities.get("1x2") if isinstance(probabilities.get("1x2"), dict) else {}
    elif market == "btts":
        raw = probabilities.get("btts") if isinstance(probabilities.get("btts"), dict) else {}
    else:
        if line is None or selection not in ({"home", "away"} if market == "asian_handicap" else {"over", "under"}):
            raise HTTPException(status_code=422, detail="valid_line_market_expression_required_for_forecast")
        distribution_name = {
            "asian_handicap": "goal_difference",
            "over_under": "total_goals",
            "home_team_total": "home_goals",
            "away_team_total": "away_goals",
        }[market]
        distribution = get_nested(probabilities, ["settlement_distributions", distribution_name], {}) or {}
        raw = {category: 0.0 for category in categories}
        for raw_outcome, raw_probability in distribution.items():
            outcome, probability = as_float(raw_outcome), as_float(raw_probability)
            if outcome is None or probability is None or probability < 0:
                continue
            category = _line_settlement_category(outcome, line, selection, market)
            if category is None:
                raise HTTPException(status_code=422, detail="unsupported_line_settlement_shape")
            raw[category] += probability
    values = {category: as_float(raw.get(category)) for category in categories}
    if any(value is None or value < 0 for value in values.values()):
        raise HTTPException(status_code=422, detail="frozen_replay_missing_selected_market_probabilities")
    total = sum(values.values())
    if total <= 0 or abs(total - 1.0) > 0.02:
        raise HTTPException(status_code=422, detail="selected_market_probabilities_must_sum_to_one")
    normalized = {category: round(values[category] / total, 8) for category in categories}
    return {
        "schema": "learning_market_forecast_v1",
        "market": market,
        "selection": selection,
        "line": line,
        "categories": list(categories),
        "probabilities": normalized,
        "source": "frozen_probability_replay",
        "probability_replay_hash": replay.get("replay_hash"),
    }


def _normalize_learning_market_forecast(value: Any, expression: Dict[str, Any], field_name: str) -> Dict[str, Any]:
    value = value if isinstance(value, dict) else {}
    market = str(expression.get("market") or "").strip()
    selection = str(expression.get("selection") or "").strip()
    line = as_float(expression.get("line"))
    categories = _learning_forecast_categories(market)
    if value.get("schema") != "learning_market_forecast_v1":
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_schema_required")
    if str(value.get("market") or "").strip() != market or str(value.get("selection") or "").strip() != selection:
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_expression_mismatch")
    if market in {"asian_handicap", "over_under", "home_team_total", "away_team_total"} and as_float(value.get("line")) != line:
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_line_mismatch")
    supplied_categories = value.get("categories") if isinstance(value.get("categories"), list) else []
    if supplied_categories != list(categories):
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_categories_mismatch")
    raw = value.get("probabilities") if isinstance(value.get("probabilities"), dict) else {}
    if set(raw) != set(categories):
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_probability_categories_mismatch")
    probabilities = {category: as_float(raw.get(category)) for category in categories}
    if any(probability is None or not 0 <= probability <= 1 for probability in probabilities.values()):
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_probabilities_invalid")
    total = sum(probabilities.values())
    if abs(total - 1.0) > 0.01:
        raise HTTPException(status_code=422, detail=f"{field_name}_forecast_probabilities_must_sum_to_one")
    return {
        "schema": "learning_market_forecast_v1",
        "market": market, "selection": selection, "line": line,
        "categories": list(categories),
        "probabilities": {category: round(probabilities[category] / total, 8) for category in categories},
    }


def _learning_market_outcome_category(expression: Dict[str, Any], result: Dict[str, Any]) -> str:
    market = str(expression.get("market") or "").strip()
    selection = str(expression.get("selection") or "").strip()
    home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    if market == "1x2":
        return "home" if home_goals > away_goals else ("away" if home_goals < away_goals else "draw")
    if market == "btts":
        return "yes" if home_goals > 0 and away_goals > 0 else "no"
    line = as_float(expression.get("line"))
    if line is None:
        raise HTTPException(status_code=422, detail="settlement_line_required")
    outcome = {
        "asian_handicap": home_goals - away_goals,
        "over_under": home_goals + away_goals,
        "home_team_total": home_goals,
        "away_team_total": away_goals,
    }.get(market)
    if outcome is None:
        raise HTTPException(status_code=422, detail="unsupported_learning_forecast_market")
    category = _line_settlement_category(float(outcome), line, selection, market)
    if category is None:
        raise HTTPException(status_code=422, detail="unsupported_line_settlement_shape")
    return category


def _brier_market_forecast(forecast: Dict[str, Any], expression: Dict[str, Any], result: Dict[str, Any]) -> float:
    normalized = _normalize_learning_market_forecast(forecast, expression, "locked")
    actual = _learning_market_outcome_category(expression, result)
    probabilities = normalized["probabilities"]
    return round(sum((probabilities[key] - (1.0 if key == actual else 0.0)) ** 2 for key in normalized["categories"]) / len(normalized["categories"]), 8)


def _brier_1x2(probabilities: Dict[str, float], result: Dict[str, Any]) -> float:
    home_goals, away_goals = int(result.get("home_goals")), int(result.get("away_goals"))
    actual = "home" if home_goals > away_goals else ("away" if home_goals < away_goals else "draw")
    return round(sum((probabilities[key] - (1.0 if key == actual else 0.0)) ** 2 for key in ("home", "draw", "away")) / 3.0, 8)


def _learning_poisson_tail_risk(model: Dict[str, Any]) -> float:
    """Use the model's high-total mass as a stable distribution-tail diagnostic."""
    distribution = get_nested(model, ["probabilities", "settlement_distributions", "total_goals"], {}) or {}
    risk = sum(
        float(probability)
        for raw_total, probability in distribution.items()
        if as_float(raw_total) is not None and float(raw_total) >= 5 and as_float(probability) is not None
    )
    return round(min(1.0, max(0.0, risk)), 8)


def _learning_adjusted_poisson_model(replay: Dict[str, Any], home_delta: float, away_delta: float) -> Dict[str, Any]:
    base_inputs = get_nested(replay, ["model", "inputs"], {}) or {}
    home_xg, away_xg = as_float(base_inputs.get("home_expected_goals")), as_float(base_inputs.get("away_expected_goals"))
    confidence = as_float(base_inputs.get("input_confidence"))
    provenance = base_inputs.get("provenance") if isinstance(base_inputs.get("provenance"), dict) else {}
    if home_xg is None or away_xg is None or confidence is None:
        raise HTTPException(status_code=422, detail="frozen_poisson_inputs_required_for_challenger")
    model = poisson_probability_model(
        home_xg * math.exp(home_delta), away_xg * math.exp(away_delta), confidence, provenance,
    )
    if model.get("ok") is not True:
        raise HTTPException(status_code=422, detail={"error": "challenger_poisson_model_invalid", "reasons": model.get("errors")})
    return model


def builtin_preregistered_poisson_challenger(request: Dict[str, Any]) -> Dict[str, Any]:
    """Execute only the bounded module deltas frozen in the registered hypothesis."""
    request = request if isinstance(request, dict) else {}
    freeze = request.get("freeze") if isinstance(request.get("freeze"), dict) else {}
    hypothesis = request.get("hypothesis") if isinstance(request.get("hypothesis"), dict) else {}
    replay = get_nested(freeze, ["analysis", "probability_replay"], {}) or {}
    ablation_plan = hypothesis.get("ablation_plan") if isinstance(hypothesis.get("ablation_plan"), dict) else {}
    required_modules = list(ablation_plan.get("required_modules") or [])
    spec = _normalize_learning_challenger_spec(hypothesis.get("challenger_spec"), required_modules)
    expression = request.get("frozen_selected_expression") if isinstance(request.get("frozen_selected_expression"), dict) else {}
    generated_at = int(request.get("requested_at") or 0)
    combined = spec["combined_log_rate_delta"]
    challenger_model = _learning_adjusted_poisson_model(replay, combined["home"], combined["away"])
    challenger_replay = {
        "model": challenger_model,
        "replay_hash": _content_hash({"base_replay_hash": replay.get("replay_hash"), "spec_hash": spec["spec_hash"]}),
    }
    module_ablations = {}
    for module in required_modules:
        contribution = spec["module_log_rate_deltas"][module]
        ablated_model = _learning_adjusted_poisson_model(
            replay,
            combined["home"] - contribution["home"],
            combined["away"] - contribution["away"],
        )
        ablated_replay = {
            "model": ablated_model,
            "replay_hash": _content_hash({
                "base_replay_hash": replay.get("replay_hash"), "spec_hash": spec["spec_hash"], "ablated_module": module,
            }),
        }
        module_ablations[module] = {
            "probabilities": get_nested(ablated_model, ["probabilities", "1x2"]),
            "forecast": market_forecast_from_probability_replay(ablated_replay, expression),
        }
    entry_price = as_float(expression.get("price"))
    content = {
        "schema": "learning_shadow_model_run_v1",
        "runner_id": "builtin_preregistered_poisson_challenger",
        "runner_version": "1",
        "challenger_spec_hash": spec["spec_hash"],
        "generated_at": generated_at,
        "input_hash": request.get("input_hash"),
        "freeze_hash": freeze.get("content_hash"),
        "hypothesis_hash": hypothesis.get("hypothesis_hash"),
        "champion_probabilities": request.get("frozen_champion_probabilities"),
        "challenger_probabilities": get_nested(challenger_model, ["probabilities", "1x2"]),
        "champion_forecast": request.get("frozen_champion_forecast"),
        "challenger_forecast": market_forecast_from_probability_replay(challenger_replay, expression),
        "module_ablations": module_ablations,
        "selected_expression": {
            "market": expression.get("market"), "selection": expression.get("selection"),
            "line": expression.get("line"), "entry_decimal_price": entry_price,
            "entry_price_evidence_ref": f"freeze:{freeze.get('content_hash')}:decision.selected_expression",
        },
        "risk": {
            "champion_tail_risk": _learning_poisson_tail_risk(replay.get("model") or {}),
            "challenger_tail_risk": _learning_poisson_tail_risk(challenger_model),
        },
    }
    return {**content, "run_hash": _content_hash(content)}


LEARNING_SHADOW_MODEL_RUNNER = builtin_preregistered_poisson_challenger


def lock_hypothesis_shadow_prediction(
    hypothesis_id: Any,
    payload: Dict[str, Any],
    now_ts: Optional[int] = None,
    calculator_provenance: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Lock Champion, Challenger and ablation outputs before kickoff for later OOS scoring."""
    hypothesis_id = str(hypothesis_id or "").strip()
    payload = payload if isinstance(payload, dict) else {}
    freeze_id = str(payload.get("freeze_id") or "").strip()
    if not hypothesis_id or not freeze_id:
        raise HTTPException(status_code=400, detail="hypothesis_id_and_freeze_id_required")
    champion_probabilities = _normalize_1x2_probabilities(payload.get("champion_probabilities"), "champion")
    challenger_probabilities = _normalize_1x2_probabilities(payload.get("challenger_probabilities"), "challenger")
    selected_expression = payload.get("selected_expression") if isinstance(payload.get("selected_expression"), dict) else {}
    market = str(selected_expression.get("market") or "").strip()
    selection = str(selected_expression.get("selection") or "").strip()
    entry_decimal_price = as_float(selected_expression.get("entry_decimal_price"))
    entry_price_evidence_ref = str(selected_expression.get("entry_price_evidence_ref") or "").strip()
    if not market or not selection or entry_decimal_price is None or not 1.01 <= entry_decimal_price <= 1000:
        raise HTTPException(status_code=422, detail="valid_shadow_selected_expression_required")
    if not entry_price_evidence_ref:
        raise HTTPException(status_code=422, detail="entry_price_evidence_ref_required")
    raw_champion_forecast = payload.get("champion_forecast")
    raw_challenger_forecast = payload.get("challenger_forecast")
    market_forecast_mode = raw_champion_forecast is not None or raw_challenger_forecast is not None
    champion_forecast = _normalize_learning_market_forecast(raw_champion_forecast, selected_expression, "champion") if market_forecast_mode else None
    challenger_forecast = _normalize_learning_market_forecast(raw_challenger_forecast, selected_expression, "challenger") if market_forecast_mode else None
    risk = payload.get("risk") if isinstance(payload.get("risk"), dict) else {}
    champion_tail_risk = as_float(risk.get("champion_tail_risk"))
    challenger_tail_risk = as_float(risk.get("challenger_tail_risk"))
    if any(value is None or not 0 <= value <= 1 for value in (champion_tail_risk, challenger_tail_risk)):
        raise HTTPException(status_code=422, detail="valid_champion_and_challenger_tail_risk_required")
    locked_at = int(now_ts or time.time())
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
        if not hypothesis:
            raise HTTPException(status_code=404, detail="hypothesis_not_found")
        validation_plan = hypothesis.get("pre_registered_validation_plan") if isinstance(hypothesis.get("pre_registered_validation_plan"), dict) else {}
        ablation_plan = validation_plan.get("ablation_plan") if isinstance(validation_plan.get("ablation_plan"), dict) else {}
        challenger_spec = validation_plan.get("challenger_spec") if isinstance(validation_plan.get("challenger_spec"), dict) else {}
        calculator_policy = validation_plan.get("calculator_policy") if isinstance(validation_plan.get("calculator_policy"), dict) else {}
        required_modules = list(ablation_plan.get("required_modules") or [])
        submitted_ablations = payload.get("module_ablation_outputs") if isinstance(payload.get("module_ablation_outputs"), dict) else {}
        if set(submitted_ablations) != set(required_modules):
            raise HTTPException(status_code=422, detail={
                "error": "exact_preregistered_module_ablation_outputs_required",
                "required_modules": required_modules,
                "submitted_modules": sorted(submitted_ablations),
            })
        if freeze_id in (hypothesis.get("discovery_freeze_ids") or []):
            raise HTTPException(status_code=409, detail="discovery_sample_cannot_be_shadow_validation_sample")
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze:
            raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
        if freeze_id in (store.get("learning_postmatch") or {}):
            raise HTTPException(status_code=409, detail="shadow_prediction_must_be_locked_before_postmatch_settlement")
        if locked_at >= int(freeze.get("kickoff_at") or 0):
            raise HTTPException(status_code=409, detail="shadow_prediction_must_be_locked_before_kickoff")
        if locked_at < int(freeze.get("captured_at") or 0):
            raise HTTPException(status_code=409, detail="shadow_prediction_cannot_precede_frozen_sample")
        if int(freeze.get("captured_at") or 0) < int(hypothesis.get("registered_at") or 0):
            raise HTTPException(status_code=409, detail="validation_sample_predates_hypothesis_registration")
        structured_scope = get_nested(hypothesis, ["pre_registered_validation_plan", "structured_scope"], {}) or {}
        if get_nested(freeze, ["scope", "competition_id"]) not in set(structured_scope.get("competition_ids") or []):
            raise HTTPException(status_code=409, detail="validation_fixture_outside_preregistered_competition_scope")
        if market not in set(structured_scope.get("markets") or []):
            raise HTTPException(status_code=409, detail="validation_market_outside_preregistered_scope")
        module_ablations = {}
        for module in required_modules:
            row = submitted_ablations.get(module) if isinstance(submitted_ablations.get(module), dict) else {}
            probabilities = _normalize_1x2_probabilities(row.get("probabilities"), f"ablation_{module.lower()}")
            module_forecast = _normalize_learning_market_forecast(
                row.get("forecast"), selected_expression, f"ablation_{module.lower()}"
            ) if market_forecast_mode else None
            output_reference = str(row.get("output_reference") or "").strip()
            computed_at = _parse_timestamp(row.get("computed_at"))
            if not output_reference:
                raise HTTPException(status_code=422, detail={"error": "module_ablation_output_reference_required", "module": module})
            if computed_at is None or not int(freeze.get("captured_at") or 0) <= computed_at <= locked_at:
                raise HTTPException(status_code=422, detail={"error": "module_ablation_computed_at_must_be_pit", "module": module})
            intervention = get_nested(ablation_plan, ["module_interventions", module, "intervention"])
            module_content = {
                "module": module, "intervention": intervention,
                "probabilities": probabilities, "output_reference": output_reference,
                "computed_at": computed_at, "freeze_hash": freeze.get("content_hash"),
            }
            if module_forecast is not None:
                module_content["forecast"] = module_forecast
            module_ablations[module] = {**module_content, "output_hash": _content_hash(module_content)}
        expected_calculator_input_hash = _content_hash({
            "freeze_hash": freeze.get("content_hash"),
            "hypothesis_hash": hypothesis.get("content_hash"),
            "ablation_plan_hash": _content_hash(ablation_plan),
            "probability_replay_hash": get_nested(freeze, ["analysis", "probability_replay", "replay_hash"]),
        })
        if calculator_provenance is None:
            normalized_calculator_provenance = {
                "origin": "external_submitted",
                "promotion_eligible": False,
                "reason": "not_generated_by_internal_shadow_runner",
            }
        else:
            calculator_provenance = calculator_provenance if isinstance(calculator_provenance, dict) else {}
            generated_at = _parse_timestamp(calculator_provenance.get("generated_at"))
            runner_id = str(calculator_provenance.get("runner_id") or "").strip()
            runner_version = str(calculator_provenance.get("runner_version") or "").strip()
            run_hash = str(calculator_provenance.get("run_hash") or "").strip()
            challenger_spec_hash = str(calculator_provenance.get("challenger_spec_hash") or "").strip()
            if (
                calculator_provenance.get("schema") != "learning_shadow_model_run_v1"
                or not runner_id or not runner_version or len(run_hash) != 64
                or calculator_provenance.get("input_hash") != expected_calculator_input_hash
                or runner_id != calculator_policy.get("required_runner_id")
                or runner_version != calculator_policy.get("required_runner_version")
                or challenger_spec_hash != challenger_spec.get("spec_hash")
                or generated_at is None
                or not int(freeze.get("captured_at") or 0) <= generated_at <= locked_at
            ):
                raise HTTPException(status_code=422, detail="invalid_internal_shadow_runner_provenance")
            if not market_forecast_mode:
                raise HTTPException(status_code=422, detail="internal_shadow_runner_requires_market_forecast_contract")
            normalized_calculator_provenance = {
                "origin": "internal_shadow_runner", "promotion_eligible": True,
                "schema": "learning_shadow_model_run_v1",
                "runner_id": runner_id, "runner_version": runner_version,
                "generated_at": generated_at, "input_hash": expected_calculator_input_hash,
                "run_hash": run_hash, "challenger_spec_hash": challenger_spec_hash,
            }
        content = {
            "hypothesis_id": hypothesis_id,
            "hypothesis_hash": hypothesis.get("content_hash"),
            "freeze_id": freeze_id,
            "freeze_hash": freeze.get("content_hash"),
            "champion_decision_hash": _content_hash(freeze.get("decision") or {}),
            "probability_replay_hash": get_nested(freeze, ["analysis", "probability_replay", "replay_hash"]),
            "champion_probabilities": champion_probabilities,
            "challenger_probabilities": challenger_probabilities,
            "scoring_contract": "learning_market_forecast_v1" if market_forecast_mode else "legacy_1x2_brier",
            "champion_forecast": champion_forecast,
            "challenger_forecast": challenger_forecast,
            "ablation_plan_hash": _content_hash(ablation_plan),
            "challenger_spec_hash": challenger_spec.get("spec_hash"),
            "module_ablations": module_ablations,
            "calculator_provenance": normalized_calculator_provenance,
            "selected_expression": {
                "market": market, "selection": selection,
                "line": selected_expression.get("line"), "entry_decimal_price": entry_decimal_price,
                "entry_price_evidence_ref": entry_price_evidence_ref,
            },
            "risk": {
                "champion_tail_risk": champion_tail_risk,
                "challenger_tail_risk": challenger_tail_risk,
            },
            "locked_at": locked_at,
        }
        lock_hash = _content_hash(content)
        locks = store.setdefault("learning_shadow_locks", {}).setdefault(hypothesis_id, {})
        duplicate_fixture_lock = next((
            existing_lock for existing_freeze_id, existing_lock in locks.items()
            if str(((_learning_freeze_by_id(store, existing_freeze_id) or {}).get("fixture")) or "") == str(freeze.get("fixture") or "")
            and existing_freeze_id != freeze_id
        ), None)
        if duplicate_fixture_lock:
            raise HTTPException(status_code=409, detail="validation_fixture_already_locked_for_hypothesis")
        existing = locks.get(freeze_id)
        if existing:
            if existing.get("lock_hash") == lock_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="shadow_prediction_already_locked")
        record = {
            **content, "lock_hash": lock_hash, "immutable": True,
            "champion_effect": False, "action": "locked",
        }
        locks[freeze_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def generate_internal_shadow_lock(
    hypothesis_id: Any,
    freeze_id: Any,
    model_runner: Optional[Any] = None,
    now_ts: Optional[int] = None,
) -> Dict[str, Any]:
    """Run a trusted in-process PIT calculator and lock its outputs; never synthesize missing variants."""
    hypothesis_id = str(hypothesis_id or "").strip()
    freeze_id = str(freeze_id or "").strip()
    runner = model_runner or LEARNING_SHADOW_MODEL_RUNNER
    if not callable(runner):
        raise HTTPException(status_code=503, detail="internal_shadow_model_runner_not_configured")
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
    freeze = _learning_freeze_by_id(store, freeze_id)
    if not hypothesis:
        raise HTTPException(status_code=404, detail="hypothesis_not_found")
    if not freeze:
        raise HTTPException(status_code=404, detail="frozen_learning_sample_not_found")
    probability_replay = get_nested(freeze, ["analysis", "probability_replay"], {}) or {}
    replay_audit = audit_learning_probability_replay(
        probability_replay,
        expected_fixture=freeze.get("fixture"),
        data_cutoff_at=freeze.get("data_cutoff_at"),
        kickoff_at=freeze.get("kickoff_at"),
    )
    if replay_audit.get("status") != "ready" or replay_audit.get("decision_eligible") is not True:
        raise HTTPException(status_code=409, detail={
            "error": "forward_shadow_requires_replayable_pit_probability_inputs",
            "replay_audit": replay_audit,
        })
    frozen_champion_probabilities = _normalize_1x2_probabilities(
        get_nested(probability_replay, ["model", "probabilities", "1x2"]),
        "frozen_champion",
    )
    frozen_expression = _learning_selected_expression(freeze.get("decision"))
    if not frozen_expression.get("market") or not frozen_expression.get("selection"):
        raise HTTPException(status_code=409, detail="forward_shadow_requires_frozen_selected_expression")
    frozen_champion_forecast = market_forecast_from_probability_replay(probability_replay, frozen_expression)
    validation_plan = hypothesis.get("pre_registered_validation_plan") if isinstance(hypothesis.get("pre_registered_validation_plan"), dict) else {}
    ablation_plan = validation_plan.get("ablation_plan") if isinstance(validation_plan.get("ablation_plan"), dict) else {}
    challenger_spec = validation_plan.get("challenger_spec") if isinstance(validation_plan.get("challenger_spec"), dict) else {}
    input_hash = _content_hash({
        "freeze_hash": freeze.get("content_hash"),
        "hypothesis_hash": hypothesis.get("content_hash"),
        "ablation_plan_hash": _content_hash(ablation_plan),
        "probability_replay_hash": probability_replay.get("replay_hash"),
    })
    request_content = {
        "schema": "learning_shadow_model_request_v1",
        "requested_at": now_ts,
        "input_hash": input_hash,
        "probability_replay_hash": probability_replay.get("replay_hash"),
        "frozen_champion_probabilities": frozen_champion_probabilities,
        "frozen_selected_expression": frozen_expression,
        "frozen_champion_forecast": frozen_champion_forecast,
        "freeze": freeze,
        "hypothesis": {
            "hypothesis_id": hypothesis_id,
            "hypothesis_hash": hypothesis.get("content_hash"),
            "definition": hypothesis.get("definition"),
            "expected_direction": hypothesis.get("expected_direction"),
            "structured_scope": get_nested(hypothesis, ["pre_registered_validation_plan", "structured_scope"], {}),
            "ablation_plan": ablation_plan,
            "challenger_spec": challenger_spec,
        },
    }
    try:
        result = runner(request_content)
    except Exception as exc:
        raise HTTPException(status_code=502, detail={"error": "internal_shadow_model_runner_failed", "error_type": type(exc).__name__}) from exc
    result = result if isinstance(result, dict) else {}
    supplied_run_hash = str(result.get("run_hash") or "").strip()
    run_content = {key: value for key, value in result.items() if key != "run_hash"}
    if supplied_run_hash != _content_hash(run_content):
        raise HTTPException(status_code=422, detail="internal_shadow_model_run_hash_mismatch")
    generated_at = _parse_timestamp(result.get("generated_at"))
    if (
        result.get("schema") != "learning_shadow_model_run_v1"
        or result.get("input_hash") != input_hash
        or result.get("freeze_hash") != freeze.get("content_hash")
        or result.get("hypothesis_hash") != hypothesis.get("content_hash")
        or generated_at is None
        or not int(freeze.get("captured_at") or 0) <= generated_at <= now_ts
        or now_ts >= int(freeze.get("kickoff_at") or 0)
    ):
        raise HTTPException(status_code=422, detail="internal_shadow_model_run_identity_or_pit_invalid")
    runner_id = str(result.get("runner_id") or "").strip()
    runner_version = str(result.get("runner_version") or "").strip()
    calculator_policy = validation_plan.get("calculator_policy") if isinstance(validation_plan.get("calculator_policy"), dict) else {}
    if not runner_id or not runner_version:
        raise HTTPException(status_code=422, detail="internal_shadow_model_runner_identity_required")
    if (
        runner_id != calculator_policy.get("required_runner_id")
        or runner_version != calculator_policy.get("required_runner_version")
        or result.get("challenger_spec_hash") != challenger_spec.get("spec_hash")
    ):
        raise HTTPException(status_code=422, detail="internal_shadow_model_runner_or_spec_mismatch")
    if _normalize_1x2_probabilities(result.get("champion_probabilities"), "champion") != frozen_champion_probabilities:
        raise HTTPException(status_code=422, detail="internal_shadow_champion_must_match_frozen_probability_replay")
    selected_expression = result.get("selected_expression") if isinstance(result.get("selected_expression"), dict) else {}
    if (
        str(selected_expression.get("market") or "") != str(frozen_expression.get("market") or "")
        or str(selected_expression.get("selection") or "") != str(frozen_expression.get("selection") or "")
        or as_float(selected_expression.get("line")) != as_float(frozen_expression.get("line"))
    ):
        raise HTTPException(status_code=422, detail="internal_shadow_expression_must_match_frozen_selection")
    normalized_champion_forecast = _normalize_learning_market_forecast(
        result.get("champion_forecast"), frozen_expression, "champion",
    )
    expected_champion_forecast = _normalize_learning_market_forecast(
        frozen_champion_forecast, frozen_expression, "frozen_champion",
    )
    if normalized_champion_forecast != expected_champion_forecast:
        raise HTTPException(status_code=422, detail="internal_shadow_champion_forecast_must_match_frozen_probability_replay")
    normalized_challenger_forecast = _normalize_learning_market_forecast(
        result.get("challenger_forecast"), frozen_expression, "challenger",
    )
    required_modules = list(ablation_plan.get("required_modules") or [])
    raw_modules = result.get("module_ablations") if isinstance(result.get("module_ablations"), dict) else {}
    if set(raw_modules) != set(required_modules):
        raise HTTPException(status_code=422, detail="internal_shadow_model_run_module_set_mismatch")
    module_outputs = {}
    for module in required_modules:
        row = raw_modules.get(module) if isinstance(raw_modules.get(module), dict) else {}
        module_outputs[module] = {
            "probabilities": row.get("probabilities"),
            "forecast": row.get("forecast"),
            "computed_at": generated_at,
            "output_reference": f"internal-run:{supplied_run_hash}:{module}",
        }
    payload = {
        "freeze_id": freeze_id,
        "champion_probabilities": result.get("champion_probabilities"),
        "challenger_probabilities": result.get("challenger_probabilities"),
        "champion_forecast": normalized_champion_forecast,
        "challenger_forecast": normalized_challenger_forecast,
        "module_ablation_outputs": module_outputs,
        "selected_expression": selected_expression,
        "risk": result.get("risk"),
    }
    provenance = {
        "schema": "learning_shadow_model_run_v1",
        "runner_id": runner_id, "runner_version": runner_version,
        "generated_at": generated_at, "input_hash": input_hash,
        "run_hash": supplied_run_hash, "challenger_spec_hash": challenger_spec.get("spec_hash"),
    }
    return lock_hypothesis_shadow_prediction(
        hypothesis_id, payload, now_ts=now_ts, calculator_provenance=provenance,
    )


def _learning_t1h_price_evidence_for_expression(
    freeze: Dict[str, Any], expression: Dict[str, Any], store: Dict[str, Any],
) -> Dict[str, Any]:
    """Return the exact-line T-1h consensus for one frozen market expression."""
    fixture = str(freeze.get("fixture") or "")
    kickoff_at = int(freeze.get("kickoff_at") or 0)
    rows = [
        row for row in ((store.get("fixtures") or {}).get(fixture) or [])
        if isinstance(row, dict) and row.get("stage") == "T-1h"
        and 0 < int(row.get("snapshot_at") or 0) < kickoff_at
        and get_nested(row, ["stage_timing_audit", "status"]) != "invalid"
        and get_nested(row, ["sequence_timing_audit", "status"]) != "invalid"
    ]
    terminal = max(rows, key=lambda row: int(row.get("snapshot_at") or 0), default=None)
    if not terminal:
        return {"ok": False, "reason": "verified_learning_t1h_snapshot_required"}
    market_snapshot = terminal.get("market_snapshot") if isinstance(terminal.get("market_snapshot"), dict) else {}
    if market_snapshot.get("available") is not True:
        return {"ok": False, "reason": "learning_t1h_market_snapshot_unavailable"}
    expression = expression if isinstance(expression, dict) else {}
    market = str(expression.get("market") or "").strip()
    selection = str(expression.get("selection") or "").strip().casefold()
    line = as_float(expression.get("line"))
    aliases = {
        "match_winner": "1x2", "moneyline": "1x2", "total": "over_under", "goals": "over_under",
        "both_teams_to_score": "btts",
    }
    market = aliases.get(market, market)
    selection_aliases = {"主胜": "home", "客胜": "away", "平": "draw", "大": "over", "小": "under", "是": "yes", "否": "no"}
    selection = selection_aliases.get(selection, selection)
    main_line = get_nested(market_snapshot, ["consensus_main_line", market], {}) or get_nested(market_snapshot, ["primary", market], {}) or {}
    if market in {"asian_handicap", "over_under", "home_team_total", "away_team_total"}:
        reference_line = as_float(main_line.get("line"))
        if line is None or reference_line is None or abs(line - reference_line) > 1e-9:
            return {"ok": False, "reason": "learning_t1h_consensus_same_line_required", "market": market, "entry_line": line, "reference_line": reference_line}
    price = as_float(main_line.get(selection))
    bookmaker_count = int(as_float(main_line.get("bookmaker_count")) or 0)
    if price is None or not 1.01 <= price <= 1000 or bookmaker_count < MIN_CONSENSUS_BOOKMAKERS:
        return {"ok": False, "reason": "learning_t1h_consensus_price_or_coverage_missing", "market": market, "selection": selection}
    evidence_hash = _content_hash({
        "fixture": fixture, "stage": "T-1h", "snapshot_at": terminal.get("snapshot_at"),
        "market": market, "selection": selection, "line": line,
        "price": price, "bookmaker_count": bookmaker_count, "market_snapshot": market_snapshot,
    })
    return {
        "ok": True, "reference_stage": "T-1h", "reference_decimal_price": price,
        "price_reference_evidence_ref": f"snapshot:{evidence_hash}:T-1h:{market}:{selection}",
        "snapshot_at": terminal.get("snapshot_at"), "evidence_hash": evidence_hash,
    }


def _learning_terminal_price_evidence(
    freeze: Dict[str, Any], shadow_lock: Dict[str, Any], store: Dict[str, Any],
) -> Dict[str, Any]:
    """Return the exact-line T-1h consensus used by forward Shadow validation."""
    expression = shadow_lock.get("selected_expression") if isinstance(shadow_lock.get("selected_expression"), dict) else {}
    return _learning_t1h_price_evidence_for_expression(freeze, expression, store)


def record_hypothesis_validation(hypothesis_id: Any, payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    hypothesis_id = str(hypothesis_id or "").strip()
    payload = payload if isinstance(payload, dict) else {}
    freeze_id = str(payload.get("freeze_id") or "").strip()
    if not freeze_id:
        raise HTTPException(status_code=400, detail="valid_freeze_id_required")
    if not str(payload.get("evidence_summary") or "").strip():
        raise HTTPException(status_code=400, detail="evidence_summary_required")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
        if not hypothesis:
            raise HTTPException(status_code=404, detail="hypothesis_not_found")
        if freeze_id in (hypothesis.get("discovery_freeze_ids") or []):
            raise HTTPException(status_code=409, detail="discovery_sample_cannot_validate_same_hypothesis")
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze or freeze_id not in (store.get("learning_postmatch") or {}):
            raise HTTPException(status_code=409, detail="independent_settled_frozen_sample_required")
        shadow_lock = ((store.get("learning_shadow_locks") or {}).get(hypothesis_id) or {}).get(freeze_id)
        if not shadow_lock:
            raise HTTPException(status_code=409, detail="pre_kickoff_shadow_lock_required")
        if str(payload.get("shadow_lock_hash") or "").strip() != str(shadow_lock.get("lock_hash") or ""):
            raise HTTPException(status_code=409, detail="matching_shadow_lock_hash_required")
        caller_price_fields = {
            "price_reference_stage", "reference_decimal_price", "price_reference_evidence_ref",
            "closing_decimal_price", "closing_price_evidence_ref",
        }
        if any(key in payload for key in caller_price_fields):
            raise HTTPException(status_code=400, detail="caller_supplied_price_reference_forbidden")
        reference = _learning_terminal_price_evidence(freeze, shadow_lock, store)
        if reference.get("ok") is not True:
            raise HTTPException(status_code=409, detail=reference.get("reason"))
        reference_stage = str(reference.get("reference_stage") or "")
        reference_decimal_price = float(reference["reference_decimal_price"])
        price_reference_evidence_ref = str(reference.get("price_reference_evidence_ref") or "")
        postmatch = store["learning_postmatch"][freeze_id]
        final_result = postmatch.get("result") if isinstance(postmatch.get("result"), dict) else {}
        locked_expression = shadow_lock.get("selected_expression") if isinstance(shadow_lock.get("selected_expression"), dict) else {}
        market_forecast_scoring = shadow_lock.get("scoring_contract") == "learning_market_forecast_v1"
        if market_forecast_scoring:
            champion_brier = _brier_market_forecast(shadow_lock["champion_forecast"], locked_expression, final_result)
            challenger_brier = _brier_market_forecast(shadow_lock["challenger_forecast"], locked_expression, final_result)
            realized_forecast_category = _learning_market_outcome_category(locked_expression, final_result)
        else:
            champion_brier = _brier_1x2(shadow_lock["champion_probabilities"], final_result)
            challenger_brier = _brier_1x2(shadow_lock["challenger_probabilities"], final_result)
            realized_forecast_category = "legacy_1x2"
        ablation_plan = get_nested(hypothesis, ["pre_registered_validation_plan", "ablation_plan"], {}) or {}
        module_briers = {
            module: (
                _brier_market_forecast(row["forecast"], locked_expression, final_result)
                if market_forecast_scoring
                else _brier_1x2(row["probabilities"], final_result)
            )
            for module, row in (shadow_lock.get("module_ablations") or {}).items()
        }
        module_brier_gains = {
            module: round(value - challenger_brier, 8) for module, value in module_briers.items()
        }
        entry_decimal_price = float(get_nested(shadow_lock, ["selected_expression", "entry_decimal_price"]))
        t1h_price_probability_delta = round(1.0 / reference_decimal_price - 1.0 / entry_decimal_price, 8)
        risk_delta = round(float(get_nested(shadow_lock, ["risk", "challenger_tail_risk"])) - float(get_nested(shadow_lock, ["risk", "champion_tail_risk"])), 8)
        champion_brier_gain = round(champion_brier - challenger_brier, 8)
        minimum_champion_gain = float(ablation_plan.get("minimum_challenger_brier_gain_over_champion") or 0.0)
        maximum_risk_increase = as_float(get_nested(hypothesis, ["pre_registered_validation_plan", "maximum_mean_tail_risk_increase"]))
        maximum_risk_increase = 0.0 if maximum_risk_increase is None else maximum_risk_increase
        module_thresholds = {
            module: float(get_nested(ablation_plan, ["module_interventions", module, "minimum_brier_gain"], 0.0) or 0.0)
            for module in ablation_plan.get("required_modules") or []
        }
        metric_failures = []
        if champion_brier_gain < minimum_champion_gain:
            metric_failures.append("challenger_brier_gain_below_preregistered_minimum")
        for module, threshold in module_thresholds.items():
            if module not in module_brier_gains or module_brier_gains[module] < threshold:
                metric_failures.append(f"{module}_ablation_gain_below_preregistered_minimum")
        if risk_delta > maximum_risk_increase:
            metric_failures.append("tail_risk_increase_above_preregistered_maximum")
        outcome = "support" if not metric_failures else "counterexample"
        process_classification = str(postmatch.get("process_classification") or "")
        process_event_clean = process_classification in {
            "PROCESS_CORRECT_RESULT_WIN", "PROCESS_CORRECT_RESULT_LOSS",
            "PROCESS_ERROR_RESULT_WIN", "PROCESS_ERROR_RESULT_LOSS",
        }
        evidence_content = {
            "freeze_id": freeze_id,
            "freeze_hash": freeze.get("content_hash"),
            "postmatch_hash": store["learning_postmatch"][freeze_id].get("postmatch_hash"),
            "outcome": outcome,
            "outcome_derivation": {
                "method": "preregistered_forward_metric_thresholds_v1",
                "metric_failures": metric_failures,
                "caller_supplied_outcome": payload.get("outcome"),
                "caller_supplied_outcome_used": False,
            },
            "evidence_summary": str(payload.get("evidence_summary")).strip(),
            "pit_audit": {
                "status": "passed",
                "derived_from_freeze": True,
                "freeze_hash": freeze.get("content_hash"),
                "captured_before_kickoff": int(freeze.get("captured_at") or 0) < int(freeze.get("kickoff_at") or 0),
                "data_cutoff_before_kickoff": int(freeze.get("data_cutoff_at") or 0) < int(freeze.get("kickoff_at") or 0),
                "caller_status_used": False,
            },
            "event_pollution_audit": {
                "status": "passed" if process_event_clean else "failed",
                "derived_from_process_classification": process_classification,
                "postmatch_event_audit_hash": _content_hash(postmatch.get("event_audit") or {}),
                "caller_status_used": False,
            },
            "shadow_lock_hash": shadow_lock.get("lock_hash"),
            "price_reference": {
                "stage": reference_stage,
                "decimal_price": reference_decimal_price,
                "evidence_ref": price_reference_evidence_ref,
            },
            "derived_metrics": {
                "scoring_contract": shadow_lock.get("scoring_contract"),
                "forecast_market": locked_expression.get("market"),
                "forecast_selection": locked_expression.get("selection"),
                "forecast_line": locked_expression.get("line"),
                "realized_forecast_category": realized_forecast_category,
                "champion_brier": champion_brier,
                "challenger_brier": challenger_brier,
                "champion_brier_1x2": champion_brier if locked_expression.get("market") == "1x2" else None,
                "challenger_brier_1x2": challenger_brier if locked_expression.get("market") == "1x2" else None,
                "challenger_brier_gain_over_champion": champion_brier_gain,
                "module_ablation_brier": module_briers,
                "module_ablation_brier_1x2": module_briers if locked_expression.get("market") == "1x2" else None,
                "module_ablation_brier_gain": module_brier_gains,
                "t1h_price_probability_delta": t1h_price_probability_delta,
                "champion_tail_risk": get_nested(shadow_lock, ["risk", "champion_tail_risk"]),
                "challenger_tail_risk": get_nested(shadow_lock, ["risk", "challenger_tail_risk"]),
                "tail_risk_delta": risk_delta,
                "process_classification": process_classification,
                "result_outcome_used_as_rule_label": False,
            },
        }
        evidence_hash = _content_hash(evidence_content)
        rows = hypothesis.setdefault("validation_evidence", [])
        existing = next((row for row in rows if row.get("freeze_id") == freeze_id), None)
        if existing:
            if existing.get("evidence_hash") == evidence_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="validation_sample_already_recorded")
        evidence = {**evidence_content, "evidence_hash": evidence_hash, "recorded_at": int(now_ts or time.time()), "action": "recorded"}
        rows.append(evidence)
        hypothesis["status"] = "SHADOW_VALIDATION"
        hypothesis["champion_effect"] = False
        store["version"] = VERSION
        write_snapshot_store(store)
        return evidence


def refresh_learning_forward_validation_evidence(now_ts: Optional[int] = None, apply_changes: bool = True) -> Dict[str, Any]:
    """Settle locked forward samples from verified postmatch and exact-line T-1h evidence."""
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    results = []
    for hypothesis_id, locks in (store.get("learning_shadow_locks") or {}).items():
        hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
        if not isinstance(hypothesis, dict):
            continue
        recorded = {str(row.get("freeze_id")) for row in (hypothesis.get("validation_evidence") or []) if isinstance(row, dict)}
        for freeze_id, shadow_lock in (locks or {}).items():
            if freeze_id in recorded:
                results.append({"hypothesis_id": hypothesis_id, "freeze_id": freeze_id, "action": "skipped", "reason": "validation_evidence_already_recorded"})
                continue
            postmatch = (store.get("learning_postmatch") or {}).get(freeze_id)
            freeze = _learning_freeze_by_id(store, freeze_id)
            if not isinstance(postmatch, dict) or not isinstance(freeze, dict):
                results.append({"hypothesis_id": hypothesis_id, "freeze_id": freeze_id, "action": "blocked", "reason": "independent_settled_frozen_sample_required"})
                continue
            reference = _learning_terminal_price_evidence(freeze, shadow_lock, store)
            if reference.get("ok") is not True:
                results.append({
                    "hypothesis_id": hypothesis_id, "freeze_id": freeze_id,
                    "action": "blocked", "reason": reference.get("reason"),
                })
                continue
            if not apply_changes:
                results.append({"hypothesis_id": hypothesis_id, "freeze_id": freeze_id, "action": "would_record", "price_reference_evidence_hash": reference.get("evidence_hash")})
                continue
            try:
                evidence = record_hypothesis_validation(hypothesis_id, {
                    "freeze_id": freeze_id,
                    "evidence_summary": "Automatically derived from the immutable Shadow lock, verified postmatch record and exact-line T-1h learning snapshot.",
                    "shadow_lock_hash": shadow_lock.get("lock_hash"),
                }, now_ts=now_ts)
                results.append({
                    "hypothesis_id": hypothesis_id, "freeze_id": freeze_id,
                    "action": evidence.get("action"), "evidence_hash": evidence.get("evidence_hash"),
                    "outcome": evidence.get("outcome"), "champion_effect": False,
                })
            except HTTPException as exc:
                results.append({
                    "hypothesis_id": hypothesis_id, "freeze_id": freeze_id,
                    "action": "rejected", "status_code": exc.status_code, "reason": exc.detail,
                })
    return {
        "version": VERSION, "result_count": len(results), "results": results,
        "recorded_count": sum(row.get("action") == "recorded" for row in results),
        "caller_supplied_validation_outcome_used": False,
        "automatic_champion_change": False,
    }


def promotion_evidence_report(hypothesis_id: Any, store_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    hypothesis_id = str(hypothesis_id or "").strip()
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
    if not hypothesis:
        raise HTTPException(status_code=404, detail="hypothesis_not_found")
    evidence = hypothesis.get("validation_evidence") or []
    plan = hypothesis.get("pre_registered_validation_plan") if isinstance(hypothesis.get("pre_registered_validation_plan"), dict) else {}
    try:
        planned_minimum = int(plan.get("minimum_samples") or LEARNING_MIN_VALIDATION_SAMPLES)
    except (TypeError, ValueError):
        planned_minimum = LEARNING_MIN_VALIDATION_SAMPLES
    required = max(LEARNING_MIN_VALIDATION_SAMPLES, planned_minimum, 2)
    support_count = sum(row.get("outcome") == "support" for row in evidence)
    counterexamples = [row for row in evidence if row.get("outcome") == "counterexample"]
    metrics = [row.get("derived_metrics") for row in evidence if isinstance(row.get("derived_metrics"), dict)]
    locks = (store.get("learning_shadow_locks") or {}).get(hypothesis_id) or {}
    def mean(key: str) -> Optional[float]:
        values = [as_float(row.get(key)) for row in metrics]
        valid = [value for value in values if value is not None]
        return round(sum(valid) / len(valid), 8) if len(valid) == len(evidence) and valid else None
    def gate(status: str, refs: List[str], **values: Any) -> Dict[str, Any]:
        return {"status": status, "evidence_refs": refs, **values}
    enough = len(evidence) >= required and support_count >= required
    all_hashes = [str(row.get("evidence_hash")) for row in evidence if row.get("evidence_hash")]
    preregistered = bool(plan) and bool(evidence) and all(
        (locks.get(row.get("freeze_id")) or {}).get("lock_hash") == row.get("shadow_lock_hash")
        and int((locks.get(row.get("freeze_id")) or {}).get("locked_at") or 0) >= int(hypothesis.get("registered_at") or 0)
        and get_nested(locks.get(row.get("freeze_id")) or {}, ["calculator_provenance", "origin"]) == "internal_shadow_runner"
        and get_nested(locks.get(row.get("freeze_id")) or {}, ["calculator_provenance", "promotion_eligible"]) is True
        and (locks.get(row.get("freeze_id")) or {}).get("scoring_contract") == "learning_market_forecast_v1"
        and get_nested(locks.get(row.get("freeze_id")) or {}, ["calculator_provenance", "runner_id"]) == "builtin_preregistered_poisson_challenger"
        and (locks.get(row.get("freeze_id")) or {}).get("challenger_spec_hash") == get_nested(plan, ["challenger_spec", "spec_hash"])
        for row in evidence
    )
    pit_passed = enough and all(get_nested(row, ["pit_audit", "status"]) == "passed" for row in evidence)
    clean_process_classes = {"PROCESS_CORRECT_RESULT_WIN", "PROCESS_CORRECT_RESULT_LOSS", "PROCESS_ERROR_RESULT_WIN", "PROCESS_ERROR_RESULT_LOSS"}
    event_passed = enough and all(
        get_nested(row, ["event_pollution_audit", "status"]) == "passed"
        and get_nested(row, ["derived_metrics", "process_classification"]) in clean_process_classes
        for row in evidence
    )
    champion_brier = mean("champion_brier")
    challenger_brier = mean("challenger_brier")
    ablation_plan = plan.get("ablation_plan") if isinstance(plan.get("ablation_plan"), dict) else {}
    required_ablation_modules = list(ablation_plan.get("required_modules") or [])
    module_ablation_metrics = {}
    for module in required_ablation_modules:
        brier_values = [as_float(get_nested(row, ["module_ablation_brier", module])) for row in metrics]
        gain_values = [as_float(get_nested(row, ["module_ablation_brier_gain", module])) for row in metrics]
        complete = bool(evidence) and len(brier_values) == len(evidence) and all(value is not None for value in brier_values + gain_values)
        module_ablation_metrics[module] = {
            "sample_count": len(gain_values) if complete else 0,
            "mean_ablated_brier": round(sum(brier_values) / len(brier_values), 8) if complete else None,
            "mean_challenger_gain_over_ablation": round(sum(gain_values) / len(gain_values), 8) if complete else None,
            "minimum_required_gain": as_float(get_nested(ablation_plan, ["module_interventions", module, "minimum_brier_gain"])),
        }
    mean_t1h_price_delta = mean("t1h_price_probability_delta")
    mean_risk_delta = mean("tail_risk_delta")
    process_eligible = [get_nested(row, ["derived_metrics", "process_classification"]) for row in evidence]
    process_accuracy = round(sum(str(value).startswith("PROCESS_CORRECT_") for value in process_eligible) / len(process_eligible), 8) if process_eligible else None
    max_brier = as_float(plan.get("maximum_challenger_brier"))
    max_brier = 0.34 if max_brier is None else max_brier
    min_t1h_price_delta = as_float(plan.get("minimum_mean_t1h_price_probability_delta"))
    min_t1h_price_delta = 0.0 if min_t1h_price_delta is None else min_t1h_price_delta
    min_process_accuracy = as_float(plan.get("minimum_process_accuracy"))
    min_process_accuracy = 0.6 if min_process_accuracy is None else min_process_accuracy
    max_risk_increase = as_float(plan.get("maximum_mean_tail_risk_increase"))
    max_risk_increase = 0.0 if max_risk_increase is None else max_risk_increase
    module_ablation_passed = bool(required_ablation_modules) and all(
        row["sample_count"] == len(evidence)
        and row["mean_challenger_gain_over_ablation"] is not None
        and row["minimum_required_gain"] is not None
        and row["mean_challenger_gain_over_ablation"] >= row["minimum_required_gain"]
        for row in module_ablation_metrics.values()
    )
    sample_status = "passed" if enough and not counterexamples else ("failed" if counterexamples else "missing")
    gates = {
        "pre_registration": gate(
            "passed" if preregistered and enough else "missing",
            all_hashes,
            registered_at=hypothesis.get("registered_at"),
            required_calculator_origin="internal_shadow_runner",
        ),
        "pit_integrity": gate("passed" if pit_passed else ("failed" if enough else "missing"), all_hashes),
        "event_pollution_audit": gate("passed" if event_passed else ("failed" if enough else "missing"), all_hashes),
        "out_of_sample_shadow": gate(sample_status, all_hashes, support_count=support_count, required_support_count=required, counterexample_count=len(counterexamples)),
        "ablation": gate(
            "passed" if enough and module_ablation_passed else ("failed" if enough else "missing"),
            all_hashes,
            schema_version=ablation_plan.get("schema_version"),
            required_modules=required_ablation_modules,
            module_metrics=module_ablation_metrics,
        ),
        "calibration": gate("passed" if enough and challenger_brier is not None and champion_brier is not None and challenger_brier <= champion_brier and challenger_brier <= max_brier else ("failed" if enough else "missing"), all_hashes, champion_brier=champion_brier, challenger_brier=challenger_brier, maximum_challenger_brier=max_brier),
        "clv_or_price_quality": gate(
            "passed" if enough and mean_t1h_price_delta is not None and mean_t1h_price_delta >= min_t1h_price_delta else ("failed" if enough else "missing"),
            all_hashes,
            reference_stage="T-1h",
            mean_t1h_price_probability_delta=mean_t1h_price_delta,
            minimum_mean_t1h_price_probability_delta=min_t1h_price_delta,
        ),
        "process_accuracy": gate("passed" if enough and process_accuracy is not None and process_accuracy >= min_process_accuracy else ("failed" if enough else "missing"), all_hashes, process_accuracy=process_accuracy, minimum_process_accuracy=min_process_accuracy),
        "risk_review": gate("passed" if enough and mean_risk_delta is not None and mean_risk_delta <= max_risk_increase else ("failed" if enough else "missing"), all_hashes, mean_tail_risk_delta=mean_risk_delta, maximum_mean_tail_risk_increase=max_risk_increase),
    }
    missing_or_failed = [name for name in LEARNING_PROMOTION_REQUIRED_GATES if gates[name]["status"] != "passed"]
    report_content = {
        "hypothesis_id": hypothesis_id, "hypothesis_hash": hypothesis.get("content_hash"),
        "ablation_plan_hash": _content_hash(ablation_plan),
        "required_samples": required, "validation_sample_count": len(evidence), "support_count": support_count,
        "counterexample_count": len(counterexamples), "gates": gates,
    }
    return {
        **report_content, "report_hash": _content_hash(report_content),
        "promotion_ready": not missing_or_failed,
        "incomplete_gates": missing_or_failed,
        "caller_supplied_gate_status_used": False,
        "caller_supplied_validation_outcome_used": False,
        "result_outcome_used_as_optimization_target": False,
        "automatic_champion_change": False,
    }


def create_promotion_candidate(hypothesis_id: Any, gate_audit: Any = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    hypothesis_id = str(hypothesis_id or "").strip()
    if isinstance(gate_audit, dict) and gate_audit:
        raise HTTPException(status_code=400, detail="caller_supplied_promotion_gate_audit_forbidden")
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
        if not hypothesis:
            raise HTTPException(status_code=404, detail="hypothesis_not_found")
        evidence = hypothesis.get("validation_evidence") or []
        evidence_report = promotion_evidence_report(hypothesis_id, store_override=store)
        if not evidence_report["promotion_ready"]:
            blockers = []
            if evidence_report["support_count"] < evidence_report["required_samples"]:
                blockers.append("independent_support_sample_minimum_not_reached")
            if evidence_report["counterexample_count"]:
                blockers.append("unresolved_counterexamples_present")
            if evidence_report["incomplete_gates"]:
                blockers.append("derived_promotion_evidence_incomplete")
            raise HTTPException(status_code=409, detail={
                "error": "promotion_candidate_not_ready",
                "blockers": blockers,
                "support_count": evidence_report["support_count"],
                "required_support_count": evidence_report["required_samples"],
                "counterexample_count": evidence_report["counterexample_count"],
                "missing_gates": evidence_report["incomplete_gates"],
                "evidence_report_hash": evidence_report["report_hash"],
            })
        candidate_content = {
            "hypothesis_id": hypothesis_id,
            "hypothesis_hash": hypothesis.get("content_hash"),
            "support_count": evidence_report["support_count"],
            "validation_evidence_hashes": [row.get("evidence_hash") for row in evidence],
            "gate_audit": evidence_report["gates"],
            "promotion_evidence_report_hash": evidence_report["report_hash"],
        }
        candidate_hash = _content_hash(candidate_content)
        promotions = store.setdefault("learning_promotions", {})
        existing = promotions.get(hypothesis_id)
        if existing:
            if existing.get("candidate_hash") == candidate_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="promotion_candidate_already_exists")
        record = {
            **candidate_content,
            "candidate_hash": candidate_hash,
            "created_at": int(now_ts or time.time()),
            "status": "AWAITING_EXPLICIT_USER_CONFIRMATION",
            "champion_effect": False,
            "automatic_promotion": False,
            "action": "candidate_created",
        }
        promotions[hypothesis_id] = record
        hypothesis["status"] = "PROMOTION_CANDIDATE"
        hypothesis["champion_effect"] = False
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def refresh_learning_promotion_candidates(now_ts: Optional[int] = None, apply_changes: bool = True) -> Dict[str, Any]:
    """Create confirmation-only candidates when all derived gates pass; never activate Champion."""
    now_ts = int(now_ts or time.time())
    store = load_snapshot_store()
    results = []
    for hypothesis_id in sorted((store.get("learning_hypotheses") or {})):
        existing = (store.get("learning_promotions") or {}).get(hypothesis_id)
        if isinstance(existing, dict):
            results.append({
                "hypothesis_id": hypothesis_id, "action": "skipped",
                "reason": "promotion_candidate_already_exists", "candidate_hash": existing.get("candidate_hash"),
            })
            continue
        report = promotion_evidence_report(hypothesis_id, store_override=store)
        if report.get("promotion_ready") is not True:
            results.append({
                "hypothesis_id": hypothesis_id, "action": "blocked",
                "reason": "derived_promotion_evidence_incomplete",
                "incomplete_gates": report.get("incomplete_gates"),
                "report_hash": report.get("report_hash"),
            })
            continue
        if not apply_changes:
            results.append({"hypothesis_id": hypothesis_id, "action": "would_create", "report_hash": report.get("report_hash")})
            continue
        try:
            candidate = create_promotion_candidate(hypothesis_id, None, now_ts=now_ts)
            results.append({
                "hypothesis_id": hypothesis_id, "action": candidate.get("action"),
                "candidate_hash": candidate.get("candidate_hash"),
                "status": candidate.get("status"), "champion_effect": False,
            })
        except HTTPException as exc:
            results.append({
                "hypothesis_id": hypothesis_id, "action": "rejected",
                "status_code": exc.status_code, "reason": exc.detail,
            })
    return {
        "version": VERSION, "result_count": len(results), "results": results,
        "candidate_created_count": sum(row.get("action") == "candidate_created" for row in results),
        "automatic_champion_change": False,
        "explicit_user_confirmation_required": True,
    }


def _league_dna_candidate_status(candidate: Dict[str, Any], store: Dict[str, Any]) -> Dict[str, Any]:
    hypothesis = (store.get("learning_hypotheses") or {}).get(candidate.get("hypothesis_id")) or {}
    evidence = hypothesis.get("validation_evidence") or []
    support_count = sum(row.get("outcome") == "support" for row in evidence)
    counterexample_count = sum(row.get("outcome") == "counterexample" for row in evidence)
    required = LEARNING_MIN_VALIDATION_SAMPLES
    activation = (store.get("league_dna_activation_candidates") or {}).get(candidate.get("tag_id"))
    active = (store.get("league_dna_active") or {}).get(candidate.get("tag_id"))
    if (
        active and active.get("status") == "VERIFIED_ACTIVE"
        and active.get("evidence_confidence") == 100
        and get_nested(active, ["user_confirmation", "confirmed"]) is True
    ):
        status, confidence = "VERIFIED_ACTIVE", 100
    elif activation:
        status, confidence = "AWAITING_EXPLICIT_USER_CONFIRMATION", 99
    elif evidence:
        status = "SHADOW_VALIDATION"
        confidence = min(90, int(90 * min(support_count, required) / required))
    else:
        status, confidence = "LEAGUE_TAG_CANDIDATE", 0
    return {
        "status": status, "evidence_confidence": confidence,
        "independent_support_count": support_count,
        "counterexample_count": counterexample_count,
        "required_independent_support_count": required,
        "champion_effect": status == "VERIFIED_ACTIVE",
    }


def register_league_dna_candidate(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    hypothesis_id = str(payload.get("hypothesis_id") or "").strip()
    tag_id = str(payload.get("tag_id") or "").strip()
    if not hypothesis_id or not tag_id or len(tag_id) > 120 or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_" for char in tag_id):
        raise HTTPException(status_code=400, detail="safe_tag_id_and_hypothesis_id_required")
    category = str(payload.get("category") or "").strip().lower()
    market = str(payload.get("market") or "").strip().lower()
    if category not in LEAGUE_DNA_CATEGORIES or market not in LEAGUE_DNA_MARKETS:
        raise HTTPException(status_code=400, detail="invalid_league_dna_category_or_market")
    magnitude = as_float(payload.get("magnitude_score"))
    if magnitude is None or not -3 <= magnitude <= 3:
        raise HTTPException(status_code=400, detail="magnitude_score_must_be_between_minus_3_and_plus_3")
    scope_audit = audit_learning_scope(payload.get("scope"))
    if not scope_audit["eligible"]:
        raise HTTPException(status_code=422, detail={"error": "league_dna_scope_not_verified", "reasons": scope_audit["reasons"]})
    sample_window = payload.get("sample_window") if isinstance(payload.get("sample_window"), dict) else {}
    required_window_fields = ("training_start", "training_end", "validation_start", "validation_end", "minimum_independent_samples")
    missing_window = [key for key in required_window_fields if sample_window.get(key) in (None, "")]
    try:
        planned_minimum = int(sample_window.get("minimum_independent_samples"))
    except (TypeError, ValueError):
        planned_minimum = 0
    if missing_window or planned_minimum < LEARNING_MIN_VALIDATION_SAMPLES:
        raise HTTPException(status_code=400, detail={
            "error": "complete_league_dna_sample_window_required",
            "missing": missing_window,
            "minimum_independent_samples": LEARNING_MIN_VALIDATION_SAMPLES,
        })
    required_text = ("label", "metric_definition", "baseline_definition", "expected_model_effect", "anti_double_counting_rule")
    missing_text = [key for key in required_text if not str(payload.get(key) or "").strip()]
    if missing_text:
        raise HTTPException(status_code=400, detail={"error": "league_dna_fields_required", "missing": missing_text})
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        hypothesis = (store.get("learning_hypotheses") or {}).get(hypothesis_id)
        if not hypothesis:
            raise HTTPException(status_code=404, detail="linked_hypothesis_not_found")
        if hypothesis.get("type") != "LEAGUE_TAG_CANDIDATE":
            raise HTTPException(status_code=409, detail="league_dna_requires_league_tag_candidate_hypothesis")
        content = {
            "tag_id": tag_id, "hypothesis_id": hypothesis_id,
            "scope": scope_audit["normalized"], "category": category, "market": market,
            "label": str(payload.get("label")).strip(), "magnitude_score": magnitude,
            "metric_definition": str(payload.get("metric_definition")).strip(),
            "baseline_definition": str(payload.get("baseline_definition")).strip(),
            "expected_model_effect": str(payload.get("expected_model_effect")).strip(),
            "anti_double_counting_rule": str(payload.get("anti_double_counting_rule")).strip(),
            "sample_window": {**sample_window, "minimum_independent_samples": planned_minimum},
        }
        content_hash = _content_hash(content)
        candidates = store.setdefault("league_dna_candidates", {})
        existing = candidates.get(tag_id)
        if existing:
            if existing.get("content_hash") == content_hash:
                return {**existing, **_league_dna_candidate_status(existing, store), "action": "unchanged"}
            raise HTTPException(status_code=409, detail="league_dna_tag_id_already_registered")
        record = {
            **content, "content_hash": content_hash, "registered_at": int(time.time()),
            "status": "LEAGUE_TAG_CANDIDATE", "evidence_confidence": 0,
            "champion_effect": False, "immutable": True,
        }
        candidates[tag_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "registered"}


def create_league_dna_activation_candidate(tag_id: Any) -> Dict[str, Any]:
    tag_id = str(tag_id or "").strip()
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        candidate = (store.get("league_dna_candidates") or {}).get(tag_id)
        if not candidate:
            raise HTTPException(status_code=404, detail="league_dna_candidate_not_found")
        promotion = (store.get("learning_promotions") or {}).get(candidate.get("hypothesis_id"))
        if not promotion or promotion.get("status") != "AWAITING_EXPLICIT_USER_CONFIRMATION":
            raise HTTPException(status_code=409, detail="validated_hypothesis_promotion_candidate_required")
        content = {
            "tag_id": tag_id, "tag_hash": candidate.get("content_hash"),
            "hypothesis_id": candidate.get("hypothesis_id"),
            "promotion_candidate_hash": promotion.get("candidate_hash"),
            "proposed_magnitude_score": candidate.get("magnitude_score"),
            "proposed_scope": candidate.get("scope"),
            "rollback_conditions": [
                "new_season_or_format_change", "unresolved_counterexample",
                "out_of_sample_decay", "bookmaker_coverage_regime_change",
            ],
        }
        activation_hash = _content_hash(content)
        activations = store.setdefault("league_dna_activation_candidates", {})
        existing = activations.get(tag_id)
        if existing:
            if existing.get("activation_hash") == activation_hash:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="league_dna_activation_candidate_already_exists")
        record = {
            **content, "activation_hash": activation_hash, "created_at": int(time.time()),
            "status": "AWAITING_EXPLICIT_USER_CONFIRMATION", "evidence_confidence": 99,
            "champion_effect": False, "automatic_activation": False,
        }
        activations[tag_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return {**record, "action": "activation_candidate_created"}


def confirm_league_dna_activation(tag_id: Any, payload: Dict[str, Any], now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Activate one fully validated League DNA tag only from an explicit hash-bound user confirmation."""
    tag_id = str(tag_id or "").strip()
    payload = payload if isinstance(payload, dict) else {}
    activation_hash = str(payload.get("activation_hash") or "").strip()
    actor = str(payload.get("confirmed_by") or "").strip()
    reference = str(payload.get("confirmation_reference") or "").strip()
    statement = str(payload.get("confirmation_statement") or "").strip()
    expected_statement = f"CONFIRM LEAGUE_DNA {tag_id} {activation_hash}"
    if payload.get("confirmed") is not True:
        raise HTTPException(status_code=422, detail="explicit_confirmation_true_required")
    if not activation_hash or not actor or not reference:
        raise HTTPException(status_code=422, detail="activation_hash_actor_and_confirmation_reference_required")
    if not hmac.compare_digest(statement, expected_statement):
        raise HTTPException(status_code=422, detail={"error": "exact_confirmation_statement_required", "expected": expected_statement})
    now_ts = int(now_ts or time.time())
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        candidate = (store.get("league_dna_candidates") or {}).get(tag_id)
        activation = (store.get("league_dna_activation_candidates") or {}).get(tag_id)
        if not candidate or not activation:
            raise HTTPException(status_code=404, detail="league_dna_activation_candidate_not_found")
        if activation.get("status") != "AWAITING_EXPLICIT_USER_CONFIRMATION":
            raise HTTPException(status_code=409, detail="league_dna_activation_not_waiting_for_confirmation")
        if activation_hash != activation.get("activation_hash"):
            raise HTTPException(status_code=409, detail="latest_activation_candidate_hash_required")
        promotion = (store.get("learning_promotions") or {}).get(candidate.get("hypothesis_id"))
        if not promotion or promotion.get("candidate_hash") != activation.get("promotion_candidate_hash"):
            raise HTTPException(status_code=409, detail="matching_validated_promotion_candidate_required")
        active_rows = store.setdefault("league_dna_active", {})
        existing = active_rows.get(tag_id)
        if existing:
            existing_confirmation = existing.get("user_confirmation") if isinstance(existing.get("user_confirmation"), dict) else {}
            same_confirmation = (
                existing.get("activation_candidate_hash") == activation_hash
                and existing_confirmation.get("confirmed_by") == actor
                and existing_confirmation.get("confirmation_reference") == reference
                and existing_confirmation.get("confirmation_statement_hash") == _content_hash(statement)
            )
            if same_confirmation:
                return {**existing, "action": "unchanged"}
            raise HTTPException(status_code=409, detail="league_dna_tag_already_active")
        confirmation = {
            "confirmed": True, "confirmed_by": actor,
            "confirmation_reference": reference,
            "confirmation_statement_hash": _content_hash(statement),
            "confirmed_at": now_ts,
            "activation_hash": activation_hash,
        }
        immutable_content = {
            "tag_id": tag_id,
            "tag_hash": candidate.get("content_hash"),
            "hypothesis_id": candidate.get("hypothesis_id"),
            "promotion_candidate_hash": promotion.get("candidate_hash"),
            "activation_candidate_hash": activation_hash,
            "scope": candidate.get("scope"),
            "category": candidate.get("category"),
            "market": candidate.get("market"),
            "label": candidate.get("label"),
            "magnitude_score": candidate.get("magnitude_score"),
            "metric_definition": candidate.get("metric_definition"),
            "baseline_definition": candidate.get("baseline_definition"),
            "expected_model_effect": candidate.get("expected_model_effect"),
            "anti_double_counting_rule": candidate.get("anti_double_counting_rule"),
            "rollback_conditions": activation.get("rollback_conditions"),
            "user_confirmation": confirmation,
        }
        active_hash = _content_hash(immutable_content)
        record = {
            **immutable_content,
            "active_hash": active_hash,
            "version": "league-dna-active-" + active_hash[:16],
            "activated_at": now_ts,
            "status": "VERIFIED_ACTIVE",
            "evidence_confidence": 100,
            "champion_effect": True,
            "automatic_activation": False,
            "immutable": True,
            "action": "activated",
        }
        active_rows[tag_id] = record
        store["version"] = VERSION
        write_snapshot_store(store)
        return record


def league_dna_model_view(scope: Dict[str, Any], store_override: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Return only explicitly confirmed VERIFIED_ACTIVE priors to prematch callers."""
    store = store_override if store_override is not None else load_snapshot_store()
    try:
        competition_id = int((scope or {}).get("competition_id"))
    except (TypeError, ValueError):
        competition_id = None
    season = str((scope or {}).get("season") or "").strip()
    phase = str((scope or {}).get("phase") or "unknown").strip()
    active = []
    rejected_active = []
    for row in (store.get("league_dna_active") or {}).values():
        row_scope = row.get("scope") if isinstance(row.get("scope"), dict) else {}
        matches = (
            row_scope.get("competition_id") == competition_id
            and str(row_scope.get("season") or "") == season
            and str(row_scope.get("phase") or "all") in {"all", phase}
        )
        if not matches:
            continue
        confirmed = get_nested(row, ["user_confirmation", "confirmed"]) is True
        eligible = row.get("status") == "VERIFIED_ACTIVE" and row.get("evidence_confidence") == 100 and confirmed
        if eligible:
            active.append(row)
        else:
            rejected_active.append({"tag_id": row.get("tag_id"), "reason": "verified_active_confirmation_invariant_failed"})
    active.sort(key=lambda row: str(row.get("tag_id")))
    matching_candidates = [
        row for row in (store.get("league_dna_candidates") or {}).values()
        if get_nested(row, ["scope", "competition_id"]) == competition_id
        and str(get_nested(row, ["scope", "season"]) or "") == season
    ]
    version = "league-dna-" + _content_hash([{"tag_id": row.get("tag_id"), "version": row.get("version"), "hash": row.get("content_hash")} for row in active])[:16] if active else None
    return {
        "status": "VERIFIED_ACTIVE" if active else ("candidate_only" if matching_candidates else "data_missing"),
        "version": version, "active_tags": active,
        "active_tag_count": len(active), "candidate_tag_count": len(matching_candidates),
        "rejected_active_records": rejected_active,
        "champion_effect": bool(active),
        "policy": "only evidence_confidence=100 VERIFIED_ACTIVE records with explicit user confirmation enter the prematch prior",
    }


def league_dna_status_report(competition_id: Optional[int] = None) -> Dict[str, Any]:
    store = load_snapshot_store()
    candidates = []
    for row in (store.get("league_dna_candidates") or {}).values():
        if competition_id is not None and get_nested(row, ["scope", "competition_id"]) != int(competition_id):
            continue
        candidates.append({**row, **_league_dna_candidate_status(row, store)})
    active_rows = [row for row in (store.get("league_dna_active") or {}).values() if competition_id is None or get_nested(row, ["scope", "competition_id"]) == int(competition_id)]
    valid_active = [row for row in active_rows if row.get("status") == "VERIFIED_ACTIVE" and row.get("evidence_confidence") == 100 and get_nested(row, ["user_confirmation", "confirmed"]) is True]
    return {
        "version": VERSION, "competition_id": competition_id,
        "candidate_count": len(candidates), "candidates": sorted(candidates, key=lambda row: str(row.get("tag_id"))),
        "activation_candidate_count": sum(row.get("tag_id") in {candidate.get("tag_id") for candidate in candidates} for row in (store.get("league_dna_activation_candidates") or {}).values()),
        "verified_active_count": len(valid_active), "verified_active": valid_active,
        "automatic_activation": False,
        "policy": "candidate and shadow tags never affect Champion; activation requires a separately recorded explicit user confirmation",
    }


def _learning_selected_expression(decision: Any) -> Dict[str, Any]:
    decision = decision if isinstance(decision, dict) else {}
    for candidate in (
        decision.get("selected_expression"), decision.get("best_market"),
        get_nested(decision, ["recommendation_tiers", "first_choice_high_consistency"]),
    ):
        if isinstance(candidate, dict):
            return {
                "market": str(candidate.get("market") or "data_missing"),
                "selection": candidate.get("selection"), "line": candidate.get("line"),
                "price": candidate.get("price"),
            }
    return {"market": "data_missing", "selection": None, "line": None, "price": None}


def _learning_frozen_rating(decision: Dict[str, Any], rating_name: str) -> Any:
    """Read an explicitly frozen rating without translating or inventing a score."""
    candidates = [decision.get(rating_name)]
    if rating_name == "market_rating":
        selected = decision.get("selected_expression") if isinstance(decision.get("selected_expression"), dict) else {}
        candidates.extend((selected.get("market_rating"), selected.get("rating")))
    for value in candidates:
        if isinstance(value, bool) or value in (None, ""):
            continue
        if isinstance(value, (int, float)):
            return value
        text = str(value).strip()
        if text:
            return text
    return None


def _learning_quality_label(statuses: List[Any], required_count: int) -> Dict[str, Any]:
    normalized = [str(value or "data_missing").strip().lower() for value in statuses]
    if any(value == "failed" for value in normalized):
        label = "failed"
    elif len(normalized) == required_count and all(value == "passed" for value in normalized):
        label = "passed"
    else:
        label = "ungraded"
    return {
        "label": label,
        "calibration_eligible": label in {"passed", "failed"},
        "component_statuses": normalized,
    }


def _learning_quality_card_candidate(freeze: Dict[str, Any], postmatch: Dict[str, Any]) -> Dict[str, Any]:
    """Derive one immutable process card; result direction never assigns a quality label."""
    decision = freeze.get("decision") if isinstance(freeze.get("decision"), dict) else {}
    review = postmatch.get("review") if isinstance(postmatch.get("review"), dict) else {}
    expression = _learning_selected_expression(decision)
    priority_quality = _learning_quality_label([get_nested(review, ["match_selection_quality", "status"])], 1)
    selection_quality = _learning_quality_label([
        get_nested(review, ["expression_audit", "status"]),
        get_nested(review, ["price_execution_audit", "status"]),
    ], 2)
    process_class = str(postmatch.get("process_classification") or "DATA_INSUFFICIENT")
    outcome_independence_attested = review.get("outcome_not_used_for_process_grade") is True
    result_backfit_rejected = get_nested(review, ["learning_disposition", "result_backfit_used"]) is False
    section_evidence_eligible = (
        process_class != "EVENT_CONTAMINATED"
        and outcome_independence_attested and result_backfit_rejected
    )
    sample_eligible = section_evidence_eligible and process_class != "DATA_INSUFFICIENT"
    priority_eligible = section_evidence_eligible and priority_quality["calibration_eligible"]
    selection_eligible = section_evidence_eligible and selection_quality["calibration_eligible"]
    match_rating = _learning_frozen_rating(decision, "match_rating")
    market_rating = _learning_frozen_rating(decision, "market_rating")
    immutable = {
        "schema_version": "learning_quality_card_v1",
        "freeze_id": freeze.get("freeze_id"), "fixture": freeze.get("fixture"),
        "freeze_version_number": freeze.get("version_number"),
        "freeze_hash": freeze.get("content_hash"), "postmatch_hash": postmatch.get("postmatch_hash"),
        "fact_hash": postmatch.get("fact_hash"),
        "scope": freeze.get("scope") if isinstance(freeze.get("scope"), dict) else {},
        "frozen_decision": {
            "action": decision.get("decision"), "match_rating": match_rating,
            "market_rating": market_rating, "selected_expression": expression,
        },
        "priority_quality": {
            **priority_quality, "source_section": "match_selection_quality",
            "rating_available": match_rating is not None,
            "label_derived_before_outcome": outcome_independence_attested,
            "priority_board_id": get_nested(review, ["match_selection_quality", "priority_board_id"]),
            "priority_board_hash": get_nested(review, ["match_selection_quality", "priority_board_hash"]),
            "frozen_priority_rank": get_nested(review, ["match_selection_quality", "frozen_priority_rank"]),
            "frozen_priority_role": get_nested(review, ["match_selection_quality", "frozen_priority_role"]),
            "core_priority_count": get_nested(review, ["match_selection_quality", "core_priority_count"]),
            "priority_board_used": get_nested(review, ["match_selection_quality", "priority_board_used"]) is True,
        },
        "selection_quality": {
            **selection_quality, "source_sections": ["expression_audit", "price_execution_audit"],
            "rating_available": market_rating is not None,
            "label_derived_before_outcome": outcome_independence_attested,
        },
        "diagnostics": {section: get_nested(review, [section, "status"]) for section in LEARNING_REVIEW_SECTIONS},
        "sample_eligibility": {
            "eligible": sample_eligible,
            "priority_calibration_eligible": priority_eligible,
            "selection_calibration_eligible": selection_eligible,
            "section_evidence_isolated": True,
            "reason": (
                "verified_outcome_independent_process_review" if sample_eligible else
                "independent_sections_only; overall_process_data_insufficient" if process_class == "DATA_INSUFFICIENT" and section_evidence_eligible else
                process_class.lower() if process_class in {"EVENT_CONTAMINATED", "DATA_INSUFFICIENT"} else
                "outcome_independence_attestation_missing"
            ),
            "outcome_not_used_for_process_grade": outcome_independence_attested,
            "result_backfit_explicitly_rejected": result_backfit_rejected,
        },
        "outcome_context": {
            "process_classification": process_class, "result_reference": postmatch.get("postmatch_hash"),
            "final_score_copied_into_card": False,
            "excluded_from_priority_quality_label": True,
            "excluded_from_selection_quality_label": True,
        },
        "result_backfit_used": False, "automatic_weight_change": False, "champion_effect": False,
    }
    card_hash = _content_hash(immutable)
    return {**immutable, "card_id": "learning-card-" + card_hash[:16], "card_hash": card_hash, "immutable": True}


def learning_quality_card_candidates() -> Dict[str, Any]:
    store = load_snapshot_store()
    cards, rejected = [], []
    for freeze_id, postmatch in (store.get("learning_postmatch") or {}).items():
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze:
            rejected.append({"freeze_id": freeze_id, "reason": "frozen_sample_missing"})
            continue
        cards.append(_learning_quality_card_candidate(freeze, postmatch))
    cards.sort(key=lambda row: (str(row.get("fixture")), int(row.get("freeze_version_number") or 0)))
    return {
        "version": VERSION, "candidate_count": len(cards), "candidates": cards, "rejected": rejected,
        "result_outcome_used_for_labels": False, "automatic_weight_change": False,
        "automatic_champion_change": False,
    }


def refresh_learning_quality_cards(now_ts: Optional[int] = None) -> Dict[str, Any]:
    """Persist hash-bound Learning Cards; existing cards can never be rewritten."""
    now_ts = int(now_ts or time.time())
    derived = learning_quality_card_candidates()
    results = []
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        saved = store.setdefault("learning_quality_cards", {})
        for candidate in derived["candidates"]:
            freeze_id = str(candidate.get("freeze_id") or "")
            existing = saved.get(freeze_id)
            if existing:
                if existing.get("card_hash") != candidate.get("card_hash"):
                    raise HTTPException(status_code=409, detail="immutable_learning_quality_card_conflict")
                results.append({**existing, "action": "unchanged"})
                continue
            record = {**candidate, "created_at": now_ts, "action": "created"}
            saved[freeze_id] = record
            results.append(record)
        store["version"] = VERSION
        write_snapshot_store(store)
    return {
        "version": VERSION, "created_count": sum(row.get("action") == "created" for row in results),
        "results": results, "rejected": derived["rejected"],
        "result_outcome_used_for_labels": False, "automatic_weight_change": False,
        "automatic_champion_change": False,
    }


def _learning_rating_bucket(value: Any) -> Optional[str]:
    if value in (None, "") or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return f"numeric:{float(value):g}"
    return "categorical:" + str(value).strip().upper()


def learning_quality_calibration_report(minimum_samples: Optional[int] = None) -> Dict[str, Any]:
    """Calibrate frozen ratings against process audits, never result wins."""
    minimum_samples = max(2, int(minimum_samples or LEARNING_MIN_VALIDATION_SAMPLES))
    store = load_snapshot_store()
    cards = [row for row in (store.get("learning_quality_cards") or {}).values() if isinstance(row, dict)]
    latest_by_fixture: Dict[str, Dict[str, Any]] = {}
    superseded = 0
    for card in cards:
        fixture = str(card.get("fixture") or "")
        current = latest_by_fixture.get(fixture)
        if current and int(current.get("freeze_version_number") or 0) >= int(card.get("freeze_version_number") or 0):
            superseded += 1
            continue
        if current:
            superseded += 1
        latest_by_fixture[fixture] = card
    groups: Dict[Tuple[str, Any, str], Dict[str, Any]] = {}
    excluded = {
        "superseded_freeze_version": superseded, "sample_ineligible": 0,
        "priority_sample_ineligible": 0, "selection_sample_ineligible": 0,
        "priority_rating_missing": 0, "priority_label_ungraded": 0,
        "selection_rating_missing": 0, "selection_label_ungraded": 0,
    }
    for card in latest_by_fixture.values():
        any_dimension_eligible = False
        competition_id = get_nested(card, ["scope", "competition_id"])
        competition_name = get_nested(card, ["scope", "competition_name"])
        for dimension, rating_field, prefix in (
            ("priority_quality", "match_rating", "priority"),
            ("selection_quality", "market_rating", "selection"),
        ):
            eligibility_key = f"{prefix}_calibration_eligible"
            dimension_eligible = get_nested(card, ["sample_eligibility", eligibility_key])
            if dimension_eligible is None:
                dimension_eligible = get_nested(card, ["sample_eligibility", "eligible"])
            if dimension_eligible is not True:
                excluded[f"{prefix}_sample_ineligible"] += 1
                continue
            any_dimension_eligible = True
            rating = get_nested(card, ["frozen_decision", rating_field])
            bucket = _learning_rating_bucket(rating)
            label = get_nested(card, [dimension, "label"])
            if bucket is None:
                excluded[f"{prefix}_rating_missing"] += 1
                continue
            if label not in {"passed", "failed"}:
                excluded[f"{prefix}_label_ungraded"] += 1
                continue
            key = (dimension, competition_id, bucket)
            group = groups.setdefault(key, {
                "dimension": dimension, "competition_id": competition_id,
                "competition_name": competition_name, "rating_bucket": bucket,
                "frozen_rating": rating, "eligible_sample_count": 0,
                "passed_count": 0, "failed_count": 0, "card_ids": [], "freeze_ids": [], "fixture_ids": [],
            })
            group["eligible_sample_count"] += 1
            group[f"{label}_count"] += 1
            group["card_ids"].append(card.get("card_id"))
            group["freeze_ids"].append(card.get("freeze_id"))
            group["fixture_ids"].append(card.get("fixture"))
        if not any_dimension_eligible:
            excluded["sample_ineligible"] += 1
    calibration_groups = []
    for group in groups.values():
        count = group["eligible_sample_count"]
        calibration_groups.append({
            **group, "minimum_samples": minimum_samples, "sample_ready": count >= minimum_samples,
            "observed_process_pass_rate": round(group["passed_count"] / count, 6),
            "interpretation": "internal_process_calibration_only", "result_win_rate_used": False,
            "automatic_weight_change": False, "champion_effect": False,
        })
    calibration_groups.sort(key=lambda row: (str(row.get("competition_name")), row["dimension"], row["rating_bucket"]))
    return {
        "version": VERSION, "minimum_samples": minimum_samples, "stored_card_count": len(cards),
        "latest_fixture_card_count": len(latest_by_fixture), "calibration_group_count": len(calibration_groups),
        "sample_ready_group_count": sum(row["sample_ready"] for row in calibration_groups),
        "groups": calibration_groups, "excluded_counts": excluded,
        "result_outcome_used_for_calibration": False, "automatic_weight_change": False,
        "automatic_champion_change": False,
        "policy": "calibrate only explicitly frozen ratings against hash-bound process audits; missing ratings are excluded rather than inferred",
    }


def learning_selection_quality_report(minimum_samples: Optional[int] = None) -> Dict[str, Any]:
    """Aggregate process quality without using win/loss as a model-change signal."""
    minimum_samples = max(2, int(minimum_samples or LEARNING_MIN_VALIDATION_SAMPLES))
    store = load_snapshot_store()
    groups: Dict[str, Dict[str, Any]] = {}
    excluded_counts = {
        "event_contaminated": 0, "data_insufficient": 0,
        "frozen_sample_missing": 0, "superseded_freeze_version": 0,
    }
    latest_by_fixture: Dict[str, Tuple[Dict[str, Any], Dict[str, Any]]] = {}
    for freeze_id, postmatch in (store.get("learning_postmatch") or {}).items():
        freeze = _learning_freeze_by_id(store, freeze_id)
        if not freeze:
            excluded_counts["frozen_sample_missing"] += 1
            continue
        fixture = str(freeze.get("fixture") or "")
        existing = latest_by_fixture.get(fixture)
        if existing and int(existing[0].get("version_number") or 0) >= int(freeze.get("version_number") or 0):
            excluded_counts["superseded_freeze_version"] += 1
            continue
        if existing:
            excluded_counts["superseded_freeze_version"] += 1
        latest_by_fixture[fixture] = (freeze, postmatch)
    for freeze, postmatch in latest_by_fixture.values():
        freeze_id = str(freeze.get("freeze_id") or "")
        fixture = str(freeze.get("fixture") or "")
        process_class = str(postmatch.get("process_classification") or "")
        if process_class == "EVENT_CONTAMINATED":
            excluded_counts["event_contaminated"] += 1
            continue
        if process_class == "DATA_INSUFFICIENT":
            excluded_counts["data_insufficient"] += 1
            continue
        expression = _learning_selected_expression(freeze.get("decision"))
        competition_id = get_nested(freeze, ["scope", "competition_id"])
        competition_name = get_nested(freeze, ["scope", "competition_name"])
        group_key = f"{competition_id}:{expression['market']}"
        group = groups.setdefault(group_key, {
            "competition_id": competition_id, "competition_name": competition_name,
            "market": expression["market"], "eligible_sample_count": 0,
            "process_correct_count": 0, "process_error_count": 0,
            "match_selection_failed_count": 0, "expression_failed_count": 0,
            "price_execution_failed_count": 0, "inconclusive_review_count": 0,
            "freeze_ids": [], "fixture_ids": [],
            "failure_freeze_ids": {
                "process_error": [], "match_selection": [],
                "expression": [], "price_execution": [],
            },
        })
        group["eligible_sample_count"] += 1
        group["freeze_ids"].append(freeze_id)
        group["fixture_ids"].append(fixture)
        if process_class.startswith("PROCESS_CORRECT_"):
            group["process_correct_count"] += 1
        elif process_class.startswith("PROCESS_ERROR_"):
            group["process_error_count"] += 1
            group["failure_freeze_ids"]["process_error"].append(freeze_id)
        review = postmatch.get("review") if isinstance(postmatch.get("review"), dict) else {}
        section_statuses = {
            "match_selection": get_nested(review, ["match_selection_quality", "status"]),
            "expression": get_nested(review, ["expression_audit", "status"]),
            "price_execution": get_nested(review, ["price_execution_audit", "status"]),
        }
        if section_statuses["match_selection"] == "failed":
            group["match_selection_failed_count"] += 1
            group["failure_freeze_ids"]["match_selection"].append(freeze_id)
        if section_statuses["expression"] == "failed":
            group["expression_failed_count"] += 1
            group["failure_freeze_ids"]["expression"].append(freeze_id)
        if section_statuses["price_execution"] == "failed":
            group["price_execution_failed_count"] += 1
            group["failure_freeze_ids"]["price_execution"].append(freeze_id)
        if any(status in {"inconclusive", "data_missing", None} for status in section_statuses.values()):
            group["inconclusive_review_count"] += 1
    cards = []
    for group in groups.values():
        count = group["eligible_sample_count"]
        sample_ready = count >= minimum_samples
        rates = {
            "process_accuracy": round(group["process_correct_count"] / count, 6) if count else None,
            "match_selection_failure_rate": round(group["match_selection_failed_count"] / count, 6) if count else None,
            "expression_failure_rate": round(group["expression_failed_count"] / count, 6) if count else None,
            "price_execution_failure_rate": round(group["price_execution_failed_count"] / count, 6) if count else None,
        }
        has_failure_evidence = any(group[key] > 0 for key in ("match_selection_failed_count", "expression_failed_count", "price_execution_failed_count", "process_error_count"))
        cards.append({
            **group,
            "freeze_ids": group["freeze_ids"],
            "fixture_ids": group["fixture_ids"],
            "failure_freeze_ids": group["failure_freeze_ids"],
            "freeze_ids_truncated": False,
            "minimum_samples": minimum_samples, "sample_ready": sample_ready, "rates": rates,
            "research_signal": "HYPOTHESIS_ONLY_REVIEW_ALLOWED" if sample_ready and has_failure_evidence else "COLLECT_MORE_INDEPENDENT_SAMPLES",
            "champion_effect": False, "automatic_weight_change": False,
            "result_outcome_used_for_optimization": False,
        })
    cards.sort(key=lambda row: (str(row.get("competition_name")), str(row.get("market"))))
    return {
        "version": VERSION, "minimum_samples": minimum_samples,
        "card_count": len(cards), "sample_ready_card_count": sum(row["sample_ready"] for row in cards),
        "cards": cards, "excluded_counts": excluded_counts,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
        "policy": "aggregate frozen process reviews only; final win/loss is not an optimization target and any signal must be preregistered as a new hypothesis",
    }


def learning_research_proposal_candidates(minimum_matches: Optional[int] = None) -> Dict[str, Any]:
    """Derive repeated multi-match research signals without asserting a causal theory."""
    minimum_matches = max(3, int(minimum_matches or LEARNING_RESEARCH_PROPOSAL_MIN_MATCHES))
    quality = learning_selection_quality_report(minimum_samples=minimum_matches)
    dimensions = (
        ("process_error", "process_error_count", "process_accuracy"),
        ("match_selection", "match_selection_failed_count", "match_selection_failure_rate"),
        ("expression", "expression_failed_count", "expression_failure_rate"),
        ("price_execution", "price_execution_failed_count", "price_execution_failure_rate"),
    )
    candidates = []
    rejected = []
    for card in quality.get("cards") or []:
        if not card.get("sample_ready"):
            rejected.append({
                "competition_id": card.get("competition_id"), "market": card.get("market"),
                "reason": "minimum_distinct_match_sample_not_reached",
                "eligible_match_count": card.get("eligible_sample_count"),
            })
            continue
        for dimension, count_key, rate_key in dimensions:
            failure_count = int(card.get(count_key) or 0)
            if dimension == "process_error":
                accuracy = get_nested(card, ["rates", rate_key])
                failure_rate = round(1.0 - float(accuracy), 6) if accuracy is not None else None
            else:
                failure_rate = get_nested(card, ["rates", rate_key])
            supporting_freezes = list(get_nested(card, ["failure_freeze_ids", dimension], []) or [])
            reasons = []
            if failure_count < LEARNING_RESEARCH_PROPOSAL_MIN_FAILURES:
                reasons.append("minimum_repeated_failures_not_reached")
            if failure_rate is None or float(failure_rate) < LEARNING_RESEARCH_PROPOSAL_MIN_FAILURE_RATE:
                reasons.append("minimum_failure_rate_not_reached")
            if len(set(supporting_freezes)) < LEARNING_RESEARCH_PROPOSAL_MIN_FAILURES:
                reasons.append("independent_supporting_freezes_insufficient")
            if reasons:
                rejected.append({
                    "competition_id": card.get("competition_id"), "market": card.get("market"),
                    "signal_dimension": dimension, "failure_count": failure_count,
                    "failure_rate": failure_rate, "reasons": reasons,
                })
                continue
            identity = {
                "competition_id": card.get("competition_id"),
                "market": card.get("market"), "signal_dimension": dimension,
            }
            proposal_id = "research-" + _content_hash(identity)[:16]
            evidence = {
                "eligible_distinct_match_count": int(card.get("eligible_sample_count") or 0),
                "failure_count": failure_count,
                "failure_rate": failure_rate,
                "supporting_freeze_ids": sorted(set(supporting_freezes)),
                "context_freeze_ids": sorted(set(card.get("freeze_ids") or [])),
                "context_fixture_ids": sorted(set(card.get("fixture_ids") or [])),
                "selection_quality_rates": card.get("rates"),
                "result_outcome_used": False,
            }
            immutable = {
                "proposal_id": proposal_id,
                "status": "RESEARCH_PROPOSAL",
                "scope": {
                    "competition_id": card.get("competition_id"),
                    "competition_name": card.get("competition_name"),
                    "market": card.get("market"),
                },
                "signal_dimension": dimension,
                "evidence": evidence,
                "research_question": f"Why does the frozen {dimension} audit repeatedly fail in this competition and market, and what prematch-observable intervention would reduce that failure?",
                "causal_claim_status": "not_formulated",
                "required_next_action": "formulate_a_falsifiable_causal_hypothesis_and_challenger_before_forward_validation",
                "forward_validation_blueprint": {
                    "discovery_samples_must_be_excluded": True,
                    "minimum_independent_validation_samples": LEARNING_MIN_VALIDATION_SAMPLES,
                    "pre_registration_required": True,
                    "pre_kickoff_shadow_lock_required": True,
                    "required_comparators": ["Champion", "Challenger", "Ablation"],
                    "allowed_ablation_modules": sorted(LEARNING_ABLATION_INTERVENTIONS),
                    "module_ablation_plan_must_be_preregistered": True,
                    "required_metrics": ["Brier", "CLV", "Process Accuracy", "Tail Risk"],
                    "unresolved_counterexample_blocks_promotion": True,
                    "explicit_user_confirmation_required_for_champion": True,
                },
                "automatic_hypothesis_registration": False,
                "automatic_model_effect": False,
                "champion_effect": False,
                "result_backfit_used": False,
            }
            candidates.append({**immutable, "proposal_hash": _content_hash(immutable)})
    candidates.sort(key=lambda row: (
        str(get_nested(row, ["scope", "competition_name"])),
        str(get_nested(row, ["scope", "market"])), str(row.get("signal_dimension")),
    ))
    return {
        "version": VERSION, "minimum_distinct_matches": minimum_matches,
        "minimum_repeated_failures": LEARNING_RESEARCH_PROPOSAL_MIN_FAILURES,
        "minimum_failure_rate": LEARNING_RESEARCH_PROPOSAL_MIN_FAILURE_RATE,
        "candidate_count": len(candidates), "candidates": candidates,
        "rejected_count": len(rejected), "rejected": rejected,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
        "policy": "multi-match process signals create research proposals only; a proposal is not a causal hypothesis and has no model effect",
    }


def refresh_learning_research_proposals(now_ts: Optional[int] = None, minimum_matches: Optional[int] = None) -> Dict[str, Any]:
    """Persist immutable versions of repeated-signal proposals; never register hypotheses."""
    now_ts = int(now_ts or time.time())
    derived = learning_research_proposal_candidates(minimum_matches=minimum_matches)
    results = []
    with SNAPSHOT_STORE_LOCK:
        store = load_snapshot_store()
        proposals = store.setdefault("learning_research_proposals", {})
        for candidate in derived["candidates"]:
            proposal_id = candidate["proposal_id"]
            rows = proposals.setdefault(proposal_id, [])
            duplicate = next((row for row in rows if row.get("proposal_hash") == candidate.get("proposal_hash")), None)
            if duplicate:
                results.append({**duplicate, "action": "unchanged"})
                continue
            version_number = max([int(row.get("version_number") or 0) for row in rows] + [0]) + 1
            record = {
                **candidate, "version_number": version_number,
                "created_at": now_ts, "immutable": True, "action": "proposed",
            }
            rows.append(record)
            results.append(record)
        store["version"] = VERSION
        write_snapshot_store(store)
    return {
        "version": VERSION, "generated_at": now_ts,
        "proposal_result_count": len(results),
        "new_proposal_version_count": sum(row.get("action") == "proposed" for row in results),
        "results": results,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
    }


def learning_research_proposal_report() -> Dict[str, Any]:
    store = load_snapshot_store()
    rows = []
    for proposal_id, versions in (store.get("learning_research_proposals") or {}).items():
        latest = max((row for row in versions or [] if isinstance(row, dict)), key=lambda row: int(row.get("version_number") or 0), default=None)
        if latest:
            rows.append(latest)
    rows.sort(key=lambda row: (
        str(get_nested(row, ["scope", "competition_name"])),
        str(get_nested(row, ["scope", "market"])), str(row.get("signal_dimension")),
    ))
    return {
        "version": VERSION, "proposal_count": len(rows), "proposals": rows,
        "automatic_hypothesis_registration": False,
        "automatic_champion_change": False,
        "policy": "research proposals require causal formulation and preregistration before any forward Shadow validation",
    }


AUTOMATIC_LEARNING_GATE_DEFINITIONS = {
    "api_football_configured": {
        "category": "external_configuration", "owner": "operator",
        "action": "configure_primary_fixture_provider",
        "verification": "runtime readiness reports api_football_configured=true",
        "depends_on": [], "requires_external_change": True, "requires_sensitive_value": True,
    },
    "api_football_authenticated": {
        "category": "external_provider", "owner": "operator_and_provider",
        "action": "complete_primary_provider_authentication_probe",
        "verification": "startup probe reports authenticated=true",
        "depends_on": ["api_football_configured"], "requires_external_change": True, "requires_sensitive_value": False,
    },
    "the_odds_api_configured": {
        "category": "external_configuration", "owner": "operator",
        "action": "configure_odds_timeline_provider",
        "verification": "runtime readiness reports the_odds_api_configured=true",
        "depends_on": [], "requires_external_change": True, "requires_sensitive_value": True,
    },
    "protected_api_configured": {
        "category": "security_configuration", "owner": "operator",
        "action": "configure_protected_shadow_access",
        "verification": "runtime readiness reports protected_api_configured=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": True,
    },
    "persistent_store_configured": {
        "category": "persistence_configuration", "owner": "operator",
        "action": "mount_and_select_persistent_data_path",
        "verification": "runtime readiness reports persistent_store_configured=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "persistent_store_operational": {
        "category": "persistence_runtime", "owner": "runtime",
        "action": "initialize_or_recover_primary_snapshot_store",
        "verification": "store integrity reports operational=true",
        "depends_on": ["persistent_store_configured"], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "persistent_backup_ready": {
        "category": "persistence_runtime", "owner": "runtime",
        "action": "create_and_verify_snapshot_backup",
        "verification": "store integrity reports recovery_ready=true",
        "depends_on": ["persistent_store_operational"], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "persistent_store_single_writer": {
        "category": "deployment_configuration", "owner": "operator",
        "action": "enforce_single_writer_process",
        "verification": "runtime readiness reports persistent_store_single_writer=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "auto_snapshot_enabled": {
        "category": "scheduler_configuration", "owner": "operator",
        "action": "enable_automatic_snapshot_scheduler",
        "verification": "runtime readiness reports auto_snapshot_enabled=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "auto_snapshot_worker_alive": {
        "category": "scheduler_runtime", "owner": "runtime",
        "action": "start_or_restart_snapshot_worker",
        "verification": "runtime readiness reports auto_snapshot_worker_alive=true",
        "depends_on": ["auto_snapshot_enabled", "persistent_store_operational"], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "major_league_registry_configured": {
        "category": "code_contract", "owner": "code",
        "action": "restore_approved_major_league_registry",
        "verification": "runtime readiness reports major_league_registry_configured=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "major_league_registry_versioned": {
        "category": "code_contract", "owner": "code",
        "action": "restore_versioned_registry_manifest",
        "verification": "registry manifest has a version and content hash",
        "depends_on": ["major_league_registry_configured"], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "node_execution_recovery_configured": {
        "category": "code_contract", "owner": "code",
        "action": "restore_bounded_node_lease_and_retry_contract",
        "verification": "runtime readiness reports node_execution_recovery_configured=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
    "daily_schedule_configured": {
        "category": "code_contract", "owner": "code",
        "action": "restore_daily_1430_schedule_contract",
        "verification": "runtime readiness reports daily_schedule_configured=true",
        "depends_on": [], "requires_external_change": False, "requires_sensitive_value": False,
    },
}
AUTOMATIC_LEARNING_REQUIRED_GATES = tuple(AUTOMATIC_LEARNING_GATE_DEFINITIONS)


def _automatic_learning_activation_plan_hash_content(plan: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: plan.get(key) for key in (
            "schema", "version", "readiness_status", "activation_allowed", "gates", "blockers",
            "external_change_required", "operator_action_required", "code_change_required",
            "automatic_mutation_allowed",
        )
    }


def audit_automatic_learning_activation_plan(plan: Any) -> Dict[str, Any]:
    row = plan if isinstance(plan, dict) else {}
    sensitive_names = {"api_key", "secret", "token", "password", "credential", "authorization", "cookie"}

    def sensitive_paths(value: Any, path: str = "") -> List[str]:
        found = []
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key).lower()
                child_path = f"{path}.{key}" if path else str(key)
                if key_text in sensitive_names or any(key_text.endswith(f"_{name}") for name in sensitive_names):
                    found.append(child_path)
                found.extend(sensitive_paths(child, child_path))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                found.extend(sensitive_paths(child, f"{path}[{index}]"))
        return found

    exposed_paths = sensitive_paths(row)
    gates = row.get("gates") if isinstance(row.get("gates"), list) else []
    observed_names = [str(gate.get("gate") or "") for gate in gates if isinstance(gate, dict)]
    blockers = [str(value) for value in (row.get("blockers") or [])]
    expected_blockers = [
        str(gate.get("gate")) for gate in gates
        if isinstance(gate, dict) and gate.get("passed") is not True
    ]
    semantic_issues = []
    allowed_top_level = {
        "schema", "version", "readiness_status", "activation_allowed", "gates", "blockers",
        "external_change_required", "operator_action_required", "code_change_required",
        "automatic_mutation_allowed", "plan_hash", "immutable", "audit", "policy",
    }
    unexpected_top_level = sorted(set(row) - allowed_top_level)
    if unexpected_top_level:
        semantic_issues.append("unexpected_top_level_fields:" + ",".join(unexpected_top_level))
    if row.get("schema") != "automatic_learning_activation_plan_v1":
        semantic_issues.append("schema_invalid")
    if row.get("version") != VERSION:
        semantic_issues.append("version_mismatch")
    if row.get("immutable") is not True:
        semantic_issues.append("immutable_marker_missing")
    if exposed_paths:
        semantic_issues.append("sensitive_fields_present")
    if observed_names != list(AUTOMATIC_LEARNING_REQUIRED_GATES):
        semantic_issues.append("required_gate_order_or_membership_invalid")
    if blockers != expected_blockers:
        semantic_issues.append("blocker_list_does_not_match_failed_gates")
    for gate in gates:
        if not isinstance(gate, dict):
            semantic_issues.append("gate_record_must_be_an_object")
            continue
        allowed_gate_fields = {
            "gate", "passed", "blocking", "category", "owner", "depends_on",
            "requires_external_change", "requires_sensitive_value", "next_action", "verification",
        }
        unexpected_gate_fields = sorted(set(gate) - allowed_gate_fields)
        if unexpected_gate_fields:
            semantic_issues.append(
                f"unexpected_gate_fields:{gate.get('gate')}:" + ",".join(unexpected_gate_fields)
            )
        name = str(gate.get("gate") or "")
        definition = AUTOMATIC_LEARNING_GATE_DEFINITIONS.get(name)
        if not definition:
            semantic_issues.append(f"unknown_gate:{name}")
            continue
        for field in (
            "category", "owner", "depends_on", "requires_external_change", "requires_sensitive_value",
        ):
            if gate.get(field) != definition.get(field):
                semantic_issues.append(f"gate_definition_mismatch:{name}:{field}")
        expected_action = None if gate.get("passed") is True else definition["action"]
        expected_verification = None if gate.get("passed") is True else definition["verification"]
        if gate.get("blocking") is not (gate.get("passed") is not True):
            semantic_issues.append(f"gate_blocking_state_mismatch:{name}")
        if gate.get("next_action") != expected_action:
            semantic_issues.append(f"gate_action_mismatch:{name}")
        if gate.get("verification") != expected_verification:
            semantic_issues.append(f"gate_verification_mismatch:{name}")
    expected_hash = _content_hash(_automatic_learning_activation_plan_hash_content(row)) if row else ""
    hash_valid = bool(row.get("plan_hash")) and hmac.compare_digest(str(row.get("plan_hash")), expected_hash)
    if not hash_valid:
        semantic_issues.append("plan_hash_invalid")
    activation_allowed = not expected_blockers
    expected_external = any(
        gate.get("requires_external_change") is True for gate in gates
        if isinstance(gate, dict) and gate.get("passed") is not True
    )
    expected_operator = any(
        gate.get("owner") in {"operator", "operator_and_provider"} for gate in gates
        if isinstance(gate, dict) and gate.get("passed") is not True
    )
    expected_code = any(
        gate.get("owner") == "code" for gate in gates
        if isinstance(gate, dict) and gate.get("passed") is not True
    )
    if row.get("readiness_status") != ("ready" if activation_allowed else "not_ready"):
        semantic_issues.append("readiness_status_inconsistent")
    if row.get("activation_allowed") is not activation_allowed:
        semantic_issues.append("activation_allowed_inconsistent")
    if row.get("automatic_mutation_allowed") is not activation_allowed:
        semantic_issues.append("automatic_mutation_allowed_inconsistent")
    if row.get("external_change_required") is not expected_external:
        semantic_issues.append("external_change_required_inconsistent")
    if row.get("operator_action_required") is not expected_operator:
        semantic_issues.append("operator_action_required_inconsistent")
    if row.get("code_change_required") is not expected_code:
        semantic_issues.append("code_change_required_inconsistent")
    issues = list(dict.fromkeys(semantic_issues))
    return {
        "schema": "automatic_learning_activation_plan_audit_v1",
        "status": "passed" if not issues else "failed",
        "valid": not issues,
        "hash_valid": hash_valid,
        "semantic_issues": issues,
        "expected_plan_hash": expected_hash or None,
        "observed_plan_hash": row.get("plan_hash"),
        "secrets_exposed": bool(exposed_paths),
        "sensitive_field_paths": exposed_paths,
    }


def build_automatic_learning_activation_plan(readiness: Dict[str, Any]) -> Dict[str, Any]:
    """Turn runtime gates into a deterministic, secret-free and tamper-evident activation plan."""
    checks = readiness.get("checks") if isinstance(readiness.get("checks"), dict) else {}
    gates = []
    for name, definition in AUTOMATIC_LEARNING_GATE_DEFINITIONS.items():
        passed = checks.get(name) is True
        gates.append({
            "gate": name, "passed": passed, "blocking": not passed,
            "category": definition["category"], "owner": definition["owner"],
            "depends_on": list(definition["depends_on"]),
            "requires_external_change": definition["requires_external_change"],
            "requires_sensitive_value": definition["requires_sensitive_value"],
            "next_action": None if passed else definition["action"],
            "verification": None if passed else definition["verification"],
        })
    blockers = [gate["gate"] for gate in gates if gate["blocking"]]
    failed = [gate for gate in gates if gate["blocking"]]
    content = {
        "schema": "automatic_learning_activation_plan_v1",
        "version": VERSION,
        "readiness_status": "ready" if not blockers else "not_ready",
        "activation_allowed": not blockers,
        "gates": gates,
        "blockers": blockers,
        "external_change_required": any(gate["requires_external_change"] for gate in failed),
        "operator_action_required": any(gate["owner"] in {"operator", "operator_and_provider"} for gate in failed),
        "code_change_required": any(gate["owner"] == "code" for gate in failed),
        "automatic_mutation_allowed": not blockers,
    }
    plan = {**content, "plan_hash": _content_hash(content), "immutable": True}
    audit = audit_automatic_learning_activation_plan(plan)
    return {
        **plan, "audit": audit,
        "policy": "the plan may describe blocked gates but cannot bypass them, expose credentials, start workers, call providers or activate learning",
    }


def automatic_learning_runtime_readiness(
    store_integrity: Optional[Dict[str, Any]] = None,
    worker_alive: Optional[bool] = None,
) -> Dict[str, Any]:
    """Expose the exact configuration gate for an unattended learning loop without secrets."""
    integrity = store_integrity if isinstance(store_integrity, dict) else snapshot_store_integrity()
    if worker_alive is None:
        worker_alive = bool(AUTO_SNAPSHOT_THREAD and AUTO_SNAPSHOT_THREAD.is_alive())
    registry_manifest = learning_major_league_registry_manifest()
    checks = {
        "api_football_configured": bool(API_FOOTBALL_KEY),
        "api_football_authenticated": API_FOOTBALL_STARTUP_PROBE.get("authenticated") is True,
        "the_odds_api_configured": bool(THE_ODDS_API_KEY),
        "optional_event_enrichment_configured": bool(THESTATS_API_KEY),
        "protected_api_configured": bool(SHADOW_ACCESS_TOKEN),
        "persistent_store_configured": str(SNAPSHOT_STORE_PATH).replace("\\", "/").startswith("/data/"),
        "persistent_store_operational": integrity.get("operational") is True,
        "persistent_backup_ready": integrity.get("recovery_ready") is True,
        "persistent_store_single_writer": SNAPSHOT_STORE_WRITER_WORKERS == 1,
        "auto_snapshot_enabled": AUTO_SNAPSHOT_ENABLED is True,
        "auto_snapshot_worker_alive": worker_alive is True,
        "major_league_registry_configured": bool(LEARNING_MAJOR_LEAGUES),
        "major_league_registry_versioned": bool(registry_manifest.get("version") and registry_manifest.get("registry_hash")),
        "node_execution_recovery_configured": (
            60 <= LEARNING_NODE_LEASE_SECONDS <= 3600
            and 60 <= LEARNING_NODE_RETRY_BASE_SECONDS <= LEARNING_NODE_RETRY_MAX_SECONDS
        ),
        "daily_schedule_configured": True,
    }
    blockers = [name for name in AUTOMATIC_LEARNING_REQUIRED_GATES if checks.get(name) is not True]
    report = {
        "status": "ready" if not blockers else "not_ready",
        "ready": not blockers,
        "checks": checks,
        "blockers": blockers,
        "provider_contract": {
            "fixture_and_primary_facts": "api_football",
            "odds_timeline_and_independent_result": "the_odds_api",
            "independent_event_timeline": "optional_enrichment_only",
            "configured_optional_event_provider": "thestats" if THESTATS_API_KEY else None,
            "missing_secondary_event_policy": "DATA_INSUFFICIENT",
            "event_dependent_learning_effect": False if not THESTATS_API_KEY else "requires_verified_matching_event_signature",
        },
        "major_league_registry": {
            "version": registry_manifest["version"],
            "registry_hash": registry_manifest["registry_hash"],
            "league_count": registry_manifest["league_count"],
        },
        "schedule": {"timezone": "Asia/Shanghai", "daily_local_time": "14:30", "same_day_catch_up": True},
        "node_execution": {
            "lease_seconds": LEARNING_NODE_LEASE_SECONDS,
            "retry_base_seconds": LEARNING_NODE_RETRY_BASE_SECONDS,
            "retry_max_seconds": LEARNING_NODE_RETRY_MAX_SECONDS,
            "attempt_retention": LEARNING_NODE_ATTEMPT_RETENTION,
            "persistent_attempt_ledger": True,
        },
        "persistence": {
            "writer_workers": SNAPSHOT_STORE_WRITER_WORKERS,
            "single_writer_required": True,
        },
        "secrets_exposed": False,
        "policy": "automatic learning is authorized only when every provider, persistence, worker and security gate is ready",
    }
    report["activation_plan"] = build_automatic_learning_activation_plan(report)
    report["activation_authorized"] = bool(
        report["ready"] is True
        and report["activation_plan"]["audit"]["valid"] is True
        and report["activation_plan"]["activation_allowed"] is True
    )
    return report


def automatic_learning_activation_authorized(readiness: Any) -> bool:
    row = readiness if isinstance(readiness, dict) else {}
    plan = row.get("activation_plan") if isinstance(row.get("activation_plan"), dict) else {}
    audit = audit_automatic_learning_activation_plan(plan)
    return bool(
        row.get("ready") is True
        and not row.get("blockers")
        and plan.get("activation_allowed") is True
        and audit.get("valid") is True
    )


LEARNING_RUN_HASH_FIELDS = (
    "run_id", "mode", "started_at", "postmatch_plan_generated_at", "prematch_plan_generated_at",
    "execution_order", "settlement_results", "postmatch_fact_results",
    "market_language_postmatch_fact_results",
    "postmatch_review_draft_results", "review_completion_results", "quality_card_results",
    "forward_validation_evidence_results", "promotion_candidate_results", "research_proposal_results",
    "hypothesis_registration_results", "learning_admission_results", "freeze_results", "learning_priority_board",
    "market_language_observation_results", "shadow_lock_results", "forward_validation_queue",
    "automatic_hypothesis_registration", "automatic_hypothesis_registration_policy",
    "automatic_champion_change", "automatic_promotion_candidate_creation",
    "explicit_user_confirmation_required_for_champion", "result_backfit_allowed",
)


def learning_run_hash_content(record: Dict[str, Any]) -> Dict[str, Any]:
    record = record if isinstance(record, dict) else {}
    return {field: record.get(field) for field in LEARNING_RUN_HASH_FIELDS}


def learning_run_record_integrity(record: Any) -> Dict[str, Any]:
    record = record if isinstance(record, dict) else {}
    supplied = str(record.get("run_hash") or "")
    expected = _content_hash(learning_run_hash_content(record)) if record else ""
    hash_valid = bool(
        supplied and len(supplied) == 64
        and hmac.compare_digest(supplied, expected)
    )
    immutable = record.get("immutable") is True
    return {
        "valid": bool(record and immutable and hash_valid),
        "immutable": immutable,
        "hash_valid": hash_valid,
        "expected_run_hash": expected or None,
    }


def automatic_learning_schedule_health(
    now_ts: Optional[int] = None,
    store_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Audit today's persisted 14:30 run instead of trusting in-memory worker state."""
    now_ts = int(now_ts or time.time())
    local_now = datetime.fromtimestamp(now_ts, tz=timezone.utc).astimezone(ZoneInfo("Asia/Shanghai"))
    scheduled_local = local_now.replace(hour=14, minute=30, second=0, microsecond=0)
    scheduled_at = int(scheduled_local.astimezone(timezone.utc).timestamp())
    run_id = f"daily-{local_now.date().strftime('%Y%m%d')}-1430"
    grace_seconds = max(AUTO_SNAPSHOT_POLL_SECONDS * 2, 600)
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    record = (store.get("learning_runs") or {}).get(run_id)
    run_integrity = learning_run_record_integrity(record)
    run_hash_valid = run_integrity["hash_valid"]
    record_valid = bool(
        isinstance(record, dict)
        and run_integrity["valid"] is True
        and record.get("execution_order") == ["past_36h_postmatch", "future_24h_prematch"]
        and int(record.get("started_at") or 0) >= scheduled_at
        and int(record.get("started_at") or 0) <= now_ts + 300
    )
    if record_valid:
        started_at = int(record.get("started_at") or 0)
        return {
            "status": "completed", "healthy": True, "run_id": run_id,
            "scheduled_at": scheduled_at, "started_at": started_at,
            "trigger_delay_seconds": max(0, started_at - scheduled_at),
            "run_hash": record.get("run_hash"), "grace_seconds": grace_seconds,
            "persisted_evidence": True, "run_hash_valid": True,
        }
    if isinstance(record, dict):
        return {
            "status": "invalid_persisted_run", "healthy": False, "run_id": run_id,
            "scheduled_at": scheduled_at, "grace_seconds": grace_seconds,
            "persisted_evidence": False, "run_hash_valid": run_hash_valid,
        }
    seconds_until_due = scheduled_at - now_ts
    if seconds_until_due > 0:
        status, healthy = "not_due", True
    elif now_ts - scheduled_at <= grace_seconds:
        status, healthy = "awaiting_worker_poll", True
    else:
        status, healthy = "overdue", False
    return {
        "status": status, "healthy": healthy, "run_id": run_id,
        "scheduled_at": scheduled_at, "grace_seconds": grace_seconds,
        "seconds_until_due": max(0, seconds_until_due),
        "overdue_seconds": max(0, now_ts - scheduled_at - grace_seconds),
        "persisted_evidence": False, "run_hash_valid": False,
    }


def learning_status_report(now_ts: Optional[int] = None) -> Dict[str, Any]:
    store = load_snapshot_store()
    admissions = store.get("learning_admissions") or {}
    frozen = store.get("learning_frozen") or {}
    postmatches = store.get("learning_postmatch") or {}
    postmatch_facts = store.get("learning_postmatch_facts") or {}
    postmatch_drafts = store.get("learning_postmatch_drafts") or {}
    research_proposals = store.get("learning_research_proposals") or {}
    quality_cards = store.get("learning_quality_cards") or {}
    runs = store.get("learning_runs") or {}
    hypotheses = store.get("learning_hypotheses") or {}
    hypothesis_templates = store.get("learning_hypothesis_templates") or {}
    shadow_locks = store.get("learning_shadow_locks") or {}
    promotions = store.get("learning_promotions") or {}
    league_dna_candidates = store.get("league_dna_candidates") or {}
    league_dna_activations = store.get("league_dna_activation_candidates") or {}
    verified_active_dna = [
        row for row in (store.get("league_dna_active") or {}).values()
        if row.get("status") == "VERIFIED_ACTIVE"
        and row.get("evidence_confidence") == 100
        and get_nested(row, ["user_confirmation", "confirmed"]) is True
    ]
    frozen_rows = [row for rows in frozen.values() for row in (rows or [])]
    postmatch_rows = list(postmatches.values())
    implementation_gap_count = sum(get_nested(row, ["review", "learning_disposition", "existing_rule_implementation_gap"]) is True for row in postmatch_rows)
    hypothesis_disposition_count = sum(get_nested(row, ["review", "learning_disposition", "new_theory_status"]) in LEARNING_HYPOTHESIS_TYPES for row in postmatch_rows)
    runtime_readiness = automatic_learning_runtime_readiness()
    schedule_health = automatic_learning_schedule_health(now_ts=now_ts, store_override=store)
    node_plan = learning_node_execution_plan(now_ts=now_ts, store_override=store)
    data_health = automatic_learning_data_health(now_ts=now_ts, store_override=store)
    registry = learning_major_league_registry_manifest()
    observation_frozen = store.get("market_language_frozen") or {}
    observation_settlements = store.get("market_language_settlements") or {}
    observation_facts = store.get("market_language_postmatch_facts") or {}
    observation_plan = market_language_settlement_plan(now_ts=now_ts, store_override=store)
    return {
        "version": VERSION,
        "admitted_fixture_count": len(admissions),
        "frozen_fixture_count": len(frozen),
        "frozen_version_count": len(frozen_rows),
        "postmatch_count": len(postmatches),
        "postmatch_fact_queue_count": len(postmatch_facts),
        "postmatch_review_draft_queue_count": len(postmatch_drafts),
        "postmatch_review_draft_version_count": sum(len(rows or []) for rows in postmatch_drafts.values()),
        "research_proposal_count": len(research_proposals),
        "research_proposal_version_count": sum(len(rows or []) for rows in research_proposals.values()),
        "learning_quality_card_count": len(quality_cards),
        "postmatch_fact_pending_verification_count": sum(
            bool(rows) and max(rows, key=lambda row: int(row.get("version_number") or 0)).get("verification", {}).get("settlement_eligible") is not True
            for rows in postmatch_facts.values()
        ),
        "cycle_run_count": len(runs),
        "postmatch_process_class_counts": {status: sum(row.get("process_classification") == status for row in postmatch_rows) for status in sorted(LEARNING_PROCESS_CLASSES)},
        "implementation_gap_count": implementation_gap_count,
        "single_match_hypothesis_disposition_count": hypothesis_disposition_count,
        "hypothesis_count": len(hypotheses),
        "hypothesis_template_count": len(hypothesis_templates),
        "shadow_validation_lock_count": sum(len(rows or {}) for rows in shadow_locks.values()),
        "hypothesis_status_counts": {status: sum(row.get("status") == status for row in hypotheses.values()) for status in (*LEARNING_HYPOTHESIS_TYPES, "SHADOW_VALIDATION", "PROMOTION_CANDIDATE")},
        "promotion_candidate_count": len(promotions),
        "league_dna_candidate_count": len(league_dna_candidates),
        "league_dna_activation_candidate_count": len(league_dna_activations),
        "league_dna_verified_active_count": len(verified_active_dna),
        "minimum_independent_support_samples": LEARNING_MIN_VALIDATION_SAMPLES,
        "schedule": {
            "timezone": "Asia/Shanghai",
            "daily_local_time": "14:30",
            "execution_order": ["past_36h_postmatch", "future_24h_prematch"],
        },
        "learning_prematch_stages": LEARNING_PREMATCH_STAGE_ORDER,
        "match_limit": None,
        "full_historical_odds_sample_limit": None,
        "automatic_champion_promotion": False,
        "automatic_learning_runtime": runtime_readiness,
        "automatic_learning_schedule": schedule_health,
        "major_league_registry": {
            "version": registry["version"],
            "registry_hash": registry["registry_hash"],
            "league_count": registry["league_count"],
        },
        "node_executor": {
            "due_count": node_plan["due_count"],
            "skipped_count": node_plan["skipped_count"],
            "discovery_performed": False,
            "scope_expansion_allowed": False,
        },
        "data_health": data_health,
        "market_language_observation": {
            "fixture_count": len(observation_frozen),
            "frozen_version_count": sum(len(rows or []) for rows in observation_frozen.values()),
            "postmatch_fact_version_count": sum(len(rows or []) for rows in observation_facts.values()),
            "settlement_count": len(observation_settlements),
            "settlement_ready_count": observation_plan["settlement_ready_count"],
            "model_learning_effect": False,
        },
        "policy": "single matches cannot create model rules; promotion candidates require all gates and explicit user confirmation",
    }


def operations_status_report(now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(now_ts or time.time())
    integrity = snapshot_store_integrity()
    thread_alive = bool(AUTO_SNAPSHOT_THREAD and AUTO_SNAPSHOT_THREAD.is_alive())
    worker_expected = AUTO_SNAPSHOT_ENABLED
    if not integrity.get("operational"):
        alerts = [{"severity": "critical", "code": "snapshot_store_unavailable"}]
        if integrity.get("manual_recovery_available"):
            alerts.append({
                "severity": "critical", "code": "snapshot_store_manual_recovery_available",
                "action": "use_protected_recovery_preview_then_exact_confirmation",
            })
        else:
            alerts.append({"severity": "critical", "code": "snapshot_store_backup_unavailable"})
        return {
            "version": VERSION, "generated_at": now_ts, "status": "blocked", "alerts": alerts,
            "store": integrity,
            "auto_snapshot_worker": {
                "enabled": worker_expected, "started": AUTO_SNAPSHOT_THREAD_STARTED,
                "alive": thread_alive, "last_cycle_at": AUTO_SNAPSHOT_LAST_CYCLE_AT,
                "last_cycle_age_seconds": None, "last_error": AUTO_SNAPSHOT_LAST_ERROR,
            },
            "auto_learning_cycle": {"status": "blocked", "reason": "snapshot_store_unavailable"},
            "automatic_learning_schedule": {"status": "blocked", "healthy": False, "reason": "snapshot_store_unavailable"},
            "automatic_learning_data_health": {"status": "critical", "reason": "snapshot_store_unavailable"},
            "external_fixture_freshness": {"fixture_count": 0, "state_counts": {}, "status": "data_missing"},
            "revalidation_queue": {"pending_count": None, "overdue_count": None, "status": "data_missing"},
            "calibration": {"settled_count": 0, "minimum_sample": CALIBRATION_MIN_SAMPLE, "sample_ready": False, "average_brier_score": None, "roi": None},
            "learning": {"status": "data_missing", "reason": "snapshot_store_unavailable"},
        }
    store = load_snapshot_store()
    fixtures = []
    for fixture, metadata in (store.get("external_prematch") or {}).items():
        fixtures.append({"fixture": fixture, "freshness": imported_fixture_freshness(metadata, get_fixture_snapshots(fixture), now_ts=now_ts)})
    freshness_counts = {state: sum(row["freshness"]["state"] == state for row in fixtures) for state in ("fresh", "stale", "historical", "invalid_timestamp", "data_missing")}
    queue = revalidation_queue_view(list((store.get("fundamental_revalidation_queue") or {}).values()), now_ts=now_ts)
    pending = [task for task in queue if task.get("status") == "pending"]
    overdue = [task for task in pending if task.get("overdue")]
    calibration = calibration_report()
    learning_schedule = automatic_learning_schedule_health(now_ts=now_ts, store_override=store)
    learning_data_health = automatic_learning_data_health(now_ts=now_ts, store_override=store)
    alerts = []
    if not integrity.get("operational"):
        alerts.append({"severity": "critical", "code": "snapshot_store_unavailable"})
    if integrity.get("operational") and not integrity.get("recovery_ready"):
        alerts.append({"severity": "warning", "code": "snapshot_backup_unavailable"})
    if not integrity.get("capacity_ok"):
        alerts.append({"severity": "warning", "code": "snapshot_store_capacity_warning"})
    if SNAPSHOT_STORE_WRITER_WORKERS != 1:
        alerts.append({
            "severity": "critical", "code": "snapshot_store_single_writer_required",
            "configured_writer_workers": SNAPSHOT_STORE_WRITER_WORKERS,
        })
    if worker_expected and not thread_alive:
        alerts.append({"severity": "critical", "code": "auto_snapshot_worker_not_running"})
    worker_cycle_age = now_ts - AUTO_SNAPSHOT_LAST_CYCLE_AT if AUTO_SNAPSHOT_LAST_CYCLE_AT is not None else None
    if worker_expected and thread_alive and worker_cycle_age is not None and worker_cycle_age > max(AUTO_SNAPSHOT_POLL_SECONDS * 2, 600):
        alerts.append({"severity": "warning", "code": "auto_snapshot_cycle_stale", "age_seconds": worker_cycle_age})
    if AUTO_SNAPSHOT_LAST_ERROR:
        alerts.append({"severity": "warning", "code": "auto_snapshot_last_cycle_failed", "error_type": AUTO_SNAPSHOT_LAST_ERROR})
    if isinstance(AUTO_LEARNING_LAST_RESULT, dict) and AUTO_LEARNING_LAST_RESULT.get("status") == "error":
        alerts.append({"severity": "warning", "code": "auto_learning_last_cycle_failed", "error_type": AUTO_LEARNING_LAST_RESULT.get("error_type")})
    if learning_schedule.get("status") in {"overdue", "invalid_persisted_run"}:
        alerts.append({
            "severity": "critical", "code": "auto_learning_daily_cycle_not_completed",
            "schedule_status": learning_schedule.get("status"),
            "run_id": learning_schedule.get("run_id"),
            "overdue_seconds": learning_schedule.get("overdue_seconds"),
        })
    if learning_data_health.get("expired_unfrozen_count", 0) > 0:
        alerts.append({
            "severity": "critical", "code": "learning_admission_expired_without_freeze",
            "count": learning_data_health.get("expired_unfrozen_count"),
        })
    if learning_data_health.get("overdue_node_count", 0) > 0:
        alerts.append({
            "severity": "warning", "code": "learning_node_execution_overdue",
            "count": learning_data_health.get("overdue_node_count"),
        })
    if learning_data_health.get("unresolved_failed_node_attempt_count", 0) > 0:
        alerts.append({
            "severity": "warning", "code": "learning_node_attempt_failed",
            "count": learning_data_health.get("unresolved_failed_node_attempt_count"),
        })
    if learning_data_health.get("stale_node_lease_count", 0) > 0:
        alerts.append({
            "severity": "warning", "code": "learning_node_lease_stale",
            "count": learning_data_health.get("stale_node_lease_count"),
        })
    if learning_data_health.get("invalid_learning_priority_board_count", 0) > 0:
        alerts.append({
            "severity": "critical", "code": "learning_priority_board_integrity_invalid",
            "count": learning_data_health.get("invalid_learning_priority_board_count"),
        })
    if learning_data_health.get("unranked_active_freeze_count", 0) > 0:
        alerts.append({
            "severity": "warning", "code": "learning_freeze_missing_priority_board",
            "count": learning_data_health.get("unranked_active_freeze_count"),
        })
    if learning_data_health.get("postmatch_overdue_count", 0) > 0:
        alerts.append({
            "severity": "warning", "code": "learning_postmatch_manual_recovery_required",
            "count": learning_data_health.get("postmatch_overdue_count"),
        })
    if overdue:
        alerts.append({"severity": "warning", "code": "overdue_fundamental_revalidation", "count": len(overdue)})
    unhealthy_freshness = freshness_counts["stale"] + freshness_counts["invalid_timestamp"]
    if unhealthy_freshness:
        alerts.append({"severity": "warning", "code": "external_fixture_freshness_issue", "count": unhealthy_freshness})
    if calibration["settled_count"] < CALIBRATION_MIN_SAMPLE:
        alerts.append({"severity": "info", "code": "calibration_sample_collecting", "current": calibration["settled_count"], "required": CALIBRATION_MIN_SAMPLE})
    severities = {alert["severity"] for alert in alerts}
    status = "blocked" if "critical" in severities else ("degraded" if "warning" in severities else "healthy")
    return {
        "version": VERSION, "generated_at": now_ts, "status": status, "alerts": alerts,
        "store": integrity,
        "auto_snapshot_worker": {"enabled": worker_expected, "started": AUTO_SNAPSHOT_THREAD_STARTED, "alive": thread_alive, "last_cycle_at": AUTO_SNAPSHOT_LAST_CYCLE_AT, "last_cycle_age_seconds": worker_cycle_age, "last_error": AUTO_SNAPSHOT_LAST_ERROR},
        "auto_learning_cycle": AUTO_LEARNING_LAST_RESULT or {"status": "not_run"},
        "automatic_learning_schedule": learning_schedule,
        "automatic_learning_data_health": learning_data_health,
        "external_fixture_freshness": {"fixture_count": len(fixtures), "state_counts": freshness_counts},
        "revalidation_queue": {"pending_count": len(pending), "overdue_count": len(overdue)},
        "calibration": {"settled_count": calibration["settled_count"], "minimum_sample": CALIBRATION_MIN_SAMPLE, "sample_ready": calibration["settled_count"] >= CALIBRATION_MIN_SAMPLE, "average_brier_score": calibration["average_brier_score"], "roi": calibration["roi"]},
        "learning": learning_status_report(now_ts=now_ts),
    }


def fixture_acceptance_summary(fixture_ids: Optional[List[Any]] = None, now_ts: Optional[int] = None) -> Dict[str, Any]:
    if fixture_ids is None:
        fixture_ids = list((load_snapshot_store().get("external_prematch") or {}).keys())
    reports = [fixture_readiness_report(fixture, now_ts=now_ts) for fixture in fixture_ids]
    counts = {
        status: sum(report.get("status") == status for report in reports)
        for status in ("not_ready", "shadow_ready", "decision_ready")
    }
    shadow_validated = counts["shadow_ready"] + counts["decision_ready"] > 0
    decision_validated = counts["decision_ready"] > 0
    status = (
        "decision_path_validated" if decision_validated else
        "shadow_path_validated" if shadow_validated else
        "awaiting_real_fixture" if not reports else
        "real_fixtures_not_ready"
    )
    return {
        "status": status,
        "fixture_count": len(reports),
        "status_counts": counts,
        "shadow_path_validated": shadow_validated,
        "decision_path_validated": decision_validated,
        "fixtures": [{
            "fixture": report.get("fixture"), "status": report.get("status"),
            "latest_stage": report.get("latest_stage"),
            "market_blockers": list(report.get("market_blockers") or [])[:20],
            "decision_blockers": list(report.get("decision_blockers") or [])[:20],
        } for report in reports[:50]],
        "fixtures_truncated": len(reports) > 50,
        "policy": "platform readiness and real-fixture end-to-end validation are reported separately",
    }


def release_candidate_self_test() -> Dict[str, Any]:
    safe_pass = decision_layer(empty_market_snapshot())
    safe_pass_audit = audit_decision_output(safe_pass)
    actionable_market = empty_market_snapshot()
    actionable_market["consensus_main_line"]["1x2"] = {
        "home": 2.0, "draw": 3.5, "away": 4.0,
        "bookmaker_count": max(2, MIN_CONSENSUS_BOOKMAKERS),
        "source": "complete_company_array", "dispersion_eligible": True,
    }
    actionable = decision_layer(
        actionable_market,
        {"1x2": {"home": .60, "draw": .23, "away": .17}},
        {"1x2": {"home": .90, "draw": .30, "away": .20}},
        crowding=.30, lineup_confidence=.90, death_path=[],
    )
    actionable_audit = audit_decision_output(actionable)
    estimator = {
        "uses_market_odds": False, "method": "release_self_test", "inputs": {},
        "expected_goals": {"home": 1.4, "away": 1.0}, "confidence": .8,
    }
    model = {
        "method": "independent_poisson", "model_hash": "release-self-test-model",
        "probabilities": {"1x2": {"home": .48, "draw": .28, "away": .24}},
    }
    fundamental_script = _fundamental_evaluation_script({"fixture": "release-self-test"}, estimator, model)
    fundamental_audit = audit_fundamental_chain(fundamental_script)
    fundamental_freeze = freeze_fundamental_stage(
        "release-self-test", fundamental_script, fundamental_audit, estimator, model, int(time.time())
    )
    fundamental_pipeline = audit_fundamental_first_pipeline(fundamental_freeze, fundamental_script)
    tampered_freeze = {**fundamental_freeze, "frozen_at": int(fundamental_freeze["frozen_at"]) + 1}
    tampered_pipeline = audit_fundamental_first_pipeline(tampered_freeze, fundamental_script)
    repricing = build_repricing_attribution(
        {"comparison_status": "compared", "classification": "Market-Only Move", "revalidation_trigger": {"triggered": True}},
        {"status": "proxy_available", "is_real_money": False},
        "Accepted Repricing",
    )
    priority_candidate = {
        "market": "1x2", "selection": "home", "price": 2.0,
        "script_coverage": .8, "edge": .05, "ev": .08,
    }
    priority_rows = [{
        "fixture": "release-priority-a", "correlation_group": "release-priority-a",
        "evaluation": {
            "fundamental_chain_audit": {"decision_eligible": True, "completeness_score": .9},
            "decision_layer": {"lineup_confidence": .9, "recommendation_tiers": {"first_choice_high_consistency": priority_candidate}},
        },
    }, {
        "fixture": "release-priority-b", "correlation_group": "release-priority-b",
        "evaluation": {
            "fundamental_chain_audit": {"decision_eligible": True, "completeness_score": .8},
            "decision_layer": {"lineup_confidence": .8, "recommendation_tiers": {"first_choice_high_consistency": priority_candidate}},
        },
    }]
    priority_portfolio = build_portfolio(priority_rows, 2)
    priority_board = build_portfolio_priority_board(priority_portfolio, _portfolio_priority_evidence(priority_rows), "T-1h", 1000)
    tampered_priority_board = copy.deepcopy(priority_board)
    tampered_priority_board["match_priority_ranking"][0]["rank"] = 99
    tampered_priority_board["board_hash"] = _content_hash({
        key: value for key, value in tampered_priority_board.items() if key not in {"board_hash", "immutable"}
    })
    learning_priority_board = build_learning_priority_board("release-learning-priority", [{
        "fixture": "release-learning-a", "freeze_id": "release-learning-a:v1", "content_hash": "release-freeze-hash",
        "captured_at": 900, "data_cutoff_at": 900, "kickoff_at": 1100,
        "analysis": {"analysis_node": "T-1h"},
        "decision": {
            "decision": "BET", "execution_action": "BET", "lineup_confidence": .9, "match_rating": "B+",
            "fundamental_chain_audit": {"decision_eligible": True, "completeness_score": .9},
            "selected_expression": priority_candidate,
        },
    }], 1000)
    tampered_learning_priority_board = copy.deepcopy(learning_priority_board)
    tampered_learning_priority_board["match_priority_ranking"][0]["execution_action"] = "WAIT"
    tampered_learning_priority_board["board_hash"] = _content_hash({
        key: value for key, value in tampered_learning_priority_board.items() if key not in {"board_hash", "immutable", "action"}
    })
    activation_plan = build_automatic_learning_activation_plan({
        "checks": {name: True for name in AUTOMATIC_LEARNING_REQUIRED_GATES},
    })
    tampered_activation_plan = copy.deepcopy(activation_plan)
    tampered_activation_plan["gates"][0]["owner"] = "unknown"
    tampered_activation_plan["plan_hash"] = _content_hash(
        _automatic_learning_activation_plan_hash_content(tampered_activation_plan)
    )
    ready_runtime = {
        "status": "ready", "ready": True, "blockers": [],
        "activation_plan": activation_plan,
    }
    blocked_runtime = copy.deepcopy(ready_runtime)
    blocked_runtime["ready"] = False
    blocked_runtime["blockers"] = ["persistent_backup_ready"]
    blocked_checks = {name: True for name in AUTOMATIC_LEARNING_REQUIRED_GATES}
    blocked_checks["persistent_backup_ready"] = False
    blocked_runtime["activation_plan"] = build_automatic_learning_activation_plan({"checks": blocked_checks})
    checks = {
        "missing_data_returns_pass": safe_pass.get("decision") == "PASS",
        "pass_output_contract_complete": safe_pass_audit.get("decision_eligible") is True,
        "eligible_data_returns_actionable": actionable.get("decision") != "PASS",
        "actionable_output_contract_complete": actionable_audit.get("decision_eligible") is True,
        "fundamental_first_freeze_hash_valid": fundamental_freeze.get("script_hash_valid") is True,
        "fundamental_first_pipeline_passes": fundamental_pipeline.get("status") == "passed",
        "fundamental_freeze_tamper_rejected": tampered_pipeline.get("status") == "failed",
        "portfolio_priority_board_tamper_rejected": (
            audit_portfolio_priority_board(tampered_priority_board).get("status") == "failed"
            and audit_portfolio_priority_board(tampered_priority_board).get("hash_valid") is True
        ),
        "learning_priority_board_tamper_rejected": (
            audit_learning_priority_board(tampered_learning_priority_board).get("status") == "failed"
            and audit_learning_priority_board(tampered_learning_priority_board).get("hash_valid") is True
        ),
        "activation_plan_semantic_tamper_rejected": (
            audit_automatic_learning_activation_plan(tampered_activation_plan).get("status") == "failed"
            and audit_automatic_learning_activation_plan(tampered_activation_plan).get("hash_valid") is True
        ),
        "automatic_learning_execution_gate_fail_closed": (
            automatic_learning_activation_authorized(ready_runtime) is True
            and automatic_learning_activation_authorized(blocked_runtime) is False
        ),
        "accepted_repricing_does_not_claim_causation": (
            repricing.get("status") == "market_move_cause_unverified"
            and repricing.get("market_response_is_cause_evidence") is False
            and repricing.get("source_attribution_verified") is False
        ),
    }
    return {
        "version": VERSION, "status": "passed" if all(checks.values()) else "failed",
        "passed": all(checks.values()), "checks": checks,
        "pass_reasons": safe_pass.get("pass_reasons"),
        "actionable_selection": actionable.get("decision"),
        "fundamental_first_pipeline": fundamental_pipeline,
        "repricing_attribution": repricing,
        "policy": "release requires safe PASS, complete actionable output, immutable fundamental-first execution and priority boards, a tamper-evident activation plan, and non-causal market-response language",
    }


def _release_revision_valid(value: Any) -> bool:
    revision = str(value or "").strip().lower()
    return 7 <= len(revision) <= 64 and all(character in "0123456789abcdef" for character in revision)


def _release_artifact_hash_content(manifest: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: manifest.get(key) for key in (
            "schema", "version", "release_channel", "source_revision",
            "rules_version", "required_contract_schemas", "required_release_endpoints", "self_test_hash",
        )
    }


def audit_release_artifact_manifest(manifest: Any, expected_revision: Optional[str] = None) -> Dict[str, Any]:
    row = manifest if isinstance(manifest, dict) else {}
    content = _release_artifact_hash_content(row)
    revision = str(row.get("source_revision") or "").strip().lower()
    revision_valid = _release_revision_valid(revision)
    expected = str(expected_revision or "").strip().lower() or None
    sensitive_names = {"api_key", "secret", "token", "password", "credential", "authorization", "cookie"}

    def sensitive_paths(value: Any, path: str = "") -> List[str]:
        found = []
        if isinstance(value, dict):
            for key, child in value.items():
                key_text = str(key).lower()
                child_path = f"{path}.{key}" if path else str(key)
                if key_text in sensitive_names or any(key_text.endswith(f"_{name}") for name in sensitive_names):
                    found.append(child_path)
                found.extend(sensitive_paths(child, child_path))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                found.extend(sensitive_paths(child, f"{path}[{index}]"))
        return found

    secret_paths = sensitive_paths(row)
    checks = {
        "schema_valid": row.get("schema") == "release_artifact_manifest_v1",
        "version_matches_runtime": row.get("version") == VERSION,
        "release_channel_matches_runtime": row.get("release_channel") == RELEASE_CHANNEL,
        "rules_version_matches_runtime": row.get("rules_version") == LEARNING_RULES_VERSION,
        "source_revision_bound": revision_valid,
        "source_revision_matches_expected": expected is None or revision == expected,
        "required_contracts_complete": set(row.get("required_contract_schemas") or []) == {
            "repricing_attribution_v1", "fundamental_first_freeze_v1",
            "fundamental_first_pipeline_audit_v1", "release_preflight_v1",
            "prematch_priority_vector_v1", "prematch_priority_board_v1",
            "learning_prematch_priority_board_v1", "release_artifact_manifest_v1",
            "automatic_learning_activation_plan_v1", "automatic_learning_execution_gate_v1",
        },
        "required_endpoints_bound": isinstance(row.get("required_release_endpoints"), list) and bool(row.get("required_release_endpoints")),
        "self_test_hash_bound": bool(row.get("self_test_hash")),
        "artifact_hash_valid": bool(row.get("artifact_hash")) and _content_hash(content) == row.get("artifact_hash"),
        "contains_no_sensitive_fields": not secret_paths,
    }
    blockers = [name for name, passed in checks.items() if not passed]
    return {
        "schema": "release_artifact_manifest_audit_v1",
        "status": "ready" if not blockers else "blocked",
        "deployment_artifact_ready": not blockers,
        "checks": checks, "blockers": blockers,
        "expected_artifact_hash": _content_hash(content),
        "observed_artifact_hash": row.get("artifact_hash"),
        "secrets_included": bool(secret_paths),
        "sensitive_field_paths": secret_paths,
        "policy": "deployment must use the exact version, source revision, endpoint set, contract set, and self-test result bound by the artifact hash",
    }


def build_release_artifact_manifest(
    self_test: Dict[str, Any],
    required_paths: Any,
    source_revision: Optional[str] = None,
) -> Dict[str, Any]:
    revision = str(source_revision if source_revision is not None else RELEASE_SOURCE_REVISION).strip().lower() or None
    manifest = {
        "schema": "release_artifact_manifest_v1",
        "version": VERSION,
        "release_channel": RELEASE_CHANNEL,
        "source_revision": revision,
        "rules_version": LEARNING_RULES_VERSION,
        "required_contract_schemas": sorted([
            "repricing_attribution_v1", "fundamental_first_freeze_v1",
            "fundamental_first_pipeline_audit_v1", "release_preflight_v1",
            "prematch_priority_vector_v1", "prematch_priority_board_v1",
            "learning_prematch_priority_board_v1", "release_artifact_manifest_v1",
            "automatic_learning_activation_plan_v1", "automatic_learning_execution_gate_v1",
        ]),
        "required_release_endpoints": sorted(str(path) for path in (required_paths or [])),
        "self_test_hash": _content_hash({"version": self_test.get("version"), "checks": self_test.get("checks")}),
    }
    manifest["artifact_hash"] = _content_hash(_release_artifact_hash_content(manifest))
    audit = audit_release_artifact_manifest(manifest, expected_revision=revision if revision else None)
    return {
        **manifest,
        "status": audit["status"],
        "deployment_artifact_ready": audit["deployment_artifact_ready"],
        "blockers": audit["blockers"],
        "audit": audit,
    }


def build_release_preflight(
    checks: Dict[str, Any],
    self_test: Dict[str, Any],
    automatic_learning_authorized: bool,
    artifact_manifest: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    code_checks = {
        "release_version_bound_to_self_test": self_test.get("version") == VERSION,
        "required_release_endpoints_present": checks.get("required_release_endpoints_present") is True,
        "internal_decision_contract_self_test": checks.get("internal_decision_contract_self_test") is True,
        "fundamental_first_contract": get_nested(self_test, ["checks", "fundamental_first_pipeline_passes"]) is True,
        "fundamental_freeze_tamper_rejected": get_nested(self_test, ["checks", "fundamental_freeze_tamper_rejected"]) is True,
        "portfolio_priority_board_tamper_rejected": get_nested(self_test, ["checks", "portfolio_priority_board_tamper_rejected"]) is True,
        "learning_priority_board_tamper_rejected": get_nested(self_test, ["checks", "learning_priority_board_tamper_rejected"]) is True,
        "activation_plan_semantic_tamper_rejected": get_nested(self_test, ["checks", "activation_plan_semantic_tamper_rejected"]) is True,
        "automatic_learning_execution_gate_fail_closed": get_nested(self_test, ["checks", "automatic_learning_execution_gate_fail_closed"]) is True,
        "repricing_causation_contract": get_nested(self_test, ["checks", "accepted_repricing_does_not_claim_causation"]) is True,
        "opening_requires_verified_source": checks.get("opening_requires_verified_source") is True,
        "missing_history_never_backfilled": checks.get("missing_history_never_backfilled") is True,
        "pang_access_is_read_only": checks.get("pang_access_is_read_only") is True,
    }
    shadow_runtime_checks = {
        "persistent_store_operational": checks.get("persistent_store_operational") is True,
        "protected_api_configured": checks.get("protected_api_configured") is True,
        "operations_not_blocked": checks.get("operations_not_blocked") is True,
    }
    code_blockers = [name for name, passed in code_checks.items() if not passed]
    shadow_runtime_blockers = [name for name, passed in shadow_runtime_checks.items() if not passed]
    code_ready = not code_blockers
    artifact_audit = audit_release_artifact_manifest(artifact_manifest or {})
    deployment_artifact_ready = artifact_audit.get("deployment_artifact_ready") is True
    shadow_runtime_ready = code_ready and not shadow_runtime_blockers
    production_activation_ready = code_ready and deployment_artifact_ready and shadow_runtime_ready and automatic_learning_authorized
    status = (
        "production_activation_ready" if production_activation_ready else
        "code_ready_artifact_unbound" if code_ready and not deployment_artifact_ready else
        "shadow_runtime_ready_learning_blocked" if shadow_runtime_ready else
        "code_ready_runtime_blocked" if code_ready else
        "code_not_ready"
    )
    return {
        "schema": "release_preflight_v1", "version": VERSION, "status": status,
        "code_release_ready": code_ready,
        "deployment_artifact_ready": deployment_artifact_ready,
        "shadow_runtime_ready": shadow_runtime_ready,
        "automatic_learning_runtime_ready": bool(automatic_learning_authorized),
        "production_activation_ready": production_activation_ready,
        "code_checks": code_checks, "code_blockers": code_blockers,
        "artifact_manifest_audit": artifact_audit,
        "shadow_runtime_checks": shadow_runtime_checks,
        "shadow_runtime_blockers": shadow_runtime_blockers,
        "external_runtime_gaps_do_not_fail_code_release": True,
        "policy": "artifact readiness, shadow runtime readiness, and unattended learning activation are independent gates; no lower gate overrides a higher one",
    }


def release_acceptance_report(now_ts: Optional[int] = None) -> Dict[str, Any]:
    operations = operations_status_report(now_ts=now_ts)
    fixture_acceptance = fixture_acceptance_summary(now_ts=now_ts)
    self_test = release_candidate_self_test()
    automatic_learning = automatic_learning_runtime_readiness(
        store_integrity=operations.get("store"),
        worker_alive=get_nested(operations, ["auto_snapshot_worker", "alive"]),
    )
    automatic_learning_schedule = operations.get("automatic_learning_schedule")
    if not isinstance(automatic_learning_schedule, dict):
        automatic_learning_schedule = automatic_learning_schedule_health(now_ts=now_ts)
    automatic_learning_authorized = (
        automatic_learning_activation_authorized(automatic_learning)
        and automatic_learning_schedule.get("healthy") is True
        and operations.get("status") != "blocked"
        and get_nested(operations, ["automatic_learning_data_health", "status"]) != "critical"
    )
    available_paths = {route.path for route in app.routes}
    required_paths = {
        "/shadow/readiness/{fixture}", "/shadow/calibration/lock", "/shadow/calibration/settle",
        "/shadow/calibration/report", "/shadow/operations/status", "/shadow/import-prematch-packets",
        "/shadow/store-health", "/shadow/store-recovery-preview", "/shadow/store-recover",
        "/shadow/learning/registry", "/shadow/learning/activation-plan", "/shadow/learning/node-plan", "/shadow/learning/node-run",
        "/shadow/learning/priority-boards", "/shadow/learning/data-health", "/shadow/market-language/status",
        "/shadow/learning/postmatch-recovery", "/shadow/learning/collect-facts",
        "/shadow/market-language/collect-facts", "/shadow/market-language/settlement-plan",
        "/shadow/model/prematch-evaluate", "/shadow/release-acceptance",
    }
    checks = {
        "persistent_store_operational": operations["store"].get("operational") is True,
        "persistent_backup_ready": operations["store"].get("recovery_ready") is True,
        "protected_api_configured": bool(SHADOW_ACCESS_TOKEN),
        "operations_not_blocked": operations.get("status") != "blocked",
        "required_release_endpoints_present": required_paths.issubset(available_paths),
        "opening_requires_verified_source": True,
        "missing_history_never_backfilled": True,
        "nami_failure_is_optional": True,
        "pang_access_is_read_only": True,
        "real_fixture_shadow_path_validated": fixture_acceptance["shadow_path_validated"],
        "real_fixture_decision_path_validated": fixture_acceptance["decision_path_validated"],
        "internal_decision_contract_self_test": self_test["passed"],
    }
    blockers = [name for name in ("persistent_store_operational", "protected_api_configured", "operations_not_blocked", "required_release_endpoints_present", "internal_decision_contract_self_test") if not checks[name]]
    warnings = []
    if not checks["persistent_backup_ready"]:
        warnings.append("persistent_backup_not_ready")
    if not operations["calibration"].get("sample_ready"):
        warnings.append("calibration_minimum_sample_not_reached")
    if not checks["real_fixture_shadow_path_validated"]:
        warnings.append("real_fixture_shadow_path_not_yet_validated")
    if not automatic_learning_authorized:
        warnings.append("automatic_learning_runtime_not_ready")
    shadow_usable = not blockers
    controlled_decision_candidate = shadow_usable and operations["calibration"].get("sample_ready") is True
    operating_mode = "live_feed_shadow" if fixture_acceptance["shadow_path_validated"] else "manual_or_api_import_shadow"
    release_artifact_manifest = build_release_artifact_manifest(self_test, required_paths)
    release_preflight = build_release_preflight(
        checks, self_test, automatic_learning_authorized, release_artifact_manifest
    )
    return {
        "version": VERSION, "release_channel": RELEASE_CHANNEL,
        "status": "shadow_usable" if shadow_usable else "not_ready",
        "shadow_use_authorized": shadow_usable,
        "automatic_learning_runtime_authorized": automatic_learning_authorized,
        "real_money_use_authorized": False,
        "controlled_decision_candidate": controlled_decision_candidate,
        "usable_scope": {
            "system_usable": shadow_usable,
            "operating_mode": operating_mode,
            "manual_or_api_import_available": checks["required_release_endpoints_present"],
            "automatic_live_feed_validated": fixture_acceptance["shadow_path_validated"],
            "safe_pass_without_fresh_data": True,
            "approved_uses": ["prematch_shadow_analysis", "manual_or_api_packet_validation", "calibration_collection"],
            "not_authorized": ["real_money_betting", "automatic_recommendations_without_fresh_verified_data"],
        },
        "checks": checks, "blockers": blockers, "warnings": warnings,
        "release_artifact_manifest": release_artifact_manifest,
        "release_preflight": release_preflight,
        "operations_status": operations.get("status"),
        "calibration_sample": operations.get("calibration"),
        "fixture_acceptance": fixture_acceptance,
        "automatic_learning_runtime": automatic_learning,
        "automatic_learning_activation_plan": automatic_learning.get("activation_plan"),
        "automatic_learning_schedule": automatic_learning_schedule,
        "release_candidate_self_test": self_test,
        "per_fixture_gate": "/shadow/readiness/{fixture} must return decision_ready before any recommendation is considered",
        "remaining_external_gaps": [
            "verified opening/history coverage depends on upstream source availability",
            "result utility and tactical evidence still require verified external or analyst inputs",
            "minimum settled calibration sample must be reached before controlled decision evaluation",
        ],
        "freeze_policy": "v1.30 freezes the shadow-use contract; subsequent changes require compatibility tests and explicit version notes",
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
        "real_money": {
            "registered_authority_count": len(REAL_MONEY_SOURCE_REGISTRY),
            "registered_authorities": sorted(REAL_MONEY_SOURCE_REGISTRY),
            "maximum_age_seconds": REAL_MONEY_MAX_AGE_SECONDS,
            "status": "configured" if REAL_MONEY_SOURCE_REGISTRY else "data_missing",
            "missing_policy": "odds paths remain Capital Pressure Proxy; real Money%/Bet%/turnover stay null",
        },
        "market_data_route": market_data_route_report(store_override=store),
    })


@app.get("/shadow/store-health")
def shadow_store_health(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "version": VERSION, "integrity": snapshot_store_integrity()})


@app.get("/shadow/store-recovery-preview")
def shadow_store_recovery_preview(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "version": VERSION, "recovery": snapshot_store_recovery_preview()})


@app.post("/shadow/store-recover")
async def shadow_store_recover(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="invalid_json_body")
    return JSONResponse({"ok": True, "version": VERSION, "recovery": restore_snapshot_store_from_backup(payload)})


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
    portfolio_run = save_portfolio_run(
        portfolio_id, fixtures, portfolio, max_legs, trigger_reasons, stage,
        _portfolio_change_drivers(rows), _portfolio_priority_evidence(rows),
    )
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
            "order": ["pure_fundamental_script", *FUNDAMENTAL_CHAIN, "market_timeline", "fundamental_revalidation", "model_probability_vs_market_no_vig_probability", "edge", "ev", "script_coverage", "crowding", "capital_pressure_proxy", "line_response", "market_acceptance", "expression_optimizer", "lineup_confidence", "death_path", "bet_wait_or_pass"],
            "missing_data_rule": "Missing historical checkpoints and facts remain data_missing; never backfill them from current odds.",
            "prematch_only": True, "line_move_is_not_edge": True,
        },
        "market_move_classes": MOVE_CLASSES, "decision_layer": decision,
    }


@app.get("/shadow/imported-prematch/{fixture}")
def shadow_imported_prematch(fixture: str, include_companies: bool = False, include_lineups: bool = False, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse(build_imported_ai_packet(fixture, include_companies=include_companies, include_lineups=include_lineups))


@app.get("/shadow/readiness/{fixture}")
def shadow_fixture_readiness(fixture: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "readiness": fixture_readiness_report(fixture)})


@app.post("/shadow/calibration/lock")
async def shadow_calibration_lock(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    record = lock_calibration_prediction(payload.get("fixture"), payload.get("probabilities") or {}, payload.get("recommendation"), payload.get("captured_at"))
    return JSONResponse({"ok": True, "record": record})


@app.post("/shadow/calibration/settle")
async def shadow_calibration_settle(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    settlement = settle_calibration_prediction(payload.get("fixture"), payload.get("home_goals"), payload.get("away_goals"), payload.get("settled_at"))
    return JSONResponse({"ok": True, "settlement": settlement})


@app.get("/shadow/calibration/report")
def shadow_calibration_report(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "calibration": calibration_report()})


@app.post("/shadow/learning/freeze")
async def shadow_learning_freeze(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = freeze_learning_sample(await request.json())
    return JSONResponse({"ok": True, "record": record})


@app.post("/shadow/market-language/freeze")
async def shadow_market_language_freeze(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = freeze_market_language_observation(await request.json())
    return JSONResponse({"ok": True, "record": record})


@app.post("/shadow/market-language/collect-facts")
async def shadow_market_language_collect_facts(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    record = collect_market_language_postmatch_facts(payload.get("observation_id"))
    return JSONResponse({"ok": True, "facts": record})


@app.get("/shadow/market-language/settlement-plan")
def shadow_market_language_settlement_plan(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "plan": market_language_settlement_plan(now_ts=now_ts)})


@app.post("/shadow/market-language/settle")
async def shadow_market_language_settle(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    record = settle_market_language_observation(payload.get("observation_id"), payload)
    return JSONResponse({"ok": True, "settlement": record})


@app.get("/shadow/market-language/status")
def shadow_market_language_status(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "status": market_language_observation_status_report(now_ts=now_ts)})


@app.get("/shadow/learning/registry")
def shadow_learning_registry(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "registry": learning_major_league_registry_manifest()})


@app.get("/shadow/learning/activation-plan")
def shadow_learning_activation_plan(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    readiness = automatic_learning_runtime_readiness()
    return JSONResponse({
        "ok": True, "version": VERSION,
        "readiness_status": readiness["status"],
        "activation_plan": readiness["activation_plan"],
    })


@app.get("/shadow/learning/node-plan")
def shadow_learning_node_plan(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "plan": learning_node_execution_plan(now_ts=now_ts)})


@app.post("/shadow/learning/node-run")
async def shadow_learning_node_run(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    if payload.get("apply") is True and payload.get("now_ts") is not None:
        raise HTTPException(status_code=422, detail="apply_node_run_cannot_override_server_time")
    return JSONResponse({"ok": True, "node_run": run_learning_node_executor(
        now_ts=payload.get("now_ts"), apply_changes=payload.get("apply", False),
    )})


@app.get("/shadow/learning/data-health")
def shadow_learning_data_health(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "data_health": automatic_learning_data_health(now_ts=now_ts)})


@app.get("/shadow/learning/postmatch-recovery")
def shadow_learning_postmatch_recovery(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "recovery": learning_postmatch_recovery_queue(now_ts=now_ts)})


@app.post("/shadow/learning/collect-facts")
async def shadow_learning_collect_facts(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    record = collect_learning_postmatch_facts(payload.get("freeze_id"))
    return JSONResponse({"ok": True, "facts": record})


@app.get("/shadow/learning/cycle-plan")
def shadow_learning_cycle_plan(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "plan": learning_cycle_plan(now_ts=now_ts)})


@app.post("/shadow/learning/run")
async def shadow_learning_run(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    body = await request.body()
    if len(body) > 256 * 1024:
        raise HTTPException(status_code=413, detail="request_body_too_large")
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="invalid_json_body") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    return JSONResponse({"ok": True, "run": run_learning_cycle(payload)})


@app.get("/shadow/learning/review-queue")
def shadow_learning_review_queue(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "review_queue": learning_review_queue()})


@app.post("/shadow/learning/review-draft")
async def shadow_learning_review_draft(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    draft = build_learning_postmatch_review_draft(payload.get("freeze_id"), payload.get("drafted_at"))
    return JSONResponse({"ok": True, "draft": draft})


@app.post("/shadow/learning/complete-review")
async def shadow_learning_complete_review(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    postmatch = complete_learning_postmatch_review(payload)
    return JSONResponse({"ok": True, "postmatch": postmatch})


@app.post("/shadow/learning/settle")
async def shadow_learning_settle(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    record = settle_learning_sample(payload.get("freeze_id"), payload.get("result"), payload.get("process_classification"), payload.get("event_audit"), payload.get("settled_at"), payload.get("review"), payload.get("fact_hash"))
    return JSONResponse({"ok": True, "postmatch": record})


@app.post("/shadow/learning/hypotheses")
async def shadow_learning_hypothesis(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = register_learning_hypothesis(await request.json())
    return JSONResponse({"ok": True, "hypothesis": record})


@app.post("/shadow/learning/hypothesis-templates")
async def shadow_learning_hypothesis_template(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = register_learning_hypothesis_template(await request.json())
    return JSONResponse({"ok": True, "hypothesis_template": record})


@app.get("/shadow/learning/hypothesis-templates")
def shadow_learning_hypothesis_templates(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "hypothesis_templates": learning_hypothesis_template_report()})


@app.get("/shadow/learning/validation-queue")
def shadow_learning_validation_queue(now_ts: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "validation_queue": learning_forward_validation_queue(now_ts=now_ts)})


@app.post("/shadow/learning/hypotheses/{hypothesis_id}/validation")
async def shadow_learning_validation(hypothesis_id: str, request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = record_hypothesis_validation(hypothesis_id, await request.json())
    return JSONResponse({"ok": True, "evidence": record})


@app.post("/shadow/learning/hypotheses/{hypothesis_id}/shadow-lock")
async def shadow_learning_hypothesis_lock(hypothesis_id: str, request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = lock_hypothesis_shadow_prediction(hypothesis_id, await request.json())
    return JSONResponse({"ok": True, "shadow_lock": record})


@app.get("/shadow/learning/hypotheses/{hypothesis_id}/promotion-evidence")
def shadow_learning_promotion_evidence(hypothesis_id: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "promotion_evidence": promotion_evidence_report(hypothesis_id)})


@app.post("/shadow/learning/hypotheses/{hypothesis_id}/promotion-candidate")
async def shadow_learning_promotion_candidate(hypothesis_id: str, request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    record = create_promotion_candidate(hypothesis_id, payload.get("gate_audit"))
    return JSONResponse({"ok": True, "promotion_candidate": record})


@app.post("/shadow/learning/league-dna")
async def shadow_learning_league_dna_candidate(request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = register_league_dna_candidate(await request.json())
    return JSONResponse({"ok": True, "league_dna_candidate": record})


@app.post("/shadow/learning/league-dna/{tag_id}/activation-candidate")
def shadow_learning_league_dna_activation_candidate(tag_id: str, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    record = create_league_dna_activation_candidate(tag_id)
    return JSONResponse({"ok": True, "activation_candidate": record})


@app.post("/shadow/learning/league-dna/{tag_id}/confirm")
async def shadow_learning_league_dna_confirm(tag_id: str, request: Request, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    payload = await request.json()
    if not isinstance(payload, dict):
        raise HTTPException(status_code=422, detail="json_body_must_be_an_object")
    record = confirm_league_dna_activation(tag_id, payload)
    return JSONResponse({"ok": True, "verified_active": record})


@app.get("/shadow/learning/league-dna/status")
def shadow_learning_league_dna_status(competition_id: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "league_dna": league_dna_status_report(competition_id)})


@app.get("/shadow/learning/selection-quality")
def shadow_learning_selection_quality(minimum_samples: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    if minimum_samples is not None and not 2 <= minimum_samples <= 10000:
        raise HTTPException(status_code=400, detail="minimum_samples_must_be_between_2_and_10000")
    return JSONResponse({"ok": True, "selection_quality": learning_selection_quality_report(minimum_samples)})


@app.get("/shadow/learning/quality-calibration")
def shadow_learning_quality_calibration(minimum_samples: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    if minimum_samples is not None and not 2 <= minimum_samples <= 10000:
        raise HTTPException(status_code=400, detail="minimum_samples_must_be_between_2_and_10000")
    return JSONResponse({"ok": True, "quality_calibration": learning_quality_calibration_report(minimum_samples)})


@app.get("/shadow/learning/priority-boards")
def shadow_learning_priority_boards(board_id: Optional[str] = None, limit: int = 20, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "priority_boards": learning_priority_board_report(board_id, limit)})


@app.post("/shadow/learning/quality-cards/refresh")
def shadow_learning_quality_cards_refresh(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "quality_cards": refresh_learning_quality_cards()})


@app.get("/shadow/learning/research-proposals")
def shadow_learning_research_proposals(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "research_proposals": learning_research_proposal_report()})


@app.post("/shadow/learning/research-proposals/refresh")
def shadow_learning_research_proposals_refresh(minimum_matches: Optional[int] = None, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    if minimum_matches is not None and not 3 <= minimum_matches <= 10000:
        raise HTTPException(status_code=400, detail="minimum_matches_must_be_between_3_and_10000")
    return JSONResponse({"ok": True, "research_proposals": refresh_learning_research_proposals(minimum_matches=minimum_matches)})


@app.get("/shadow/learning/status")
def shadow_learning_status(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_learning_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "learning": learning_status_report()})


@app.get("/shadow/operations/status")
def shadow_operations_status(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "operations": operations_status_report()})


@app.get("/shadow/release-acceptance")
def shadow_release_acceptance(token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    return JSONResponse({"ok": True, "release": release_acceptance_report()})



def build_shadow_ai_packet(fixture: int) -> Dict[str, Any]:
    """Build one AI-ready prematch packet from persisted market history + one current fundamentals fetch."""
    data = collect_prematch_data(fixture, include_raw=False)
    external_metadata = (load_snapshot_store().get("external_prematch") or {}).get(str(fixture)) or {}
    external_match = external_metadata.get("match") if isinstance(external_metadata.get("match"), dict) else {}
    external_provider_ids = external_metadata.get("provider_fixture_ids") if isinstance(external_metadata.get("provider_fixture_ids"), dict) else {}
    provider_identity = None
    if external_metadata.get("source") == "the_odds_api" and external_provider_ids.get("the_odds_api"):
        provider_identity_content = {
            "source": "the_odds_api",
            "sport_key": str(external_match.get("the_odds_api_sport_key") or "").strip().lower(),
            "event_id": str(external_provider_ids.get("the_odds_api") or "").strip().lower(),
            "home_team": str(external_match.get("home_team_name") or "").strip(),
            "away_team": str(external_match.get("away_team_name") or "").strip(),
        }
        if all(str(provider_identity_content.get(key) or "").strip() for key in ("sport_key", "event_id", "home_team", "away_team")):
            provider_identity = {**provider_identity_content, "source_hash": _content_hash(provider_identity_content)}
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
            "source_content_hash": row.get("source_content_hash") or _content_hash({
                "stage": row.get("stage"), "snapshot_at": row.get("snapshot_at"),
                "market_snapshot": row.get("market_snapshot"), "market_dynamics": row.get("market_dynamics"),
                "team_news_snapshot": row.get("team_news_snapshot"),
            }),
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
    available_timeline_rows = [row for row in timeline if row.get("status") == "available"]
    latest_timeline_row = max(
        available_timeline_rows,
        key=lambda row: (PREMATCH_STAGE_ORDER.index(row.get("stage")), int(row.get("snapshot_at") or 0)),
        default=None,
    )
    first = min(available_history, key=lambda row: PREMATCH_STAGE_ORDER.index(row.get("stage")), default=None)
    first_ah = line_from_primary(get_nested(first or {}, ["market_snapshot", "primary", "asian_handicap"]))
    latest_ah = line_from_primary(get_nested(latest or {}, ["market_snapshot", "primary", "asian_handicap"]))
    total_ah_move = latest_ah - first_ah if first_ah is not None and latest_ah is not None else None
    si = data.get("structured_inputs") or {}
    script = pure_fundamental_script(data)
    versions = get_fundamental_versions(fixture)
    generated_at = int(data.get("generated_at") or time.time())
    provider_identities = [provider_identity] if provider_identity else []
    thestats_identity = _resolve_thestats_prematch_identity(data.get("fixture") or {}, generated_at)
    if thestats_identity.get("ok") is True:
        provider_identities.append({key: value for key, value in thestats_identity.items() if key != "ok"})
    probability_replay = build_learning_probability_replay(
        fixture, si.get("independent_model_inputs"), generated_at=generated_at,
    )
    replay_model = probability_replay.get("model") if isinstance(probability_replay.get("model"), dict) else {}
    replay_inputs = probability_replay.get("inputs") if isinstance(probability_replay.get("inputs"), dict) else {}
    # Only a persisted, timing-audited snapshot may drive the frozen market decision.
    # The just-fetched current quote remains observable upstream but cannot silently
    # masquerade as Opening/T-12h/T-6h/T-1h evidence.
    decision_market = (latest or {}).get("market_snapshot") or empty_market_snapshot()
    decision = decision_layer(
        decision_market,
        replay_model.get("probabilities") if replay_model.get("ok") is True else None,
        None,
        None,
        as_float(replay_inputs.get("lineup_confidence")),
        None,
    )
    decision = apply_line_movement_gate(decision, history)
    chain_audit = audit_fundamental_chain(script, now_ts=generated_at)
    decision["fundamental_chain_audit"] = chain_audit
    decision["probability_replay_hash"] = probability_replay.get("replay_hash")
    decision["market_evidence_binding"] = {
        "source": "persisted_timing_audited_snapshot" if latest_timeline_row else "data_missing",
        "stage": (latest_timeline_row or {}).get("stage"),
        "snapshot_at": (latest_timeline_row or {}).get("snapshot_at"),
        "source_content_hash": (latest_timeline_row or {}).get("source_content_hash"),
        "current_unpersisted_quote_used": False,
    }
    if probability_replay.get("decision_eligible") is not True:
        force_pass_decision(decision, "learning_probability_replay_inputs_missing")
    if chain_audit.get("decision_eligible") is not True:
        force_pass_decision(decision, "fundamental_chain_insufficient")
    upstream_ok = bool(data.get("ok")) and bool(data.get("fixture"))
    return {
        "ok": upstream_ok,
        "status": "ready" if upstream_ok else "upstream_unavailable_or_data_missing",
        "version": VERSION,
        "generated_at": generated_at,
        "fixture": data.get("fixture"),
        "provider_identity": provider_identity,
        "provider_identities": provider_identities,
        "provider_identity_audit": {
            "the_odds_api": "bound" if provider_identity else "data_missing",
            "thestats": "bound" if thestats_identity.get("ok") is True else "data_missing",
            "thestats_reason": thestats_identity.get("error"),
        },
        "data_quality": data.get("data_quality"),
        "coverage": data.get("coverage"),
        "fundamentals": {
            "standings": si.get("standings"),
            "recent_form_last_10": si.get("recent_form_last_10"),
            "season_stats": si.get("season_stats"),
            "injuries": si.get("injuries"),
            "lineups_available": si.get("lineups_available"),
            "prediction": si.get("prediction"),
            "independent_model_inputs": si.get("independent_model_inputs"),
        },
        "probability_replay": probability_replay,
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
            "order": ["pure_fundamental_script", *FUNDAMENTAL_CHAIN, "market_timeline", "fundamental_revalidation", "model_probability_vs_market_no_vig_probability", "edge", "ev", "script_coverage", "crowding", "capital_pressure_proxy", "line_response", "market_acceptance", "expression_optimizer", "lineup_confidence", "death_path", "bet_wait_or_pass"],
            "missing_data_rule": "Any unavailable injuries, lineups, odds, standings or other inputs must be marked 数据缺失; never infer missing facts.",
            "prematch_only": True,
            "line_move_is_not_edge": True,
            "probability_source_rule": "builtin odds-independent venue-split standings calculator; full inputs and hashes must replay exactly",
            "market_binding_rule": "only persisted timing-audited snapshots may drive a frozen learning decision",
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
    evaluation_cutoff = int(time.time())
    kickoff_at = get_nested(latest or {}, ["fixture_info", "date"]) or get_nested(latest or {}, ["fixture_info", "kickoff_at"])
    real_money_audit = audit_real_money_data(
        payload.get("real_money_data"), expected_fixture=fixture,
        data_cutoff_at=evaluation_cutoff, kickoff_at=kickoff_at, now_ts=evaluation_cutoff,
    )
    result = decision_layer(
        current, payload.get("model_probabilities"), payload.get("script_coverage"),
        as_float(payload.get("crowding")), as_float(payload.get("lineup_confidence")), payload.get("death_path") if "death_path" in payload else None
    )
    result = apply_line_movement_gate(result, history, real_money_audit)
    if payload.get("real_money_data") not in (None, {}) and real_money_audit.get("status") == "rejected":
        force_pass_decision(result, "real_money_data_rejected")
    return JSONResponse({"ok": True, "version": VERSION, "fixture": fixture, "evaluation": result})


@app.get("/shadow/report", response_class=PlainTextResponse)
def shadow_report(fixture: int, token: Optional[str] = None, authorization: Optional[str] = Header(None), x_shadow_token: Optional[str] = Header(None)):
    require_shadow_token(resolve_shadow_token(token, authorization, x_shadow_token))
    data = collect_prematch_data(fixture, include_raw=False)
    fx, si = data.get("fixture", {}), data.get("structured_inputs", {})
    lines = [f"【比赛】{fx.get('home')} vs {fx.get('away')} / {fx.get('league')} / {fx.get('league_round')}", f"【状态】{fx.get('status')}  开赛时间UTC：{fx.get('date')}", f"【数据完整度】{data.get('data_quality')}", f"【近期状态】主队：{get_nested(si, ['recent_form_last_10', 'home'])}", f"【近期状态】客队：{get_nested(si, ['recent_form_last_10', 'away'])}", f"【积分】主队：{get_nested(si, ['standings', 'home'])}", f"【积分】客队：{get_nested(si, ['standings', 'away'])}", f"【赛季统计】主队：{get_nested(si, ['season_stats', 'home'])}", f"【赛季统计】客队：{get_nested(si, ['season_stats', 'away'])}", f"【伤停】{si.get('injuries')}", f"【预测】{si.get('prediction')}", f"【赔率摘要】{si.get('odds')}", f"【盘口快照】{si.get('odds_market_snapshot')}", f"【影子摘要】{data.get('shadow_summary')}", "【注意】这是数据摘要，不是最终投注建议；临场前必须按 T-24h → T-12h → T-6h → T-3h → T-1h → T-30m → Closing → FT 追踪刷新。"]
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
    if stage.get("key") == "Opening":
        return False
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


def the_odds_api_watchlist_health(
    now_ts: Optional[int] = None,
    store_override: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Return a compact, public-safe view of discovery and watchlist state."""
    now_ts = int(time.time()) if now_ts is None else int(now_ts)
    try:
        store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    except SnapshotStoreReadError:
        return {"status": "store_unavailable", "active_count": 0, "expired_count": 0, "latest_run": None}
    rows = [row for row in (store.get("the_odds_api_watchlist") or {}).values() if isinstance(row, dict)]
    archived_rows = [row for row in (store.get("the_odds_api_watchlist_archive") or {}).values() if isinstance(row, dict)]
    active_count = sum((_parse_timestamp(row.get("expires_at")) or 0) >= now_ts for row in rows)
    expired_count = len(rows) - active_count
    runs = [row for row in (store.get("the_odds_api_discovery_runs") or {}).values() if isinstance(row, dict)]
    latest = max(runs, key=lambda row: int(row.get("at") or 0), default=None)
    latest_summary = None if latest is None else {
        "run_id": latest.get("run_id"), "status": latest.get("status"),
        "at": latest.get("at"), "attempt_count": latest.get("attempt_count", 1),
        "discovered_count": latest.get("discovered_count", 0),
        "failed_request_count": latest.get("failed_request_count", 0),
        "next_retry_at": latest.get("next_retry_at"),
    }
    return {
        "status": "ready" if latest_summary else "awaiting_first_run",
        "active_count": active_count, "expired_count": expired_count,
        "archived_count": len(archived_rows),
        "archived_without_any_node_count": sum(not row.get("recorded_stages") for row in archived_rows),
        "latest_run": latest_summary,
        "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
    }


def discover_the_odds_api_watchlist(
    now: Optional[datetime] = None,
    store_override: Optional[Dict[str, Any]] = None,
    api_caller: Optional[Any] = None,
    persist: bool = True,
) -> Dict[str, Any]:
    """Discover the next 24h major-league events without admitting model samples.

    Discovery is idempotent per Shanghai calendar day after 14:30. It stores
    provider identity only; it neither creates a fundamental packet nor a
    betting direction, and it does not spend historical-odds requests.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    now_ts = int(now.timestamp())
    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    scheduled = local_now.replace(hour=14, minute=30, second=0, microsecond=0)
    run_id = f"odds-watchlist-{local_now.date().strftime('%Y%m%d')}-1430"
    if local_now < scheduled:
        return {"status": "not_due", "run_id": run_id, "at": now_ts, "discovered_count": 0}
    if not THE_ODDS_API_KEY:
        return {"status": "skipped", "reason": "the_odds_api_not_configured", "run_id": run_id, "at": now_ts, "discovered_count": 0}

    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    watchlist = store.setdefault("the_odds_api_watchlist", {})
    expired_fixtures = [
        fixture for fixture, row in watchlist.items()
        if not isinstance(row, dict) or (_parse_timestamp(row.get("expires_at")) or 0) < now_ts
    ]
    archive = store.setdefault("the_odds_api_watchlist_archive", {})
    fixture_rows = store.get("fixtures") or {}
    for fixture in expired_fixtures:
        row = watchlist.pop(fixture, None)
        history = [item for item in (fixture_rows.get(str(fixture)) or []) if isinstance(item, dict)]
        recorded_stages = sorted({item.get("stage") for item in history if item.get("stage") in LEARNING_PREMATCH_STAGE_ORDER})
        available_stages = sorted({item.get("stage") for item in history if item.get("stage") in LEARNING_PREMATCH_STAGE_ORDER and snapshot_stage_usable(item)})
        data_missing_stages = sorted({item.get("stage") for item in history if item.get("stage") in LEARNING_PREMATCH_STAGE_ORDER and item.get("import_status") == "data_missing"})
        archive[f"{fixture}:{now_ts}"] = {
            **(row if isinstance(row, dict) else {"fixture": str(fixture), "invalid_watchlist_row": True}),
            "archived_at": now_ts, "archive_reason": "watchlist_expired",
            "required_stages": list(LEARNING_PREMATCH_STAGE_ORDER),
            "recorded_stages": recorded_stages, "available_stages": available_stages,
            "data_missing_stages": data_missing_stages,
            "unobserved_stages": [stage for stage in LEARNING_PREMATCH_STAGE_ORDER if stage not in recorded_stages],
            "model_learning_effect": False,
        }
    if len(archive) > THE_ODDS_API_WATCHLIST_ARCHIVE_RETENTION:
        ordered_archive_keys = sorted(archive, key=lambda key: int((archive.get(key) or {}).get("archived_at") or 0))
        for archive_key in ordered_archive_keys[:-THE_ODDS_API_WATCHLIST_ARCHIVE_RETENTION]:
            archive.pop(archive_key, None)

    def persist_pruning_if_needed() -> None:
        if expired_fixtures and persist and store_override is None:
            store["version"] = VERSION
            write_snapshot_store(store)

    prior = (store.get("the_odds_api_discovery_runs") or {}).get(run_id)
    prior_status = str((prior or {}).get("status") or "completed") if isinstance(prior, dict) else None
    if isinstance(prior, dict) and prior_status == "completed":
        persist_pruning_if_needed()
        return {
            "status": "already_completed", "run_id": run_id, "at": now_ts,
            "discovered_count": int(prior.get("discovered_count") or 0),
            "request_count": int(prior.get("request_count") or 0),
            "archived_count": len(expired_fixtures), "pruned_count": len(expired_fixtures),
        }
    prior_attempt_count = int((prior or {}).get("attempt_count") or 0) if isinstance(prior, dict) else 0
    if isinstance(prior, dict) and prior_status == "degraded":
        next_retry_at = int(prior.get("next_retry_at") or 0)
        if prior_attempt_count >= THE_ODDS_API_DISCOVERY_MAX_ATTEMPTS:
            persist_pruning_if_needed()
            return {
                "status": "retry_exhausted", "run_id": run_id, "at": now_ts,
                "attempt_count": prior_attempt_count, "failed_request_count": prior.get("failed_request_count", 0),
                "archived_count": len(expired_fixtures), "pruned_count": len(expired_fixtures),
            }
        if now_ts < next_retry_at:
            persist_pruning_if_needed()
            return {
                "status": "retry_scheduled", "run_id": run_id, "at": now_ts,
                "attempt_count": prior_attempt_count, "next_retry_at": next_retry_at,
                "failed_request_count": prior.get("failed_request_count", 0),
                "archived_count": len(expired_fixtures), "pruned_count": len(expired_fixtures),
            }

    caller = api_caller or call_the_odds_api
    horizon_end = now + timedelta(hours=LEARNING_DISCOVERY_HORIZON_HOURS)
    params = {
        "dateFormat": "iso",
        "commenceTimeFrom": now.isoformat().replace("+00:00", "Z"),
        "commenceTimeTo": horizon_end.isoformat().replace("+00:00", "Z"),
    }
    request_audit: List[Dict[str, Any]] = []
    discovered_ids = []
    rejected_count = 0
    failed_sport_keys = set((prior or {}).get("failed_sport_keys") or []) if isinstance(prior, dict) else set()
    league_items = [
        (league_id, sport_key) for league_id, sport_key in THE_ODDS_API_MAJOR_LEAGUE_SPORT_KEYS.items()
        if not failed_sport_keys or sport_key in failed_sport_keys
    ]
    for league_id, sport_key in league_items:
        league = LEARNING_MAJOR_LEAGUES.get(league_id)
        if not league:
            rejected_count += 1
            continue
        response = caller(f"/sports/{sport_key}/events", params)
        rows = response.get("data") if isinstance(response, dict) and isinstance(response.get("data"), list) else []
        request_audit.append({
            "league_id": league_id, "sport_key": sport_key,
            "ok": bool(response.get("ok")) if isinstance(response, dict) else False,
            "status_code": response.get("status_code") if isinstance(response, dict) else None,
            "returned_count": len(rows),
        })
        if not isinstance(response, dict) or not response.get("ok"):
            continue
        for event in rows:
            if not isinstance(event, dict):
                rejected_count += 1
                continue
            event_id = str(event.get("id") or "").strip()
            kickoff_ts = _parse_timestamp(event.get("commence_time"))
            home_team = str(event.get("home_team") or "").strip()
            away_team = str(event.get("away_team") or "").strip()
            if (
                not event_id or len(event_id) > 200 or any(ord(char) < 32 for char in event_id)
                or not kickoff_ts or not (now_ts < kickoff_ts <= int(horizon_end.timestamp()))
                or not home_team or not away_team
            ):
                rejected_count += 1
                continue
            fixture = f"odds-watch-{event_id}"
            watchlist[fixture] = {
                "schema": "the_odds_api_watchlist_v1",
                "fixture": fixture,
                "source": "the_odds_api",
                "discovered_at": now_ts,
                "expires_at": kickoff_ts + 3 * 3600,
                "league_id": league_id,
                "league": league["name"],
                "country": league["country"],
                "provider_fixture_ids": {"the_odds_api": event_id},
                "match": {
                    "kickoff_utc": datetime.fromtimestamp(kickoff_ts, tz=timezone.utc).isoformat(),
                    "home_team_name": home_team, "away_team_name": away_team,
                    "the_odds_api_sport_key": sport_key,
                },
                "tracking_mode": "fixed_node_market_only",
                "learning_eligibility": "MODEL_LEARNING_EXCLUDED",
                "model_learning_effect": False,
                "direction_generation_allowed": False,
                "fundamentals_status": "data_missing",
                "admission_blockers": ["verified_fundamental_chain_missing", "provider_identity_not_reconciled"],
            }
            discovered_ids.append(fixture)

    current_failed_sport_keys = sorted(row["sport_key"] for row in request_audit if not row["ok"])
    failed_count = len(current_failed_sport_keys)
    attempt_count = prior_attempt_count + 1
    prior_fixtures = set((prior or {}).get("discovered_fixtures") or []) if isinstance(prior, dict) else set()
    all_discovered = sorted(prior_fixtures | set(discovered_ids))
    retry_seconds = THE_ODDS_API_DISCOVERY_RETRY_SECONDS * (2 ** max(0, attempt_count - 1))
    audit = {
        "schema": "the_odds_api_discovery_run_v1", "run_id": run_id,
        "status": "completed" if failed_count == 0 else "degraded",
        "at": now_ts, "horizon_end": int(horizon_end.timestamp()),
        "attempt_count": attempt_count,
        "request_count": int((prior or {}).get("request_count") or 0) + len(request_audit),
        "failed_request_count": failed_count, "failed_sport_keys": current_failed_sport_keys,
        "next_retry_at": now_ts + retry_seconds if failed_count and attempt_count < THE_ODDS_API_DISCOVERY_MAX_ATTEMPTS else None,
        "discovered_count": len(all_discovered), "rejected_count": int((prior or {}).get("rejected_count") or 0) + rejected_count,
        "discovered_fixtures": all_discovered, "request_audit": request_audit,
        "archived_count": len(expired_fixtures), "pruned_count": len(expired_fixtures),
        "historical_odds_requested": False, "model_learning_effect": False,
    }
    store.setdefault("the_odds_api_discovery_runs", {})[run_id] = audit
    if persist and store_override is None:
        store["version"] = VERSION
        write_snapshot_store(store)
    return audit


def auto_collect_the_odds_api_due_stages(
    now: Optional[datetime] = None,
    store_override: Optional[Dict[str, Any]] = None,
    collector: Optional[Any] = None,
) -> Dict[str, Any]:
    """Advance persisted The Odds API fixtures at due fixed timeline nodes.

    Recorded data_missing attempts are intentionally not retried automatically: a
    retry remains an explicit and auditable quota decision.
    """
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    if not THE_ODDS_API_KEY:
        return {"status": "skipped", "reason": "the_odds_api_not_configured", "due_count": 0, "collected_count": 0, "results": []}
    store = store_override if isinstance(store_override, dict) else load_snapshot_store()
    collect = collector or collect_the_odds_api_timeline
    results: List[Dict[str, Any]] = []
    due_count = 0
    watchlist_rows = store.get("the_odds_api_watchlist") or {}
    external_rows = store.get("external_prematch") or {}
    watchlist_keys = set(watchlist_rows)
    external_keys = set(external_rows)
    tracked = dict(watchlist_rows)
    tracked.update(external_rows)
    for fixture, metadata in tracked.items():
        if not isinstance(metadata, dict) or metadata.get("source") != "the_odds_api":
            continue
        match = metadata.get("match") if isinstance(metadata.get("match"), dict) else {}
        kickoff_ts = _parse_timestamp(match.get("kickoff_utc"))
        event_id = str(get_nested(metadata, ["provider_fixture_ids", "the_odds_api"], "") or "").strip()
        sport_key = str(match.get("the_odds_api_sport_key") or "").strip()
        home_team = str(match.get("home_team_name") or match.get("home") or "").strip()
        away_team = str(match.get("away_team_name") or match.get("away") or "").strip()
        if not kickoff_ts or kickoff_ts <= int(now.timestamp()) or not all((event_id, sport_key, home_team, away_team)):
            results.append({"fixture": str(fixture), "status": "skipped", "reason": "incomplete_or_non_prematch_provider_identity"})
            continue
        kickoff = datetime.fromtimestamp(kickoff_ts, tz=timezone.utc)
        recorded = {
            row.get("stage") for row in ((store.get("fixtures") or {}).get(str(fixture)) or [])
            if isinstance(row, dict) and row.get("stage") in PREMATCH_STAGE_ORDER
        }
        allowed_stage_keys = (
            set(LEARNING_PREMATCH_STAGE_ORDER)
            if fixture in watchlist_keys and fixture not in external_keys
            else set(PREMATCH_STAGE_ORDER)
        )
        due_stages = [
            stage["key"] for stage in TRACKING_STAGES
            if stage.get("key") not in {"Opening", "FT"}
            and stage.get("key") in allowed_stage_keys
            and stage.get("key") not in recorded
            and auto_snapshot_stage_due(now, kickoff, stage)
        ]
        if not due_stages:
            continue
        due_count += len(due_stages)
        payload = {
            "fixture": str(fixture), "sport_key": sport_key,
            "league": str(metadata.get("league") or ""),
            "home_team": home_team, "away_team": away_team,
            "home_aliases": list(match.get("home_aliases") or []),
            "away_aliases": list(match.get("away_aliases") or []),
            "kickoff_utc": match.get("kickoff_utc"),
            "requested_stages": due_stages, "only_missing": True,
            "retry_data_missing": False, "persist": True,
            "the_odds_api_event_id": event_id,
        }
        try:
            outcome = collect(payload)
            results.append({
                "fixture": str(fixture), "status": "collected" if outcome.get("ok") else "failed",
                "requested_stages": due_stages, "request_count": int(outcome.get("request_count") or 0),
                "persist_changed": bool(get_nested(outcome, ["persist_result", "changed"], False)),
            })
        except Exception as exc:
            results.append({"fixture": str(fixture), "status": "failed", "requested_stages": due_stages, "error_type": type(exc).__name__})
    collected_count = sum(row.get("status") == "collected" for row in results)
    failed_count = sum(row.get("status") == "failed" for row in results)
    return {
        "status": "completed" if due_count else "not_due", "at": int(now.timestamp()),
        "due_count": due_count, "collected_count": collected_count,
        "failed_count": failed_count, "results": results,
    }


def auto_learning_daily_cycle(
    now: datetime,
    fixture_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Run once after 14:30 Asia/Shanghai; a late worker poll catches up the same day."""
    now = now.astimezone(timezone.utc)
    local_now = now.astimezone(ZoneInfo("Asia/Shanghai"))
    now_ts = int(now.timestamp())
    scheduled_local = local_now.replace(hour=14, minute=30, second=0, microsecond=0)
    scheduled_at = int(scheduled_local.astimezone(timezone.utc).timestamp())
    trigger_delay_seconds = max(0, now_ts - scheduled_at)
    if local_now < scheduled_local:
        return {
            "status": "not_due", "at": now_ts,
            "schedule_timezone": "Asia/Shanghai", "scheduled_local_time": "14:30",
            "scheduled_at": scheduled_at,
        }
    run_id = f"daily-{local_now.date().strftime('%Y%m%d')}-1430"
    existing = (load_snapshot_store().get("learning_runs") or {}).get(run_id)
    if existing:
        if learning_run_record_integrity(existing).get("valid") is not True:
            return {
                "status": "error", "at": now_ts, "run_id": run_id,
                "scheduled_at": scheduled_at, "trigger_delay_seconds": trigger_delay_seconds,
                "error_type": "LearningRunIntegrityError",
                "reason": "persisted_daily_run_integrity_invalid",
            }
        return {
            "status": "already_completed", "at": now_ts, "run_id": run_id,
            "scheduled_at": scheduled_at, "trigger_delay_seconds": trigger_delay_seconds,
            "admitted_count": existing.get("admitted_count"),
            "frozen_count": existing.get("frozen_count"),
            "market_language_observation_frozen_count": existing.get("market_language_observation_frozen_count"),
            "market_language_postmatch_fact_collected_count": existing.get("market_language_postmatch_fact_collected_count"),
            "probability_replay_ready_count": existing.get("probability_replay_ready_count"),
            "probability_replay_missing_count": existing.get("probability_replay_missing_count"),
            "review_draft_count": existing.get("review_draft_count"),
            "research_proposal_version_count": existing.get("research_proposal_version_count"),
            "hypothesis_registered_count": existing.get("hypothesis_registered_count"),
            "forward_validation_evidence_recorded_count": existing.get("forward_validation_evidence_recorded_count"),
            "promotion_candidate_created_count": existing.get("promotion_candidate_created_count"),
        }
    # Never accept a caller-supplied readiness object here. The unattended writer
    # must re-evaluate its own live environment immediately before mutation.
    readiness = automatic_learning_runtime_readiness()
    if not automatic_learning_activation_authorized(readiness):
        plan = readiness.get("activation_plan") if isinstance(readiness.get("activation_plan"), dict) else {}
        return {
            "schema": "automatic_learning_execution_gate_v1",
            "target": "daily_1430_learning_cycle",
            "status": "blocked", "at": now_ts, "run_id": run_id,
            "scheduled_at": scheduled_at, "trigger_delay_seconds": trigger_delay_seconds,
            "reason": "automatic_learning_activation_gate_not_ready",
            "blockers": list(readiness.get("blockers") or []),
            "activation_plan_hash": plan.get("plan_hash"),
            "activation_plan_audit": get_nested(plan, ["audit", "status"]),
            "learning_run_persisted": False,
            "automatic_mutation_performed": False,
        }
    try:
        result = run_learning_cycle(
            {
                "apply": True, "run_id": run_id,
                "auto_prepare_prematch": True,
                "auto_collect_postmatch_facts": True,
                "auto_build_postmatch_review_drafts": True,
                "auto_refresh_research_proposals": True,
                "auto_register_preregistered_hypotheses": True,
                "auto_record_forward_validation": True,
                "auto_create_promotion_candidates": True,
            },
            now_ts=now_ts,
            fixture_rows=fixture_rows,
        )
        return {
            "status": "completed", "at": now_ts, "run_id": run_id,
            "scheduled_at": scheduled_at, "trigger_delay_seconds": trigger_delay_seconds,
            "execution_order": result.get("execution_order"),
            "admitted_count": result.get("admitted_count"),
            "frozen_count": result.get("frozen_count"),
            "market_language_observation_frozen_count": result.get("market_language_observation_frozen_count"),
            "market_language_postmatch_fact_collected_count": result.get("market_language_postmatch_fact_collected_count"),
            "probability_replay_ready_count": result.get("probability_replay_ready_count"),
            "probability_replay_missing_count": result.get("probability_replay_missing_count"),
            "review_draft_count": result.get("review_draft_count"),
            "quality_card_created_count": result.get("quality_card_created_count"),
            "shadow_lock_created_count": result.get("shadow_lock_created_count"),
            "research_proposal_version_count": result.get("research_proposal_version_count"),
            "hypothesis_registered_count": result.get("hypothesis_registered_count"),
            "forward_validation_evidence_recorded_count": result.get("forward_validation_evidence_recorded_count"),
            "promotion_candidate_created_count": result.get("promotion_candidate_created_count"),
            "settled_count": result.get("settled_count"),
            "rejected_count": result.get("rejected_count"),
            "postmatch_fact_results": result.get("postmatch_fact_results"),
            "market_language_postmatch_fact_results": result.get("market_language_postmatch_fact_results"),
            "freeze_results": result.get("freeze_results"),
            "postmatch_review_draft_results": result.get("postmatch_review_draft_results"),
            "review_completion_results": result.get("review_completion_results"),
            "quality_card_results": result.get("quality_card_results"),
            "shadow_lock_results": result.get("shadow_lock_results"),
            "research_proposal_results": result.get("research_proposal_results"),
            "hypothesis_registration_results": result.get("hypothesis_registration_results"),
            "forward_validation_evidence_results": result.get("forward_validation_evidence_results"),
            "promotion_candidate_results": result.get("promotion_candidate_results"),
            "forward_validation_queue": result.get("forward_validation_queue"),
            "automatic_hypothesis_registration": result.get("automatic_hypothesis_registration") is True,
            "automatic_hypothesis_registration_policy": "preregistered_templates_only",
            "automatic_champion_change": False,
            "automatic_promotion_candidate_creation": True,
            "explicit_user_confirmation_required_for_champion": True,
        }
    except Exception as exc:
        print("[AUTO_LEARNING] daily cycle failed: " + str(exc))
        return {
            "status": "error", "at": now_ts, "run_id": run_id,
            "scheduled_at": scheduled_at, "trigger_delay_seconds": trigger_delay_seconds,
            "error_type": type(exc).__name__,
        }


def auto_snapshot_cycle(now: Optional[datetime] = None) -> Dict[str, Any]:
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    learning_result = auto_learning_daily_cycle(now)
    the_odds_api_discovery = discover_the_odds_api_watchlist(now)
    the_odds_api_due_collection = auto_collect_the_odds_api_due_stages(now)
    api_football_authenticated = API_FOOTBALL_STARTUP_PROBE.get("authenticated") is True
    dates = sorted({(now + timedelta(days=i)).astimezone(ZoneInfo(AUTO_FETCH_TIMEZONE)).date().isoformat() for i in range(AUTO_SNAPSHOT_DAYS_AHEAD + 1)}) if api_football_authenticated else []
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
                            full_data = collect_prematch_data(int(fx["fixture_id"]), include_raw=False) if (not versions or trigger.get("triggered") or key in ("T-1h", "T-30m", "Closing")) else None
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
    # Re-evaluate immediately before the second mutation boundary; do not reuse
    # a readiness snapshot captured before provider collection or the daily run.
    runtime_readiness = automatic_learning_runtime_readiness()
    if automatic_learning_activation_authorized(runtime_readiness):
        node_result = run_learning_node_executor(now_ts=int(now.timestamp()), apply_changes=True)
    else:
        activation_plan = runtime_readiness.get("activation_plan") or {}
        node_result = {
            "schema": "automatic_learning_execution_gate_v1",
            "target": "fixed_node_learning_executor",
            "status": "blocked", "advanced_count": 0, "discovery_performed": False,
            "reason": "automatic_learning_activation_gate_not_ready",
            "blockers": list(runtime_readiness.get("blockers") or []),
            "activation_plan_hash": activation_plan.get("plan_hash"),
            "activation_plan_audit": get_nested(activation_plan, ["audit", "status"]),
            "automatic_mutation_performed": False,
        }
    api_football_due_collection = {
        "status": "completed" if api_football_authenticated else "skipped",
        "authenticated": api_football_authenticated,
        "reason": None if api_football_authenticated else "api_football_not_authenticated",
    }
    combined = {
        **learning_result, "node_execution": node_result,
        "the_odds_api_discovery": the_odds_api_discovery,
        "the_odds_api_due_collection": the_odds_api_due_collection,
        "api_football_due_collection": api_football_due_collection,
    }
    if learning_result.get("status") == "not_due" and node_result.get("advanced_count", 0) > 0:
        combined["status"] = "node_cycle_completed"
    return combined


def auto_snapshot_worker() -> None:
    global AUTO_SNAPSHOT_LAST_CYCLE_AT, AUTO_SNAPSHOT_LAST_ERROR, AUTO_RECONCILIATION_LAST_RESULT, AUTO_LEARNING_LAST_RESULT
    while True:
        try:
            try:
                AUTO_RECONCILIATION_LAST_RESULT = auto_provider_reconciliation_cycle()
            except Exception as exc:
                AUTO_RECONCILIATION_LAST_RESULT = {"status": "error", "error_type": type(exc).__name__, "at": int(time.time())}
                print("[AUTO_RECONCILIATION] failed: " + str(exc))
            learning_result = auto_snapshot_cycle()
            if learning_result.get("status") != "not_due":
                AUTO_LEARNING_LAST_RESULT = learning_result
            AUTO_SNAPSHOT_LAST_CYCLE_AT = int(time.time())
            AUTO_SNAPSHOT_LAST_ERROR = None
        except Exception as exc:
            AUTO_SNAPSHOT_LAST_CYCLE_AT = int(time.time())
            AUTO_SNAPSHOT_LAST_ERROR = type(exc).__name__
            print("[AUTO_SNAPSHOT] cycle failed: " + str(exc))
        time.sleep(AUTO_SNAPSHOT_POLL_SECONDS)


def auto_provider_reconciliation_cycle(now: Optional[datetime] = None) -> Dict[str, Any]:
    """Run provider reconciliation at most once per local calendar date."""
    now = now or datetime.now(timezone.utc)
    date_str = now.astimezone(ZoneInfo(AUTO_FETCH_TIMEZONE)).date().isoformat()
    store = load_snapshot_store()
    if not (store.get("external_prematch") or {}):
        return {"status": "skipped", "reason": "no_pang_fixtures_imported", "date": date_str, "at": int(now.timestamp())}
    previous = next((row for row in reversed(store.get("provider_reconciliation_audit") or []) if row.get("date") == date_str and int(row.get("matcher_schema_version") or 1) == PROVIDER_RECONCILIATION_SCHEMA_VERSION), None)
    if previous:
        return {"status": "skipped", "reason": "already_completed_for_date", "date": date_str, "previous": previous, "at": int(now.timestamp())}
    result = apply_provider_reconciliation(date_str)
    return {
        "status": "completed" if result.get("ok") else "degraded", "date": date_str,
        "applied_count": result["audit"]["applied_count"], "unchanged_count": result["audit"]["unchanged_count"],
        "rejected_count": result["audit"]["rejected_count"], "at": int(now.timestamp()),
    }


def start_auto_snapshot_worker() -> None:
    global AUTO_SNAPSHOT_THREAD_STARTED, AUTO_SNAPSHOT_THREAD
    if not AUTO_SNAPSHOT_ENABLED or AUTO_SNAPSHOT_THREAD_STARTED:
        return
    AUTO_SNAPSHOT_THREAD_STARTED = True
    AUTO_SNAPSHOT_THREAD = threading.Thread(target=auto_snapshot_worker, name="shadow-auto-snapshot", daemon=True)
    AUTO_SNAPSHOT_THREAD.start()
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
    prematch_stages = [x for x in TRACKING_STAGES if x["key"] not in ("Opening", "FT")]
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
    start_api_football_startup_probe()
    start_nami_odds_startup_probe()
    startup_ai_packet_selfcheck()
    live_check = sportradar_live_selfcheck()
    print("[SPORTRADAR_LIVE_SELFCHECK] " + json.dumps(live_check, ensure_ascii=False))
    print("[STARTUP] API-light mode enabled; no duplicate prematch/bootstrap fetches")
