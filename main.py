import os
import time
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# API-FOOTBALL by API-SPORTS
# Docs style: https://v3.football.api-sports.io/fixtures
API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")

# TheStatsAPI generic REST proxy
# Common style: Authorization: Bearer <key>
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")

# Optional legacy iSports support. Keep it here in case we add iSports later.
ISPORTS_API_KEY = os.getenv("ISPORTS_API_KEY", "")
ISPORTS_BASE_URL = os.getenv("ISPORTS_BASE_URL", "http://isports.feijing88.com").rstrip("/")

app = FastAPI(
    title="Football Data Monitor",
    description="Railway service for API-Football, TheStatsAPI, and future odds/live-stat integrations.",
    version="0.2.0",
)

LAST_PUSH_EVENTS = []
LAST_PUSH_STATISTICS = []


def mask_secret(text: str) -> str:
    for key in [API_FOOTBALL_KEY, THESTATS_API_KEY, ISPORTS_API_KEY]:
        if key:
            text = text.replace(key, "YOUR_API_KEY")
    return text


def safe_json_response(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text[:2000]}


def call_api_football(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not API_FOOTBALL_KEY:
        return {
            "ok": False,
            "error": "Missing API_FOOTBALL_KEY. Set it in Railway Variables.",
        }

    if not path.startswith("/"):
        path = "/" + path

    url = f"{API_FOOTBALL_BASE_URL}{path}"
    headers = {
        "x-apisports-key": API_FOOTBALL_KEY,
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "request_url": mask_secret(resp.url),
            "data": safe_json_response(resp),
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


def call_thestats(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not THESTATS_API_KEY:
        return {
            "ok": False,
            "error": "Missing THESTATS_API_KEY. Set it in Railway Variables.",
        }

    if not path.startswith("/"):
        path = "/" + path

    url = f"{THESTATS_BASE_URL}{path}"
    headers = {
        "Authorization": f"Bearer {THESTATS_API_KEY}",
        "Accept": "application/json",
    }

    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "request_url": mask_secret(resp.url),
            "data": safe_json_response(resp),
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


def call_isports(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not ISPORTS_API_KEY:
        return {
            "ok": False,
            "error": "Missing ISPORTS_API_KEY. Set it in Railway Variables.",
        }

    if not path.startswith("/"):
        path = "/" + path

    params = dict(params or {})
    params["api_key"] = ISPORTS_API_KEY
    url = f"{ISPORTS_BASE_URL}{path}"

    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "request_url": mask_secret(resp.url),
            "data": safe_json_response(resp),
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


@app.get("/")
def root():
    return {
        "service": "football-data-monitor",
        "status": "running",
        "main_test_urls": [
            "/health",
            "/api-football/live",
            "/api-football/fixtures?date=YYYY-MM-DD",
            "/api-football/events?fixture=FIXTURE_ID",
            "/api-football/statistics?fixture=FIXTURE_ID",
            "/thestats/raw?path=/YOUR_ENDPOINT",
        ],
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": int(time.time()),
        "api_football_base_url": API_FOOTBALL_BASE_URL,
        "thestats_base_url": THESTATS_BASE_URL,
        "isports_base_url": ISPORTS_BASE_URL,
        "has_api_football_key": bool(API_FOOTBALL_KEY),
        "has_thestats_key": bool(THESTATS_API_KEY),
        "has_isports_key": bool(ISPORTS_API_KEY),
    }


@app.get("/api-football/live")
def api_football_live():
    """API-Football live fixtures: scores + status; often includes live match context."""
    return JSONResponse(call_api_football("/fixtures", {"live": "all"}))


@app.get("/api-football/fixtures")
def api_football_fixtures(
    date: Optional[str] = None,
    league: Optional[int] = None,
    season: Optional[int] = None,
    status: Optional[str] = None,
    live: Optional[str] = None,
):
    """Search fixtures by date/league/season/status/live."""
    params: Dict[str, Any] = {}
    if date:
        params["date"] = date
    if league is not None:
        params["league"] = league
    if season is not None:
        params["season"] = season
    if status:
        params["status"] = status
    if live:
        params["live"] = live
    if not params:
        params["live"] = "all"
    return JSONResponse(call_api_football("/fixtures", params))


@app.get("/api-football/events")
def api_football_events(fixture: int):
    """Match timeline events: goals, cards, substitutions, etc."""
    return JSONResponse(call_api_football("/fixtures/events", {"fixture": fixture}))


@app.get("/api-football/statistics")
def api_football_statistics(fixture: int):
    """Match team statistics: shots, corners, possession, fouls, cards, etc."""
    return JSONResponse(call_api_football("/fixtures/statistics", {"fixture": fixture}))


@app.get("/api-football/raw")
def api_football_raw(request: Request, path: str = Query(..., description="Example: /fixtures")):
    """Generic API-Football test route. Example: /api-football/raw?path=/fixtures&live=all"""
    params = dict(request.query_params)
    params.pop("path", None)
    return JSONResponse(call_api_football(path, params))


@app.get("/thestats/raw")
def thestats_raw(request: Request, path: str = Query(..., description="Example: /football/matches/live")):
    """Generic TheStatsAPI test route. Use docs endpoint path after base URL."""
    params = dict(request.query_params)
    params.pop("path", None)
    return JSONResponse(call_thestats(path, params))


@app.get("/isports/odds-main")
def isports_odds_main():
    return JSONResponse(call_isports("/sport/football/odds/main"))


@app.get("/isports/odds-changes")
def isports_odds_changes():
    return JSONResponse(call_isports("/sport/football/odds/main/changes"))


@app.post("/push/events")
async def push_events(request: Request):
    payload = await request.json()
    record = {"received_at": int(time.time()), "payload": payload}
    LAST_PUSH_EVENTS.append(record)
    if len(LAST_PUSH_EVENTS) > 50:
        del LAST_PUSH_EVENTS[:-50]
    return {"ok": True, "received": "events", "count": len(LAST_PUSH_EVENTS)}


@app.post("/push/statistics")
async def push_statistics(request: Request):
    payload = await request.json()
    record = {"received_at": int(time.time()), "payload": payload}
    LAST_PUSH_STATISTICS.append(record)
    if len(LAST_PUSH_STATISTICS) > 50:
        del LAST_PUSH_STATISTICS[:-50]
    return {"ok": True, "received": "statistics", "count": len(LAST_PUSH_STATISTICS)}


@app.get("/debug/last-push-events")
def debug_last_push_events():
    return {"count": len(LAST_PUSH_EVENTS), "items": LAST_PUSH_EVENTS[-10:]}


@app.get("/debug/last-push-statistics")
def debug_last_push_statistics():
    return {"count": len(LAST_PUSH_STATISTICS), "items": LAST_PUSH_STATISTICS[-10:]}
