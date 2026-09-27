import json
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
AUTO_FETCH_DATE = os.getenv("AUTO_FETCH_DATE", "2026-09-27")
AUTO_FETCH_TIMEZONE = os.getenv("AUTO_FETCH_TIMEZONE", "Asia/Shanghai")
AUTO_FETCH_FIXTURE_ID = os.getenv("AUTO_FETCH_FIXTURE_ID", "")

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")
ISPORTS_API_KEY = os.getenv("ISPORTS_API_KEY", "")
ISPORTS_BASE_URL = os.getenv("ISPORTS_BASE_URL", "http://isports.feijing88.com").rstrip("/")

app = FastAPI(
    title="Football Data Monitor",
    description="Railway service for pre-match football analysis data collection.",
    version="0.3.1",
)

LAST_PUSH_EVENTS: List[Dict[str, Any]] = []
LAST_PUSH_STATISTICS: List[Dict[str, Any]] = []
STARTUP_FIXTURES: Dict[str, Any] = {}
STARTUP_COLLECT: Dict[str, Any] = {}


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
        return {"ok": False, "error": "Missing API_FOOTBALL_KEY. Set it in Railway Variables."}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{API_FOOTBALL_BASE_URL}{path}"
    headers = {"x-apisports-key": API_FOOTBALL_KEY, "Accept": "application/json"}
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
        return {"ok": False, "error": "Missing THESTATS_API_KEY. Set it in Railway Variables."}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{THESTATS_BASE_URL}{path}"
    headers = {"Authorization": f"Bearer {THESTATS_API_KEY}", "Accept": "application/json"}
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
        return {"ok": False, "error": "Missing ISPORTS_API_KEY. Set it in Railway Variables."}
    if not path.startswith("/"):
        path = "/" + path
    final_params = dict(params or {})
    final_params["api_key"] = ISPORTS_API_KEY
    url = f"{ISPORTS_BASE_URL}{path}"
    try:
        resp = requests.get(url, params=final_params, timeout=REQUEST_TIMEOUT)
        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "request_url": mask_secret(resp.url),
            "data": safe_json_response(resp),
        }
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


def response_list(result: Dict[str, Any]) -> List[Any]:
    data = result.get("data") or {}
    if isinstance(data, dict) and isinstance(data.get("response"), list):
        return data["response"]
    return []


def response_first(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    rows = response_list(result)
    if rows and isinstance(rows[0], dict):
        return rows[0]
    return None


def compact_result(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "ok": result.get("ok"),
        "status_code": result.get("status_code"),
        "request_url": result.get("request_url"),
        "data": result.get("data"),
    }


def fixture_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    fixture = row.get("fixture", {}) or {}
    league = row.get("league", {}) or {}
    teams = row.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    status = fixture.get("status", {}) or {}
    goals = row.get("goals", {}) or {}
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league_id": league.get("id"),
        "league": league.get("name"),
        "country": league.get("country"),
        "season": league.get("season"),
        "home_id": home.get("id"),
        "home": home.get("name"),
        "away_id": away.get("id"),
        "away": away.get("name"),
        "status": status.get("short"),
        "elapsed": status.get("elapsed"),
        "goals": goals,
    }


def parse_fixture_context(fixture_id: int) -> Dict[str, Any]:
    fixture_detail = call_api_football("/fixtures", {"id": fixture_id})
    fixture_row = response_first(fixture_detail)
    if not fixture_row:
        return {
            "fixture_id": fixture_id,
            "fixture_detail": compact_result(fixture_detail),
            "error": "Fixture not found or API-Football did not return a response row.",
        }
    s = fixture_summary(fixture_row)
    return {"fixture_detail": compact_result(fixture_detail), "fixture_row": fixture_row, **s}


def collect_prematch_data(fixture_id: int, include_raw: bool = True) -> Dict[str, Any]:
    ctx = parse_fixture_context(fixture_id)
    if ctx.get("error"):
        return ctx
    home_id = ctx.get("home_id")
    away_id = ctx.get("away_id")
    league_id = ctx.get("league_id")
    season = ctx.get("season")

    calls: Dict[str, Dict[str, Any]] = {
        "predictions": call_api_football("/predictions", {"fixture": fixture_id}),
        "odds_prematch": call_api_football("/odds", {"fixture": fixture_id}),
        "injuries": call_api_football("/injuries", {"fixture": fixture_id}),
        "lineups": call_api_football("/fixtures/lineups", {"fixture": fixture_id}),
        "events": call_api_football("/fixtures/events", {"fixture": fixture_id}),
        "statistics": call_api_football("/fixtures/statistics", {"fixture": fixture_id}),
        "players": call_api_football("/fixtures/players", {"fixture": fixture_id}),
    }
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
        coverage[name] = {
            "ok": result.get("ok"),
            "status_code": result.get("status_code"),
            "results": data.get("results") if isinstance(data, dict) else None,
            "has_data": bool(response),
            "errors": data.get("errors") if isinstance(data, dict) else None,
            "request_url": result.get("request_url"),
        }

    output = {
        "ok": True,
        "generated_at": int(time.time()),
        "fixture": ctx,
        "coverage": coverage,
        "analysis_inputs": {
            "must_have": ["fixture_detail", "standings", "home_recent_10", "away_recent_10", "head_to_head_last_10", "home_team_season_stats", "away_team_season_stats"],
            "strong_if_available": ["odds_prematch", "injuries", "lineups", "predictions"],
            "mostly_live_or_post_match": ["events", "statistics", "players"],
        },
    }
    if include_raw:
        output["raw"] = {"fixture_detail": ctx.get("fixture_detail"), **{k: compact_result(v) for k, v in calls.items()}}
    return output


def log_json(label: str, payload: Any, max_chars: int = 12000) -> None:
    text = json.dumps(payload, ensure_ascii=False, default=str)
    if len(text) > max_chars:
        text = text[:max_chars] + "...TRUNCATED"
    print(f"[AUTO_PREMATCH] {label}: {text}", flush=True)


@app.on_event("startup")
def startup_auto_fetch() -> None:
    global STARTUP_FIXTURES, STARTUP_COLLECT
    if not API_FOOTBALL_KEY:
        print("[AUTO_PREMATCH] skipped: missing API_FOOTBALL_KEY", flush=True)
        return
    if AUTO_FETCH_DATE:
        result = call_api_football("/fixtures", {"date": AUTO_FETCH_DATE, "timezone": AUTO_FETCH_TIMEZONE})
        rows = response_list(result)
        summaries = [fixture_summary(r) for r in rows if isinstance(r, dict)]
        STARTUP_FIXTURES = {
            "date": AUTO_FETCH_DATE,
            "timezone": AUTO_FETCH_TIMEZONE,
            "count": len(summaries),
            "fixtures": summaries[:80],
        }
        log_json("fixtures", STARTUP_FIXTURES, 30000)
    if AUTO_FETCH_FIXTURE_ID:
        try:
            fixture_id = int(AUTO_FETCH_FIXTURE_ID)
            STARTUP_COLLECT = collect_prematch_data(fixture_id, include_raw=True)
            log_json("collect", STARTUP_COLLECT, 50000)
        except Exception as exc:
            log_json("collect_error", {"fixture_id": AUTO_FETCH_FIXTURE_ID, "error": str(exc)})


@app.get("/")
def root():
    return {
        "service": "football-data-monitor",
        "status": "running",
        "main_urls": ["/health", "/prematch/fixtures?date=YYYY-MM-DD", "/prematch/collect?fixture=FIXTURE_ID", "/debug/startup-fixtures", "/debug/startup-collect"],
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
        "version": "0.3.1",
        "auto_fetch_date": AUTO_FETCH_DATE,
        "auto_fetch_fixture_id": AUTO_FETCH_FIXTURE_ID,
    }


@app.get("/debug/startup-fixtures")
def debug_startup_fixtures():
    return STARTUP_FIXTURES


@app.get("/debug/startup-collect")
def debug_startup_collect():
    return STARTUP_COLLECT


@app.get("/prematch/fixtures")
def prematch_fixtures(date: str = Query(...), league: Optional[int] = None, season: Optional[int] = None, timezone: Optional[str] = "Asia/Shanghai"):
    params: Dict[str, Any] = {"date": date}
    if league is not None:
        params["league"] = league
    if season is not None:
        params["season"] = season
    if timezone:
        params["timezone"] = timezone
    return JSONResponse(call_api_football("/fixtures", params))


@app.get("/prematch/collect")
def prematch_collect(fixture: int, raw: bool = True):
    return JSONResponse(collect_prematch_data(fixture, include_raw=raw))


@app.get("/api-football/live")
def api_football_live():
    return JSONResponse(call_api_football("/fixtures", {"live": "all"}))


@app.get("/api-football/raw")
def api_football_raw(request: Request, path: str = Query(...)):
    params = dict(request.query_params)
    params.pop("path", None)
    return JSONResponse(call_api_football(path, params))


@app.get("/thestats/raw")
def thestats_raw(request: Request, path: str = Query(...)):
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
