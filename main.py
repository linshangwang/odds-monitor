import json
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse
from dotenv import load_dotenv

load_dotenv()

VERSION = "0.5.0"
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
AUTO_FETCH_DATE = os.getenv("AUTO_FETCH_DATE", "2026-09-28")
AUTO_FETCH_TIMEZONE = os.getenv("AUTO_FETCH_TIMEZONE", "Asia/Shanghai")
AUTO_FETCH_FIXTURE_ID = os.getenv("AUTO_FETCH_FIXTURE_ID", "")
SHADOW_ACCESS_TOKEN = os.getenv("SHADOW_ACCESS_TOKEN", "")

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")
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
TARGET_LEAGUE_IDS_RAW = os.getenv("TARGET_LEAGUE_IDS", "")
if TARGET_LEAGUE_IDS_RAW.strip():
    TARGET_LEAGUE_IDS = {int(x.strip()) for x in TARGET_LEAGUE_IDS_RAW.split(",") if x.strip().isdigit()}
else:
    TARGET_LEAGUE_IDS = set(DEFAULT_TARGET_LEAGUES.keys())

app = FastAPI(
    title="Football Shadow Analysis Data Service",
    description="Target match filtering, prematch data collection, and shadow-analysis tracking pipeline.",
    version=VERSION,
)

LAST_PUSH_EVENTS: List[Dict[str, Any]] = []
LAST_PUSH_STATISTICS: List[Dict[str, Any]] = []
STARTUP_FIXTURES: Dict[str, Any] = {}
STARTUP_COLLECT: Dict[str, Any] = {}


def require_shadow_token(token: Optional[str]) -> None:
    if SHADOW_ACCESS_TOKEN and token != SHADOW_ACCESS_TOKEN:
        raise HTTPException(status_code=401, detail="Invalid or missing token")


def mask_secret(text: str) -> str:
    for key in [API_FOOTBALL_KEY, THESTATS_API_KEY, ISPORTS_API_KEY, SHADOW_ACCESS_TOKEN]:
        if key:
            text = text.replace(key, "YOUR_SECRET")
    return text


def safe_json_response(resp: requests.Response) -> Any:
    try:
        return resp.json()
    except Exception:
        return {"raw_text": resp.text[:2000]}


def call_api_football(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not API_FOOTBALL_KEY:
        return {"ok": False, "error": "Missing API_FOOTBALL_KEY"}
    if not path.startswith("/"):
        path = "/" + path
    url = f"{API_FOOTBALL_BASE_URL}{path}"
    headers = {"x-apisports-key": API_FOOTBALL_KEY, "Accept": "application/json"}
    try:
        resp = requests.get(url, params=params or {}, headers=headers, timeout=REQUEST_TIMEOUT)
        return {"ok": resp.ok, "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": safe_json_response(resp)}
    except requests.RequestException as exc:
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


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
        return {"ok": False, "error": str(exc), "request_url": mask_secret(url)}


def call_isports(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not ISPORTS_API_KEY:
        return {"ok": False, "error": "Missing ISPORTS_API_KEY"}
    if not path.startswith("/"):
        path = "/" + path
    final_params = dict(params or {})
    final_params["api_key"] = ISPORTS_API_KEY
    url = f"{ISPORTS_BASE_URL}{path}"
    try:
        resp = requests.get(url, params=final_params, timeout=REQUEST_TIMEOUT)
        return {"ok": resp.ok, "status_code": resp.status_code, "request_url": mask_secret(resp.url), "data": safe_json_response(resp)}
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
    data = result.get("data") or {}
    return {
        "ok": result.get("ok"),
        "status_code": result.get("status_code"),
        "request_url": result.get("request_url"),
        "results": data.get("results") if isinstance(data, dict) else None,
        "errors": data.get("errors") if isinstance(data, dict) else None,
        "response": data.get("response") if isinstance(data, dict) else data,
    }


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


def fixture_summary(row: Dict[str, Any]) -> Dict[str, Any]:
    fixture = row.get("fixture", {}) or {}
    league = row.get("league", {}) or {}
    teams = row.get("teams", {}) or {}
    home = teams.get("home", {}) or {}
    away = teams.get("away", {}) or {}
    status = fixture.get("status", {}) or {}
    goals = row.get("goals", {}) or {}
    league_id = league.get("id")
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "timestamp": fixture.get("timestamp"),
        "venue": fixture.get("venue"),
        "referee": fixture.get("referee"),
        "league_id": league_id,
        "league": league.get("name"),
        "league_round": league.get("round"),
        "league_target_name": DEFAULT_TARGET_LEAGUES.get(league_id),
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


def filter_target_rows(rows: List[Any]) -> List[Dict[str, Any]]:
    out = []
    for r in rows:
        if isinstance(r, dict) and get_nested(r, ["league", "id"]) in TARGET_LEAGUE_IDS:
            out.append(r)
    return out


def target_fixtures_for_date(date: str, timezone_name: str = "Asia/Shanghai") -> Dict[str, Any]:
    result = call_api_football("/fixtures", {"date": date, "timezone": timezone_name})
    rows = response_list(result)
    targets = filter_target_rows(rows)
    summaries = [fixture_summary(r) for r in targets]
    return {
        "ok": result.get("ok"),
        "date": date,
        "timezone": timezone_name,
        "mode": "target_major_leagues_only",
        "target_league_ids": sorted(TARGET_LEAGUE_IDS),
        "all_count": len(rows),
        "target_count": len(summaries),
        "fixtures": summaries,
        "source_status_code": result.get("status_code"),
    }


def parse_fixture_context(fixture_id: int) -> Dict[str, Any]:
    fixture_detail = call_api_football("/fixtures", {"id": fixture_id})
    fixture_row = response_first(fixture_detail)
    if not fixture_row:
        return {"fixture_id": fixture_id, "error": "fixture_not_found", "fixture_detail": compact_result(fixture_detail)}
    return {"fixture_detail": compact_result(fixture_detail), "fixture_row": fixture_row, **fixture_summary(fixture_row)}


def recent_form(rows: List[Any], team_id: Optional[int]) -> Dict[str, Any]:
    if not team_id:
        return {"available": False}
    summary = {"available": True, "played": 0, "wins": 0, "draws": 0, "losses": 0, "goals_for": 0, "goals_against": 0, "last_results": []}
    for row in rows[:10]:
        teams = row.get("teams", {}) or {}
        goals = row.get("goals", {}) or {}
        home_id = get_nested(teams, ["home", "id"])
        away_id = get_nested(teams, ["away", "id"])
        gh = goals.get("home")
        ga = goals.get("away")
        if gh is None or ga is None:
            continue
        if home_id == team_id:
            gf, gc = gh, ga
        elif away_id == team_id:
            gf, gc = ga, gh
        else:
            continue
        if gf > gc:
            res = "W"; summary["wins"] += 1
        elif gf < gc:
            res = "L"; summary["losses"] += 1
        else:
            res = "D"; summary["draws"] += 1
        summary["played"] += 1
        summary["goals_for"] += gf
        summary["goals_against"] += gc
        summary["last_results"].append(res)
    summary["goal_diff"] = summary["goals_for"] - summary["goals_against"]
    return summary


def standings_for_team(standings_result: Dict[str, Any], team_id: Optional[int]) -> Optional[Dict[str, Any]]:
    response = response_list(standings_result)
    groups = get_nested(response, [0, "league", "standings"], [])
    for group in groups:
        if not isinstance(group, list):
            continue
        for row in group:
            if get_nested(row, ["team", "id"]) == team_id:
                return {
                    "rank": row.get("rank"),
                    "team": get_nested(row, ["team", "name"]),
                    "points": row.get("points"),
                    "goalsDiff": row.get("goalsDiff"),
                    "form": row.get("form"),
                    "description": row.get("description"),
                    "all": row.get("all"),
                }
    return None


def season_stats_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    row = get_nested(result, ["data", "response"], {})
    if not isinstance(row, dict) or not row:
        return {"available": False}
    fixtures = row.get("fixtures", {}) or {}
    goals = row.get("goals", {}) or {}
    return {
        "available": True,
        "played_total": get_nested(fixtures, ["played", "total"]),
        "wins_total": get_nested(fixtures, ["wins", "total"]),
        "draws_total": get_nested(fixtures, ["draws", "total"]),
        "loses_total": get_nested(fixtures, ["loses", "total"]),
        "goals_for_avg": get_nested(goals, ["for", "average", "total"]),
        "goals_against_avg": get_nested(goals, ["against", "average", "total"]),
        "clean_sheet": (row.get("clean_sheet") or {}).get("total"),
        "failed_to_score": (row.get("failed_to_score") or {}).get("total"),
    }


def injuries_summary(result: Dict[str, Any], home_id: Optional[int], away_id: Optional[int]) -> Dict[str, Any]:
    rows = response_list(result)
    home, away = [], []
    for item in rows:
        team_id = get_nested(item, ["team", "id"])
        compact = {
            "team": get_nested(item, ["team", "name"]),
            "player": get_nested(item, ["player", "name"]),
            "reason": get_nested(item, ["player", "reason"]),
            "type": get_nested(item, ["player", "type"]),
        }
        if team_id == home_id:
            home.append(compact)
        elif team_id == away_id:
            away.append(compact)
    return {"home_count": len(home), "away_count": len(away), "home": home, "away": away}


def odds_summary(result: Dict[str, Any]) -> Dict[str, Any]:
    rows = response_list(result)
    if not rows:
        return {"available": False}
    bookmakers = rows[0].get("bookmakers", []) or []
    key_markets: List[Dict[str, Any]] = []
    wanted = {"Match Winner", "Asian Handicap", "Goals Over/Under", "Both Teams Score", "Double Chance"}
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
    return {
        "available": True,
        "winner": p.get("winner"),
        "win_or_draw": p.get("win_or_draw"),
        "advice": p.get("advice"),
        "percent": p.get("percent"),
        "comparison": row.get("comparison"),
    }


def data_quality(coverage: Dict[str, Any]) -> Dict[str, Any]:
    must = ["standings", "home_recent_10", "away_recent_10", "home_team_season_stats", "away_team_season_stats"]
    strong = ["odds_prematch", "predictions", "injuries", "lineups"]
    missing_must = [x for x in must if not coverage.get(x, {}).get("has_data")]
    missing_strong = [x for x in strong if not coverage.get(x, {}).get("has_data")]
    if not missing_must and coverage.get("odds_prematch", {}).get("has_data"):
        level = "high"
    elif len(missing_must) <= 1:
        level = "medium"
    else:
        level = "low"
    return {"level": level, "missing_must": missing_must, "missing_strong": missing_strong}


def collect_prematch_data(fixture_id: int, include_raw: bool = False) -> Dict[str, Any]:
    ctx = parse_fixture_context(fixture_id)
    if ctx.get("error"):
        return {"ok": False, **ctx}
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
            "request_url": result.get("request_url"),
        }

    structured = {
        "standings": {"home": standings_for_team(calls.get("standings", {}), home_id), "away": standings_for_team(calls.get("standings", {}), away_id)},
        "recent_form_last_10": {
            "home": recent_form(response_list(calls.get("home_recent_10", {})), home_id),
            "away": recent_form(response_list(calls.get("away_recent_10", {})), away_id),
        },
        "season_stats": {
            "home": season_stats_summary(calls.get("home_team_season_stats", {})),
            "away": season_stats_summary(calls.get("away_team_season_stats", {})),
        },
        "head_to_head_count": len(response_list(calls.get("head_to_head_last_10", {}))),
        "injuries": injuries_summary(calls.get("injuries", {}), home_id, away_id),
        "prediction": prediction_summary(calls.get("predictions", {})),
        "odds": odds_summary(calls.get("odds_prematch", {})),
        "lineups_available": coverage.get("lineups", {}).get("has_data", False),
    }
    quality = data_quality(coverage)
    return {
        "ok": True,
        "version": VERSION,
        "generated_at": int(time.time()),
        "fixture": {k: v for k, v in ctx.items() if k not in ["fixture_detail", "fixture_row"]},
        "coverage": coverage,
        "data_quality": quality,
        "structured_inputs": structured,
        "shadow_summary": make_shadow_summary(ctx, structured, quality),
        "football_ai_prompt": make_prompt(ctx),
        "raw_data_pack": {"fixture_detail": ctx.get("fixture_detail"), **{k: compact_result(v) for k, v in calls.items()}} if include_raw else None,
    }


def make_shadow_summary(ctx: Dict[str, Any], structured: Dict[str, Any], quality: Dict[str, Any]) -> Dict[str, Any]:
    notes: List[str] = []
    risks: List[str] = []
    home = ctx.get("home")
    away = ctx.get("away")
    hr = get_nested(structured, ["recent_form_last_10", "home"], {}) or {}
    ar = get_nested(structured, ["recent_form_last_10", "away"], {}) or {}
    if hr.get("available") and ar.get("available"):
        if hr.get("goal_diff", 0) > ar.get("goal_diff", 0):
            notes.append(f"近10场净胜球偏向 {home}")
        elif ar.get("goal_diff", 0) > hr.get("goal_diff", 0):
            notes.append(f"近10场净胜球偏向 {away}")
        if hr.get("losses", 0) > ar.get("losses", 0):
            notes.append(f"近期稳定性偏向 {away}")
        elif ar.get("losses", 0) > hr.get("losses", 0):
            notes.append(f"近期稳定性偏向 {home}")
    pred = structured.get("prediction", {}) or {}
    if pred.get("available"):
        notes.append(f"官方预测：{pred.get('advice')}，概率 {pred.get('percent')}")
    odds = structured.get("odds", {}) or {}
    if odds.get("available"):
        notes.append(f"赔率覆盖可用，博彩公司数量：{odds.get('bookmaker_count')}")
    if not structured.get("lineups_available"):
        risks.append("lineups_not_available_yet")
    if quality.get("missing_must"):
        risks.append("missing_must_data:" + ",".join(quality.get("missing_must", [])))
    return {"directional_notes": notes, "risk_flags": risks, "data_quality": quality.get("level")}


def make_prompt(ctx: Dict[str, Any]) -> str:
    return (
        "你是足球AI影子分析。请只基于下面 JSON 数据做赛前分析，不要凭空补充。\n"
        f"分析比赛：{ctx.get('home')} vs {ctx.get('away')}\n"
        "要求输出：基本面、近期状态、积分/战意、伤停阵容、赔率盘口、风险项、影子倾向。\n"
        "注意：如果某项数据缺失，必须写明缺失，不要臆测。"
    )


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


def tracking_plan_for_fixture(summary: Dict[str, Any]) -> Dict[str, Any]:
    dt = fixture_datetime_utc(summary)
    now = datetime.now(timezone.utc)
    if not dt:
        return {"fixture_id": summary.get("fixture_id"), "status": summary.get("status"), "tracking": []}
    checkpoints = [
        ("T-24h 初判", dt - timedelta(hours=24)),
        ("T-6h 赔率/盘口确认", dt - timedelta(hours=6)),
        ("T-1h 阵容伤停确认", dt - timedelta(hours=1)),
        ("T-15m 最终影子判断", dt - timedelta(minutes=15)),
        ("FT 赛后复盘", dt + timedelta(hours=2)),
    ]
    items = []
    for label, when in checkpoints:
        items.append({
            "stage": label,
            "utc_time": when.isoformat(),
            "status": "due_or_passed" if now >= when else "pending",
            "action_url": f"/shadow/snapshot?fixture={summary.get('fixture_id')}&stage={label}",
        })
    return {"fixture_id": summary.get("fixture_id"), "match": f"{summary.get('home')} vs {summary.get('away')}", "kickoff_utc": dt.isoformat(), "status": summary.get("status"), "tracking": items}


@app.get("/")
def root():
    return {"service": "football-shadow-data-service", "version": VERSION, "main_endpoints": ["/shadow/target-fixtures", "/shadow/analyze-fixture", "/shadow/tracking-plan", "/shadow/snapshot"]}


@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": int(time.time()),
        "version": VERSION,
        "api_football_base_url": API_FOOTBALL_BASE_URL,
        "thestats_base_url": THESTATS_BASE_URL,
        "isports_base_url": ISPORTS_BASE_URL,
        "has_api_football_key": bool(API_FOOTBALL_KEY),
        "has_thestats_key": bool(THESTATS_API_KEY),
        "has_isports_key": bool(ISPORTS_API_KEY),
        "shadow_token_enabled": bool(SHADOW_ACCESS_TOKEN),
        "auto_fetch_date": AUTO_FETCH_DATE,
        "auto_fetch_fixture_id": AUTO_FETCH_FIXTURE_ID,
        "target_leagues": {str(k): v for k, v in DEFAULT_TARGET_LEAGUES.items() if k in TARGET_LEAGUE_IDS},
    }


@app.get("/api-football/live")
def api_football_live():
    return JSONResponse(call_api_football("/fixtures", {"live": "all"}))


@app.get("/api-football/fixtures")
def api_football_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone")):
    return JSONResponse(call_api_football("/fixtures", {"date": date, "timezone": timezone_name}))


@app.get("/thestats/raw")
def thestats_raw(path: str, token: Optional[str] = None):
    require_shadow_token(token)
    return JSONResponse(call_thestats(path))


@app.get("/prematch/target-fixtures")
def prematch_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone")):
    return JSONResponse(target_fixtures_for_date(date, timezone_name))


@app.get("/shadow/target-fixtures")
def shadow_target_fixtures(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None):
    require_shadow_token(token)
    return JSONResponse(target_fixtures_for_date(date, timezone_name))


@app.get("/shadow/analyze-fixture")
def shadow_analyze_fixture(fixture: int, raw: bool = False, token: Optional[str] = None):
    require_shadow_token(token)
    return JSONResponse(collect_prematch_data(fixture, include_raw=raw))


@app.get("/shadow/analyze")
def shadow_analyze(date: str, max_games: int = 5, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None):
    require_shadow_token(token)
    fixtures = target_fixtures_for_date(date, timezone_name)
    rows = fixtures.get("fixtures", [])
    ns_rows = [x for x in rows if x.get("status") in ["NS", "TBD"]]
    selected = (ns_rows or rows)[:max_games]
    analyses = [collect_prematch_data(int(x["fixture_id"]), include_raw=False) for x in selected if x.get("fixture_id")]
    return JSONResponse({"ok": True, "date": date, "selected_count": len(selected), "fixtures": selected, "analyses": analyses})


@app.get("/shadow/tracking-plan")
def shadow_tracking_plan(date: str, timezone_name: str = Query("Asia/Shanghai", alias="timezone"), token: Optional[str] = None):
    require_shadow_token(token)
    fixtures = target_fixtures_for_date(date, timezone_name)
    plans = [tracking_plan_for_fixture(x) for x in fixtures.get("fixtures", [])]
    return JSONResponse({"ok": True, "version": VERSION, "date": date, "target_count": fixtures.get("target_count"), "plans": plans})


@app.get("/shadow/snapshot")
def shadow_snapshot(fixture: int, stage: str = "manual", raw: bool = False, token: Optional[str] = None):
    require_shadow_token(token)
    data = collect_prematch_data(fixture, include_raw=raw)
    payload = {"ok": True, "stage": stage, "snapshot_at": int(time.time()), "fixture": fixture, "data": data}
    print("[SHADOW_SNAPSHOT] " + json.dumps({"stage": stage, "fixture": fixture, "data_quality": data.get("data_quality"), "summary": data.get("shadow_summary")}, ensure_ascii=False))
    return JSONResponse(payload)


@app.get("/shadow/report", response_class=PlainTextResponse)
def shadow_report(fixture: int, token: Optional[str] = None):
    require_shadow_token(token)
    data = collect_prematch_data(fixture, include_raw=False)
    fx = data.get("fixture", {})
    si = data.get("structured_inputs", {})
    lines = [
        f"【比赛】{fx.get('home')} vs {fx.get('away')} / {fx.get('league')} / {fx.get('league_round')}",
        f"【状态】{fx.get('status')}  开赛时间UTC：{fx.get('date')}",
        f"【数据完整度】{data.get('data_quality')}",
        f"【近期状态】主队：{get_nested(si, ['recent_form_last_10', 'home'])}",
        f"【近期状态】客队：{get_nested(si, ['recent_form_last_10', 'away'])}",
        f"【积分】主队：{get_nested(si, ['standings', 'home'])}",
        f"【积分】客队：{get_nested(si, ['standings', 'away'])}",
        f"【赛季统计】主队：{get_nested(si, ['season_stats', 'home'])}",
        f"【赛季统计】客队：{get_nested(si, ['season_stats', 'away'])}",
        f"【伤停】{si.get('injuries')}",
        f"【预测】{si.get('prediction')}",
        f"【赔率摘要】{si.get('odds')}",
        f"【影子摘要】{data.get('shadow_summary')}",
        "【注意】这是数据摘要，不是最终投注建议；临场前必须再刷新阵容与盘口。",
    ]
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


@app.on_event("startup")
def startup_fetch():
    global STARTUP_FIXTURES, STARTUP_COLLECT
    try:
        STARTUP_FIXTURES = target_fixtures_for_date(AUTO_FETCH_DATE, AUTO_FETCH_TIMEZONE)
        print("[AUTO_PREMATCH] target_fixtures: " + json.dumps(STARTUP_FIXTURES, ensure_ascii=False)[:6000])
        if AUTO_FETCH_FIXTURE_ID.strip().isdigit():
            STARTUP_COLLECT = collect_prematch_data(int(AUTO_FETCH_FIXTURE_ID), include_raw=False)
            print("[AUTO_PREMATCH] shadow_collect_summary: " + json.dumps({
                "fixture": STARTUP_COLLECT.get("fixture"),
                "data_quality": STARTUP_COLLECT.get("data_quality"),
                "shadow_summary": STARTUP_COLLECT.get("shadow_summary"),
                "coverage": STARTUP_COLLECT.get("coverage"),
            }, ensure_ascii=False)[:6000])
    except Exception as exc:
        print("[AUTO_PREMATCH] startup fetch failed: " + str(exc))
