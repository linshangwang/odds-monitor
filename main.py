import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from fastapi import FastAPI, Request, Query, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
AUTO_FETCH_DATE = os.getenv("AUTO_FETCH_DATE", "2026-09-27")
AUTO_FETCH_TIMEZONE = os.getenv("AUTO_FETCH_TIMEZONE", "Asia/Shanghai")
AUTO_FETCH_FIXTURE_ID = os.getenv("AUTO_FETCH_FIXTURE_ID", "")
SHADOW_ACCESS_TOKEN = os.getenv("SHADOW_ACCESS_TOKEN", "")

API_FOOTBALL_KEY = os.getenv("API_FOOTBALL_KEY", "")
API_FOOTBALL_BASE_URL = os.getenv("API_FOOTBALL_BASE_URL", "https://v3.football.api-sports.io").rstrip("/")
THESTATS_API_KEY = os.getenv("THESTATS_API_KEY", "")
THESTATS_BASE_URL = os.getenv("THESTATS_BASE_URL", "https://api.thestatsapi.com/api").rstrip("/")
ISPORTS_API_KEY = os.getenv("ISPORTS_API_KEY", "")
ISPORTS_BASE_URL = os.getenv("ISPORTS_BASE_URL", "http://isports.feijing88.com").rstrip("/")

# 只保留甲级 / 一级联赛 + 大型赛事。可在 Railway Variables 用 TARGET_LEAGUE_IDS 覆盖。
# API-Football 常用 League IDs：2 UCL, 3 UEL, 5 Nations League, 39 EPL, 61 Ligue 1,
# 78 Bundesliga, 88 Eredivisie, 94 Primeira Liga, 135 Serie A, 140 La Liga,
# 144 Belgium Pro League, 203 Turkey Super Lig, 848 Conference League.
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
    title="Football Data Monitor",
    description="Railway service for football pre-match data and shadow analysis inputs.",
    version="0.4.0",
)

LAST_PUSH_EVENTS: List[Dict[str, Any]] = []
LAST_PUSH_STATISTICS: List[Dict[str, Any]] = []
STARTUP_FIXTURES: Dict[str, Any] = {}
STARTUP_COLLECT: Dict[str, Any] = {}


def require_shadow_token(token: Optional[str]) -> None:
    """If SHADOW_ACCESS_TOKEN is set in Railway, require it on shadow endpoints."""
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
    league_id = league.get("id")
    return {
        "fixture_id": fixture.get("id"),
        "date": fixture.get("date"),
        "league_id": league_id,
        "league": league.get("name"),
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


def is_target_fixture(row: Dict[str, Any]) -> bool:
    league = row.get("league", {}) or {}
    league_id = league.get("id")
    return league_id in TARGET_LEAGUE_IDS


def filter_target_fixtures(rows: List[Any]) -> List[Dict[str, Any]]:
    return [r for r in rows if isinstance(r, dict) and is_target_fixture(r)]


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


def recent_form(rows: List[Any], team_id: Optional[int]) -> Dict[str, Any]:
    if not team_id:
        return {"available": False}
    summary = {"available": True, "played": 0, "wins": 0, "draws": 0, "losses": 0, "goals_for": 0, "goals_against": 0, "last_results": []}
    for row in rows[:10]:
        if not isinstance(row, dict):
            continue
        teams = row.get("teams", {}) or {}
        goals = row.get("goals", {}) or {}
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}
        home_id = home.get("id")
        away_id = away.get("id")
        gh = goals.get("home")
        ga = goals.get("away")
        if gh is None or ga is None:
            continue
        is_home = home_id == team_id
        is_away = away_id == team_id
        if not (is_home or is_away):
            continue
        gf = gh if is_home else ga
        gc = ga if is_home else gh
        result = "D"
        if gf > gc:
            result = "W"
            summary["wins"] += 1
        elif gf < gc:
            result = "L"
            summary["losses"] += 1
        else:
            summary["draws"] += 1
        summary["played"] += 1
        summary["goals_for"] += gf
        summary["goals_against"] += gc
        summary["last_results"].append(result)
    summary["goal_diff"] = summary["goals_for"] - summary["goals_against"]
    return summary


def standings_for_team(standings_result: Dict[str, Any], team_id: Optional[int]) -> Optional[Dict[str, Any]]:
    if not team_id:
        return None
    response = response_list(standings_result)
    groups = get_nested(response, [0, "league", "standings"], [])
    for group in groups:
        if isinstance(group, list):
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


def season_stats_digest(stats_result: Dict[str, Any]) -> Dict[str, Any]:
    data = stats_result.get("data") or {}
    resp = data.get("response") if isinstance(data, dict) else None
    if not isinstance(resp, dict):
        return {"available": False}
    fixtures = resp.get("fixtures", {}) or {}
    goals = resp.get("goals", {}) or {}
    return {
        "available": True,
        "played_total": get_nested(fixtures, ["played", "total"]),
        "wins_total": get_nested(fixtures, ["wins", "total"]),
        "draws_total": get_nested(fixtures, ["draws", "total"]),
        "loses_total": get_nested(fixtures, ["loses", "total"]),
        "goals_for_avg": get_nested(goals, ["for", "average", "total"]),
        "goals_against_avg": get_nested(goals, ["against", "average", "total"]),
        "clean_sheet": get_nested(resp, ["clean_sheet", "total"]),
        "failed_to_score": get_nested(resp, ["failed_to_score", "total"]),
    }


def count_response(result: Dict[str, Any]) -> int:
    return len(response_list(result))


def first_prediction(predictions_result: Dict[str, Any]) -> Dict[str, Any]:
    row = response_first(predictions_result)
    if not row:
        return {"available": False}
    predictions = row.get("predictions", {}) or {}
    return {
        "available": True,
        "winner": predictions.get("winner"),
        "win_or_draw": predictions.get("win_or_draw"),
        "advice": predictions.get("advice"),
        "percent": predictions.get("percent"),
    }


def odds_digest(odds_result: Dict[str, Any]) -> Dict[str, Any]:
    rows = response_list(odds_result)
    if not rows:
        return {"available": False}
    bookmakers = get_nested(rows, [0, "bookmakers"], []) or []
    markets = []
    for bm in bookmakers[:5]:
        for bet in (bm.get("bets") or [])[:8]:
            markets.append({"bookmaker": bm.get("name"), "market": bet.get("name"), "values": bet.get("values")})
    return {"available": True, "bookmaker_count": len(bookmakers), "sample_markets": markets[:10]}


def build_shadow_analysis(pack: Dict[str, Any]) -> Dict[str, Any]:
    fixture = pack.get("fixture", {}) or {}
    raw = pack.get("raw", {}) or {}
    home_id = fixture.get("home_id")
    away_id = fixture.get("away_id")
    home_name = fixture.get("home")
    away_name = fixture.get("away")

    home_recent = recent_form(response_list(raw.get("home_recent_10", {})), home_id)
    away_recent = recent_form(response_list(raw.get("away_recent_10", {})), away_id)
    h2h_rows = response_list(raw.get("head_to_head_last_10", {}))

    standing_home = standings_for_team(raw.get("standings", {}), home_id)
    standing_away = standings_for_team(raw.get("standings", {}), away_id)
    home_season = season_stats_digest(raw.get("home_team_season_stats", {}))
    away_season = season_stats_digest(raw.get("away_team_season_stats", {}))
    prediction = first_prediction(raw.get("predictions", {}))
    odds = odds_digest(raw.get("odds_prematch", {}))

    injuries_home = []
    injuries_away = []
    for row in response_list(raw.get("injuries", {})):
        tid = get_nested(row, ["team", "id"])
        item = {"team": get_nested(row, ["team", "name"]), "player": get_nested(row, ["player", "name"]), "reason": row.get("reason"), "type": row.get("type")}
        if tid == home_id:
            injuries_home.append(item)
        elif tid == away_id:
            injuries_away.append(item)

    risk_flags = []
    coverage = pack.get("coverage", {}) or {}
    for k in ["standings", "home_recent_10", "away_recent_10", "home_team_season_stats", "away_team_season_stats"]:
        if not coverage.get(k, {}).get("has_data"):
            risk_flags.append(f"missing_{k}")
    if not coverage.get("odds_prematch", {}).get("has_data"):
        risk_flags.append("missing_odds")
    if not coverage.get("lineups", {}).get("has_data"):
        risk_flags.append("lineups_not_available_yet")
    if injuries_home or injuries_away:
        risk_flags.append("injury_info_available_check_manually")

    # 这是给影子分析使用的“数据倾向摘要”，不是投注建议。
    directional_notes = []
    if home_recent.get("available") and away_recent.get("available"):
        if home_recent.get("goal_diff", 0) - away_recent.get("goal_diff", 0) >= 5:
            directional_notes.append(f"近期状态差偏向 {home_name}")
        elif away_recent.get("goal_diff", 0) - home_recent.get("goal_diff", 0) >= 5:
            directional_notes.append(f"近期状态差偏向 {away_name}")
    if standing_home and standing_away:
        if (standing_home.get("points") or 0) - (standing_away.get("points") or 0) >= 6:
            directional_notes.append(f"积分基本面偏向 {home_name}")
        elif (standing_away.get("points") or 0) - (standing_home.get("points") or 0) >= 6:
            directional_notes.append(f"积分基本面偏向 {away_name}")
    if prediction.get("available") and prediction.get("advice"):
        directional_notes.append(f"官方预测提示：{prediction.get('advice')}")
    if not directional_notes:
        directional_notes.append("暂未形成明显单边数据倾向，需要结合盘口变化")

    ai_prompt = f"""你是足球AI影子分析。请只基于下面 JSON 数据做赛前分析，不要凭空补充。
分析比赛：{home_name} vs {away_name}
要求输出：基本面、近期状态、主客场/积分战意、伤停阵容、赔率盘口、风险项、影子倾向。
注意：如果某项数据缺失，必须写明缺失，不要臆测。"""

    return {
        "fixture": fixture,
        "coverage": coverage,
        "structured_inputs": {
            "standings": {"home": standing_home, "away": standing_away},
            "recent_form_last_10": {"home": home_recent, "away": away_recent},
            "season_stats": {"home": home_season, "away": away_season},
            "head_to_head_count": len(h2h_rows),
            "injuries": {"home_count": len(injuries_home), "away_count": len(injuries_away), "home": injuries_home[:10], "away": injuries_away[:10]},
            "prediction": prediction,
            "odds": odds,
        },
        "shadow_summary": {
            "directional_notes": directional_notes,
            "risk_flags": risk_flags,
            "data_quality": "high" if len(risk_flags) <= 2 else "medium" if len(risk_flags) <= 5 else "low",
        },
        "football_ai_prompt": ai_prompt,
    }


def collect_shadow_analysis(fixture_id: int, include_raw: bool = False) -> Dict[str, Any]:
    pack = collect_prematch_data(fixture_id, include_raw=True)
    if pack.get("error"):
        return pack
    analysis = build_shadow_analysis(pack)
    if include_raw:
        analysis["raw_data_pack"] = pack
    return analysis


def list_target_fixtures(date: str, timezone: str = "Asia/Shanghai") -> Dict[str, Any]:
    result = call_api_football("/fixtures", {"date": date, "timezone": timezone})
    rows = response_list(result)
    target_rows = filter_target_fixtures(rows)
    return {
        "ok": result.get("ok"),
        "date": date,
        "timezone": timezone,
        "mode": "target_major_leagues_only",
        "target_league_ids": sorted(TARGET_LEAGUE_IDS),
        "all_count": len(rows),
        "target_count": len(target_rows),
        "fixtures": [fixture_summary(r) for r in target_rows],
        "source_status_code": result.get("status_code"),
    }


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
        STARTUP_FIXTURES = list_target_fixtures(AUTO_FETCH_DATE, AUTO_FETCH_TIMEZONE)
        log_json("target_fixtures", STARTUP_FIXTURES, 30000)
    if AUTO_FETCH_FIXTURE_ID:
        try:
            fixture_id = int(AUTO_FETCH_FIXTURE_ID)
            STARTUP_COLLECT = collect_shadow_analysis(fixture_id, include_raw=True)
            log_json("shadow_collect", STARTUP_COLLECT, 50000)
        except Exception as exc:
            log_json("collect_error", {"fixture_id": AUTO_FETCH_FIXTURE_ID, "error": str(exc)})


@app.get("/")
def root():
    return {
        "service": "football-data-monitor",
        "status": "running",
        "version": "0.4.0",
        "main_urls": [
            "/health",
            "/shadow/target-fixtures?date=YYYY-MM-DD",
            "/shadow/analyze-fixture?fixture=FIXTURE_ID",
            "/shadow/analyze?date=YYYY-MM-DD&max_games=3",
            "/prematch/collect?fixture=FIXTURE_ID",
            "/debug/startup-fixtures",
        ],
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": int(time.time()),
        "version": "0.4.0",
        "api_football_base_url": API_FOOTBALL_BASE_URL,
        "thestats_base_url": THESTATS_BASE_URL,
        "isports_base_url": ISPORTS_BASE_URL,
        "has_api_football_key": bool(API_FOOTBALL_KEY),
        "has_thestats_key": bool(THESTATS_API_KEY),
        "has_isports_key": bool(ISPORTS_API_KEY),
        "shadow_token_enabled": bool(SHADOW_ACCESS_TOKEN),
        "auto_fetch_date": AUTO_FETCH_DATE,
        "auto_fetch_fixture_id": AUTO_FETCH_FIXTURE_ID,
        "target_league_ids": sorted(TARGET_LEAGUE_IDS),
        "target_leagues": DEFAULT_TARGET_LEAGUES,
    }


@app.get("/shadow/target-fixtures")
def shadow_target_fixtures(date: str = Query(...), timezone: str = "Asia/Shanghai", token: Optional[str] = None):
    require_shadow_token(token)
    return JSONResponse(list_target_fixtures(date, timezone))


@app.get("/shadow/analyze-fixture")
def shadow_analyze_fixture(fixture: int, raw: bool = False, token: Optional[str] = None):
    require_shadow_token(token)
    return JSONResponse(collect_shadow_analysis(fixture, include_raw=raw))


@app.get("/shadow/analyze")
def shadow_analyze(date: str = Query(...), timezone: str = "Asia/Shanghai", max_games: int = 3, raw: bool = False, token: Optional[str] = None):
    require_shadow_token(token)
    target = list_target_fixtures(date, timezone)
    fixtures = target.get("fixtures", [])[: max(1, min(max_games, 8))]
    analyses = []
    for f in fixtures:
        fixture_id = f.get("fixture_id")
        if fixture_id:
            analyses.append(collect_shadow_analysis(int(fixture_id), include_raw=raw))
    return JSONResponse({"ok": True, "date": date, "target_count": target.get("target_count"), "analyzed_count": len(analyses), "fixtures": fixtures, "analyses": analyses})


@app.get("/debug/startup-fixtures")
def debug_startup_fixtures():
    return STARTUP_FIXTURES


@app.get("/debug/startup-collect")
def debug_startup_collect():
    return STARTUP_COLLECT


@app.get("/prematch/fixtures")
def prematch_fixtures(date: str = Query(...), league: Optional[int] = None, season: Optional[int] = None, timezone: Optional[str] = "Asia/Shanghai", target_only: bool = False):
    params: Dict[str, Any] = {"date": date}
    if league is not None:
        params["league"] = league
    if season is not None:
        params["season"] = season
    if timezone:
        params["timezone"] = timezone
    result = call_api_football("/fixtures", params)
    if target_only:
        rows = filter_target_fixtures(response_list(result))
        return JSONResponse({"ok": result.get("ok"), "date": date, "target_count": len(rows), "fixtures": [fixture_summary(r) for r in rows]})
    return JSONResponse(result)


@app.get("/prematch/target-fixtures")
def prematch_target_fixtures(date: str = Query(...), timezone: str = "Asia/Shanghai"):
    return JSONResponse(list_target_fixtures(date, timezone))


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
