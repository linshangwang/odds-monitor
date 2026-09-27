import os
import time
from typing import Any, Dict, Optional

import requests
from fastapi import FastAPI, Request, Query
from fastapi.responses import JSONResponse
from dotenv import load_dotenv

load_dotenv()

ISPORTS_API_KEY = os.getenv("ISPORTS_API_KEY", "")
ISPORTS_BASE_URL = os.getenv("ISPORTS_BASE_URL", "http://isports.feijing88.com").rstrip("/")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

app = FastAPI(
    title="Football Odds Monitor",
    description="Railway-ready service for iSports odds data and Sportradar push/webhook testing.",
    version="0.1.0",
)

# 临时内存缓存：Railway 重启会清空。后面正式版再换 PostgreSQL。
LAST_PUSH_EVENTS = []
LAST_PUSH_STATISTICS = []


def mask_key(url: str) -> str:
    if ISPORTS_API_KEY:
        return url.replace(ISPORTS_API_KEY, "YOUR_API_KEY")
    return url


def call_isports(path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not ISPORTS_API_KEY:
        return {
            "ok": False,
            "error": "Missing ISPORTS_API_KEY. Set it in Railway Variables.",
        }

    params = dict(params or {})
    params["api_key"] = ISPORTS_API_KEY
    url = f"{ISPORTS_BASE_URL}{path}"

    try:
        resp = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        try:
            data = resp.json()
        except Exception:
            data = {"raw_text": resp.text[:2000]}

        return {
            "ok": resp.ok,
            "status_code": resp.status_code,
            "request_url": mask_key(resp.url),
            "data": data,
        }
    except requests.RequestException as exc:
        return {
            "ok": False,
            "error": str(exc),
            "request_url": mask_key(url),
        }


@app.get("/")
def root():
    return {
        "service": "football-odds-monitor",
        "status": "running",
        "next_steps": [
            "Open /health to verify Railway service",
            "Open /isports/odds-main to test full odds snapshot",
            "Open /isports/odds-changes to test odds changes",
            "Use /sportradar/push/events as a push/webhook test endpoint",
            "Use /sportradar/push/statistics as a push/webhook test endpoint",
        ],
    }


@app.get("/health")
def health():
    return {
        "ok": True,
        "timestamp": int(time.time()),
        "isports_base_url": ISPORTS_BASE_URL,
        "has_isports_key": bool(ISPORTS_API_KEY),
    }


@app.get("/isports/odds-main")
def isports_odds_main():
    """获取 iSports 当前主盘口快照：handicap / europeOdds / overUnder。"""
    return JSONResponse(call_isports("/sport/football/odds/main"))


@app.get("/isports/odds-changes")
def isports_odds_changes():
    """获取 iSports 最近盘口变化增量。"""
    return JSONResponse(call_isports("/sport/football/odds/main/changes"))


@app.get("/isports/raw")
def isports_raw(path: str = Query(..., description="Example: /sport/football/odds/main")):
    """调试用：测试任意 iSports path。"""
    if not path.startswith("/"):
        path = "/" + path
    return JSONResponse(call_isports(path))


@app.post("/sportradar/push/events")
async def sportradar_push_events(request: Request):
    """Sportradar Push Events / Webhook 测试接收口。"""
    payload = await request.json()
    record = {"received_at": int(time.time()), "payload": payload}
    LAST_PUSH_EVENTS.append(record)
    if len(LAST_PUSH_EVENTS) > 50:
        del LAST_PUSH_EVENTS[:-50]
    return {"ok": True, "received": "events", "count": len(LAST_PUSH_EVENTS)}


@app.post("/sportradar/push/statistics")
async def sportradar_push_statistics(request: Request):
    """Sportradar Push Statistics / Webhook 测试接收口。"""
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
