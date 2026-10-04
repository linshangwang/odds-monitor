"""Read-only prematch packet sync client.

This program runs locally. It may read one existing file from pang through SSH,
but it never uploads, creates, edits, or schedules anything on pang.
"""

import argparse
import gzip
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


SAFE_REMOTE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")
DEFAULT_ENDPOINT = "https://web-production-f1134.up.railway.app"
EXCLUDED_REPORT_LIMIT = 50


def decode_bundle(raw: bytes) -> Dict[str, Any]:
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    value = json.loads(raw.decode("utf-8"))
    if isinstance(value, list):
        return {"packets": value}
    if isinstance(value, dict) and isinstance(value.get("packets"), list):
        return value
    if isinstance(value, dict) and value.get("schema_version") == "shadow_prematch_packet_v1":
        return {"packets": [value]}
    raise ValueError("bundle_must_contain_shadow_prematch_packets")


def read_local(path: str) -> bytes:
    return Path(path).read_bytes()


def read_pang_file(host: str, remote_path: str, timeout: int = 60) -> bytes:
    if not host or any(char.isspace() for char in host):
        raise ValueError("invalid_ssh_host")
    if not SAFE_REMOTE_PATH.fullmatch(remote_path or "") or ".." in remote_path.split("/"):
        raise ValueError("unsafe_remote_path")
    reader = "gzip -cd --" if remote_path.endswith(".gz") else "cat --"
    command = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", host, reader, remote_path]
    completed = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
    if completed.returncode:
        error = completed.stderr.decode("utf-8", errors="replace").strip()
        raise RuntimeError(f"pang_read_failed: {error[:300]}")
    return completed.stdout


def select_packets(bundle: Dict[str, Any], league: Optional[str] = None) -> List[Dict[str, Any]]:
    packets = bundle.get("packets") or []
    selected = []
    for packet in packets:
        if not isinstance(packet, dict) or packet.get("schema_version") != "shadow_prematch_packet_v1":
            raise ValueError("unsupported_packet_schema")
        packet_league = str(packet.get("league") or "")
        if league and packet_league.casefold() != league.casefold():
            continue
        selected.append(packet)
    return selected


def select_prematch_window(packets: List[Dict[str, Any]], window_hours: int, now_ts: Optional[int] = None) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Keep only genuinely upcoming fixtures inside a bounded time window."""
    if not 1 <= int(window_hours) <= 168:
        raise ValueError("prematch_window_hours_must_be_1_to_168")
    current_ts = int(datetime.now(timezone.utc).timestamp()) if now_ts is None else int(now_ts)
    upper_ts = current_ts + int(window_hours) * 3600
    selected, excluded = [], []
    for packet in packets:
        match = packet.get("match") or {}
        fixture = str(match.get("match_id") or "").strip() or None
        try:
            kickoff = datetime.fromisoformat(str(match.get("kickoff_utc")).replace("Z", "+00:00"))
            if kickoff.tzinfo is None:
                kickoff = kickoff.replace(tzinfo=timezone.utc)
            kickoff_ts = int(kickoff.timestamp())
        except (TypeError, ValueError):
            excluded.append({"fixture": fixture, "reason": "missing_or_invalid_kickoff"})
            continue
        if kickoff_ts <= current_ts:
            excluded.append({"fixture": fixture, "reason": "fixture_not_prematch"})
        elif kickoff_ts > upper_ts:
            excluded.append({"fixture": fixture, "reason": "outside_prematch_window"})
        else:
            selected.append(packet)
    return selected, excluded


def compressed_payload(packets: List[Dict[str, Any]]) -> bytes:
    raw = json.dumps({"packets": packets}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw, compresslevel=6)


def upload_packets(endpoint: str, token: str, packets: List[Dict[str, Any]], timeout: int = 45, expected_date: Optional[str] = None, require_prematch: bool = False) -> Dict[str, Any]:
    if not endpoint.startswith("https://"):
        raise ValueError("https_endpoint_required")
    if not token:
        raise ValueError("SHADOW_ACCESS_TOKEN_required")
    response = requests.post(
        endpoint.rstrip("/") + "/shadow/import-prematch-packets",
        data=compressed_payload(packets),
        headers={
            "Authorization": f"Bearer {token}", "Content-Type": "application/json",
            "Content-Encoding": "gzip", "X-Sync-Mode": "incremental", "X-Sync-Source": "pang-readonly-local",
            **({"X-Expected-Match-Date": expected_date} if expected_date else {}),
            **({"X-Require-Prematch": "true"} if require_prematch else {}),
        },
        timeout=timeout,
    )
    response.raise_for_status()
    return response.json()


def summary(packets: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "packet_count": len(packets),
        "fixtures": [str((packet.get("match") or {}).get("match_id") or "") for packet in packets],
        "leagues": sorted({str(packet.get("league") or "") for packet in packets}),
        "pang_policy": "read_only_existing_file; no remote writes or tasks",
    }


def packet_preflight(packets: List[Dict[str, Any]], expected_date: Optional[str] = None, require_prematch: bool = False, now_ts: Optional[int] = None) -> Dict[str, Any]:
    now_ts = int(datetime.now(timezone.utc).timestamp()) if now_ts is None else int(now_ts)
    seen, rows = set(), []
    for packet in packets:
        match = packet.get("match") or {}
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
        if not isinstance(packet.get("timeline"), list):
            reasons.append("timeline_not_array")
        rows.append({"fixture": fixture or None, "kickoff_date": kickoff_date, "status": "rejected" if reasons else "ready", "reasons": reasons})
    rejected = [row for row in rows if row["status"] == "rejected"]
    return {
        "status": "ready" if packets and not rejected else "rejected",
        "packet_count": len(packets), "ready_count": len(rows) - len(rejected), "rejected_count": len(rejected),
        "expected_date": expected_date, "require_prematch": require_prematch, "rows": rows,
        "policy": "historical_or_wrong-date_packets_are_never_relabelled_as_current",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only pang prematch packet sync")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="Local JSON or JSON.GZ bundle")
    source.add_argument("--ssh-host", help="SSH host configured in ~/.ssh/config")
    parser.add_argument("--remote-path", help="Absolute existing JSON/JSON.GZ path on pang")
    parser.add_argument("--league", help="Exact league name filter")
    parser.add_argument("--prematch-window-hours", type=int, help="Keep fixtures starting in the next 1-168 hours")
    parser.add_argument("--endpoint", default=os.getenv("SHADOW_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--expected-date", help="Require every kickoff to use this YYYY-MM-DD date")
    parser.add_argument("--require-prematch", action="store_true", help="Reject fixtures whose kickoff has already passed")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.ssh_host and not args.remote_path:
        parser.error("--remote-path is required with --ssh-host")
    raw = read_local(args.input) if args.input else read_pang_file(args.ssh_host, args.remote_path)
    packets = select_packets(decode_bundle(raw), args.league)
    source_selected_count = len(packets)
    excluded: List[Dict[str, Any]] = []
    if args.prematch_window_hours is not None:
        packets, excluded = select_prematch_window(packets, args.prematch_window_hours)
    exclusion_reasons: Dict[str, int] = {}
    for item in excluded:
        reason = str(item.get("reason") or "unknown")
        exclusion_reasons[reason] = exclusion_reasons.get(reason, 0) + 1
    preflight = packet_preflight(packets, args.expected_date, args.require_prematch)
    report = {
        **summary(packets),
        "preflight": preflight,
        "selection": {
            "prematch_window_hours": args.prematch_window_hours,
            "source_selected_count": source_selected_count,
            "excluded_count": len(excluded),
            "excluded_reason_counts": exclusion_reasons,
            "excluded_sample": excluded[:EXCLUDED_REPORT_LIMIT],
            "excluded_sample_limit": EXCLUDED_REPORT_LIMIT,
            "excluded_truncated": len(excluded) > EXCLUDED_REPORT_LIMIT,
        },
    }
    if args.prematch_window_hours is not None and not packets:
        print(json.dumps({**report, "status": "no_current_prematch_packets", "action": "safe_noop"}, ensure_ascii=False))
        return 0
    if args.dry_run:
        print(json.dumps({**report, "status": "dry_run"}, ensure_ascii=False))
        return 0 if preflight["status"] == "ready" else 2
    if preflight["status"] != "ready":
        print(json.dumps({**report, "status": "rejected_before_upload"}, ensure_ascii=False))
        return 2
    result = upload_packets(args.endpoint, os.getenv("SHADOW_ACCESS_TOKEN", ""), packets, expected_date=args.expected_date, require_prematch=args.require_prematch)
    print(json.dumps({**report, "status": "uploaded", "server": result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
