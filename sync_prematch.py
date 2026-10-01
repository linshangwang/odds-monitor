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
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests


SAFE_REMOTE_PATH = re.compile(r"^/[A-Za-z0-9._/-]+$")
DEFAULT_ENDPOINT = "https://web-production-f1134.up.railway.app"


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


def compressed_payload(packets: List[Dict[str, Any]]) -> bytes:
    raw = json.dumps({"packets": packets}, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return gzip.compress(raw, compresslevel=6)


def upload_packets(endpoint: str, token: str, packets: List[Dict[str, Any]], timeout: int = 45) -> Dict[str, Any]:
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


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only pang prematch packet sync")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--input", help="Local JSON or JSON.GZ bundle")
    source.add_argument("--ssh-host", help="SSH host configured in ~/.ssh/config")
    parser.add_argument("--remote-path", help="Absolute existing JSON/JSON.GZ path on pang")
    parser.add_argument("--league", help="Exact league name filter")
    parser.add_argument("--endpoint", default=os.getenv("SHADOW_ENDPOINT", DEFAULT_ENDPOINT))
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if args.ssh_host and not args.remote_path:
        parser.error("--remote-path is required with --ssh-host")
    raw = read_local(args.input) if args.input else read_pang_file(args.ssh_host, args.remote_path)
    packets = select_packets(decode_bundle(raw), args.league)
    report = summary(packets)
    if args.dry_run:
        print(json.dumps({**report, "status": "dry_run"}, ensure_ascii=False))
        return 0
    result = upload_packets(args.endpoint, os.getenv("SHADOW_ACCESS_TOKEN", ""), packets)
    print(json.dumps({**report, "status": "uploaded", "server": result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
