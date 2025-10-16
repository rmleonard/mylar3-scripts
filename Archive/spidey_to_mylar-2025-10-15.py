#!/usr/bin/env python3
"""
spidey_to_mylar.py - Annotated, resumable ComicVine→Mylar sync
Now with filtering (publisher/name/year/size) and optional appearance gating.
"""

from __future__ import annotations

import argparse
import configparser
import json
import logging
from logging.handlers import RotatingFileHandler
import math
import os
import re
import sys
import time
from functools import lru_cache
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urlencode

import requests

# ==============================
# Defaults
# ==============================

DEFAULT_CV_BASE = "https://comicvine.gamespot.com/api"
DEFAULT_LOG_DIR = Path("logs")
DEFAULT_STATE_DIR = Path("state")
DEFAULT_CHARACTER_IDS = ["4005-1443"]  # Peter Parker
DEFAULT_CV_RATE_DELAY = 1.1
DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_LOG_LEVEL = "INFO"
DEFAULT_MAX_ISSUE_PAGES_PER_RUN = 180

# ==============================
# FS / Logging / State
# ==============================

def ensure_dirs(*paths: Path) -> None:
    for p in paths:
        p.mkdir(parents=True, exist_ok=True)

def setup_logging(level: str, log_dir: Path) -> logging.Logger:
    ensure_dirs(log_dir)
    log = logging.getLogger("spidey2mylar")
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    log.handlers.clear()
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%H:%M:%S"))
    log.addHandler(ch)
    fh = RotatingFileHandler(log_dir / "spidey_to_mylar.log", maxBytes=1_000_000, backupCount=5, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-8s %(message)s", "%Y-%m-%d %H:%M:%S"))
    log.addHandler(fh)
    return log

class RunState:
    """Persist/restore progress so we can resume safely."""
    def __init__(self, state_dir: Path):
        ensure_dirs(state_dir)
        self.state_dir = state_dir
        self._processed_vols_file = state_dir / "processed_volumes.json"
        self._char_prog_file = state_dir / "character_progress.json"
        self.processed_volumes: Set[int] = set()
        self.character_progress: Dict[str, Dict[str, int]] = {}
        self._load()
    def _load(self) -> None:
        if self._processed_vols_file.exists():
            try:
                self.processed_volumes = set(json.loads(self._processed_vols_file.read_text(encoding="utf-8")))
            except Exception:
                self.processed_volumes = set()
        if self._char_prog_file.exists():
            try:
                self.character_progress = json.loads(self._char_prog_file.read_text(encoding="utf-8"))
            except Exception:
                self.character_progress = {}
    def save(self) -> None:
        self._processed_vols_file.write_text(json.dumps(sorted(self.processed_volumes)), encoding="utf-8")
        self._char_prog_file.write_text(json.dumps(self.character_progress, indent=2), encoding="utf-8")
    def get_char_offset(self, char_id: str) -> int:
        return int(self.character_progress.get(char_id, {}).get("issues_offset", 0))
    def set_char_offset(self, char_id: str, offset: int) -> None:
        d = self.character_progress.setdefault(char_id, {})
        d["issues_offset"] = int(offset)
    def get_issue_pages_done(self, char_id: str) -> int:
        return int(self.character_progress.get(char_id, {}).get("issue_pages_done", 0))
    def inc_issue_pages_done(self, char_id: str, by: int = 1) -> None:
        d = self.character_progress.setdefault(char_id, {})
        d["issue_pages_done"] = int(d.get("issue_pages_done", 0)) + by

# ==============================
# Config loading
# ==============================

def load_config(args: argparse.Namespace) -> dict:
    """
    Merge precedence: CLI > Env > config.ini > Defaults
    """
    cfg_path = Path(args.config)
    cfg = configparser.ConfigParser()
    if cfg_path.exists():
        cfg.read(cfg_path)

    def C(section: str, key: str, default: Optional[str] = None) -> str:
        val = cfg.get(section, key, fallback=None) if cfg.has_section(section) else None
        env_map = {
            ("comicvine", "api_key"): "COMICVINE_API_KEY",
            ("comicvine", "user_agent"): "COMICVINE_USER_AGENT",
            ("comicvine", "character_ids"): "SPIDEY_CHARACTER_IDS",
            ("mylar", "base_url"): "MYLAR_BASE_URL",
            ("mylar", "api_key"): "MYLAR_API_KEY",
            ("behavior", "dry_run"): "DRY_RUN",
            ("behavior", "log_level"): "LOG_LEVEL",
            ("behavior", "rate_delay"): "CV_RATE_DELAY",
            ("behavior", "request_timeout"): "REQUEST_TIMEOUT",
            ("behavior", "max_issue_pages_per_run"): "MAX_ISSUE_PAGES_PER_RUN",
            ("behavior", "use_issue_fallback"): "USE_ISSUE_FALLBACK",
            ("filters", "publisher_allow"): "PUBLISHER_ALLOW",
            ("filters", "name_allow_regex"): "NAME_ALLOW_REGEX",
            ("filters", "name_deny_regex"): "NAME_DENY_REGEX",
            ("filters", "start_year_min"): "START_YEAR_MIN",
            ("filters", "count_of_issues_min"): "COUNT_OF_ISSUES_MIN",
            ("filters", "min_appearances_in_volume"): "MIN_APPEARANCES_IN_VOLUME",
            ("filters", "min_appearance_ratio"): "MIN_APPEARANCE_RATIO",
        }
        env = os.getenv(env_map.get((section, key), ""), None)
        cli_val = getattr(args, f"{section}_{key}", None)
        return (cli_val if cli_val not in [None, ""] else (env if env not in [None, ""] else (val if val not in [None, ""] else default)))

    cv_api_key = C("comicvine", "api_key", "")
    cv_user_agent = C("comicvine", "user_agent", "Spidey2Mylar/1.0 (Richard)")
    char_ids = C("comicvine", "character_ids", ",".join(DEFAULT_CHARACTER_IDS))
    mylar_base = C("mylar", "base_url", "http://localhost:8090")
    mylar_key = C("mylar", "api_key", "")
    dry_run = str(C("behavior", "dry_run", "false")).lower() in ("1","true","yes")
    log_level = C("behavior", "log_level", DEFAULT_LOG_LEVEL)
    rate_delay = float(C("behavior", "rate_delay", str(DEFAULT_CV_RATE_DELAY)))
    req_timeout = int(C("behavior", "request_timeout", str(DEFAULT_REQUEST_TIMEOUT)))
    max_issue_pages_per_run = int(C("behavior", "max_issue_pages_per_run", str(DEFAULT_MAX_ISSUE_PAGES_PER_RUN)))
    use_issue_fallback = str(C("behavior", "use_issue_fallback", "true")).lower() in ("1","true","yes")

    # Filters (strings may be blank)
    filters = {
        "publisher_allow": [s.strip() for s in (C("filters","publisher_allow","") or "").split("|") if s.strip()],
        "name_allow_regex": C("filters","name_allow_regex",""),
        "name_deny_regex": C("filters","name_deny_regex",""),
        "start_year_min": int(C("filters","start_year_min","0") or 0),
        "count_of_issues_min": int(C("filters","count_of_issues_min","0") or 0),
        "min_appearances_in_volume": int(C("filters","min_appearances_in_volume","0") or 0),
        "min_appearance_ratio": float(C("filters","min_appearance_ratio","0") or 0.0),
    }

    log_dir = Path(args.log_dir or DEFAULT_LOG_DIR)
    state_dir = Path(args.state_dir or DEFAULT_STATE_DIR)

    return {
        "cv_base": DEFAULT_CV_BASE,
        "cv_api_key": cv_api_key,
        "cv_user_agent": cv_user_agent,
        "character_ids": [c.strip() for c in char_ids.split(",") if c.strip()],
        "mylar_base": mylar_base,
        "mylar_key": mylar_key,
        "dry_run": dry_run,
        "log_level": log_level,
        "rate_delay": rate_delay,
        "request_timeout": req_timeout,
        "max_issue_pages_per_run": max_issue_pages_per_run,
        "use_issue_fallback": use_issue_fallback,
        "filters": filters,
        "log_dir": log_dir,
        "state_dir": state_dir,
    }

# ==============================
# HTTP helpers
# ==============================

def http_get_json(url: str, params: Optional[dict], headers: dict, timeout: int) -> dict:
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    return r.json()

def comicvine_search(path: str, cfg: dict, extra_params: dict) -> dict:
    url = f"{cfg['cv_base']}/{path.strip('/')}"
    params = {"api_key": cfg["cv_api_key"], "format": "json"}
    params.update(extra_params or {})
    headers = {"User-Agent": cfg["cv_user_agent"]}
    data = http_get_json(url, params=params, headers=headers, timeout=cfg["request_timeout"])
    if data.get("status_code") != 1:
        raise RuntimeError(f"ComicVine error: {data.get('error')} ({data.get('status_code')})")
    time.sleep(cfg["rate_delay"])
    return data

def mylar_api(cmd: str, cfg: dict, **params) -> dict:
    base = f"{cfg['mylar_base'].rstrip('/')}/api"
    query = {"cmd": cmd, "apikey": cfg["mylar_key"]}
    query.update(params)
    url = f"{base}?{urlencode(query)}"
    headers = {"User-Agent": cfg["cv_user_agent"]}
    return http_get_json(url, params=None, headers=headers, timeout=cfg["request_timeout"])

# ==============================
# Filtering helpers
# ==============================

def comicvine_volume_detail(volume_id: int, cfg: dict) -> dict:
    """Fetch minimal volume detail to apply filters."""
    path = f"volume/4050-{volume_id}/"
    data = comicvine_search(path, cfg, extra_params={
        "field_list": "id,name,publisher,start_year,count_of_issues"
    })
    return data.get("results") or {}

def _char_num(char_id: str) -> str:
    return char_id.split("-")[-1]

@lru_cache(maxsize=4096)
def count_character_appearances_in_volume(char_num: str, volume_id: int, cfg: dict) -> tuple[int,int]:
    """Return (appearances, total_issues) for a character within a volume."""
    per_page = 100
    vol = comicvine_volume_detail(volume_id, cfg)
    total_issues = int(vol.get("count_of_issues") or 0)
    offset = 0
    appearances = 0
    while True:
        params = {
            "field_list": "id",
            "filter": f"character_credits:{char_num},volume:4050-{volume_id}",
            "limit": per_page,
            "offset": offset,
            "sort": "id:asc",
        }
        data = comicvine_search("issues/", cfg, extra_params=params)
        results = data.get("results") or []
        appearances += len(results)
        if len(results) < per_page:
            break
        offset += per_page
    return appearances, total_issues

_VOLUME_DETAIL_CACHE: Dict[int, dict] = {}

def should_include_volume(volume_id: int, volume_name: Optional[str], char_id: str, cfg: dict, log: logging.Logger) -> bool:
    """
    Decide whether to include a volume based on config filters.
    """
    f = cfg.get("filters", {})
    if not any((f.get("publisher_allow"), f.get("name_allow_regex"), f.get("name_deny_regex"),
                f.get("start_year_min", 0), f.get("count_of_issues_min", 0),
                f.get("min_appearances_in_volume", 0), f.get("min_appearance_ratio", 0.0))):
        return True

    info = _VOLUME_DETAIL_CACHE.get(volume_id)
    if info is None:
        info = comicvine_volume_detail(volume_id, cfg)
        _VOLUME_DETAIL_CACHE[volume_id] = info

    name = (info.get("name") or volume_name or "") or ""
    pub = (info.get("publisher") or {})
    publisher_name = pub.get("name", "")
    publisher_id = str(pub.get("id", ""))
    start_year = int(info.get("start_year") or 0)
    count_issues = int(info.get("count_of_issues") or 0)

    # Publisher allow (supports name or ID like 4010-31)
    allow_publishers = f.get("publisher_allow") or []
    if allow_publishers:
        allowed = False
        for allowed_item in allow_publishers:
            if allowed_item.lower() in (publisher_name.lower(), publisher_id.lower()):
                allowed = True
                break
        if not allowed:
            log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: publisher '{publisher_name}' ({publisher_id}) not allowed")
            return False

    # Name allow
    allow_rx = f.get("name_allow_regex") or ""
    if allow_rx and not re.search(allow_rx, name, flags=re.I):
        log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: does not match name_allow_regex")
        return False

    # Name deny
    deny_rx = f.get("name_deny_regex") or ""
    if deny_rx and re.search(deny_rx, name, flags=re.I):
        log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: matches name_deny_regex")
        return False

    # Start year min
    if f.get("start_year_min", 0) and (not start_year or start_year < int(f["start_year_min"])):
        log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: start_year {start_year} < {f['start_year_min']}")
        return False

    # Count of issues min
    if f.get("count_of_issues_min", 0) and count_issues < int(f["count_of_issues_min"]):
        log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: count_of_issues {count_issues} < {f['count_of_issues_min']}")
        return False

    # Appearance gating
    min_apps = int(f.get("min_appearances_in_volume", 0) or 0)
    min_ratio = float(f.get("min_appearance_ratio", 0.0) or 0.0)
    if min_apps or min_ratio:
        apps, total = count_character_appearances_in_volume(_char_num(char_id), volume_id, cfg)
        ratio = (apps / total) if total else 0.0
        if (min_apps and apps < min_apps) or (min_ratio and ratio < min_ratio):
            log.debug(f"[FILTER] vol {volume_id} '{name}' rejected: appearances {apps}/{total} (ratio {ratio:.2f})")
            return False

    return True

# ==============================
# Core pipeline
# ==============================

def process_volume_if_needed(volume_id: int, volume_name: Optional[str], char_id: str,
                             existing_ids: Set[str], cfg: dict, state: RunState, log: logging.Logger) -> None:
    if volume_id in state.processed_volumes:
        return

    # Apply filters before any add
    if not should_include_volume(volume_id, volume_name, char_id, cfg, log):
        state.processed_volumes.add(volume_id)
        return

    comicid = f"4050-{volume_id}"
    if comicid in existing_ids:
        log.debug(f"[SKIP] Already in Mylar: {comicid} — {volume_name}")
        state.processed_volumes.add(volume_id)
        return

    if cfg["dry_run"]:
        log.info(f"[DRY-RUN] Would add {comicid} — {volume_name}")
    else:
        try:
            resp = mylar_api("addComic", cfg, ComicID=comicid)
            log.info(f"[ADD] {comicid} — {volume_name} :: keys={list(resp)[:5]}")
        except requests.HTTPError as e:
            log.error(f"[ADD-HTTP] {comicid} — {volume_name} :: {e} :: body={getattr(e.response, 'text', '')[:300]}")
            raise
        except Exception as e:
            log.error(f"[ADD-ERR] {comicid} — {volume_name} :: {e}")
            raise
    state.processed_volumes.add(volume_id)

def process_from_volume_credits(char_id: str, cfg: dict, state: RunState, existing_ids: Set[str], log: logging.Logger) -> int:
    data = comicvine_search(f"character/{char_id}/", cfg, extra_params={"field_list": "id,name,volume_credits"})
    results = data.get("results") or {}
    vols = results.get("volume_credits") or []
    count = 0
    for v in vols:
        vid = v.get("id")
        if not vid:
            continue
        process_volume_if_needed(vid, v.get("name"), char_id, existing_ids, cfg, state, log)
        count += 1
        if count % 25 == 0:
            state.save()
    log.info(f"[CV] {char_id}: volume_credits processed volumes={count}")
    state.save()
    return count

def process_from_issues_fallback(char_id: str, cfg: dict, state: RunState, existing_ids: Set[str], log: logging.Logger) -> Tuple[int, int]:
    per_page = 100
    pages_done_this_pass = 0
    volumes_seen_this_pass = 0
    base_params = {
        "field_list": "id,volume",
        "filter": f"character_credits:{_char_num(char_id)}",
        "limit": per_page,
        "offset": state.get_char_offset(char_id),
        "sort": "id:asc",
    }
    first = comicvine_search("issues/", cfg, extra_params=base_params)
    total = int(first.get("number_of_total_results") or 0)
    log.info(f"[CV] {char_id}: issues total={total}, starting offset={base_params['offset']}")
    def handle_results(results: List[dict]) -> int:
        added_here = 0
        for it in results or []:
            vol = it.get("volume") or {}
            vid = vol.get("id")
            if not vid:
                continue
            before = len(state.processed_volumes)
            process_volume_if_needed(vid, vol.get("name"), char_id, existing_ids, cfg, state, log)
            after = len(state.processed_volumes)
            if after > before:
                added_here += 1
        return added_here
    volumes_seen_this_pass += handle_results(first.get("results") or [])
    pages_done_this_pass += 1
    state.inc_issue_pages_done(char_id)
    state.set_char_offset(char_id, base_params["offset"] + per_page)
    state.save()
    if state.get_issue_pages_done(char_id) >= cfg["max_issue_pages_per_run"]:
        log.warning(f"[LIMIT] Hit max_issue_pages_per_run={cfg['max_issue_pages_per_run']}. Resume later.")
        return volumes_seen_this_pass, pages_done_this_pass
    total_pages = math.ceil(total / per_page)
    for p in range(1, total_pages):
        if state.get_issue_pages_done(char_id) >= cfg["max_issue_pages_per_run"]:
            log.warning(f"[LIMIT] Hit max_issue_pages_per_run={cfg['max_issue_pages_per_run']}. Stopping cleanly.")
            break
        params = dict(base_params)
        params["offset"] = state.get_char_offset(char_id)
        try:
            data = comicvine_search("issues/", cfg, extra_params=params)
        except requests.HTTPError as e:
            log.error(f"[HTTP] issues page at offset={params['offset']} failed :: {e}")
            break
        volumes_seen_this_pass += handle_results(data.get("results") or [])
        pages_done_this_pass += 1
        state.inc_issue_pages_done(char_id)
        state.set_char_offset(char_id, params["offset"] + per_page)
        state.save()
        if (pages_done_this_pass % 10) == 0:
            log.info(f"[CV] {char_id}: paged {pages_done_this_pass}/{total_pages} (offset {params['offset']}) unique volumes processed so far this pass ≈ {volumes_seen_this_pass}")
    return volumes_seen_this_pass, pages_done_this_pass

def get_mylar_existing_comicids(cfg: dict, log: logging.Logger) -> Set[str]:
    try:
        idx = mylar_api("getIndex", cfg)
    except Exception as e:
        log.error(f"[Mylar] getIndex failed: {e}")
        raise
    items = idx.get("data") or idx.get("results") or idx
    existing: Set[str] = set()
    def harvest(obj):
        if isinstance(obj, dict):
            cid = obj.get("ComicID") or obj.get("comicid") or obj.get("comic_id")
            if cid:
                existing.add(str(cid))
        elif isinstance(obj, list):
            for x in obj:
                harvest(x)
    harvest(items)
    log.info(f"[Mylar] Existing series detected: {len(existing)}")
    return existing

# ==============================
# Main
# ==============================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Sync Spider-Man volumes from ComicVine to Mylar (resumable + filters).")
    p.add_argument("--config", default="config.ini", help="Path to config.ini (default: ./config.ini)")
    p.add_argument("--log-dir", default=None, help=f"Directory for logs (default: {DEFAULT_LOG_DIR})")
    p.add_argument("--state-dir", default=None, help=f"Directory for checkpoints (default: {DEFAULT_STATE_DIR})")
    # optional CLI overrides for any config key
    p.add_argument("--comicvine-api-key", dest="comicvine_api_key", default=None)
    p.add_argument("--comicvine-user-agent", dest="comicvine_user_agent", default=None)
    p.add_argument("--comicvine-character-ids", dest="comicvine_character_ids", default=None)
    p.add_argument("--mylar-base-url", dest="mylar_base_url", default=None)
    p.add_argument("--mylar-api-key", dest="mylar_api_key", default=None)
    p.add_argument("--behavior-dry-run", dest="behavior_dry_run", default=None)
    p.add_argument("--behavior-log-level", dest="behavior_log_level", default=None)
    p.add_argument("--behavior-rate-delay", dest="behavior_rate_delay", default=None)
    p.add_argument("--behavior-request-timeout", dest="behavior_request_timeout", default=None)
    p.add_argument("--behavior-max-issue-pages-per-run", dest="behavior_max_issue_pages_per_run", default=None)
    p.add_argument("--behavior-use-issue-fallback", dest="behavior_use_issue_fallback", default=None)
    p.add_argument("--filters-publisher-allow", dest="filters_publisher_allow", default=None)
    p.add_argument("--filters-name-allow-regex", dest="filters_name_allow_regex", default=None)
    p.add_argument("--filters-name-deny-regex", dest="filters_name_deny_regex", default=None)
    p.add_argument("--filters-start-year-min", dest="filters_start_year_min", default=None)
    p.add_argument("--filters-count-of-issues-min", dest="filters_count_of_issues_min", default=None)
    p.add_argument("--filters-min-appearances-in-volume", dest="filters_min_appearances_in_volume", default=None)
    p.add_argument("--filters-min-appearance-ratio", dest="filters_min_appearance_ratio", default=None)
    return p.parse_args()

def main() -> int:
    args = parse_args()
    cfg = load_config(args)
    if not cfg["cv_api_key"]:
        print("Missing ComicVine API key. Set COMICVINE_API_KEY or config.ini [comicvine] api_key.", file=sys.stderr)
        return 2
    if not cfg["mylar_key"]:
        print("Missing Mylar API key. Set MYLAR_API_KEY or config.ini [mylar] api_key.", file=sys.stderr)
        return 2
    log = setup_logging(cfg["log_level"], cfg["log_dir"])
    state = RunState(cfg["state_dir"])
    log.info("=== Spidey2Mylar start ===")
    log.info(f"Characters: {cfg['character_ids']}")
    log.info(f"Dry run: {cfg['dry_run']} | Rate delay: {cfg['rate_delay']}s | Timeout: {cfg['request_timeout']}s")
    log.info(f"Log dir: {cfg['log_dir'].resolve()} | State dir: {cfg['state_dir'].resolve()}")
    log.info(f"Max issues pages per run: {cfg['max_issue_pages_per_run']} | use_issue_fallback={cfg['use_issue_fallback']}")
    try:
        existing = get_mylar_existing_comicids(cfg, log)
        total_vols_streamed = 0
        total_pages = 0
        for char_id in cfg["character_ids"]:
            total_vols_streamed += process_from_volume_credits(char_id, cfg, state, existing, log)
            if cfg.get("use_issue_fallback", True):
                vols_this_pass, pages_this_pass = process_from_issues_fallback(char_id, cfg, state, existing, log)
                total_vols_streamed += vols_this_pass
                total_pages += pages_this_pass
            else:
                log.info("[CFG] use_issue_fallback = false → skipping issues/ paging sweep")
        summary = {
            "characters": cfg["character_ids"],
            "volumes_processed_total": len(state.processed_volumes),
            "volumes_streamed_this_run": total_vols_streamed,
            "issues_pages_this_run": total_pages,
            "dry_run": cfg["dry_run"],
        }
        print(json.dumps(summary, indent=2))
        log.info("=== Spidey2Mylar complete ===")
        return 0
    except requests.HTTPError as e:
        log.error(f"HTTP error: {e} — body: {getattr(e.response, 'text', '')[:500]}")
        return 1
    except KeyboardInterrupt:
        log.warning("Interrupted by user (Ctrl+C). Saving state and exiting gracefully.")
        state.save()
        return 130
    except Exception as e:
        log.exception(f"Unhandled exception: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
