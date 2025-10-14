#!/usr/bin/env python3
import os
import sys
import time
import json
import math
import logging
import configparser
from urllib.parse import urlencode
import requests

CONFIG_PATH = os.getenv("SPIDEY_CONFIG", "config.ini")

def load_config():
    cfg = configparser.ConfigParser()
    if os.path.exists(CONFIG_PATH):
        cfg.read(CONFIG_PATH)
    def g(section, key, default=None):
        val = None
        if cfg.has_option(section, key):
            val = cfg.get(section, key)
        if val is None:
            return default
        return val

    cv_api_key = os.getenv("COMICVINE_API_KEY", g("comicvine", "api_key", ""))
    cv_user_agent = os.getenv("COMICVINE_USER_AGENT", g("comicvine", "user_agent", "Spidey2Mylar/1.0 (Richard)"))
    char_ids = os.getenv("SPIDEY_CHARACTER_IDS", g("comicvine", "character_ids", "4005-1443"))

    mylar_base = os.getenv("MYLAR_BASE_URL", g("mylar", "base_url", "http://localhost:8090"))
    mylar_key = os.getenv("MYLAR_API_KEY", g("mylar", "api_key", ""))

    dry_run = os.getenv("DRY_RUN", g("behavior", "dry_run", "false")).lower() in ("1", "true", "yes")
    log_level = os.getenv("LOG_LEVEL", g("behavior", "log_level", "INFO")).upper()
    rate_delay = float(os.getenv("CV_RATE_DELAY", g("behavior", "rate_delay", "1.1")))
    req_timeout = int(os.getenv("REQUEST_TIMEOUT", g("behavior", "request_timeout", "30")))

    return {
        "cv_api_key": cv_api_key,
        "cv_user_agent": cv_user_agent,
        "character_ids": [c.strip() for c in char_ids.split(",") if c.strip()],
        "mylar_base": mylar_base,
        "mylar_key": mylar_key,
        "dry_run": dry_run,
        "log_level": log_level,
        "rate_delay": rate_delay,
        "request_timeout": req_timeout,
    }

def main():
    cfg = load_config()

    logging.basicConfig(
        level=cfg["log_level"],
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("spidey2mylar")

    if not cfg["cv_api_key"] or not cfg["mylar_key"]:
        log.error("Missing API keys. Set them in config.ini or environment.")
        sys.exit(2)

    CV_BASE = "https://comicvine.gamespot.com/api"

    def get_json(url, params=None, headers=None, timeout=cfg["request_timeout"]):
        h = {"User-Agent": cfg["cv_user_agent"]}
        if headers:
            h.update(headers)
        r = requests.get(url, params=params, headers=h, timeout=timeout)
        r.raise_for_status()
        return r.json()

    def comicvine(path, params=None):
        if params is None:
            params = {}
        params.setdefault("api_key", cfg["cv_api_key"])
        params.setdefault("format", "json")
        url = f"{CV_BASE}/{path.strip('/')}"
        data = get_json(url, params=params)
        if data.get("status_code") != 1:
            raise RuntimeError(f"ComicVine error: {data.get('error')} ({data.get('status_code')})")
        time.sleep(cfg["rate_delay"])
        return data

    def mylar(cmd, **params):
        base = f"{cfg['mylar_base'].rstrip('/')}/api"
        query = {"cmd": cmd, "apikey": cfg["mylar_key"]}
        query.update(params)
        url = f"{base}?{urlencode(query)}"
        data = get_json(url)
        return data

    def volumes_from_character_volume_credits(char_id):
        path = f"character/{char_id}/"
        data = comicvine(path, params={"field_list": "id,name,volume_credits"})
        results = data.get("results") or {}
        vols = results.get("volume_credits") or []
        out = {}
        for v in vols:
            vid = v.get("id")
            if vid:
                out[vid] = {"id": vid, "name": v.get("name")}
        log.info(f"[CV] {char_id}: volume_credits returned {len(out)} volumes")
        return out

    def volumes_from_issues_character_credits(char_id, max_pages=None):
        per_page = 100
        seen_vols = {}
        base_params = {
            "field_list": "id,volume",
            "filter": f"character_credits:{char_id.split('-')[-1]}",
            "limit": per_page,
            "offset": 0,
            "sort": "id:asc",
        }
        first = comicvine("issues/", params=base_params)
        total = int(first.get("number_of_total_results") or 0)
        page_results = first.get("results") or []
        for it in page_results:
            vol = it.get("volume") or {}
            vid = vol.get("id")
            if vid:
                seen_vols.setdefault(vid, {"id": vid, "name": vol.get("name")})
        log.info(f"[CV] {char_id}: issues total={total}, first page vols so far={len(seen_vols)}")

        pages = math.ceil(total / per_page)
        if max_pages is not None:
            pages = min(pages, max_pages)

        for p in range(1, pages):
            params = dict(base_params)
            params["offset"] = p * per_page
            data = comicvine("issues/", params=params)
            results = data.get("results") or []
            for it in results:
                vol = it.get("volume") or {}
                vid = vol.get("id")
                if vid:
                    seen_vols.setdefault(vid, {"id": vid, "name": vol.get("name")})
            if p % 10 == 0:
                log.info(f"[CV] {char_id}: paged {p+1}/{pages}, unique volumes={len(seen_vols)}")
        return seen_vols

    def collect_spidey_volumes(character_ids, use_issue_fallback=True, issue_pages_cap=None):
        aggregate = {}
        for cid in (c.strip() for c in character_ids if c.strip()):
            vols = volumes_from_character_volume_credits(cid)
            if use_issue_fallback:
                extra = volumes_from_issues_character_credits(cid, max_pages=issue_pages_cap)
                vols.update(extra)
            for vid, v in vols.items():
                aggregate.setdefault(vid, v)
        log.info(f"[CV] Total unique Spider-volumes collected: {len(aggregate)}")
        return aggregate

    def get_mylar_existing_comicids():
        idx = mylar("getIndex")
        items = idx.get("data") or idx.get("results") or idx
        existing = set()
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

    def add_series_to_mylar(comicid):
        if cfg["dry_run"]:
            log.info(f"[DRY RUN] Would add {comicid}")
            return {"status": "DRY_RUN"}
        resp = mylar("addComic", ComicID=comicid)
        return resp

    volumes = collect_spidey_volumes(cfg["character_ids"], use_issue_fallback=True, issue_pages_cap=None)
    existing = get_mylar_existing_comicids()

    added, skipped, errors = [], [], []
    for vid, meta in sorted(volumes.items(), key=lambda kv: (kv[1].get("name") or "", kv[0])):
        comicid = f"4050-{vid}"
        if comicid in existing:
            skipped.append((comicid, meta.get("name")))
            continue
        try:
            resp = add_series_to_mylar(comicid)
            added.append((comicid, meta.get("name"), resp))
            log.info(f"[Mylar] Added {comicid} — {meta.get('name')}")
        except Exception as e:
            log.exception(f"Failed to add {comicid} — {meta.get('name')}")
            errors.append((comicid, meta.get("name"), str(e)))

    summary = {
        "characters": cfg["character_ids"],
        "volumes_found": len(volumes),
        "already_in_mylar": len(skipped),
        "added_now": len(added),
        "errors": len(errors),
        "dry_run": cfg["dry_run"],
    }
    print(json.dumps(summary, indent=2, ensure_ascii=False))

    if skipped[:5]:
        print("\nExamples already present:")
        for c, n in skipped[:5]:
            print(f"  {c} — {n}")
    if added[:5]:
        print("\nExamples added this run:")
        for c, n, _ in added[:5]:
            print(f"  {c} — {n}")
    if errors:
        print("\nErrors:")
        for c, n, e in errors[:5]:
            print(f"  {c} — {n} :: {e}")

if __name__ == "__main__":
    try:
        main()
    except requests.HTTPError as e:
        print(f"HTTP error: {e} — body: {getattr(e.response, 'text', '')[:500]}")
        sys.exit(1)
    except Exception as e:
        print("Unhandled exception:", e)
        sys.exit(1)
