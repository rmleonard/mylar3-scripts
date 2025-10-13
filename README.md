# mylar3-scripts

Utilities for syncing Spider-Man series from ComicVine into Mylar.

## Overview

`spidey_to_mylar.py`:
- Queries ComicVine for **all volumes ("series")** that selected Spider characters appear in.
- Checks your **Mylar** instance for existing series.
- Adds any missing series by ComicVine Volume ID (e.g., `4050-<volume_id>`).

## Quickstart

### 1) Python & deps
```bash
python -V  # Python 3.9+ recommended
make setup  # installs runtime deps
```

### 2) Configure once
Copy and edit the config template:
```bash
cp config.ini.example config.ini
# edit config.ini with your API keys and host
```
> You can still override any value via environment variables at runtime.

### 3) Dry-run, then run for real
```bash
make dry-run  # no adds, prints summary
make run      # actually adds missing series to Mylar
```

### 4) (Optional) Dev tools
```bash
make lint       # runs Ruff
make format     # Ruff format
```

## Configuration

`config.ini` supports these keys. Env vars override file values.

```ini
[comicvine]
api_key = YOUR_COMICVINE_API_KEY
user_agent = Spidey2Mylar/1.0 (your-email-or-site)
character_ids = 4005-1443,4005-79420,4005-162256

[mylar]
base_url = http://localhost:8090
api_key = YOUR_MYLAR_API_KEY

[behavior]
dry_run = false
log_level = INFO
rate_delay = 1.1
request_timeout = 30
```

## Makefile targets

- `setup`   — install runtime deps (`requests`) and dev tool (`ruff`)
- `dry-run` — simulate (no adds)
- `run`     — perform adds
- `lint`    — Ruff lint
- `format`  — Ruff format

## Repo structure

```
mylar3-scripts/
├── spidey_to_mylar.py
├── config.ini.example
├── requirements.txt
├── .gitignore
├── LICENSE
├── Makefile
└── .github/workflows/lint.yml
```

## License

MIT — see `LICENSE`.