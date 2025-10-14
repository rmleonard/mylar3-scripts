# mylar3-scripts

Enhanced and annotated utilities for syncing Spider-Man series from ComicVine into Mylar.

**New in this build**
- Filters: publisher (name or ID), name allow/deny regex, start year, min issue count
- Optional appearance gating per volume (min appearances or ratio)
- Toggle to skip the heavy issues/ fallback sweep
- Rotating logs + resumable state, as before

## Quickstart

```bash
make setup
cp config.ini.example config.ini
# edit config.ini for your API keys and filters
make dry-run
make run
```

Logs: `logs/spidey_to_mylar.log` • Checkpoints: `state/`

## License
MIT — see `LICENSE`.
