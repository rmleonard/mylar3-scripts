# mylar3-scripts

Enhanced and annotated utilities for syncing Spider-Man series from ComicVine into Mylar.

Includes:
- Full inline annotations
- Rotating file logs under `logs/`
- Resumable checkpoints in `state/`
- Respect for ComicVine’s 200-query limit per run
- Streaming “read → act → write” pipeline
- GitHub Action for Ruff linting

## Quickstart

```bash
make setup
cp config.ini.example config.ini
# edit config.ini for your API keys
make dry-run
make run
```

Logs: `logs/spidey_to_mylar.log`  
Checkpoints: `state/`

## License

MIT License — see `LICENSE`.