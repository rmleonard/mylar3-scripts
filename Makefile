.PHONY: setup dry-run run lint format

PYTHON ?= python3

setup:
	$(PYTHON) -m pip install -r requirements.txt
	$(PYTHON) -m pip install ruff

dry-run:
	@[ -f config.ini ] || cp config.ini.example config.ini
	DRY_RUN=true $(PYTHON) spidey_to_mylar.py

run:
	@[ -f config.ini ] || cp config.ini.example config.ini
	$(PYTHON) spidey_to_mylar.py

lint:
	ruff check .

format:
	ruff format .