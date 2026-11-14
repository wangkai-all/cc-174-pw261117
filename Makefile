PY ?= .venv/bin/python
PIP ?= .venv/bin/pip

.PHONY: setup run test clean

setup:
	python3 -m venv .venv
	$(PIP) install -r requirements-dev.txt

run:
	$(PY) -m uvicorn lakehouse.app:app --host 127.0.0.1 --port 8765 --reload

test:
	$(PY) -m pytest tests -q

clean:
	rm -rf var/lake var/lakehouse.sqlite3 var/lakehouse.sqlite3-wal var/lakehouse.sqlite3-shm
