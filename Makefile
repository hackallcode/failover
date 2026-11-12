VENV := .venv
PY := $(VENV)/bin/python

.PHONY: venv test integration stand-build stand-build-ci stand-test chaos

venv: $(VENV)/.ok
$(VENV)/.ok: pyproject.toml
	python3 -m venv $(VENV)
	$(VENV)/bin/pip install -q -e '.[test]'
	touch $@

test: venv  ## unit tests
	$(PY) -m pytest -q tests/unit

integration: venv  ## NATS adapter against a real server
	$(PY) -m pytest -q tests/integration

stand-build:
	docker compose -f tests/stand/compose.yaml build

gha = --cache-from type=gha,scope=$(1) --cache-to type=gha,mode=max,scope=$(1)

stand-build-ci:  ## the stand images through the GitHub Actions cache
	docker buildx build --load -t failover-litestream $(call gha,litestream) litestream
	docker buildx build -o type=local,dest=.stand/litestream $(call gha,litestream) litestream
	docker buildx build --load -t failover-stand-node --build-context litestream=.stand/litestream \
		-f tests/stand/node/Dockerfile $(call gha,stand-node) .

stand-test: venv stand-build  ## failure scenarios on the stand
	$(PY) -m pytest -q -s tests/stand/test_scenarios.py

chaos: venv stand-build  ## random failures; CHAOS_STEPS, CHAOS_SEED
	$(PY) -m pytest -q -s tests/stand/test_chaos.py
