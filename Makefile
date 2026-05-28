.PHONY: help build test lint coverage examples clean version publish release restore kestra-up kestra-down kestra-logs

# Accept v=2026.5.1 or v=v2026.5.1 — VER is always the bare number
VER := $(patsubst v%,%,$(v))

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make build               Build wheel and sdist"
	@echo "  make test                Run test suite"
	@echo "  make lint                Run ruff linter"
	@echo "  make coverage            Run test coverage"
	@echo "  make examples            Run all example pipelines and report pass/fail"
	@echo "  make clean               Remove build artefacts"
	@echo "  make version v=yyyy.mm.v Prepare version, changes version number inside the code"
	@echo "  make publish v=yyyy.mm.v Bump version, commit, tag, push → triggers CI & Docs"
	@echo "  make release v=yyyy.mm.v Tag + push only (version already bumped) → Goes LIVE on PyPI"
	@echo "  make kestra-up           Start Kestra + Postgres via Docker Compose"
	@echo "  make kestra-down         Stop and remove Kestra containers"
	@echo "  make kestra-logs         Tail Kestra container logs"
	@echo ""

# ── Dev ───────────────────────────────────────────────────────────────────────

build:
	uv build

test:
	uv run --active pytest --tb=short -q

lint:
	uv run --active ruff check openmedallion/ tests/

coverage: 
	uv run --active pytest --cov=openmedallion

examples:
	@.venv/bin/python3 examples/run_examples.py

clean:
	rm -rf dist/ .pytest_cache/ site/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

version: clean lint examples
	@[ -n "$(v)" ] || (echo "Usage: make version v=2026.x.x"; exit 1)
	@echo "→ Bumping version to $(VER)"
	sed -i 's/^version = ".*"/version = "$(VER)"/' pyproject.toml
	sed -i 's/^__version__ = ".*"/__version__ = "$(VER)"/' openmedallion/__init__.py
	@echo "✅  Upgraded (local) to openmedallion v$(VER)"

# ── Release ───────────────────────────────────────────────────────────────────

publish: version build
	@[ -n "$(v)" ] || (echo "Usage: make publish v=2026.x.x"; exit 1)
	git add pyproject.toml openmedallion/__init__.py uv.lock
	git commit -m "chore: bump version to $(VER)"
	git push origin HEAD
	@echo "✅  Pushed HEAD — Publish workflow triggered."

release: publish
	@[ -n "$(v)" ] || (echo "Usage: make release v=2026.x.x"; exit 1)
	git tag v$(VER)
	git push origin v$(VER)
	@echo "✅  Pushed tag v$(VER) — Release workflow triggered."

restore:
	@[ -n "$(v)" ] || (echo "Usage: make restore v=2026.x.x"; exit 1)
	git tag -d v$(VER)
	git push --delete origin v$(VER)

# ── Kestra ────────────────────────────────────────────────────────────────────

kestra-up:
	docker compose up -d
	@echo "✅  Kestra UI → http://localhost:8080"

kestra-down:
	docker compose down

kestra-logs:
	docker compose logs -f kestra
