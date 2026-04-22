.PHONY: help build test lint clean publish release kestra-up kestra-down kestra-logs

# Accept v=2026.5.1 or v=v2026.5.1 — VER is always the bare number
VER := $(patsubst v%,%,$(v))

# ── Help ──────────────────────────────────────────────────────────────────────

help:
	@echo ""
	@echo "  make build               Build wheel and sdist"
	@echo "  make test                Run test suite"
	@echo "  make lint                Run ruff linter"
	@echo "  make clean               Remove build artefacts"
	@echo "  make publish v=2026.5.1  Bump version, commit, tag, push → triggers PyPI"
	@echo "  make release v=2026.5.1  Tag + push only (version already bumped)"
	@echo "  make kestra-up           Start Kestra + Postgres via Docker Compose"
	@echo "  make kestra-down         Stop and remove Kestra containers"
	@echo "  make kestra-logs         Tail Kestra container logs"
	@echo ""

# ── Dev ───────────────────────────────────────────────────────────────────────

build:
	uv build

test:
	uv run pytest --tb=short -q

lint:
	uv run ruff check openmedallion/ tests/

clean:
	rm -rf dist/ .pytest_cache/ site/
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

# ── Release ───────────────────────────────────────────────────────────────────

publish:
	@[ -n "$(v)" ] || (echo "Usage: make publish v=2026.5.1"; exit 1)
	@echo "→ Bumping version to $(VER)"
	sed -i 's/^version = ".*"/version = "$(VER)"/' pyproject.toml
	sed -i 's/^__version__ = ".*"/__version__ = "$(VER)"/' openmedallion/__init__.py
	git add pyproject.toml openmedallion/__init__.py
	git commit -m "chore: bump version to $(VER)"
	git tag v$(VER)
	git push origin HEAD
	git push origin v$(VER)
	@echo "✅  Tagged v$(VER) and pushed — publish workflow triggered."

release:
	@[ -n "$(v)" ] || (echo "Usage: make release v=2026.5.1"; exit 1)
	git tag v$(VER)
	git push origin v$(VER)
	@echo "✅  Pushed tag v$(VER) — publish workflow triggered."

# ── Kestra ────────────────────────────────────────────────────────────────────

kestra-up:
	docker compose up -d
	@echo "✅  Kestra UI → http://localhost:8080"

kestra-down:
	docker compose down

kestra-logs:
	docker compose logs -f kestra
