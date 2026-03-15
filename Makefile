VENV := $(CURDIR)/.venv/bin
PIP := $(VENV)/pip
PYTHON := $(VENV)/python
PYTEST := $(VENV)/pytest
RUFF := $(VENV)/ruff
SPARKDOCTOR := $(VENV)/sparkdoctor

.PHONY: all install test lint format format-check self-lint validate-rules ci build clean

# Run the full CI pipeline locally (mirrors .github/workflows/ci.yml)
all: ci

install:
	$(PIP) install -e "packages/python[dev]"

test:
	cd packages/python && $(PYTEST) tests/ -v

lint:
	cd packages/python && $(RUFF) check src/sparkdoctor/ tests/

format:
	cd packages/python && $(RUFF) format src/sparkdoctor/ tests/

format-check:
	cd packages/python && $(RUFF) format --check src/sparkdoctor/ tests/

self-lint:
	$(SPARKDOCTOR) lint packages/python/src/sparkdoctor/ --exit-code --severity error

validate-rules:
	$(PYTHON) -c "from pathlib import Path; files = list(Path('core/rules').glob('*.yaml')); assert len(files) >= 25, f'Expected 25+ rule files, got {len(files)}'; print(f'  {len(files)} rule YAML files OK')"

# Mirrors .github/workflows/ci.yml exactly
ci:
	@echo "=== CI: copy README ==="
	cp README.md packages/python/README.md
	@echo "=== CI: install ==="
	$(PIP) install -e "packages/python[dev]"
	@echo "=== CI: ruff check ==="
	cd packages/python && $(RUFF) check src/sparkdoctor/ tests/
	@echo "=== CI: ruff format check ==="
	cd packages/python && $(RUFF) format --check src/sparkdoctor/ tests/
	@echo "=== CI: pytest ==="
	cd packages/python && $(PYTEST) tests/ -v
	@echo "=== CI: self-lint ==="
	$(SPARKDOCTOR) lint packages/python/src/sparkdoctor/ --exit-code --severity error
	@echo "=== CI: validate rules ==="
	cd packages/python && $(PYTHON) -c "from pathlib import Path; files = list(Path('../../core/rules').glob('*.yaml')); assert len(files) >= 25, f'Expected 25+ rule files, got {len(files)}'; print(f'  {len(files)} rule YAML files OK')"
	@rm -f packages/python/README.md
	@echo "=== CI: all steps passed ==="

# Mirrors .github/workflows/publish.yml exactly
build:
	@echo "=== Publish: copy README ==="
	cp README.md packages/python/README.md
	@echo "=== Publish: build wheel ==="
	cd packages/python && $(PYTHON) -m build --wheel
	@echo "=== Publish: build sdist ==="
	cd packages/python && $(PYTHON) -m build --sdist
	@rm -f packages/python/README.md
	@echo "=== Publish: build succeeded ==="
	@echo "  Artifacts in packages/python/dist/"

clean:
	rm -rf packages/python/dist packages/python/build packages/python/src/*.egg-info
	rm -f packages/python/README.md
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
