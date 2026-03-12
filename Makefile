.PHONY: all install test lint format self-lint clean

all: install lint test self-lint

install:
	pip install -e "packages/python[dev]"

test:
	cd packages/python && pytest tests/ -v

lint:
	cd packages/python && ruff check src/sparkdoctor/ tests/

format:
	cd packages/python && ruff format src/sparkdoctor/ tests/

self-lint:
	sparkdoctor lint packages/python/src/sparkdoctor/ --exit-code --severity error

clean:
	rm -rf packages/python/dist packages/python/build packages/python/src/*.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
