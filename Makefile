.PHONY: test lint format install

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
