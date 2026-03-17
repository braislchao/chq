.PHONY: install dev test dry-run run format lint

install:
	uv pip install .

dev:
	uv pip install -e ".[dev]"

test:
	uv run python -m pytest tests/ -v

dry-run:
	uv run chq --format json

run:
	uv run chq

format:
	uv run ruff format .

lint:
	uv run ruff check --fix .
