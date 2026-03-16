.PHONY: install dev test dry-run run

install:
	pip install .

dev:
	pip install -e ".[dev]"

test:
	python -m pytest tests/ -v

dry-run:
	chq --format json

run:
	chq
