.PHONY: lint format test check

lint:
	ruff check .
	mypy .
	pydoclint .

format:
	black .

test:
	pytest

check: lint test