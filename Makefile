.PHONY: format lint typecheck test check

# --- auto-formatting tools (rewrite code) ---
format:
	black .
	isort .

# --- static checks (do NOT rewrite code) ---
lint:
	ruff check .

typecheck:
	mypy .

doclint:
	pydoclint .

test:
	pytest

# run everything in the correct order
check: format lint typecheck doclint test
