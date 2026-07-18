CONDA := /home/amit/anaconda3/bin/conda
ENV := tickrecorder

.PHONY: install test lint validate run

install:
	$(CONDA) run -n $(ENV) python -m pip install -e ".[dev]"

test:
	$(CONDA) run -n $(ENV) python -m pytest

lint:
	$(CONDA) run -n $(ENV) ruff check .

validate:
	$(CONDA) run -n $(ENV) tickrecorder --validate-config

run:
	$(CONDA) run --no-capture-output -n $(ENV) tickrecorder
