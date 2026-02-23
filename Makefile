PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=obsidian
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

VENV_DIR := .venv
VENV_PYTHON := $(VENV_DIR)/bin/python3

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

venv:
	python3 -m venv $(VENV_DIR)
	$(VENV_PYTHON) -m pip install --upgrade pip
	$(VENV_PYTHON) -m pip install -r requirements-dev.txt
	@echo "Run: source .venv/bin/activate"

.PHONY: venv
