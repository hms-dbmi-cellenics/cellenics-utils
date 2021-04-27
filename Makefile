#!make
#----------------------------------------
# Settings
#----------------------------------------
.DEFAULT_GOAL := help
#--------------------------------------------------
# Variables
#--------------------------------------------------
PYTHON_FILES?=$$(find biomage -name '*.py')
INSTALL_PATH=/usr/local/bin
MKFILE_PATH := $(abspath $(lastword $(MAKEFILE_LIST)))
MKFILE_DIR := $(dir $(MKFILE_PATH))
# should point to biomage-utils root folder to link ./biomage python module & ./venv
#--------------------------------------------------
# Targets
#--------------------------------------------------
install: clean ## Creates venv, and adds biomage as system command
	@echo "Creating virtual env and installing dependencies..."
	@python3 -m venv venv
	@venv/bin/pip3 install --upgrade pip
	@venv/bin/pip3 install -r requirements.txt
	@echo "    [✓]\n"
	@echo "Installing biomage into ${INSTALL_PATH}"
	@printf '#!/bin/bash\n$(MKFILE_DIR)venv/bin/python3 $(MKFILE_DIR)biomage $$@\n' > ${INSTALL_PATH}/biomage
	@chmod +x /usr/local/bin/biomage
	@echo "    [✓]\n"
fmt: ## Formats python files
	@echo "==> Formatting files..."
	@black $(PYTHON_FILES)
	@echo ""
check: ## Checks code for linting/construct errors
	@echo "==> Checking if files are well formatted..."
	@flake8 $(PYTHON_FILES)
	@echo "    [✓]\n"

test: ## Tests that biomage cmd & subcommand are available
	@echo "==> Checking if biomge is in path..."
	biomage > /dev/null
	@echo "    [✓]\n"
	@echo "==> Checking if all subcommands are available..."
	biomage configure-repo --help > /dev/null
	biomage experiment --help > /dev/null
	biomage experiment pull --help > /dev/null
	biomage experiment ls --help > /dev/null
	biomage experiment compare --help > /dev/null
	biomage rotate-ci --help > /dev/null
	biomage stage --help > /dev/null
	biomage unstage --help > /dev/null
	@echo "    [✓]\n"

clean: ## Cleans up temporary files
	@echo "==> Cleaning up..."
	@find . -name "*.pyc" -exec rm -f {} \;
	@rm -r venv
	@echo "    [✓]"
	@echo ""

.PHONY: install fmt check test clean help
help: ## Shows available targets
	@fgrep -h "## " $(MAKEFILE_LIST) | fgrep -v fgrep | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
