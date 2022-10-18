#!make
#----------------------------------------
# Settings
#----------------------------------------
.DEFAULT_GOAL := help
#--------------------------------------------------
# Variables
#--------------------------------------------------
ifeq ($(shell uname -s),Darwin)
    ENTRY_POINT=/usr/local/bin/cellenics
else
    ENTRY_POINT=/usr/bin/cellenics
endif

#--------------------------------------------------
# Targets
#--------------------------------------------------
install: clean ## Creates venv, and adds cellenics as system command
	@echo "==> Creating virtual environment..."
	@python3 -m venv venv/
	@echo "    [✓]"
	@echo

	@echo "==> Installing utility and dependencies..."
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install -e .
	@sudo ln -sf '$(CURDIR)/venv/bin/cellenics' $(ENTRY_POINT)
	@echo "    [✓]"
	@echo

uninstall: clean ## Uninstalls utility and destroys venv
	@echo "==> Uninstalling utility and dependencies..."
	@venv/bin/pip uninstall -y cellenics-utils
	@rm -rf venv/
	@sudo rm -f $(ENTRY_POINT)
	@echo "    [✓]"
	@echo

develop: ## Installs development dependencies
	@echo "==> Installing development dependencies..."
	@venv/bin/pip install -r dev-requirements.txt --quiet
	@echo "    [✓]"
	@echo

fmt: develop ## Formats python files
	@echo "==> Formatting files..."
	@venv/bin/black cellenics/
	@venv/bin/isort --sp isort.cfg cellenics/
	@echo "    [✓]"
	@echo

check: develop ## Checks code for linting/construct errors
	@echo "==> Checking if files are well formatted..."
	@venv/bin/flake8 cellenics/
	@echo "    [✓]"
	@echo

test: ## Tests that cellenics cmd & subcommand are available
	@echo "==> Checking if cellenics is in path..."
	cellenics > /dev/null
	@echo "    [✓]"
	@echo

	@echo "==> Checking if all subcommands are available..."
	cellenics configure-repo --help > /dev/null
	cellenics rotate-ci --help > /dev/null

	cellenics stage --help > /dev/null
	cellenics unstage --help > /dev/null

	cellenics experiment --help > /dev/null
	cellenics experiment download --help > /dev/null

	cellenics account --help > /dev/null
	cellenics account change-password --help > /dev/null
	cellenics account create-user --help > /dev/null
	cellenics account create-users-list --help > /dev/null

	cellenics rds --help > /dev/null
	cellenics rds run --help > /dev/null
	cellenics rds token --help > /dev/null
	cellenics rds tunnel --help > /dev/null
	@echo "    [✓]"
	@echo

clean: ## Cleans up temporary files
	@echo "==> Cleaning up..."
	@find . -name "*.pyc" -exec rm -f {} \;
	@echo "    [✓]"
	@echo

.PHONY: install uninstall develop fmt check test clean help
help: ## Shows available targets
	@fgrep -h "## " $(MAKEFILE_LIST) | fgrep -v fgrep | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
