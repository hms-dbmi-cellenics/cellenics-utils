#!make
#----------------------------------------
# Settings
#----------------------------------------
.DEFAULT_GOAL := help
#--------------------------------------------------
# Variables
#--------------------------------------------------
PYTHON_FILES?=$$(find biomage -name '*.py')

ifeq ($(shell uname -s),Darwin)
    ENTRY_POINT=/usr/local/bin/biomage
else
    ENTRY_POINT=/usr/bin/biomage
endif

#--------------------------------------------------
# Targets
#--------------------------------------------------
install: clean ## Creates venv, and adds biomage as system command
	@echo "==> Creating virtual environment..."
	@python3 -m venv venv
	@echo "    [✓]"
	@echo

	@echo "==> Installing utility and dependencies..."
	@venv/bin/pip install --upgrade pip
	@venv/bin/pip install .
	@sudo ln -sf '$(CURDIR)/venv/bin/biomage' $(ENTRY_POINT)
	@echo "    [✓]"
	@echo

uninstall: clean ## Uninstalls utility and destroys venv
	@echo "==> Uninstalling utility and dependencies..."
	@venv/bin/pip uninstall -y biomage-utils
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
	@venv/bin/black $(PYTHON_FILES)
	@echo "    [✓]"
	@echo

check: develop ## Checks code for linting/construct errors
	@echo "==> Checking if files are well formatted..."
	@venv/bin/flake8 $(PYTHON_FILES)
	@echo "    [✓]"
	@echo

test: ## Tests that biomage cmd & subcommand are available
	@echo "==> Checking if biomage is in path..."
	biomage > /dev/null
	@echo "    [✓]"
	@echo

	@echo "==> Checking if all subcommands are available..."
	biomage configure-repo --help > /dev/null
	biomage experiment --help > /dev/null
	biomage experiment pull --help > /dev/null
	biomage experiment ls --help > /dev/null
	biomage experiment compare --help > /dev/null
	biomage experiment copy --help > /dev/null
	biomage rotate-ci --help > /dev/null
	biomage stage --help > /dev/null
	biomage unstage --help > /dev/null
	biomage release --help > /dev/null
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
