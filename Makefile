#!make
#----------------------------------------
# Settings
#----------------------------------------
.DEFAULT_GOAL := help
#--------------------------------------------------
# Variables
#--------------------------------------------------
PYTHON_FILES?=$$(find biomage -name '*.py')

# should point to biomage-utils root folder to link ./biomage python module & ./venv
#--------------------------------------------------
# Targets
#--------------------------------------------------
install: clean ## Creates venv, and adds biomage as system command
	@echo "Building package..."
	@python3 setup.py sdist
	@echo "    [✓]"

	@echo "Creating virtual environment..."
	@python3 -m venv venv
	@echo "    [✓]"

	@echo "Installing dependencies and utility..."
	@venv/bin/pip3 install --upgrade pip
	@venv/bin/pip3 install dist/*.tar.gz
	@echo "    [✓]"

fmt: ## Formats python files
	@echo "==> Formatting files..."
	@black $(PYTHON_FILES)
	@echo ""

check: ## Checks code for linting/construct errors
	@echo "==> Checking if files are well formatted..."
	@flake8 $(PYTHON_FILES)
	@echo "    [✓]"

test: ## Tests that biomage cmd & subcommand are available
	@echo "==> Checking if biomage is in path..."
	biomage > /dev/null
	@echo "    [✓]"
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

clean: ## Cleans up temporary files
	@echo "==> Cleaning up..."
	@rm dist/*
	@find . -name "*.pyc" -exec rm -f {} \;
	@echo "    [✓]"
	@echo ""

.PHONY: install fmt check test clean help
help: ## Shows available targets
	@fgrep -h "## " $(MAKEFILE_LIST) | fgrep -v fgrep | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
