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
#--------------------------------------------------
# Targets
#--------------------------------------------------
install: ## Creates venv, and adds biomage as system command
	@echo "Creating virtual env and installing dependencies..."
	@python3 -m venv venv
	@venv/bin/pip3 install -r requirements.txt
	@echo "    [✓]\n"
	@echo "Installing biomage into ${INSTALL_PATH}"
	@printf '#!/bin/bash\n${PWD}/venv/bin/python3 ${PWD}/biomage $$@' > ${INSTALL_PATH}/biomage
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

.PHONY: install clean help
clean: ## Cleans up temporary files
	@echo "==> Cleaning up..."
	@find . -name "*.pyc" -exec rm -f {} \;
	@rm -r venv
	@echo "    [✓]"
	@echo ""
help: ## Shows available targets
	@fgrep -h "## " $(MAKEFILE_LIST) | fgrep -v fgrep | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-13s\033[0m %s\n", $$1, $$2}'
