#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = terrific-totes-team-8
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PYTHON_INTERPRETER) -m venv venv; \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install pip-tools)
	$(call execute_in_env, $(PIP) install -r requirements.txt)

################################################################################################################
# Set Up
## Install bandit
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install pytest-cov)

## Set up dev requirements (bandit, black & coverage)
dev-setup: bandit black coverage

# Build / Run

## Run the security test (bandit + safety)
security-test:
	$(call execute_in_env, bandit -lll ./src/*/*.py tests/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, black  ./src/*/*.py ./tests/*/*.py)

## Run the pip-audit code check
run-pip-audit:
	$(call execute_in_env, pip-audit)
	$(call execute_in_env, pip-audit  -r requirements.txt)

## Run the unit tests $(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest tests/load/* -vvvrP) ($(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest tests/transform/* -vvvrP)
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest tests/*/*.py -vvvrP)

## Run the coverage check 
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run -m pytest tests/*/*.py)

## Run all checks
run-checks: security-test run-black run-pip-audit unit-test check-coverage

