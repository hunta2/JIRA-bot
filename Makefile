PYPI_SECRET_KEY ?= teamcity-nexus
PYPI_USERNAME ?= fm-pypy
PYPI_URL ?= http://repo.dfman.info:80/repository/df-pypi/simple

APP = trias-jira-bot
ECR_REPOSITORY ?= 460.dkr.ecr.eu-central-1.amazonaws.com/prefect/
APP_VERSION ?= $(shell grep -m 1 version pyproject.toml | tr -s ' ' | tr -d '"' | tr -d "'" | cut -d' ' -f3)
AWS_ACCOUNT_ID = $(shell aws sts get-caller-identity --query 'Account' --output text)
AWS_REGION ?= eu-central-1
AWS_ECR_REGISTRY=${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/

POETRY := $(shell command -v poetry 2> /dev/null)
AWSCLI := $(shell command -v aws 2> /dev/null)

BUILDTIME = $(shell date -u +"%Y%m%d%H%M%S")
BRANCH ?= $(shell git rev-parse --abbrev-ref HEAD)
REVISION ?= $(shell git rev-parse HEAD)

APP_TEST_IMAGE ?= ${ECR_REPOSITORY}${APP}:${APP_VERSION}-${REVISION}-test
APP_RELEASE_IMAGE ?= ${ECR_REPOSITORY}${APP}:${APP_VERSION}

INSTALL_STAMP:=.install.stamp

.DEFAULT_GOAL := help

## pypi_credentials - get PYPI credentials
.PHONY: pypi_credentials
pypi_credentials:
	@$(eval export PYPI_PASSWORD_ESCAPED=$(shell aws secretsmanager get-secret-value --secret-id $(PYPI_SECRET_KEY) --output json | jq '.SecretString' | jq -r | jq -r '.["$(PYPI_USERNAME)"] | @sh' ))
	@$(eval export PYPI_PASSWORD=$(shell aws secretsmanager get-secret-value --secret-id $(PYPI_SECRET_KEY) --output json | jq '.SecretString' | jq -r | jq -r '.["$(PYPI_USERNAME)"]'))
	@echo "Exported PYPI_PASSWORD var for the $(PYPI_USERNAME)"

## config - configure poetry to work with private PyPI
.PHONY: config
config: pypi_credentials
	@$(POETRY) config repositories.df-pypi $(PYPI_URL)
	@$(POETRY) config http-basic.df-pypi $(PYPI_USERNAME) $(PYPI_PASSWORD_ESCAPED)

## install - create virtual environment and setup dependencies from poetry.lock
install: $(INSTALL_STAMP)
$(INSTALL_STAMP): pyproject.toml poetry.lock
	@if [ -z $(POETRY) ]; then echo "Poetry could not be found. See https://python-poetry.org/docs/"; exit 2; fi
	$(POETRY) install
	touch $(INSTALL_STAMP)

## install/test - create virtual environement to run tests and linters
.PHONY:install/test
install/test:
	$(POETRY) install --with=test

## install/dev - create virtual environment with test and dev dependencies
.PHONY:install/dev
install/dev:
	$(POETRY) install --with=test --with=dev

## test - execute unit tests
.PHONY:test
test:
	$(POETRY) run pytest -s -vv tests/ -m "not integration"

## integration - execute integration tests
.PHONY:integration
integration:
	$(POETRY) run pytest -vv tests/ -m "integration"

## lint - execute linters against the codebase
.PHONY:lint
lint:
	$(POETRY) run ruff check .

## lint/fix - fix code automatically
.PHONY:lint/fix
lint/fix:
	$(POETRY) run ruff check . --fix

## typecheck - execute static type checker
.PHONY:typecheck
typecheck:
	$(POETRY) run mypy .

## build/wheel - build wheel package
.PHONY:build/wheel
build/wheel:
	$(POETRY) build -f wheel

## clean - delete trash files
.PHONY: clean
clean:
	@echo "Deleting unnecessary files"
	@rm -rf build/
	@rm -rf dist/
	@rm -rf *.egg-info
	@rm -rf *.pyc
	@rm -rf .*_cache
	@rm -rf __pycache__/
	@find . -name '*.pyc' -delete
	@find . -name '__pycache__' -delete
	@rm -f .install.stamp

## -
## Docker commands:

## docker/login - login to AWS docker, aws configuration need to be set first
.PHONY: docker/login
docker/login:
	@echo "Docker login"
	aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ECR_REGISTRY}


## docker/build/test - build docker image for tests
.PHONY: docker/build/test
docker/build/test: pypi_credentials
	docker build \
		-t ${APP_TEST_IMAGE} \
		--platform=linux/amd64 \
		--build-arg PYPI_URL=${PYPI_URL} \
		--build-arg PYPI_USERNAME=${PYPI_USERNAME} \
		--secret id=PYPI_PASSWORD,env=PYPI_PASSWORD \
		--no-cache \
		--target=test .

## docker/build/release - build docker image
.PHONY: docker/build/release
docker/build/release: pypi_credentials
	docker build \
		-t ${APP_RELEASE_IMAGE} \
		--platform=linux/amd64 \
		--build-arg PYPI_URL=${PYPI_URL} \
		--build-arg PYPI_USERNAME=${PYPI_USERNAME} \
		--secret id=PYPI_PASSWORD,env=PYPI_PASSWORD \
		--no-cache \
		--target=release \
		.

## docker/test - execute tests in docker
.PHONY: docker/test
docker/test:
	@echo "Preload all images required for tests"
	docker pull postgis/postgis:14-3.3
	docker pull 61.dkr.ecr.eu-central-1.amazonaws.com/fm-mart-db-migration:0.0.1

	@echo "Run unit tests in docker"
	docker run --rm \
		--entrypoint= \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD):/src \
		${APP_TEST_IMAGE} make test

## docker/lint - execute linters in docker
.PHONY: docker/lint
docker/lint:
	@echo "Run linters in docker"
	docker run --rm \
		--entrypoint= \
		-v $(PWD):/src \
		${APP_TEST_IMAGE} make lint

## docker/<local target> - universal docker wrapper for local targets
docker/%:
	docker run --rm \
		--entrypoint= \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-v $(PWD):/src \
		${APP_TEST_IMAGE} make $*

## -
## help - this message
.PHONY: help
help: Makefile
	@echo "Application: ${APP}\n"
	@echo "Run command:\n  make <target>\n"
	@grep -E -h '^## .*' $(MAKEFILE_LIST) | sed -n 's/^##//p'  | column -t -s '-' |  sed -e 's/^/ /'
