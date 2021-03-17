REPO := logdna-agent-v2

# The image repo and tag can be modified e.g.
# `make build RUST_IMAGE=docker.io/rust:latest
RUST_IMAGE_REPO ?= docker.io/logdna/build-images
RUST_IMAGE_TAG ?= rust-buster-stable
RUST_IMAGE ?= $(RUST_IMAGE_REPO):$(RUST_IMAGE_TAG)
RUST_IMAGE := $(RUST_IMAGE)

SHELLCHECK_IMAGE_REPO ?= koalaman/shellcheck-alpine
SHELLCHECK_IMAGE_TAG ?= stable
SHELLCHECK_IMAGE ?= $(SHELLCHECK_IMAGE_REPO):$(SHELLCHECK_IMAGE_TAG)
SHELLCHECK_IMAGE := $(SHELLCHECK_IMAGE)

WORKDIR :=/build
DOCKER := DOCKER_BUILDKIT=1 docker
DOCKER_DISPATCH := ./docker/dispatch.sh "$(WORKDIR)" "$(shell pwd):/build:Z"

export CARGO_CACHE ?= $(shell pwd)/.cargo_cache
RUST_COMMAND := $(DOCKER_DISPATCH) $(RUST_IMAGE)
SHELLCHECK_COMMAND := $(DOCKER_DISPATCH) $(SHELLCHECK_IMAGE)

$(info $(LOGDNA_HOST))
$(info $(LOGDNA_INGESTION_KEY))
.PHONY:check
check: ## Run unit tests
	$(RUST_COMMAND) "" "cargo check --all-targets"

test-local:
	$(RUST_COMMAND) "" "cargo test --lib --release $(TESTS) -- --skip it_works --nocapture"
.PHONY:help

.PHONY:test
test: ## Run unit tests
	$(RUST_COMMAND) "--env RUST_BACKTRACE=full --env RUST_LOG=$(RUST_LOG) --env LOGDNA_HOST=$(LOGDNA_HOST) --env API_KEY=$(LOGDNA_INGESTION_KEY) " "cargo test --no-run && cargo test --lib --release $(TESTS) -- --nocapture"

.PHONY:clean
clean: ## Clean all artifacts from the build process
	$(RUST_COMMAND) "" "rm -fr target/* \$$CARGO_HOME/registry/* \$$CARGO_HOME/git/*"

.PHONY:clean-docker
clean-docker: ## Cleans the intermediate and final agent images left over from the build-image target
	@# Clean any agent images, left over from the multi-stage build
	if [[ ! -z "$(shell docker images -q $(REPO):$(CLEAN_TAG))" ]]; then docker images -q $(REPO):$(CLEAN_TAG) | xargs docker rmi -f; fi

.PHONY:clean-all
clean-all: clean-docker clean ## Deep cleans the project and removed any docker images
	git clean -xdf

.PHONY:lint-format
lint-format: ## Checks for formatting errors
	$(RUST_COMMAND) "--env RUST_BACKTRACE=full" "cargo fmt -- --check"

.PHONY:lint-clippy
lint-clippy: ## Checks for code errors
	$(RUST_COMMAND) "--env RUST_BACKTRACE=full" "cargo clippy --all-targets -- -D warnings"

.PHONY:lint-audit
lint-audit: ## Audits packages for issues
	$(RUST_COMMAND) "--env RUST_BACKTRACE=full" "cargo audit"

.PHONY:lint-shell
lint-shell: ## Lint the Dockerfile for issues
	$(SHELLCHECK_COMMAND) "" "shellcheck docker/lib.sh"
	$(SHELLCHECK_COMMAND) "" "shellcheck docker/dispatch.sh"

.PHONY:lint
lint: lint-shell lint-format lint-clippy lint-audit ## Runs all the linters

help: ## Prints out a helpful description of each possible target
	@awk 'BEGIN {FS = ":.*?## "}; /^.+: .*?## / && !/awk/ {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)
	@$(SHELL) -c "echo '$(TEST_RULES)'" |  awk 'BEGIN {FS = ":.*?<> "}; /^.+: .*?<> / && !/awk/ {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'
