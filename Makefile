##
# Makefile targets for CI and local development.
#


THISFILE := $(realpath $(firstword $(MAKEFILE_LIST)))

# Contains trailing '/'
#
PWD := $(dir $(THISFILE))

.DEFAULT_GOAL := help

##
# help
# Displays a (hopefully) useful help screen to the user
#
# NOTE: Keep 'help' as first target in case .DEFAULT_GOAL is not honored
# NOTE: ONLY targets with ## comments are shown
#
.PHONY: help
help: list ## Shows this help screen.
	@echo
	@echo  "Make targets:"
	@echo
	@cat $(THISFILE)* | \
	sed -n -E 's/^([^.][^: ]+)\s*:(([^=#]*##\s*(.*[^[:space:]])\s*)|[^=].*)$$/    \1	\4/p' | \
	expand -t15
	@echo

.PHONY: lint
lint: ## Runs go fmt.
	@golangci-lint run

.PHONY: test
test: ## Runs go test.
	@CGO_ENABLED=1 GOTRACEBACK=all go test -race -v ./...

.PHONY: test-ci
test-ci:
	@CGO_ENABLED=1 GOTRACEBACK=all go test -coverprofile=cover.out -failfast -count 10 -race -shuffle on -v ./...

.PHONY: bench
bench: ## Runs go test with benchmarks.
	@go test -bench=. -run=^$$ ./...
##
# list
# Displays a list of targets, using '##' comment as target description
#
# NOTE: ONLY targets with ## comments are shown
#
.PHONY: list
list:


##
# Make targets for local development.
#

.PHONY: run-gh-actions
run-gh-actions: ## Runs GitHub actions using nektos/act.
	@act -P linux/amd64=nektos/act-environments-ubuntu:latest --container-architecture linux/amd64 --matrix golang-version:1.23