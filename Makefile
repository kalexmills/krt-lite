##
# Makefile targets for CI and local development.
#

include Makefile.dev

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
	@go test -race -v ./...

.PHONY: test-ci
test-ci:
	@go test -failfast -race -count 10 -shuffle on -v ./...

.PHONY: bench
bench: ## Runs go test with benchmarks.
	@go test -bench=. ./...
##
# list
# Displays a list of targets, using '##' comment as target description
#
# NOTE: ONLY targets with ## comments are shown
#
.PHONY: list
list:


