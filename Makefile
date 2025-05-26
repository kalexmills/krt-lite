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
	@CGO_ENABLED=1 GOTRACEBACK=all go test -failfast -race -v ./...

.PHONY: test-ci
test-ci: ## Runs go test 10 times
	@CGO_ENABLED=1 GOTRACEBACK=all go test -coverprofile=cover.out -count 10 -race -shuffle on -v ./...

.PHONY: test-isolate-leak
test-isolate-leak: ## Runs each test individually to detect leak failures.
	@go test -c -o tests.tmp
	@for test in $$(go test -list . | grep -E "^(Test|Example)"); do ./tests.tmp -test.run "^$$test\$$" &>/dev/null && echo -n "." || echo -e "\n$$test failed"; done
	@rm tests.tmp

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

ENVTEST?=setup-envtest
ENVTEST_K8S_VERSION?=1.33
LOCALBIN=.bin

.PHONY: bench-envtest
bench-envtest:
	mkdir -p .bin
	KUBEBUILDER_ASSETS="$(shell pwd)/$(shell $(ENVTEST) use $(ENVTEST_K8S_VERSION) -i --bin-dir $(LOCALBIN) -p path)" \
		go test -bench '^.*$$' -benchtime 20x -run ^$$ ./internal/bench