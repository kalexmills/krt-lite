# Introduction

Thank you for considering contributing! We'd love to get your help.

## Contributions 

krt-lite is still pre-production, so we are most interested in bug reports, bug-fixes, improved documentation, and additional tests/benchmarks/examples.

Before contributing additional features, please start by [filing an issue](https://github.com/kalexmills/krt-lite/issues) for discussion. Please provide example usage for any API changes you propose. 

Performance optimizations are welcome, but we may ask you to delay implementation if the library is not stable enough to support them, or if more test coverage is needed.

### Getting Started

Clone the repository and install Make. We provide a minimal Makefile that runs the same commands used in CI. Run `make help` to see what targets are available.

### Before Opening a Pull Request

Make sure your change is covered by appropriate tests, that all tests and linter checks pass in CI.

The below commands should all pass before opening a PR. 
```shell
make lint
make test
```

Our CI runs the test suite multiple times to check for flakes, so you may find a failure in CI even if you don't see one locally. Running `make test-ci` may help you to reproduce locally.

We use [goleak](https://github.com/uber-go/goleak) to check for leaked goroutines after tests. You will need to ensure your tests clean up any stale goroutines before finishing. If LeakTest is failing for your tests, running `make test-isolate-leak` may help to diagnose the issue. Please open a bug report for any leaks you find on main.

### Code Review

PRs are reviewed by project maintainers, who are volunteers. Please be patient in waiting on a response, and be open to making any requested changes.

Feature requests in particular tend to garner more PR feedback than other sorts of requests. You can help cut down on review iterations by [filing an issue](https://github.com/kalexmills/krt-lite/issues) to discuss your feature before writing code.

Reviewers will aim to adhere to the standards laid out in [Effective Go](https://go.dev/doc/effective_go) and [CodeReviewComments](https://go.dev/wiki/CodeReviewComments).

## Support Requests

If you have a question about how to use krt-lite, please use the [Discussions](https://github.com/kalexmills/krt-lite/discussions) channel instead of filing an issue.

If something is not working as expected, please [use this link to file a Bug Report](https://github.com/kalexmills/krt-lite/issues/new?template=bug_report.md).

## Community Expectations

This project follows the [Go Community Code of Conduct](https://go.dev/conduct).
