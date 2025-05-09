![golang-ci](https://github.com/kalexmills/krt-lite/actions/workflows/golang-ci.yml/badge.svg?branch=main)
![coverage](https://raw.githubusercontent.com/kalexmills/krt-lite/badges/.badges/main/coverage.svg)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kalexmills/krt-lite)
![GitHub License](https://img.shields.io/github/license/kalexmills/krt-lite)

# krt-lite

istio/krt, without Istio.

## Why KRT?

Writing Kubernetes controllers is easier when you can:
- Structure your controller as a dependency graph that update events can flow through.
- Allow intermediate state to be modeled in the same way as external events.
- Handle incoming events from sources other than Kubernetes.

## How do I use it?
This library is not yet production-ready. Check out our [sample application](https://github.com/kalexmills/multitenancy)
for an example.
