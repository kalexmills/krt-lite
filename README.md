![golang-ci](https://github.com/kalexmills/krt-lite/actions/workflows/golang-ci.yml/badge.svg?branch=main)
![GitHub go.mod Go version](https://img.shields.io/github/go-mod/go-version/kalexmills/krt-lite)
![GitHub License](https://img.shields.io/github/license/kalexmills/krt-lite)


# krt-lite

istio/krt, without Istio.

*** WORK IN PROGRESS ***

## Why KRT?

Writing Kubernetes controllers is easier when you can:
- Structure your controller as a dependency graph that update events can flow through.
- Allow intermediate state to be modeled in the same way as external events.
- Handle incoming events from sources other than Kubernetes.

## Roadmap
- [X] Joiner -- joining collections into one.
- [X] StaticCollection -- collections that don't change
- [ ] Indexer -- indexing an existing collection for quicker lookup.
- [ ] Fetch -- fetching from a collection and tracking dependencies.
- [ ] CollectionOption -- tweaking how collectors work in various ways.
- [ ] Pluggable logging via slog.