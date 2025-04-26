![golang-ci](https://github.com/kalexmills/krt-lite/actions/workflows/golang-ci.yml/badge.svg?branch=main)
[![Go Coverage](https://github.com/USER/REPO/wiki/coverage.svg)](https://raw.githack.com/wiki/USER/REPO/coverage.html)
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
  - [ ] Conformance tests.
- [X] StaticCollection -- collections that don't change
- [X] Indexer -- indexing an existing collection for quicker lookup.
- [X] Fetch -- fetching from a collection and tracking dependencies.
  - [ ] UnregisterHandler
- [ ] Filtering -- Filtering collections and on fetch.
- [ ] CollectionOption -- tweaking how collectors work in various ways.
  - [X] WithName Collection names (very needed for debugging)
  - [X] WithStop
  - [ ] WithDebugging
- [ ] DiscardResult on Context.
- [ ] Pluggable logging via slog.
- [ ] Benchmarking.
- [ ] Examples.
- [ ] Documentation.

## Known Issues
- [ ] cache.WaitForCacheSync polls with an uncontrollable 100ms delay, it should be removed everywhere (also in Informer, if possible).
