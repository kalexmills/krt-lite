# krt-lite

istio/krt, without Istio dependencies.

### This repo is a work-in-progress.

## Why KRT?

- Structuring your controller as a dependency graph that events can flow through.
    - This is the correct domain model for a controller.
- Allowing intermediate state to be modeled in the same way as external events.
- Allowing events from sources other than Kubernetes.

## Roadmap
- [X] Joiner -- joining collections into one.
- [ ] StaticCollection -- collections that don't change
- [ ] Splitter -- splitting collections into two.
- [ ] Indexer -- indexing an existing collection for quicker lookup.
- [ ] Fetch -- fetching from a collection and tracking dependencies.
- [ ] CollectionOption -- tweaking how collectors work in various ways.
- [ ]