# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/), and the project adheres to
[Semantic Versioning](https://semver.org/).

## [0.2.0] - 2026-06-30

### Changed
- **Bulk `update_all` / `delete_all` now run concurrently** over a bounded
  thread pool instead of a sequential loop, cutting wall-clock for bulk
  operations to `O(matched / concurrency)`. Measured ~3x (delete) and ~6x
  (update) faster against the local emulator; the gain grows with item count
  and network latency. Tunable via the `bulk_concurrency` provider setting
  (default 32).
- **`update_all` uses server-side `patch_item`** — it writes only the changed
  fields (no full-document read or rewrite), chunked to Cosmos's 10-operations
  -per-patch limit. Lower latency and fewer request units than the previous
  read-then-replace.

## [0.1.0] - 2026-06-30

Initial release. Azure Cosmos DB (NoSQL / Core API) adapter for Protean 0.16.

- Full adapter contract: provider, DAO, model, 12 lookups, entry-point
  registration.
- Partition key defaults to the aggregate `id` (overridable per model);
  `repository.get(id)` is served by a point read.
- Capabilities: `DOCUMENT_STORE | RAW_QUERIES | NATIVE_JSON | NATIVE_ARRAY`.
- Optimistic locking via `_etag`; atomic outbox `_claim`.
- Verified against Protean's official conformance suite (147 passed,
  5 skipped — only the transaction tests Cosmos cannot support).

[0.2.0]: https://github.com/sachyyn/protean-cosmosdb/releases/tag/v0.2.0
[0.1.0]: https://github.com/sachyyn/protean-cosmosdb/releases/tag/v0.1.0
