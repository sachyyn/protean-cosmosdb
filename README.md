# protean-cosmosdb

Azure Cosmos DB (NoSQL / Core API) database adapter for the
[Protean](https://github.com/proteanhq/protean) framework. Built against
Protean 0.16 and `azure-cosmos` 4.x.

## Install

```bash
pip install protean-cosmosdb        # brings in protean + azure-cosmos
```

The provider self-registers via the `protean.providers` entry point, so it's
available under the name `cosmosdb` once installed.

## Configure

```toml
# domain.toml
[databases.default]
provider     = "cosmosdb"
database_uri = "https://<account>.documents.azure.com:443/"
key          = "<primary-key>"
database     = "myapp"     # optional, default "protean"
throughput   = 400         # optional RU/s for created containers
```

```python
domain.providers["default"]._create_database_artifacts()  # create db + containers
```

## Design

| Component | Maps to |
|---|---|
| Provider | `CosmosClient`, one Cosmos database, one container per aggregate/entity |
| DAO | `create_item` / `read_item` / `replace_item` / `delete_item` / `query_items` |
| Model | entity ⇄ JSON item (dict-based) |
| Lookups | Protean filter ops → Cosmos SQL (`=`, `CONTAINS`, `ARRAY_CONTAINS`, `STARTSWITH`, …) |

**Partition key.** Defaults to the aggregate's `id`. This is the principled
mapping, not a shortcut: Protean's consistency boundary is the aggregate, and
Cosmos's consistency boundary is the logical partition — partitioning by `id`
makes them coincide, and `repository.get(id)` becomes a cheap single-partition
point read. Override per aggregate with a custom model:

```python
@domain.database_model(part_of=Order)
class OrderModel:
    _partition_key = "tenant_id"   # container partitioned by /tenant_id
```

**Identity & types.** `id` is stored as a string (Cosmos requires it); UUIDs,
datetimes, dates, Decimals and Enums are JSON-coerced on write and restored on
read. Entity `_version` is stored as `entity_version` to avoid colliding with
Cosmos system fields.

**Optimistic locking.** Aggregate updates are guarded with the item's `_etag`
via `If-Match`, so a concurrent write is rejected with `ExpectedVersionError`
rather than silently overwritten.

## Known ceilings

- **No transactions / rollback.** Cosmos has no cross-document transactions, so
  the provider declares `DOCUMENT_STORE` capabilities (no `TRANSACTIONS`). A
  Unit of Work gives copy-forward semantics, not rollback — same as the
  Elasticsearch adapter.
- **`_filter` runs a separate `COUNT` query** to populate the total for
  pagination; callers that pass `with_total=False` skip it to save RUs.
- **Bulk `update_all` / `delete_all` loop client-side** (Cosmos has no
  server-side update-by-query).
- **Raw queries not supported** (`RAW_QUERIES` capability not declared).

## Test

```bash
pytest tests/                       # pure logic, no Cosmos needed
python tests/test_cosmosdb.py       # same checks, plain CLI

# opt-in end-to-end against a real account or the emulator:
COSMOS_ENDPOINT=... COSMOS_KEY=... pytest tests/test_cosmosdb.py -k live
```
