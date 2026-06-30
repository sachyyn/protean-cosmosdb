# protean-cosmosdb

[![CI](https://github.com/sachyyn/protean-cosmosdb/actions/workflows/ci.yml/badge.svg)](https://github.com/sachyyn/protean-cosmosdb/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/protean-cosmosdb.svg)](https://pypi.org/project/protean-cosmosdb/)
[![Python](https://img.shields.io/pypi/pyversions/protean-cosmosdb.svg)](https://pypi.org/project/protean-cosmosdb/)
[![License: MIT](https://img.shields.io/pypi/l/protean-cosmosdb.svg)](LICENSE)

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

## Quickstart

After `pip install protean-cosmosdb`, no registration code is needed — the
provider is discovered automatically. Just name it in config and use the
repository:

```python
from protean.domain import Domain
from protean.fields import String

domain = Domain(name="MyApp")
domain.config["databases"]["default"] = {
    "provider": "cosmosdb",                      # auto-discovered, no import needed
    "database_uri": "https://<account>.documents.azure.com:443/",
    "key": "<primary-key>",
    "database": "myapp",
}

@domain.aggregate
class Note:
    text = String(max_length=100)

domain.init(traverse=False)
with domain.domain_context():
    domain.providers["default"]._create_database_artifacts()   # one-time setup
    repo = domain.repository_for(Note)

    note = Note(text="hello")
    repo.add(note)
    assert repo.get(note.id).text == "hello"     # point read
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

## Protean compliance

Verified against **Protean's official adapter conformance suite** (the same
generic test battery its built-in adapters run), executed against the Linux
Cosmos emulator:

```
147 passed, 5 skipped
```

The only 5 skips are the transaction tests (`transactions`,
`atomic_transactions`) — genuinely impossible on Cosmos, which has no
cross-document transactions, so the capability isn't declared and the plugin
skips them. Everything else passes: CRUD, filtering, ordering, bulk
operations, raw queries, schema management, optimistic locking, value
objects, associations, complex fields, persistence, querysets, and native
JSON / array storage. This same suite runs in CI on every push (see
`.github/workflows/ci.yml`), with the Cosmos emulator as a service.

### Running the conformance suite

The suite ships in Protean's source tree (not the wheel). To run it against
this adapter:

```bash
# 1. Protean source at the matching version provides the generic tests
git clone --branch v0.16.0 https://github.com/proteanhq/protean.git
GEN="protean/tests/adapters/repository/generic"

# 2. A conftest that loads the official plugin and points it at Cosmos:
cat > "$GEN/conftest_cosmos.py" <<'PY'
# (see tests/conformance_conftest.py in this repo)
PY

# 3. Run with the emulator up:
export COSMOS_ENDPOINT="http://localhost:8081/" COSMOS_KEY="<emulator-key>"
pytest "$GEN" -p no:cacheprovider
```

`tests/conformance_conftest.py` in this repo is the ready-made conftest
(loads `protean.integrations.pytest.adapter_conformance` and supplies the
Cosmos `db_config`); copy it next to the generic tests, isolated from
Protean's in-tree conftests.

### Contract coverage

Implements the full adapter contract for protean 0.16:

- **All 9 DAO abstract methods**: `_filter` (with `with_total` + `only()`
  column projection), `_create`, `_update`, `_update_all`, `_delete`,
  `_delete_all`, `_count`, `_raw`, `has_table`.
- **All 12 provider abstract methods** + the `capabilities` property.
  `validate_lookups()` reports no missing lookups.
- **All 12 required lookups**: exact, iexact, contains, icontains, startswith,
  endswith, gt, gte, lt, lte, in, isnull.
- **Bulk / batch surface**: `QuerySet.update()` → `_update_all`,
  `QuerySet.delete()` → `_delete_all`, and the outbox primitives
  `_delete_top` (bounded batch delete) and `_claim` (atomic select-and-stamp,
  via etag-conditional replace) — all exercised in the live suite.
- **Raw queries**: `provider.raw()` and `QuerySet.raw()` run Cosmos SQL
  (parameterized) and return raw results / entities respectively.
- **Capabilities** = `DOCUMENT_STORE | RAW_QUERIES | NATIVE_JSON |
  NATIVE_ARRAY`: CRUD, FILTER, BULK_OPERATIONS, ORDERING, SCHEMA_MANAGEMENT,
  OPTIMISTIC_LOCKING, RAW_QUERIES, NATIVE_JSON, NATIVE_ARRAY.

Covered by the conformance suite: value objects, aggregate associations, and
native nested JSON / array fields.

### Performance

`repository.get(id)` is served by a Cosmos **point read** (`read_item` by id +
partition key) — the cheapest operation (~1 RU) — rather than a
cross-partition query. `_filter` detects a sole `id == value` lookup on an
id-partitioned container and takes this fast path automatically; all other
queries go through the SQL path.

## Known ceilings

- **No transactions / rollback.** Cosmos has no cross-document transactions, so
  the provider declares `DOCUMENT_STORE` capabilities (no `TRANSACTIONS`). A
  Unit of Work gives copy-forward semantics, not rollback — same as the
  Elasticsearch adapter.
- **`_filter` runs a separate `COUNT` query** to populate the total for
  pagination; callers that pass `with_total=False` skip it to save RUs.
  (`get(id)` bypasses this entirely via the point-read fast path above.)
- **Bulk `update_all` / `delete_all` loop client-side** (Cosmos has no
  server-side update-by-query). `_claim`, by contrast, *is* atomic: it uses an
  etag-conditional replace per row, so a concurrent consumer that loses the
  race is rejected (412) and skips — no double-claim.

## Test

```bash
pytest tests/                       # pure logic, no Cosmos needed
python tests/test_cosmosdb.py       # same checks, plain CLI
```

The live suite (`test_live_*`) is opt-in and runs the full CRUD + every
lookup + ordering + pagination + optimistic-locking path against a real
endpoint. It auto-skips unless `COSMOS_ENDPOINT` and `COSMOS_KEY` are set.

Verified against the Linux Cosmos emulator (NoSQL API, gateway mode):

```bash
docker run --detach --name cosmos-emu \
  -p 8081:8081 -p 8080:8080 -p 1234:1234 \
  mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-latest

# wait for readiness, then run:
curl -s http://localhost:8080/ready          # 200 when ready

export COSMOS_ENDPOINT="http://localhost:8081/"
export COSMOS_KEY="C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw=="
pytest tests/ -q
```

The emulator defaults to HTTP, which the Python SDK supports directly (only
the .NET/Java SDKs require HTTPS + certificate install). Note the emulator
treats Request Units / throughput as a no-op, so `offer_throughput` is
accepted but not enforced there.

## Releasing to PyPI

Publishing is automated via **PyPI Trusted Publishing** (OIDC) — no API tokens
are stored anywhere. The trusted publisher (repo `sachyyn/protean-cosmosdb`,
workflow `release.yml`, environment `pypi`) is already configured, so cutting a
release is just:

```bash
# 1. bump `version` in pyproject.toml, commit
# 2. tag and push — .github/workflows/release.yml builds and publishes
git tag vX.Y.Z
git push origin vX.Y.Z
```

Locally, `python -m build && python -m twine check dist/*` reproduces exactly
what CI builds and validates before upload.
