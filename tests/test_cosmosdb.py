"""Runnable check for the Cosmos DB adapter — no live Cosmos required.

Exercises the bug-prone, pure logic against the *real* Protean machinery:
  - model from_entity / to_entity round-trip (id->str, _version remap, JSON coercion)
  - every registered lookup -> Cosmos SQL fragment + parameter
  - _build_filters composing AND / OR / negation / nesting

CosmosClient is lazy (no network until an operation runs), so we build a real
provider + DAO with dummy credentials and never touch Azure.

Run:  pytest tests/test_cosmosdb.py    (or: python tests/test_cosmosdb.py)

A live end-to-end test against the emulator/account is gated below on
COSMOS_ENDPOINT + COSMOS_KEY env vars.
"""

import os
from datetime import datetime, timezone

from protean.domain import Domain
from protean.exceptions import ExpectedVersionError, ObjectNotFoundError
from protean.fields import DateTime, Integer, String
from protean.utils.query import Q

from protean_cosmosdb.cosmosdb import CosmosDBProvider, register

DUMMY_CONN = {
    "provider": "cosmosdb",
    "database_uri": "https://localhost:8081",  # never contacted in unit tests
    "key": "dummy-key",
    "database": "test",
}


def _domain():
    register()  # ensure "cosmosdb" is in the provider registry
    # No network in unit tests: domain.init() pings is_alive() to verify the
    # connection. We're testing query/model logic, not connectivity, so stub it.
    CosmosDBProvider.is_alive = lambda self: True
    domain = Domain(name="Test")
    domain.config["databases"]["default"] = DUMMY_CONN

    @domain.aggregate
    class Person:
        name = String(max_length=50)
        age = Integer()
        status = String(max_length=20, default="active")
        joined = DateTime()

    domain.init(traverse=False)
    return domain, Person


def _dao(domain, Person):
    provider = domain.providers["default"]
    assert isinstance(provider, CosmosDBProvider)
    model_cls = provider.construct_database_model_class(Person)
    return provider.get_dao(Person, model_cls), model_cls


def test_model_round_trip():
    domain, Person = _domain()
    with domain.domain_context():
        _, model_cls = _dao(domain, Person)
        joined = datetime(2026, 6, 30, 12, 0, tzinfo=timezone.utc)
        person = Person(name="Ada", age=37, joined=joined)

        item = model_cls.from_entity(person)
        assert isinstance(item["id"], str)                 # Cosmos needs string id
        assert item["name"] == "Ada"
        assert item["joined"] == joined.isoformat()        # datetime -> ISO string
        assert "entity_version" in item and "_version" not in item  # version remapped

        restored = model_cls.to_entity(item)
        assert restored.name == "Ada"
        assert restored.age == 37
        assert str(restored.id) == item["id"]
    print("ok: model round-trip")


def test_lookups_to_sql():
    domain, Person = _domain()
    with domain.domain_context():
        dao, _ = _dao(domain, Person)

        cases = {
            "name": ("c.name = @p0", "Ada"),                              # exact
            "name__iexact": ("STRINGEQUALS(c.name, @p0, true)", "ada"),
            "age__gt": ("c.age > @p0", 18),
            "age__gte": ("c.age >= @p0", 18),
            "age__lt": ("c.age < @p0", 65),
            "age__lte": ("c.age <= @p0", 65),
            "name__contains": ("CONTAINS(c.name, @p0, false)", "d"),
            "name__icontains": ("CONTAINS(c.name, @p0, true)", "D"),
            "name__startswith": ("STARTSWITH(c.name, @p0)", "A"),
            "name__endswith": ("ENDSWITH(c.name, @p0)", "a"),
        }
        for key, (expected_sql, value) in cases.items():
            params = []
            clause = dao._build_filters(Q(**{key: value}), params)
            assert clause == expected_sql, f"{key}: {clause!r} != {expected_sql!r}"
            assert params == [{"name": "@p0", "value": value}], f"{key}: {params}"

        # `in` -> ARRAY_CONTAINS with a list parameter
        params = []
        clause = dao._build_filters(Q(status__in=["active", "pending"]), params)
        assert clause == "ARRAY_CONTAINS(@p0, c.status)"
        assert params == [{"name": "@p0", "value": ["active", "pending"]}]

        # isnull -> structural, no parameter
        params = []
        clause = dao._build_filters(Q(age__isnull=True), params)
        assert clause == "(NOT IS_DEFINED(c.age) OR IS_NULL(c.age))"
        assert params == []
    print("ok: lookups -> SQL")


def test_build_filters_composition():
    domain, Person = _domain()
    with domain.domain_context():
        dao, _ = _dao(domain, Person)

        # AND of two leaves, distinct parameters (Protean flattens the group)
        params = []
        clause = dao._build_filters(Q(name="Ada") & Q(age__gt=18), params)
        assert clause == "c.name = @p0 AND c.age > @p1"
        assert params == [
            {"name": "@p0", "value": "Ada"},
            {"name": "@p1", "value": 18},
        ]

        # OR
        params = []
        clause = dao._build_filters(Q(status="active") | Q(status="pending"), params)
        assert clause == "c.status = @p0 OR c.status = @p1"

        # Negation wraps the whole group
        params = []
        clause = dao._build_filters(~Q(name="Ada"), params)
        assert clause == "NOT (c.name = @p0)"

        # Empty / degenerate Q tree (delete_all/filter with no criteria) must
        # NOT emit "()" — that produces invalid Cosmos SQL. Regression guard.
        params = []
        assert dao._build_filters(Q() & Q(), params) == ""
        assert params == []
    print("ok: build_filters composition")


# --- Live integration (opt-in) ---------------------------------------------
def _live_domain():
    """A domain wired to a real Cosmos endpoint, or None if not configured."""
    endpoint, key = os.getenv("COSMOS_ENDPOINT"), os.getenv("COSMOS_KEY")
    if not (endpoint and key):
        return None, None

    register()
    domain = Domain(name="Live")
    domain.config["databases"]["default"] = {
        "provider": "cosmosdb",
        "database_uri": endpoint,
        "key": key,
        "database": "protean_test",
    }

    @domain.aggregate
    class Product:
        name = String(max_length=50)
        category = String(max_length=30)
        price = Integer()
        notes = String(max_length=100)  # optional -> exercises isnull

    domain.init(traverse=False)
    return domain, Product


def test_live_crud_round_trip():
    """Create / read / update / delete against a real Cosmos endpoint."""
    domain, Product = _live_domain()
    if domain is None:
        print("skip: live test (set COSMOS_ENDPOINT + COSMOS_KEY)")
        return

    with domain.domain_context():
        provider = domain.providers["default"]
        provider._create_database_artifacts()
        provider._data_reset()
        repo = domain.repository_for(Product)

        p = Product(name="Widget", category="tools", price=10)
        repo.add(p)

        fetched = repo.get(p.id)
        assert fetched.name == "Widget" and fetched.price == 10

        fetched.price = 12
        repo.add(fetched)
        assert repo.get(p.id).price == 12

        repo._dao.delete(repo.get(p.id))
        try:
            repo.get(p.id)
            assert False, "expected ObjectNotFoundError after delete"
        except ObjectNotFoundError:
            pass

        provider._data_reset()
    print("ok: live CRUD round-trip")


def test_live_queries():
    """Every lookup + ordering + pagination, executed by the real engine."""
    domain, Product = _live_domain()
    if domain is None:
        print("skip: live test (set COSMOS_ENDPOINT + COSMOS_KEY)")
        return

    with domain.domain_context():
        provider = domain.providers["default"]
        provider._create_database_artifacts()
        provider._data_reset()
        repo = domain.repository_for(Product)

        rows = [
            ("Alpha", "tools", 10, "handy"),
            ("Beta", "tools", 25, None),       # notes absent -> isnull match
            ("Gamma", "garden", 25, "leafy"),
            ("Delta", "garden", 40, "muddy"),
            ("Epsilon", "office", 5, "paper"),
        ]
        for name, category, price, notes in rows:
            repo.add(Product(name=name, category=category, price=price, notes=notes))

        q = repo._dao.query

        def names(resultset):
            return sorted(p.name for p in resultset)

        # exact
        assert names(q.filter(category="tools").all()) == ["Alpha", "Beta"]
        # gt / gte / lt / lte
        assert names(q.filter(price__gt=25).all()) == ["Delta"]
        assert names(q.filter(price__gte=25).all()) == ["Beta", "Delta", "Gamma"]
        assert names(q.filter(price__lt=10).all()) == ["Epsilon"]
        assert names(q.filter(price__lte=10).all()) == ["Alpha", "Epsilon"]
        # in
        assert names(q.filter(category__in=["garden", "office"]).all()) == [
            "Delta", "Epsilon", "Gamma",
        ]
        # contains / icontains / startswith / endswith
        assert names(q.filter(name__contains="lph").all()) == ["Alpha"]
        assert names(q.filter(name__icontains="ALPH").all()) == ["Alpha"]
        assert names(q.filter(name__startswith="A").all()) == ["Alpha"]
        assert names(q.filter(name__endswith="a").all()) == [
            "Alpha", "Beta", "Delta", "Gamma",
        ]
        # iexact
        assert names(q.filter(category__iexact="TOOLS").all()) == ["Alpha", "Beta"]
        # isnull (Beta has no notes)
        assert names(q.filter(notes__isnull=True).all()) == ["Beta"]
        assert "Beta" not in names(q.filter(notes__isnull=False).all())

        # AND composition
        assert names(q.filter(category="garden", price__gte=40).all()) == ["Delta"]

        # ordering + pagination + total
        page = q.filter().order_by("price").limit(2).all()
        assert [p.name for p in page] == ["Epsilon", "Alpha"]
        assert page.total == 5  # COUNT across all rows, not just the page
        page2 = q.filter().order_by("-price").offset(0).limit(2).all()
        assert [p.name for p in page2] == ["Delta", "Beta"] or [
            p.name for p in page2
        ] == ["Delta", "Gamma"]  # Beta/Gamma tie at 25

        # total reflects full match count even with a page limit
        tools_page = q.filter(category="tools").limit(1).all()
        assert len(tools_page) == 1 and tools_page.total == 2

        provider._data_reset()
    print("ok: live queries (all lookups + ordering + pagination)")


def test_live_optimistic_locking():
    """A stale aggregate write is rejected with ExpectedVersionError."""
    domain, Product = _live_domain()
    if domain is None:
        print("skip: live test (set COSMOS_ENDPOINT + COSMOS_KEY)")
        return

    with domain.domain_context():
        provider = domain.providers["default"]
        provider._create_database_artifacts()
        provider._data_reset()
        repo = domain.repository_for(Product)

        p = Product(name="Lock", category="x", price=1)
        repo.add(p)

        first = repo.get(p.id)
        second = repo.get(p.id)  # same version as `first`

        first.price = 2
        repo.add(first)  # version advances in the store

        second.price = 3
        try:
            repo.add(second)  # stale -> must be rejected
            assert False, "expected ExpectedVersionError on stale write"
        except ExpectedVersionError:
            pass

        assert repo.get(p.id).price == 2  # the first write won
        provider._data_reset()
    print("ok: live optimistic locking")


def test_live_bulk_operations():
    """Every bulk path Protean exposes, run against the real engine:
    QuerySet.update() -> _update_all, QuerySet.delete() -> _delete_all,
    and the outbox primitives _delete_top and _claim (portable defaults
    that lean on column projection via QuerySet.only())."""
    domain, Product = _live_domain()
    if domain is None:
        print("skip: live test (set COSMOS_ENDPOINT + COSMOS_KEY)")
        return

    with domain.domain_context():
        provider = domain.providers["default"]
        provider._create_database_artifacts()
        provider._data_reset()
        repo = domain.repository_for(Product)
        dao = repo._dao

        for i in range(6):
            repo.add(Product(name=f"P{i}", category="bulk", price=i))

        # update_all: bump price for a subset, leave the rest untouched
        updated = dao.query.filter(price__gte=3).update(category="hi")
        assert updated == 3
        assert {p.name for p in dao.query.filter(category="hi").all()} == {
            "P3", "P4", "P5",
        }
        assert dao.query.filter(category="bulk").all().total == 3

        # _delete_top: bounded delete (used by outbox cleanup) — drains in batches
        from protean.utils.query import Q as _Q

        crit = dao.query.filter(category="bulk")._criteria
        n1 = dao._delete_top(crit, limit=2)
        assert n1 == 2
        n2 = dao._delete_top(crit, limit=10)
        assert n2 == 1
        assert dao.query.filter(category="bulk").all().total == 0

        # _claim: atomically select + stamp rows (outbox consumer primitive)
        claim_crit = dao.query.filter(category="hi")._criteria
        claimed = dao.outside_uow()._claim(claim_crit, {"category": "claimed"}, limit=2)
        assert len(claimed) == 2
        assert all(c.category == "claimed" for c in claimed)
        assert dao.query.filter(category="claimed").all().total == 2

        # delete_all with NO filter -> empty Q tree; must not emit "WHERE ()"
        repo.add(Product(name="leftover", category="z", price=99))
        deleted = dao.query.delete()
        assert deleted >= 1
        assert dao.query.all().total == 0

        provider._data_reset()
    print("ok: live bulk operations (update_all, delete_all, _delete_top, _claim)")


if __name__ == "__main__":
    import logging

    logging.disable(logging.CRITICAL)  # quiet Protean's debug logs for the CLI run
    test_model_round_trip()
    test_lookups_to_sql()
    test_build_filters_composition()
    test_live_crud_round_trip()
    test_live_queries()
    test_live_optimistic_locking()
    test_live_bulk_operations()
    print("\nAll checks passed.")
