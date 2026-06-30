"""Protean database adapter for Azure Cosmos DB (NoSQL / Core API).

Five components, per Protean's adapter contract:
  - CosmosDBProvider  : owns the CosmosClient, database + containers, lifecycle
  - CosmosDBDAO       : CRUD + filtering against a container
  - CosmosDBModel     : entity <-> JSON item conversion (dict-based)
  - lookups           : Protean filter ops -> Cosmos SQL fragments
  - register()        : registers the provider under the name "cosmosdb"

Design decisions (see README):
  - Partition key defaults to the aggregate's `id` (point reads are cheapest,
    and the aggregate boundary == Cosmos's logical-partition boundary).
    Override per model with `class Meta: partition_key = "<field>"`.
  - Capabilities = DOCUMENT_STORE: CRUD, filter, bulk ops, ordering, schema
    management, optimistic locking. No real transactions (Cosmos has none),
    so UoW gives copy-forward semantics, not rollback. Same ceiling as ES.
"""

import logging
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Any
from uuid import UUID

from azure.core import MatchConditions
from azure.cosmos import CosmosClient, PartitionKey, exceptions

from protean.core.database_model import BaseDatabaseModel
from protean.core.queryset import ResultSet
from protean.exceptions import (
    DatabaseError,
    ExpectedVersionError,
    NotSupportedError,
    ObjectNotFoundError,
    ValidationError,
)
from protean.port.dao import BaseDAO, BaseLookup
from protean.port.provider import BaseProvider, DatabaseCapabilities
from protean.utils import IdentityStrategy, IdentityType
from protean.utils.container import Options
from protean.utils.globals import current_domain, current_uow
from protean.utils.query import Q
from protean.utils.reflection import attributes, id_field

logger = logging.getLogger(__name__)


def _jsonify(value: Any) -> Any:
    """Coerce Python values into JSON-serializable forms Cosmos accepts."""
    if isinstance(value, dict):
        return {k: _jsonify(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonify(v) for v in value]
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, Enum):
        return value.value
    return value


# ---------------------------------------------------------------------------
# Model
# ---------------------------------------------------------------------------
class CosmosDBModel(BaseDatabaseModel):
    """Cosmos items are plain JSON dicts, so the model maps entity <-> dict.

    `id` is always written as a string (Cosmos requires it). `_version` is
    stored as `entity_version` to avoid clashing with Cosmos system fields.
    """

    # Field used as the container's partition key. Overridable via Meta.
    _partition_key = "id"

    @classmethod
    def from_entity(cls, entity: Any) -> dict:
        item = cls._entity_to_dict(entity)

        id_name = id_field(cls.meta_.part_of).field_name
        # Cosmos mandates a string property literally named `id`.
        item["id"] = str(item[id_name])

        if "_version" in item:
            item["entity_version"] = item.pop("_version")

        return _jsonify(item)

    @classmethod
    def to_entity(cls, item: dict) -> Any:
        part_of = cls.meta_.part_of

        item_dict: dict[str, Any] = {}
        for attr_name in attributes(part_of):
            item_dict[attr_name] = item.get(attr_name)

        id_name = id_field(part_of).field_name
        item_dict[id_name] = cls._coerce_identity(item["id"])

        if hasattr(part_of, "_version"):
            item_dict["_version"] = item.get("entity_version", -1)

        return part_of(item_dict)

    @classmethod
    def _coerce_identity(cls, value: Any) -> Any:
        """Restore a UUID identity that was stored as a string."""
        if (
            current_domain.config["identity_strategy"] == IdentityStrategy.UUID.value
            and current_domain.config["identity_type"] == IdentityType.UUID.value
            and isinstance(value, str)
        ):
            try:
                return UUID(value)
            except ValueError:
                return value
        return value


# ---------------------------------------------------------------------------
# Session — Cosmos has no transactions, so this is a no-op passthrough.
# ---------------------------------------------------------------------------
class CosmosDBSession:
    def __init__(self, provider):
        self._provider = provider
        self.is_active = True

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


# ---------------------------------------------------------------------------
# DAO
# ---------------------------------------------------------------------------
class CosmosDBDAO(BaseDAO):
    def __repr__(self) -> str:
        return f"CosmosDBDAO <{self.entity_cls.__name__}>"

    # -- helpers ----------------------------------------------------------
    def _container(self):
        return self.provider._container_for(self.database_model_cls)

    def _pk_value(self, item: dict):
        pk = self.database_model_cls._partition_key
        return item.get(pk, item["id"])

    def _build_filters(self, criteria: Q, params: list) -> str:
        """Recursively turn a Q tree into a Cosmos SQL WHERE fragment.

        `params` is the shared parameter list for the query; leaf lookups
        append their `{"name", "value"}` entries to it.
        """
        parts = []
        for child in criteria.children:
            if isinstance(child, Q):
                parts.append(f"({self._build_filters(child, params)})")
            else:
                field, lookup_cls = self.provider._extract_lookup(child[0])
                name = f"@p{len(params)}"
                lookup = lookup_cls(field, child[1])
                lookup.bind(name, params)
                parts.append(lookup.as_expression())

        joiner = " AND " if criteria.connector == criteria.AND else " OR "
        clause = joiner.join(parts) if parts else ""
        if criteria.negated and clause:
            clause = f"NOT ({clause})"
        return clause

    def _where(self, criteria: Q, params: list) -> str:
        if criteria is not None and criteria.children:
            clause = self._build_filters(criteria, params)
            return f" WHERE {clause}" if clause else ""
        return ""

    # -- abstract methods -------------------------------------------------
    def _filter(
        self,
        criteria: Q,
        offset: int = 0,
        limit: int = 10,
        order_by: list = (),
        with_total: bool = True,
        fields: list | None = None,
    ) -> ResultSet:
        container = self._container()
        params: list = []
        where = self._where(criteria, params)

        projection = "*"
        if fields:
            cols = {"c.id"} | {f"c.{f}" for f in fields}
            projection = ", ".join(sorted(cols))

        sql = f"SELECT {projection} FROM c{where}"

        if order_by:
            cols = []
            for key in order_by:
                if key.startswith("-"):
                    cols.append(f"c.{key[1:]} DESC")
                else:
                    cols.append(f"c.{key} ASC")
            sql += " ORDER BY " + ", ".join(cols)

        if limit is not None:
            sql += f" OFFSET {offset} LIMIT {limit}"
        elif offset:
            sql += f" OFFSET {offset} LIMIT 2147483647"

        try:
            items = list(container.query_items(query=sql, parameters=params))
        except exceptions.CosmosHttpResponseError as exc:
            raise DatabaseError(
                f"Database error during filtering: {exc}", original_exception=exc
            )

        # Cosmos does not return a match count for free. Run a COUNT only when
        # the caller needs the total (pagination), otherwise report page size.
        # ponytail: separate COUNT query costs extra RUs; with_total=False skips it.
        if with_total:
            total = self._count(criteria)
        else:
            total = offset + len(items)

        return ResultSet(offset=offset, limit=limit, total=total, items=items)

    def _create(self, model_obj: dict):
        try:
            self._container().create_item(body=model_obj)
        except exceptions.CosmosResourceExistsError:
            raise ValidationError(
                {
                    "_entity": f"`{self.entity_cls.__name__}` object with identifier "
                    f"{model_obj['id']} is already present."
                }
            )
        except exceptions.CosmosHttpResponseError as exc:
            raise DatabaseError(
                f"Database error during creation: {exc}", original_exception=exc
            )
        return model_obj

    def _update(self, model_obj: dict, expected_version: int | None = None):
        container = self._container()
        idv, pk = model_obj["id"], self._pk_value(model_obj)

        try:
            existing = container.read_item(item=idv, partition_key=pk)
        except exceptions.CosmosResourceNotFoundError:
            raise ObjectNotFoundError(
                f"`{self.entity_cls.__name__}` object with identifier {idv} "
                f"does not exist."
            )

        if expected_version is not None:
            stored = existing.get("entity_version")
            if stored != expected_version:
                raise ExpectedVersionError(
                    f"Wrong expected version: {expected_version} "
                    f"(Aggregate: {self.entity_cls.__name__}({idv}), Version: {stored})"
                )

        try:
            if expected_version is not None:
                # Atomic OCC: the write is rejected if the item changed since
                # our read (etag mismatch), closing the read-modify-write race.
                container.replace_item(
                    item=idv,
                    body=model_obj,
                    etag=existing["_etag"],
                    match_condition=MatchConditions.IfNotModified,
                )
            else:
                container.replace_item(item=idv, body=model_obj)
        except exceptions.CosmosAccessConditionFailedError as exc:
            raise ExpectedVersionError(
                f"Wrong expected version: {expected_version} "
                f"(Aggregate: {self.entity_cls.__name__}({idv}))"
            ) from exc
        except exceptions.CosmosHttpResponseError as exc:
            raise DatabaseError(
                f"Database error during update: {exc}", original_exception=exc
            )
        return model_obj

    def _delete(self, model_obj: dict):
        try:
            self._container().delete_item(
                item=model_obj["id"], partition_key=self._pk_value(model_obj)
            )
        except exceptions.CosmosResourceNotFoundError:
            raise ObjectNotFoundError(
                f"`{self.entity_cls.__name__}` object with identifier "
                f"{model_obj['id']} does not exist."
            )
        except exceptions.CosmosHttpResponseError as exc:
            raise DatabaseError(
                f"Database error during deletion: {exc}", original_exception=exc
            )
        return model_obj

    def _update_all(self, criteria: Q, *args, **kwargs):
        # Cosmos has no server-side update-by-query; read, merge, replace.
        # ponytail: client-side loop, O(matched) round-trips. Fine for the
        # outbox/projection-rebuild paths this method serves.
        values: dict[str, Any] = {}
        if args:
            values.update(args[0])
        values.update(kwargs)
        if not values:
            return 0

        container = self._container()
        params: list = []
        sql = f"SELECT * FROM c{self._where(criteria, params)}"

        count = 0
        for item in container.query_items(query=sql, parameters=params):
            item.update(_jsonify(values))
            container.replace_item(item=item["id"], body=item)
            count += 1
        return count

    def _delete_all(self, criteria: Q = None):
        container = self._container()
        params: list = []
        sql = f"SELECT * FROM c{self._where(criteria, params)}"

        count = 0
        for item in container.query_items(query=sql, parameters=params):
            container.delete_item(item=item["id"], partition_key=self._pk_value(item))
            count += 1
        return count

    def _count(self, criteria: Q) -> int:
        params: list = []
        sql = f"SELECT VALUE COUNT(1) FROM c{self._where(criteria, params)}"
        result = list(self._container().query_items(query=sql, parameters=params))
        return result[0] if result else 0

    def _raw(self, query: Any, data: Any = None):
        raise NotSupportedError(
            f"Provider '{self.provider.name}' ({self.provider.__class__.__name__}) "
            "does not support raw queries"
        )

    def has_table(self) -> bool:
        try:
            self._container().read()
            return True
        except exceptions.CosmosResourceNotFoundError:
            return False


# ---------------------------------------------------------------------------
# Provider
# ---------------------------------------------------------------------------
class CosmosDBProvider(BaseProvider):
    __database__ = "cosmosdb"

    def __init__(self, name, domain, conn_info: dict):
        super().__init__(name, domain, conn_info)

        # The client is created lazily: azure-cosmos performs endpoint
        # discovery (a network call) when CosmosClient is constructed, so we
        # defer it until the first real operation rather than at config time.
        self._client = None
        self._database_name = conn_info.get("database", "protean")
        self._throughput = conn_info.get("throughput", 400)
        self._database_model_classes: dict[str, type] = {}

    @property
    def capabilities(self) -> DatabaseCapabilities:
        return DatabaseCapabilities.DOCUMENT_STORE

    @property
    def client(self) -> CosmosClient:
        if self._client is None:
            self._client = CosmosClient(
                self.conn_info["database_uri"], credential=self.conn_info["key"]
            )
        return self._client

    # -- sessions / connections ------------------------------------------
    def get_session(self):
        return CosmosDBSession(self)

    def get_connection(self):
        return CosmosDBSession(self)

    def is_alive(self) -> bool:
        try:
            self.client.get_database_account()
            return True
        except Exception:
            return False

    def close(self) -> None:
        # The azure-cosmos sync client manages its own HTTP pool; nothing to
        # explicitly dispose in current SDK versions.
        pass

    # -- container access -------------------------------------------------
    def _database(self):
        return self.client.get_database_client(self._database_name)

    def _container_for(self, database_model_cls):
        return self._database().get_container_client(
            database_model_cls.derive_schema_name()
        )

    # -- DAO / model construction ----------------------------------------
    def get_dao(self, entity_cls, database_model_cls):
        return CosmosDBDAO(self.domain, self, entity_cls, database_model_cls)

    def construct_database_model_class(self, entity_cls):
        cache_key = entity_cls.meta_.schema_name
        if cache_key in self._database_model_classes:
            return self._database_model_classes[cache_key]

        meta_ = Options()
        meta_.part_of = entity_cls

        model_cls = type(
            entity_cls.__name__ + "Model",
            (CosmosDBModel,),
            {"meta_": meta_},
        )
        self._database_model_classes[cache_key] = model_cls
        return model_cls

    def decorate_database_model_class(self, entity_cls, database_model_cls):
        cache_key = entity_cls.meta_.schema_name
        if cache_key in self._database_model_classes:
            return self._database_model_classes[cache_key]

        if issubclass(database_model_cls, CosmosDBModel):
            self._database_model_classes[cache_key] = database_model_cls
            return database_model_cls

        custom_attrs = {
            k: v
            for (k, v) in vars(database_model_cls).items()
            if k not in ["Meta", "__module__", "__doc__", "__weakref__", "__dict__"]
        }
        meta_ = Options()
        meta_.part_of = entity_cls
        custom_attrs["meta_"] = meta_
        # Honor a custom partition key declared on the user model.
        custom_attrs.setdefault(
            "_partition_key", getattr(database_model_cls, "_partition_key", "id")
        )

        decorated = type(
            database_model_cls.__name__,
            (CosmosDBModel, database_model_cls),
            custom_attrs,
        )
        self._database_model_classes[cache_key] = decorated
        return decorated

    def _raw(self, query: Any, data: Any = None):
        raise NotSupportedError(
            f"Provider '{self.name}' ({self.__class__.__name__}) "
            "does not support raw queries"
        )

    # -- lifecycle --------------------------------------------------------
    def _registered_models(self):
        """Yield (entity_cls, model_cls) for every non-event-sourced element
        registered against this provider."""
        elements = {
            **self.domain.registry.aggregates,
            **self.domain.registry.entities,
            **self.domain.registry.projections,
        }
        for _, record in elements.items():
            cls = record.cls
            if getattr(cls.meta_, "is_event_sourced", False):
                continue
            part_of = getattr(cls.meta_, "part_of", None)
            if part_of and getattr(part_of.meta_, "is_event_sourced", False):
                continue
            if current_domain.providers[cls.meta_.provider] is not self:
                continue
            yield cls, self.domain.repository_for(cls)._database_model

    def _create_database_artifacts(self) -> None:
        self.client.create_database_if_not_exists(id=self._database_name)
        db = self._database()
        for _, model_cls in self._registered_models():
            pk_path = f"/{model_cls._partition_key}"
            db.create_container_if_not_exists(
                id=model_cls.derive_schema_name(),
                partition_key=PartitionKey(path=pk_path),
                offer_throughput=self._throughput,
            )

    def _drop_database_artifacts(self) -> None:
        db = self._database()
        for _, model_cls in self._registered_models():
            try:
                db.delete_container(model_cls.derive_schema_name())
            except exceptions.CosmosResourceNotFoundError:
                pass

    def _data_reset(self) -> None:
        db = self._database()
        for _, model_cls in self._registered_models():
            container = db.get_container_client(model_cls.derive_schema_name())
            try:
                for item in container.query_items(query="SELECT c.id FROM c"):
                    container.delete_item(item=item["id"], partition_key=item["id"])
            except exceptions.CosmosResourceNotFoundError:
                pass
        if current_uow and current_uow.in_progress:
            current_uow.rollback()


# ---------------------------------------------------------------------------
# Lookups -> Cosmos SQL fragments
# ---------------------------------------------------------------------------
class CosmosLookup(BaseLookup):
    """Base lookup. `bind()` wires in the parameter name and the shared
    parameter list before `as_expression()` is called."""

    def bind(self, param_name: str, params_out: list) -> "CosmosLookup":
        self.param_name = param_name
        self.params_out = params_out
        return self

    @property
    def col(self) -> str:
        return f"c.{self.process_source()}"

    def process_target(self):
        return _jsonify(self.target)

    def _param(self, value=None):
        """Register the bound parameter and return its placeholder name."""
        self.params_out.append(
            {"name": self.param_name, "value": self.process_target() if value is None else value}
        )
        return self.param_name


@CosmosDBProvider.register_lookup
class Exact(CosmosLookup):
    lookup_name = "exact"

    def as_expression(self):
        return f"{self.col} = {self._param()}"


@CosmosDBProvider.register_lookup
class IExact(CosmosLookup):
    lookup_name = "iexact"

    def as_expression(self):
        return f"STRINGEQUALS({self.col}, {self._param()}, true)"


@CosmosDBProvider.register_lookup
class GreaterThan(CosmosLookup):
    lookup_name = "gt"

    def as_expression(self):
        return f"{self.col} > {self._param()}"


@CosmosDBProvider.register_lookup
class GreaterThanOrEqual(CosmosLookup):
    lookup_name = "gte"

    def as_expression(self):
        return f"{self.col} >= {self._param()}"


@CosmosDBProvider.register_lookup
class LessThan(CosmosLookup):
    lookup_name = "lt"

    def as_expression(self):
        return f"{self.col} < {self._param()}"


@CosmosDBProvider.register_lookup
class LessThanOrEqual(CosmosLookup):
    lookup_name = "lte"

    def as_expression(self):
        return f"{self.col} <= {self._param()}"


@CosmosDBProvider.register_lookup
class In(CosmosLookup):
    lookup_name = "in"

    def process_target(self):
        assert isinstance(self.target, (list, tuple))
        return [_jsonify(v) for v in self.target]

    def as_expression(self):
        return f"ARRAY_CONTAINS({self._param()}, {self.col})"


@CosmosDBProvider.register_lookup
class Contains(CosmosLookup):
    lookup_name = "contains"

    def as_expression(self):
        return f"CONTAINS({self.col}, {self._param()}, false)"


@CosmosDBProvider.register_lookup
class IContains(CosmosLookup):
    lookup_name = "icontains"

    def as_expression(self):
        return f"CONTAINS({self.col}, {self._param()}, true)"


@CosmosDBProvider.register_lookup
class Startswith(CosmosLookup):
    lookup_name = "startswith"

    def as_expression(self):
        return f"STARTSWITH({self.col}, {self._param()})"


@CosmosDBProvider.register_lookup
class Endswith(CosmosLookup):
    lookup_name = "endswith"

    def as_expression(self):
        return f"ENDSWITH({self.col}, {self._param()})"


@CosmosDBProvider.register_lookup
class IsNull(CosmosLookup):
    lookup_name = "isnull"

    def as_expression(self):
        # No parameter: presence is expressed structurally.
        if self.target:
            return f"(NOT IS_DEFINED({self.col}) OR IS_NULL({self.col}))"
        return f"(IS_DEFINED({self.col}) AND NOT IS_NULL({self.col}))"


def register() -> None:
    """Register the Cosmos DB provider with Protean, if azure-cosmos is present."""
    from protean.port.provider import registry

    try:
        import azure.cosmos  # noqa: F401

        registry.register(
            "cosmosdb", "protean_cosmosdb.cosmosdb.CosmosDBProvider"
        )
        logger.debug("Cosmos DB provider registered successfully")
    except ImportError as e:
        logger.debug(f"Cosmos DB provider not registered: {e}")
