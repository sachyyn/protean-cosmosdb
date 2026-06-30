"""Conftest for running Protean's official adapter conformance suite against
protean-cosmosdb.

The conformance tests themselves ship in Protean's *source* tree (not the
wheel). To run them against this adapter:

    git clone --branch v0.16.0 https://github.com/proteanhq/protean.git
    GEN="protean/tests/adapters/repository/generic"
    cp tests/conformance_conftest.py "$GEN/conftest.py"   # replace in-tree conftest
    export COSMOS_ENDPOINT="http://localhost:8081/" COSMOS_KEY="<emulator-key>"
    pytest "$GEN" -p no:cacheprovider

This conftest loads the official conformance plugin and points its db_config
at a Cosmos endpoint. Capability-gated tests for features this adapter does
not declare (transactions, raw queries, native json/array) are auto-skipped
by the plugin based on the provider's `capabilities`.
"""
import os

import pytest

pytest_plugins = ["protean.integrations.pytest.adapter_conformance"]

from protean_cosmosdb.cosmosdb import register  # noqa: E402

register()


@pytest.fixture(scope="session")
def db_config():
    return {
        "provider": "cosmosdb",
        "database_uri": os.environ["COSMOS_ENDPOINT"],
        "key": os.environ["COSMOS_KEY"],
        "database": "conformance",
    }
