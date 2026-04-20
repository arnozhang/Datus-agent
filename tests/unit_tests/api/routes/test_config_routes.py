# Copyright 2025-present DatusAI, Inc.
# Licensed under the Apache License, Version 2.0.

"""Unit tests for datus/api/routes/config_routes.py."""

from unittest.mock import MagicMock

import pytest

from datus.api.routes.config_routes import (
    get_agent_config_endpoint,
    get_database_types,
    get_llm_providers,
)


def _mock_svc(namespaces, *, target="deepseek", current_namespace="starrocks", models=None, home="~/.datus"):
    svc = MagicMock()
    svc.agent_config.target = target
    svc.agent_config.models = models if models is not None else {}
    svc.agent_config.current_namespace = current_namespace
    svc.agent_config.namespaces = namespaces
    svc.agent_config.home = home
    return svc


@pytest.mark.asyncio
async def test_get_agent_config_flattens_matching_inner_key():
    """When inner key matches the namespace name, that entry is returned flat."""
    starrocks_cfg = {"logic_name": "starrocks", "type": "starrocks", "host": "h1"}
    starrocks22_cfg = {"logic_name": "starrocks22", "type": "starrocks", "host": "h2"}
    svc = _mock_svc(
        namespaces={
            "starrocks": {"starrocks": starrocks_cfg},
            "starrocks22": {"starrocks22": starrocks22_cfg},
        },
    )

    result = await get_agent_config_endpoint(svc)

    assert result.success is True
    assert result.data["namespaces"] == {
        "starrocks": starrocks_cfg,
        "starrocks22": starrocks22_cfg,
    }
    assert result.data["target"] == "deepseek"
    assert result.data["current_namespace"] == "starrocks"
    assert result.data["home"] == "~/.datus"


@pytest.mark.asyncio
async def test_get_agent_config_falls_back_to_first_inner_value():
    """When inner key does not match namespace name, first inner value is used."""
    inner_cfg = {"logic_name": "db_a", "type": "duckdb"}
    svc = _mock_svc(namespaces={"my_ns": {"db_a": inner_cfg}})

    result = await get_agent_config_endpoint(svc)

    assert result.data["namespaces"] == {"my_ns": inner_cfg}


@pytest.mark.asyncio
async def test_get_agent_config_skips_empty_inner_dict():
    """Namespaces with empty inner dicts are dropped from the response."""
    real_cfg = {"logic_name": "real", "type": "duckdb"}
    svc = _mock_svc(namespaces={"empty": {}, "real": {"real": real_cfg}})

    result = await get_agent_config_endpoint(svc)

    assert result.data["namespaces"] == {"real": real_cfg}


@pytest.mark.asyncio
async def test_get_agent_config_handles_empty_namespaces():
    svc = _mock_svc(namespaces={})

    result = await get_agent_config_endpoint(svc)

    assert result.data["namespaces"] == {}


@pytest.mark.asyncio
async def test_get_llm_providers_returns_known_templates():
    result = await get_llm_providers()
    assert result.success is True
    assert "openai" in result.data.providers
    assert result.data.default == "openai"


@pytest.mark.asyncio
async def test_get_database_types_returns_known_templates():
    result = await get_database_types()
    assert result.success is True
    types = {item.type for item in result.data.database_types}
    assert {"postgresql", "mysql", "starrocks", "duckdb", "snowflake"} <= types
