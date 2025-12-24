import pytest
from schemas import WorkflowSchema

# --- Sample DAG Fixtures ---


@pytest.fixture
async def redis_client_fixture():
    from core.redis import RedisClient

    client = RedisClient()
    await client.connect()
    yield client
    await client.close()


@pytest.fixture
def mock_redis_client(monkeypatch, redis_client_fixture):
    monkeypatch.setattr("core.redis.redis_client", redis_client_fixture)


@pytest.fixture
def linear_workflow():
    """A -> B -> C"""
    return WorkflowSchema.model_validate(
        {
            "name": "LinearWorkflow",
            "dag": {
                "nodes": [
                    {
                        "id": "A",
                        "handler": "call_external_service",
                        "dependencies": [],
                        "config": {"url": "http://example.com/A"},
                    },
                    {
                        "id": "B",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/B"},
                    },
                    {
                        "id": "C",
                        "handler": "call_external_service",
                        "dependencies": ["B"],
                        "config": {"url": "http://example.com/C"},
                    },
                ]
            },
        }
    )


@pytest.fixture
def parallel_workflow():
    """A -> [B, C]"""
    return WorkflowSchema.model_validate(
        {
            "name": "ParallelWorkflow",
            "dag": {
                "nodes": [
                    {
                        "id": "A",
                        "handler": "call_external_service",
                        "dependencies": [],
                        "config": {"url": "http://example.com/A"},
                    },
                    {
                        "id": "B",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/B"},
                    },
                    {
                        "id": "C",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/C"},
                    },
                ]
            },
        }
    )


@pytest.fixture
def diamond_workflow():
    """A -> [B, C] -> D"""
    return WorkflowSchema.model_validate(
        {
            "name": "DiamondWorkflow",
            "dag": {
                "nodes": [
                    {
                        "id": "A",
                        "handler": "call_external_service",
                        "dependencies": [],
                        "config": {"url": "http://example.com/A"},
                    },
                    {
                        "id": "B",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/B"},
                    },
                    {
                        "id": "C",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/C"},
                    },
                    {
                        "id": "D",
                        "handler": "call_external_service",
                        "dependencies": ["B", "C"],
                        "config": {"url": "http://example.com/D"},
                    },
                ]
            },
        }
    )


@pytest.fixture
def cyclic_workflow():
    """A -> B -> A (cycle)"""
    return WorkflowSchema.model_validate(
        {
            "name": "CyclicWorkflow",
            "dag": {
                "nodes": [
                    {
                        "id": "A",
                        "handler": "call_external_service",
                        "dependencies": ["B"],
                        "config": {"url": "http://example.com/A"},
                    },
                    {
                        "id": "B",
                        "handler": "call_external_service",
                        "dependencies": ["A"],
                        "config": {"url": "http://example.com/B"},
                    },
                ]
            },
        }
    )


@pytest.fixture
def missing_node_workflow():
    """B depends on missing node X"""
    return WorkflowSchema.model_validate(
        {
            "name": "MissingNodeWorkflow",
            "dag": {
                "nodes": [
                    {
                        "id": "B",
                        "handler": "call_external_service",
                        "dependencies": ["X"],
                        "config": {"url": "http://example.com/B"},
                    },
                ]
            },
        }
    )
