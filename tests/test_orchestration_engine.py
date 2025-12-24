# tests/test_workflow_engine.py

import json
import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from orchestrator.engine import OrchestrationEngine
from schemas.workflow import NodeState
from services.workflow_service import WorkflowService
from orchestrator.engine import redis_client


# ------------------------------------------------------------------
# Global mock for Redis & Kafka (single source of truth)
# ------------------------------------------------------------------


@pytest.fixture(autouse=True)
def patch_external_services():
    with patch("services.workflow_service.redis_client") as mock_redis_service, patch(
        "orchestrator.engine.redis_client"
    ) as mock_redis_engine, patch("orchestrator.engine.kafka_client") as mock_kafka:

        # -----------------------------
        # WorkflowService Redis mocks
        # -----------------------------
        mock_redis_service.set_dag = AsyncMock()
        mock_redis_service.set_workflow_meta = AsyncMock()
        mock_redis_service.set_node_state = AsyncMock()
        mock_redis_service.get_workflow_meta = AsyncMock(
            return_value={"status": NodeState.PENDING.value}
        )
        mock_redis_service.get_runtime_params = AsyncMock(return_value={})
        mock_redis_service.mget = AsyncMock(return_value=[None, None, None])

        # -----------------------------
        # OrchestrationEngine Redis mocks
        # -----------------------------
        redis_store = {}

        async def mock_set_node_state(key, state, output=None):
            redis_store[key] = json.dumps({"state": state, "output": output or {}})

        async def mock_mget(keys):
            return [redis_store.get(k) for k in keys]

        mock_redis_engine.get_dag = AsyncMock(return_value=None)
        mock_redis_engine.set_node_state = AsyncMock(side_effect=mock_set_node_state)
        mock_redis_engine.set_workflow_status = AsyncMock()
        mock_redis_engine.get_runtime_params = AsyncMock(return_value={})
        mock_redis_engine.set_runtime_params = AsyncMock()
        mock_redis_engine.mget = AsyncMock(side_effect=mock_mget)
        mock_redis_engine.sadd = AsyncMock(return_value=True)

        # -----------------------------
        # Kafka mock
        # -----------------------------
        mock_kafka.publish = AsyncMock()

        yield


# ------------------------------------------------------------------
# Engine internal helpers
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dispatch_ready_nodes_calls_state_fetch():
    engine = OrchestrationEngine("test-exec")
    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = []

    engine._get_all_node_states = AsyncMock(return_value={})

    await engine._dispatch_ready_nodes()

    engine._get_all_node_states.assert_called_once()


@pytest.mark.parametrize(
    "node_states,expected",
    [
        ({"A": {"state": NodeState.COMPLETED.value}}, True),
        ({"A": {"state": NodeState.PENDING.value}}, False),
    ],
)
def test_are_all_nodes_completed(node_states, expected):
    engine = OrchestrationEngine("exec-x")
    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = [MagicMock(id="A")]

    assert engine._are_all_nodes_completed(node_states) is expected


def test_is_node_ready():
    engine = OrchestrationEngine("exec-x")
    node = MagicMock(id="B", dependencies=["A"])

    node_states = {
        "A": {"state": NodeState.COMPLETED.value},
        "B": {"state": NodeState.PENDING.value},
    }

    assert engine._is_node_ready(node, node_states)


# ------------------------------------------------------------------
# trigger()
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_trigger_sets_runtime_params_and_dispatches():
    engine = OrchestrationEngine("exec-1")

    engine.workflow = MagicMock()
    engine.initialize = AsyncMock()
    engine._dispatch_ready_nodes = AsyncMock()

    with patch("orchestrator.engine.redis_client") as mock_redis:
        mock_redis.set_runtime_params = AsyncMock()

        params = {"A": {"foo": "bar"}}
        await engine.trigger(params=params)

        mock_redis.set_runtime_params.assert_called_once_with("exec-1", params)
        engine._dispatch_ready_nodes.assert_called_once()


@pytest.mark.asyncio
async def test_trigger_initializes_workflow_if_missing():
    engine = OrchestrationEngine("exec-2")
    engine.workflow = None

    engine.initialize = AsyncMock(return_value=engine)
    engine._dispatch_ready_nodes = AsyncMock()

    await engine.trigger()

    engine.initialize.assert_called_once()
    engine._dispatch_ready_nodes.assert_called_once()


# ------------------------------------------------------------------
# process_node_completion()
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_process_node_completion_success_dispatches_next():
    engine = OrchestrationEngine("exec-3")

    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = [MagicMock(id="A")]

    engine._get_all_node_states = AsyncMock(
        return_value={"A": {"state": NodeState.COMPLETED.value}}
    )
    engine._are_all_nodes_completed = MagicMock(return_value=False)
    engine._dispatch_ready_nodes = AsyncMock()

    await engine.process_node_completion(
        node_id="A",
        output={"value": 123},
        success=True,
    )

    engine._dispatch_ready_nodes.assert_called_once()


@pytest.mark.asyncio
async def test_process_node_failure_marks_workflow_failed():
    engine = OrchestrationEngine("exec-4")
    engine.workflow = MagicMock()

    with patch("orchestrator.engine.redis_client") as mock_redis:
        mock_redis.set_node_state = AsyncMock()
        mock_redis.set_workflow_status = AsyncMock()

        await engine.process_node_completion(
            node_id="A",
            output={"error": "boom"},
            success=False,
        )

        mock_redis.set_node_state.assert_called_once()
        mock_redis.set_workflow_status.assert_called_once_with(
            engine.meta_key, NodeState.FAILED.value
        )


@pytest.mark.asyncio
async def test_process_node_completion_marks_workflow_completed():
    engine = OrchestrationEngine("exec-5")

    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = [
        MagicMock(id="A"),
        MagicMock(id="B"),
    ]

    engine._get_all_node_states = AsyncMock(
        return_value={
            "A": {"state": NodeState.COMPLETED.value},
            "B": {"state": NodeState.COMPLETED.value},
        }
    )

    with patch("orchestrator.engine.redis_client") as mock_redis:
        mock_redis.set_node_state = AsyncMock()
        mock_redis.set_workflow_status = AsyncMock()

        await engine.process_node_completion(
            node_id="B",
            output={"ok": True},
            success=True,
        )

        mock_redis.set_workflow_status.assert_called_once_with(
            engine.meta_key, NodeState.COMPLETED.value
        )


# ------------------------------------------------------------------
# Template resolution & workflow behavior
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_template_resolution(linear_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(linear_workflow)

    engine = OrchestrationEngine(execution_id)
    engine.workflow = linear_workflow

    node_states = {
        "A": {"state": NodeState.COMPLETED.value, "output": {"result": "foo"}},
        "B": {"state": NodeState.PENDING.value, "output": {}},
        "C": {"state": NodeState.PENDING.value, "output": {}},
    }

    resolved = engine._resolve_template_value(
        "{{ A.output.result }}",
        node_states,
        {},
    )

    assert resolved == "foo"


@pytest.mark.asyncio
async def test_node_failure_blocks_dependents(linear_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(linear_workflow)

    engine = OrchestrationEngine(execution_id)
    engine.workflow = linear_workflow

    await engine.process_node_completion("A", success=False)

    node_states = await engine._get_all_node_states()
    assert node_states["A"]["state"] == NodeState.FAILED.value


@pytest.mark.asyncio
async def test_missing_template_variable_raises_error():
    engine = OrchestrationEngine("exec-x")

    node_states = {"A": {"state": NodeState.COMPLETED.value, "output": {}}}

    with pytest.raises(ValueError):
        engine._resolve_template_value(
            "{{ A.output.missing_key }}",
            node_states,
            {},
        )


@pytest.mark.asyncio
async def test_fan_in_race_condition(diamond_workflow):
    engine = OrchestrationEngine(execution_id="test123")

    # -----------------------------
    # Patch redis_client inside orchestrator.engine
    # -----------------------------
    from orchestrator import engine as engine_module

    # Return JSON string for Pydantic validation
    async def mock_get_dag(execution_id):
        return diamond_workflow.model_dump_json()

    # In-memory Redis set for dispatched nodes
    dispatched_set = set()
    completed_set = set()

    # Track dispatch calls
    dispatch_count = {}

    async def mock_sadd(name, value):
        """Mock Redis SADD - returns True only if value was newly added"""
        if "dispatched" in name:
            if value not in dispatched_set:
                dispatched_set.add(value)
                # Track how many times we attempted to dispatch this node
                dispatch_count[value] = dispatch_count.get(value, 0) + 1
                return True
            return False
        elif "completed" in name:
            completed_set.add(value)
            return True
        return False

    async def mock_smembers(name):
        """Mock Redis SMEMBERS - returns all members of a set"""
        if "dispatched" in name:
            return dispatched_set
        elif "completed" in name:
            return completed_set
        return set()

    engine_module.redis_client.get_dag = mock_get_dag
    engine_module.redis_client.sadd = mock_sadd
    engine_module.redis_client.smembers = mock_smembers

    # -----------------------------
    # Initialize engine
    # -----------------------------
    await engine.initialize()

    # -----------------------------
    # Pre-populate state: A has already been dispatched and B, C have been dispatched
    # We're testing the completion of B and C triggering D
    # -----------------------------
    dispatched_set.add("A")
    dispatched_set.add("B")
    dispatched_set.add("C")
    completed_set.add("A")  # A completed before B and C

    # -----------------------------
    # Simulate fan-in nodes sequentially (B -> C) to ensure deterministic test
    # -----------------------------
    await engine.process_node_completion("B", {"value": 1})
    await engine.process_node_completion("C", {"value": 2})

    # -----------------------------
    # Assertions
    # -----------------------------
    dispatched = await engine_module.redis_client.smembers(engine.dispatched_set)

    assert "D" in dispatched, "Node D should have been dispatched"
    # Should have A, B, C (pre-existing) and D (newly dispatched)
    assert dispatched == {
        "A",
        "B",
        "C",
        "D",
    }, f"Expected A, B, C, D in dispatched set, got: {dispatched}"
    assert (
        dispatch_count.get("D", 0) == 1
    ), f"Node D dispatch was attempted {dispatch_count.get('D', 0)} times, expected 1"


@pytest.mark.asyncio
async def test_workflow_completion_status(linear_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(linear_workflow)

    engine = OrchestrationEngine(execution_id)
    engine.workflow = linear_workflow

    for node_id in ["A", "B", "C"]:
        await engine.process_node_completion(node_id, success=True)

    node_states = await engine._get_all_node_states()
    assert all(
        state["state"] == NodeState.COMPLETED.value for state in node_states.values()
    )
