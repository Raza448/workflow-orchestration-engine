import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from pytest_asyncio.plugin import event_loop_policy
from orchestrator.engine import OrchestrationEngine
from schemas.workflow import NodeState, WorkflowSchema
import json


@pytest.fixture(scope="session")
def event_loop():
    policy = event_loop_policy()
    asyncio.set_event_loop_policy(policy)
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
def mock_redis_client():
    with patch("core.redis.RedisClient.connect", new_callable=AsyncMock) as mock_connect:
        mock_connect.return_value = MagicMock()
        mock_connect.return_value.ping = AsyncMock()
        mock_connect.return_value.get = AsyncMock(return_value='{"name": "Test Workflow", "dag": {"nodes": []}}')
        mock_connect.return_value.hset = AsyncMock()
        mock_connect.return_value.hgetall = AsyncMock(return_value={})
        mock_connect.return_value.set = AsyncMock()
        mock_connect.return_value.expire = AsyncMock()
        yield mock_connect


@pytest.mark.asyncio
async def test_initialize():
    engine = OrchestrationEngine("test_execution_id")
    with patch("core.redis_client.get_dag", new_callable=AsyncMock) as mock_get_dag:
        mock_get_dag.return_value = '{"name": "Test Workflow", "dag": {"nodes": []}}'  # Provide a valid mock response

        with patch(
            "services.workflow_service.WorkflowService.get_status",  # Updated to match existing method
            new_callable=AsyncMock,
        ) as mock_get_status:
            mock_get_status.return_value = MagicMock(name="Test Workflow")
            mock_get_status.return_value.name = "Test Workflow"

            await engine.initialize()
            assert engine.workflow.name == "Test Workflow"


@pytest.mark.asyncio
async def test_trigger(mock_redis_client):
    engine = OrchestrationEngine("test_execution_id")
    engine.initialize = AsyncMock()
    engine._dispatch_ready_nodes = AsyncMock()

    # Test without params
    await engine.trigger()
    engine.initialize.assert_called_once()
    engine._dispatch_ready_nodes.assert_called_once()
    mock_redis_client.return_value.hset.assert_not_called()

    # Reset mocks
    engine.initialize.reset_mock()
    engine._dispatch_ready_nodes.reset_mock()
    mock_redis_client.return_value.hset.reset_mock()

    # Test with params
    params = {"key": "value"}
    await engine.trigger(params=params)
    mock_redis_client.return_value.hset.assert_called_once_with(
        "workflows:test_execution_id:params", mapping={"key": json.dumps("value")}
    )
    engine.initialize.assert_called_once()
    engine._dispatch_ready_nodes.assert_called_once()


@pytest.mark.asyncio
async def test_dispatch_ready_nodes():
    engine = OrchestrationEngine("test_execution_id")
    engine._get_all_node_states = AsyncMock(return_value={})
    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = []

    with patch(
        "core.redis_client.get_runtime_params", new_callable=AsyncMock
    ) as mock_get_runtime_params:
        mock_get_runtime_params.return_value = {}
        await engine._dispatch_ready_nodes()

    engine._get_all_node_states.assert_called_once()


# Mock Redis interactions to avoid real Redis calls
@pytest.mark.asyncio
async def test_process_node_completion():
    import asyncio

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    engine = OrchestrationEngine("test_execution_id")
    engine.initialize = AsyncMock()
    engine._get_all_node_states = AsyncMock(return_value={})
    engine._dispatch_ready_nodes = AsyncMock()
    engine.workflow = MagicMock()
    engine.workflow.dag = MagicMock()

    with patch(
        "core.redis_client.set_node_state", new_callable=AsyncMock
    ) as mock_set_node_state, patch(
        "core.redis_client.set_workflow_status", new_callable=AsyncMock
    ) as mock_set_workflow_status:
        await engine.process_node_completion("node_1", success=True)
        mock_set_node_state.assert_called()
        mock_set_workflow_status.assert_called()

    loop.close()


@pytest.mark.parametrize(
    "node_states,expected",
    [
        ({"node_1": {"state": NodeState.COMPLETED.value}}, True),
        ({"node_1": {"state": NodeState.PENDING.value}}, False),
    ],
)
def test_are_all_nodes_completed(node_states, expected):
    engine = OrchestrationEngine("test_execution_id")
    engine.workflow = MagicMock()
    engine.workflow.dag.nodes = [MagicMock(id="node_1")]

    result = engine._are_all_nodes_completed(node_states)
    assert result == expected


def test_is_node_ready():
    engine = OrchestrationEngine("test_execution_id")
    node = MagicMock(id="node_1", dependencies=["node_0"])
    node_states = {
        "node_0": {"state": NodeState.COMPLETED.value},
        "node_1": {"state": NodeState.PENDING.value},
    }

    assert engine._is_node_ready(node, node_states)
