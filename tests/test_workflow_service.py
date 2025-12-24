import asyncio
import pytest
from schemas import DAGSchema, NodeSchema, WorkflowSchema
from schemas.workflow import (
    NodeState,
)
from services.workflow_service import WorkflowService
from unittest.mock import AsyncMock, patch
from services.workflow_service import redis_client
from core import get_node_key


# --- Mock Redis ---
@pytest.fixture(autouse=True)
def patch_redis():
    with patch("services.workflow_service.redis_client") as mock_redis:
        mock_redis.set_dag = AsyncMock()
        mock_redis.set_workflow_meta = AsyncMock()
        mock_redis.set_node_state = AsyncMock()
        mock_redis.get_workflow_meta = AsyncMock(
            return_value={"status": NodeState.PENDING.value}
        )
        mock_redis.get_runtime_params = AsyncMock(return_value={})
        yield


@pytest.mark.asyncio
async def test_validate_and_build_persists_workflow_and_nodes(linear_workflow):
    service = WorkflowService()

    execution_id = await service._validate_and_build(linear_workflow)

    assert execution_id is not None

    # DAG persisted
    service.redis.set_dag.assert_awaited_once()
    args, _ = service.redis.set_dag.call_args
    assert args[0] == execution_id

    # Workflow meta persisted
    service.redis.set_workflow_meta.assert_awaited_once_with(
        execution_id, NodeState.PENDING.value
    )

    # Node states initialized
    assert service.redis.set_node_state.call_count == len(linear_workflow.dag.nodes)


@pytest.mark.asyncio
async def test_direct_cycle_detection(cyclic_workflow):
    service = WorkflowService()
    with pytest.raises(ValueError, match="Circular dependency"):
        await service._validate_and_build(cyclic_workflow)


@pytest.mark.asyncio
async def test_missing_node_reference_validation(missing_node_workflow):
    service = WorkflowService()
    with pytest.raises(ValueError, match="depends on missing node"):
        await service._validate_and_build(missing_node_workflow)


@pytest.mark.asyncio
async def test_linear_workflow_execution(linear_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(linear_workflow)
    assert execution_id is not None


@pytest.mark.asyncio
async def test_parallel_workflow_execution(parallel_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(parallel_workflow)
    assert execution_id is not None


@pytest.mark.asyncio
async def test_diamond_workflow_execution(diamond_workflow):
    service = WorkflowService()
    execution_id = await service._validate_and_build(diamond_workflow)
    assert execution_id is not None
