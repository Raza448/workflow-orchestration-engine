import pytest
from unittest.mock import AsyncMock, patch

from core.utils import get_node_key
from worker.worker import KafkaWorker


# -------------------------------------------------------------------
# Fixtures
# -------------------------------------------------------------------


@pytest.fixture
def worker():
    return KafkaWorker()


@pytest.fixture
def valid_task():
    return {
        "execution_id": "exec-123",
        "node_id": "node-1",
        "handler": "call_external_service",
        "config": {"url": "http://example.com"},
    }


# -------------------------------------------------------------------
# _execute_task tests
# -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_task_success(worker, valid_task):
    with patch("worker.worker.redis_client") as mock_redis, patch(
        "worker.worker.OrchestrationEngine"
    ) as mock_engine:

        mock_redis.get_node_state = AsyncMock(return_value=None)

        engine = mock_engine.return_value
        engine.process_node_completion = AsyncMock()

        worker._run_handler_logic = AsyncMock(return_value={"data": "ok"})

        await worker._execute_task(valid_task)

        engine.process_node_completion.assert_awaited_once_with(
            "node-1", {"data": "ok"}, success=True
        )


@pytest.mark.asyncio
async def test_execute_task_skips_if_already_completed(worker, valid_task):
    with patch("worker.worker.redis_client") as mock_redis:
        mock_redis.get_node_state = AsyncMock(return_value={"state": "COMPLETED"})

        worker._run_handler_logic = AsyncMock()

        await worker._execute_task(valid_task)

        worker._run_handler_logic.assert_not_called()


@pytest.mark.asyncio
async def test_execute_task_invalid_schema(worker):
    invalid_task = {"foo": "bar"}

    with patch("worker.worker.redis_client"):
        # Should not raise
        await worker._execute_task(invalid_task)


@pytest.mark.asyncio
async def test_execute_task_retry_then_success(worker, valid_task):
    with patch("worker.worker.redis_client") as mock_redis, patch(
        "worker.worker.OrchestrationEngine"
    ) as mock_engine, patch("asyncio.sleep", new_callable=AsyncMock):

        # Node not completed
        mock_redis.get_node_state = AsyncMock(return_value=None)

        engine = mock_engine.return_value
        engine.process_node_completion = AsyncMock()

        # Fail once, then succeed
        worker._run_handler_logic = AsyncMock(
            side_effect=[Exception("fail once"), {"ok": True}]
        )

        await worker._execute_task(valid_task)

        # Ensure _run_handler_logic called twice (retry)
        assert worker._run_handler_logic.call_count == 2

        # Ensure node completion reported successfully
        engine.process_node_completion.assert_awaited_once_with(
            "node-1", {"ok": True}, success=True
        )


# -------------------------------------------------------------------
# 🔥 FAILED PATH TEST (NEW)
# -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_execute_task_fails_after_max_retries(worker, valid_task):
    with patch("worker.worker.redis_client") as mock_redis, patch(
        "worker.worker.OrchestrationEngine"
    ) as mock_engine, patch("asyncio.sleep", new_callable=AsyncMock):

        mock_redis.get_node_state = AsyncMock(return_value=None)

        engine = mock_engine.return_value
        engine.process_node_completion = AsyncMock()

        worker._run_handler_logic = AsyncMock(side_effect=Exception("always fails"))

        await worker._execute_task(valid_task)

        engine.process_node_completion.assert_awaited_once_with(
            "node-1", {}, success=False
        )


# -------------------------------------------------------------------
# _run_handler_logic tests
# -------------------------------------------------------------------


@pytest.mark.asyncio
async def test_run_handler_logic_unknown_handler(worker):
    result = await worker._run_handler_logic(handler="unknown_handler", config={})

    assert result == {"status": "unhandled"}


@pytest.mark.asyncio
async def test_run_handler_logic_dispatch(worker):
    worker._handler_registry["test_handler"] = AsyncMock(return_value={"ok": True})

    result = await worker._run_handler_logic(handler="test_handler", config={})

    assert result == {"ok": True}
