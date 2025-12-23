import asyncio
import random
import signal
import json
from core.redis import redis_client
from core.logging import get_logger
from core.kafka import kafka_client
from core.utils import get_node_key
from core.constants import WORKFLOW_TASK_TOPIC
from orchestrator import OrchestrationEngine

logger = get_logger(__name__)


class KafkaWorker:
    def __init__(self):
        """Initializes the worker with a shutdown signal tracker and handler registry."""
        self.consumer = None
        self._shutdown_event = asyncio.Event()
        self._handler_registry = {
            "call_external_service": self._handle_call_external_service,
            "call_llm_service": self._handle_call_llm_service,
            "output": self._handle_output,
            "input": self._handle_input,
        }

    async def start(self):
        """
        Main worker loop. Connects to Kafka and processes tasks until shutdown.
        """
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: self._shutdown_event.set())

        await kafka_client.connect()
        self.consumer = await kafka_client.get_consumer(
            WORKFLOW_TASK_TOPIC, group_id="workflow-workers"
        )

        logger.info("Worker active. Listening for tasks...")
        try:
            while not self._shutdown_event.is_set():
                try:
                    msg = await asyncio.wait_for(self.consumer.getone(), timeout=1.0)
                    task_data = (
                        msg.value
                        if isinstance(msg.value, dict)
                        else json.loads(msg.value)
                    )
                    await self._execute_task(task_data)
                    await self.consumer.commit()
                except asyncio.TimeoutError:
                    continue
                except Exception as e:
                    logger.error(f"Worker Loop Error: {e}")
        finally:
            await self.consumer.stop()
            await kafka_client.disconnect()

    async def _execute_task(self, task):
        """
        Executes a task with a pre-flight idempotency check.

        Args:
            task (dict): The task configuration and execution metadata.
        """
        execution_id, node_id = task["execution_id"], task["node_id"]

        # IDEMPOTENCY: Do not execute if already completed
        node_key = get_node_key(execution_id, node_id)
        state_data = await redis_client.get_node_state(node_key)
        if state_data and state_data.get("state") == "COMPLETED":
            logger.info(f"Node {node_id} already finished. Skipping.")
            return

        try:
            output = await self._run_handler_logic(
                task["handler"], task.get("config", {}), node_id
            )
            engine = OrchestrationEngine(execution_id)
            await engine.process_node_completion(node_id, output, success=True)
        except Exception as e:
            logger.error(f"Execution Error in {node_id}: {e}")
            engine = OrchestrationEngine(execution_id)
            await engine.process_node_completion(node_id, {}, success=False)

    async def _run_handler_logic(self, handler, config, node_id):
        """
        Executes the registered handler logic.

        Args:
            handler (str): The logic handler type.
            config (dict): Configuration with resolved templates.
            node_id (str): The ID of the node.

        Returns:
            dict: The output to be stored in the state store.
        """
        handler_func = self._handler_registry.get(handler)
        if not handler_func:
            logger.warning(f"Handler {handler} not found. Skipping.")
            return {"status": "unhandled"}

        return await handler_func(config, node_id)

    async def _handle_call_external_service(self, config, node_id):
        print(f"Calling external service at {config} for node {node_id}")
        """Handles the 'call_external_service' logic."""
        await asyncio.sleep(random.uniform(0.5, 1.0))
        return {"data": f"Response from {config.get('url')}"}

    async def _handle_call_llm_service(self, config, node_id):
        """Handles the 'call_llm_service' logic."""
        await asyncio.sleep(1.5)
        return {
            "result": f"LLM generated a summary for the prompt '{config.get('prompt')}'"
        }

    async def _handle_output(self, config, node_id):
        """Handles the 'output' logic."""
        aggregated_data = config.get("__parent_outputs__", {})
        return {
            "final_status": "workflow_completed",
            "aggregated_results": aggregated_data,
        }

    async def _handle_input(self, config, node_id):
        """Handles the 'input' logic."""
        return {"value": config.get("value", "default_input")}


if __name__ == "__main__":
    worker = KafkaWorker()
    try:
        asyncio.run(worker.start())
    except KeyboardInterrupt:
        logger.info("Worker stopped by user")
    except Exception as e:
        logger.error(f"Worker crashed: {e}", exc_info=True)
