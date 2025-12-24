import json
import re
from typing import Any
from fastapi import HTTPException
from core import (
    WORKFLOW_TASK_TOPIC,
    get_dispatched_set_key,
    get_logger,
    get_node_key,
    get_workflow_meta_key,
    kafka_client,
    redis_client,
)
from schemas.workflow import NodeState, WorkflowSchema

logger = get_logger(__name__)


class OrchestrationEngine:
    """OrchestrationEngine is a class responsible for managing the execution of a workflow DAG (Directed Acyclic Graph).
    It provides methods to initialize the engine, trigger workflow execution, dispatch ready nodes, and handle node
    completion callbacks. The engine interacts with Redis for state management and Kafka for task dispatching.

    Attributes:
        execution_id (str): Unique identifier for the workflow execution.
        meta_key (str): Redis key for storing workflow metadata.
        dispatched_set (str): Redis key for tracking dispatched nodes.
        workflow (WorkflowSchema | None): The hydrated workflow schema.
    """

    def __init__(self, execution_id: str):
        """Initializes the engine with the given execution ID."""
        self.execution_id = execution_id
        self.meta_key = get_workflow_meta_key(execution_id)
        self.dispatched_set = get_dispatched_set_key(execution_id)
        self.workflow: WorkflowSchema | None = None

    async def initialize(self):
        """Hydrates the engine by fetching the validated WorkflowSchema from Redis."""
        try:
            raw_json = await redis_client.get_dag(self.execution_id)
            if not raw_json:
                raise HTTPException(
                    status_code=404,
                    detail="Workflow DAG structure not found",
                )
            self.workflow = WorkflowSchema.model_validate_json(raw_json)
            logger.info(
                f"[{self.execution_id}] Engine initialized for workflow: {self.workflow.name}"
            )
            return self
        except Exception as e:
            logger.error(f"[{self.execution_id}] Error initializing engine: {e}")
            raise

    async def trigger(self, params: dict | None = None):
        """Entry point to start the workflow execution."""
        try:
            if params:
                await redis_client.set_runtime_params(self.execution_id, params)
                logger.info(f"[{self.execution_id}] Runtime params set: {params}")

            if not self.workflow:
                await self.initialize()

            logger.info(f"[{self.execution_id}] Triggering workflow...")
            await self._dispatch_ready_nodes()
        except Exception as e:
            logger.error(f"[{self.execution_id}] Error triggering engine: {e}")
            raise

    async def _dispatch_ready_nodes(self):
        """Scans all nodes and dispatches those whose dependencies are COMPLETED."""
        try:
            node_states = await self._get_all_node_states()
            runtime_params = await redis_client.get_runtime_params(self.execution_id)
            logger.info(f"[{self.execution_id}] Node states: {node_states}")
            logger.info(f"[{self.execution_id}] Runtime params: {runtime_params}")

            for node in self.workflow.dag.nodes:
                if not self._is_node_ready(node, node_states):
                    logger.debug(f"[{self.execution_id}] Node {node.id} not ready.")
                    continue

                if await redis_client.sadd(self.dispatched_set, node.id):
                    await self._dispatch_node(node, node_states, runtime_params)
        except Exception as e:
            logger.error(f"[{self.execution_id}] Error dispatching ready nodes: {e}")
            raise

    async def _dispatch_node(self, node, node_states, runtime_params):
        """Dispatches a single node."""
        try:
            config = self._resolve_inputs(node, node_states, runtime_params)
            logger.debug(
                f"[{self.execution_id}] Node {node.id} config resolved: {config}"
            )
        except Exception as e:
            await self._handle_node_dispatch_failure(node, e)
            return

        await self._mark_node_running(node)
        await self._publish_task_to_kafka(node, config)

    async def _handle_node_dispatch_failure(self, node, error):
        """Handles failures during node dispatch."""
        logger.error(
            f"[{self.execution_id}] Template resolution failed for node {node.id}: {error}"
        )
        await redis_client.set_node_state(
            get_node_key(self.execution_id, node.id),
            NodeState.FAILED.value,
            output={"error": str(error)},
        )
        await redis_client.set_workflow_status(self.meta_key, NodeState.FAILED.value)

    async def _mark_node_running(self, node):
        """Marks a node as RUNNING in Redis."""
        await redis_client.set_node_state(
            get_node_key(self.execution_id, node.id),
            NodeState.RUNNING.value,
        )
        logger.info(f"[{self.execution_id}] Node {node.id} marked RUNNING")

    async def _publish_task_to_kafka(self, node, config):
        """Publishes a task for the node to Kafka."""
        logger.info(
            f"[{self.execution_id}] Publishing task for node {node.id} to Kafka."
        )
        await kafka_client.publish(
            WORKFLOW_TASK_TOPIC,
            {
                "execution_id": self.execution_id,
                "node_id": node.id,
                "handler": node.handler,
                "config": config,
            },
        )

    async def process_node_completion(
        self, node_id: str, output: dict[str, Any] = None, success: bool = True
    ):
        """Callback handler for task completion."""
        try:
            if not self.workflow:
                await self.initialize()

            new_state = NodeState.COMPLETED.value if success else NodeState.FAILED.value
            await redis_client.set_node_state(
                get_node_key(self.execution_id, node_id), new_state, output=output or {}
            )
            logger.info(
                f"[{self.execution_id}] Node {node_id} completed with state: {new_state}, output: {output}"
            )

            if not success:
                logger.error(
                    f"[{self.execution_id}] Node {node_id} failed. Short-circuiting workflow."
                )
                await redis_client.set_workflow_status(
                    self.meta_key, NodeState.FAILED.value
                )
                return

            node_states = await self._get_all_node_states()
            if self._are_all_nodes_completed(node_states):
                logger.info(f"[{self.execution_id}] Workflow completed successfully.")
                await redis_client.set_workflow_status(
                    self.meta_key, NodeState.COMPLETED.value
                )
            else:
                await self._dispatch_ready_nodes()
        except Exception as e:
            logger.error(f"[{self.execution_id}] Error processing node completion: {e}")
            raise

    async def _get_all_node_states(self) -> dict[str, Any]:
        """Fetches status of all nodes defined in the DAG."""
        node_ids = [n.id for n in self.workflow.dag.nodes]
        keys = [get_node_key(self.execution_id, nid) for nid in node_ids]
        raw_states = await redis_client.mget(keys)

        return {
            nid: (
                json.loads(raw)
                if raw
                else {"state": NodeState.PENDING.value, "output": {}}
            )
            for nid, raw in zip(node_ids, raw_states)
        }

    def _resolve_inputs(
        self, node, node_states: dict[str, Any], runtime_params: dict[str, Any]
    ) -> dict[str, Any]:
        """Resolves the configuration for a node recursively."""
        config_dict = node.config.model_dump() if node.config else {}
        config_dict["__parent_outputs__"] = self._aggregate_parent_outputs(
            node, node_states
        )

        node_runtime_params = runtime_params.get(node.id, {}) if runtime_params else {}
        merged_config = {**config_dict, **node_runtime_params}

        logger.debug(
            f"[{self.execution_id}] Merged config for node {node.id}: {merged_config}"
        )

        def resolve(obj):
            if isinstance(obj, str):
                return self._resolve_template_value(
                    obj, node_states, node_runtime_params
                )
            elif isinstance(obj, dict):
                return {k: resolve(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [resolve(v) for v in obj]
            else:
                return obj

        return resolve(merged_config)

    def _resolve_template_value(
        self,
        value: str,
        node_states: dict[str, Any],
        node_runtime_params: dict[str, Any],
    ) -> str:
        """Resolves a single string template recursively."""

        def replace_match(match):
            """Replaces a single template match."""
            path = match.group(1).strip()
            parts = path.split(".")
            if len(parts) >= 2 and parts[1] == "output":
                # Node output reference
                resolved = str(self._resolve_node_reference(parts, node_states))
                logger.debug(
                    f"[{self.execution_id}] Resolved template {match.group(0)} to {resolved}"
                )
                return resolved
            if parts[0] == "runtime":
                val = node_runtime_params
                for key in parts[1:]:
                    if isinstance(val, dict) and key in val:
                        val = val[key]
                    else:
                        logger.warning(
                            f"[{self.execution_id}] Unresolved runtime template: {match.group(0)}"
                        )
                        return match.group(0)
                logger.debug(
                    f"[{self.execution_id}] Resolved template {match.group(0)} to {val}"
                )
                return str(val)
            return match.group(0)

        # Recursive resolution
        prev_value = None
        while prev_value != value:
            prev_value = value
            value = re.sub(r"{{\s*(.*?)\s*}}", replace_match, value)

        return value

    def _aggregate_parent_outputs(
        self, node, node_states: dict[str, Any]
    ) -> dict[str, Any]:
        """Aggregates the outputs of all parent nodes for the given node."""
        outputs = {
            dep_id: node_states.get(dep_id, {}).get("output", {})
            for dep_id in node.dependencies
        }
        logger.debug(
            f"[{self.execution_id}] Aggregated parent outputs for node {node.id}: {outputs}"
        )
        return outputs

    def _resolve_node_reference(self, parts, node_states):
        """
        Resolves a reference to a node's output value within the workflow execution.

        Args:
            parts (list of str): A list of strings representing the reference path.
                The first element is the node ID, the second element must be "output",
                and any subsequent elements represent keys to traverse the output object.
            node_states (dict): A dictionary containing the states of nodes, where
                each key is a node ID and the value is a dictionary with node details,
                including its "output".

        Returns:
            Any: The resolved value from the node's output based on the reference path.

        Raises:
            ValueError: If the referenced node does not exist in `node_states`.
            ValueError: If the reference root is not "output".
            ValueError: If the node's output is missing or the path cannot be resolved.
            ValueError: If attempting to traverse a non-dictionary value or if a key
                in the path does not exist in the output.

        Logs:
            Debug: Logs the resolved reference and its value for the current execution ID.
        """
        node_id = parts[0]

        if node_id not in node_states:
            raise ValueError(f"Referenced node '{node_id}' does not exist")

        if parts[1] != "output":
            raise ValueError(f"Invalid reference root: {'.'.join(parts)}")

        value = node_states[node_id].get("output")
        if value is None:
            raise ValueError(f"Node '{node_id}' has no output")

        for key in parts[2:]:
            if not isinstance(value, dict):
                raise ValueError(
                    f"Cannot resolve '{key}' in non-object value at '{'.'.join(parts)}'"
                )
            if key not in value:
                raise ValueError(
                    f"Output key '{key}' not found at path '{'.'.join(parts)}'"
                )
            value = value[key]

        logger.debug(
            f"[{self.execution_id}] Resolved node reference {'.'.join(parts)} to {value}"
        )
        return value

    def _is_node_ready(self, node, node_states: dict[str, Any]) -> bool:
        """Checks if a node is ready for execution based on its dependencies' states."""
        state = node_states.get(node.id, {}).get("state")
        if state != NodeState.PENDING.value:
            return False
        ready = all(
            node_states.get(dep, {}).get("state") == NodeState.COMPLETED.value
            for dep in node.dependencies
        )
        logger.debug(f"[{self.execution_id}] Node {node.id} ready: {ready}")
        return ready

    def _are_all_nodes_completed(self, node_states: dict[str, Any]) -> bool:
        """Checks if all nodes in the workflow DAG have been completed."""
        all_completed = all(
            node_states.get(n.id, {}).get("state") == NodeState.COMPLETED.value
            for n in self.workflow.dag.nodes
        )
        logger.debug(f"[{self.execution_id}] All nodes completed: {all_completed}")
        return all_completed
