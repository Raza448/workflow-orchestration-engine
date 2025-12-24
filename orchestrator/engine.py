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
    """
    OrchestrationEngine is responsible for managing the execution of a workflow DAG (Directed Acyclic Graph).
    It orchestrates the dispatching of nodes based on their dependencies and handles the completion of tasks.

    Attributes:
        execution_id (str): Unique identifier for the workflow execution.
        meta_key (str): Redis key for storing workflow metadata.
        dispatched_set (str): Redis key for tracking dispatched nodes.
        workflow (WorkflowSchema | None): The hydrated workflow schema.

    Methods:
        initialize():
            Hydrates the engine by fetching the validated WorkflowSchema
            from Redis.

        trigger():
            Entry point to start the workflow execution.

        _dispatch_ready_nodes():
            Scans all nodes and dispatches those whose dependencies are COMPLETED.

        process_node_completion(node_id, output=None, success=True):
            Callback handler for task completion. Updates node state and triggers subsequent nodes if applicable.

        _get_all_node_states():
            Fetches the status of all nodes defined in the DAG.

        _resolve_inputs(node, node_states, runtime_params):
            Resolves input templates for a node by replacing placeholders with actual values.

        _aggregate_parent_outputs(node, node_states):
            Aggregates outputs from parent nodes to provide inputs for the current node.

        _resolve_node_reference(parts, node_states, default):
            Resolves references to other nodes' outputs.

        _is_node_ready(node, node_states):
            Checks if a node is ready for dispatch based on its dependencies.

        _are_all_nodes_completed(node_states):
            Checks if all nodes in the DAG are completed.
    """

    def __init__(self, execution_id: str):
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
            logger.error(f"Error initializing engine: {e}")
            raise

    async def trigger(self, params: dict | None = None):
        """Entry point to start the workflow execution."""
        try:
            if params:
                await redis_client.set_runtime_params(self.execution_id, params)

            if not self.workflow:
                await self.initialize()

            logger.info(f"[{self.execution_id}] Triggering workflow...")
            await self._dispatch_ready_nodes()
        except Exception as e:
            logger.error(f"Error triggering engine: {e}")
            raise

    async def _dispatch_ready_nodes(self):
        """Scans all nodes and dispatches those whose dependencies are COMPLETED."""
        try:
            node_states = await self._get_all_node_states()
            runtime_params = await redis_client.get_runtime_params(self.execution_id)
            logger.info(f"runtime_params: {runtime_params}")

            for node in self.workflow.dag.nodes:
                if not self._is_node_ready(node, node_states):
                    continue

                if await redis_client.sadd(self.dispatched_set, node.id):
                    logger.info(f"[{self.execution_id}] Dispatching node: {node.id}")
                    config = self._resolve_inputs(node, node_states, runtime_params)
                    logger.info(
                        f"[{self.execution_id}] Node {node.id} config resolved: {config}"
                    )

                    await redis_client.set_node_state(
                        get_node_key(self.execution_id, node.id),
                        NodeState.RUNNING.value,
                    )

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
        except Exception as e:
            logger.error(f"Error dispatching ready nodes: {e}")
            raise

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

            if not success:
                logger.error(
                    f"[{self.execution_id}] Node {node_id} failed. Short-circuiting."
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
            logger.error(f"Error processing node completion: {e}")
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
        """
        Resolves the configuration for a node by:
        1. Merging workflow-defined config with runtime params for this node.
        2. Injecting parent node outputs.
        3. Resolving template placeholders in strings.
        """

        # 1️⃣ Base config from workflow
        config_dict = node.config.model_dump() if node.config else {}

        # 2️⃣ Add parent outputs
        config_dict["__parent_outputs__"] = self._aggregate_parent_outputs(
            node, node_states
        )

        # 3️⃣ Merge node-specific runtime params
        node_runtime_params = runtime_params.get(node.id, {}) if runtime_params else {}
        merged_config = {**config_dict, **node_runtime_params}

        # 4️⃣ Template resolution function
        def replace_match(match):
            path = match.group(1).strip()
            parts = path.split(".")
            # Reference to parent node outputs: {{ parent_node.output.key }}
            if len(parts) >= 2 and parts[1] == "output":
                return str(
                    self._resolve_node_reference(parts, node_states, match.group(0))
                )
            # Runtime param reference: {{ runtime.key }}
            if parts[0] == "runtime":
                return str(node_runtime_params.get(parts[1], match.group(0)))
            return match.group(0)

        # 5️⃣ Apply template resolution for all string values
        resolved_config = {
            k: re.sub(r"{{\s*(.*?)\s*}}", replace_match, v) if isinstance(v, str) else v
            for k, v in merged_config.items()
        }

        return resolved_config

    def _aggregate_parent_outputs(
        self, node, node_states: dict[str, Any]
    ) -> dict[str, Any]:
        """Collects outputs from all parent nodes of a given node."""
        return {
            dep_id: node_states.get(dep_id, {}).get("output", {})
            for dep_id in node.dependencies
        }

    def _resolve_node_reference(self, parts, node_states, default):
        """Resolves references to other nodes' outputs."""
        dep_id, key = parts[0], (
            parts[2] if len(parts) > 2 and parts[1] == "output" else parts[1]
        )
        return str(node_states.get(dep_id, {}).get("output", {}).get(key, default))

    def _is_node_ready(self, node, node_states: dict[str, Any]) -> bool:
        """Checks if a node is ready for dispatch."""
        if node_states.get(node.id, {}).get("state") != NodeState.PENDING.value:
            return False
        return all(
            node_states.get(dep, {}).get("state") == NodeState.COMPLETED.value
            for dep in node.dependencies
        )

    def _are_all_nodes_completed(self, node_states: dict[str, Any]) -> bool:
        """Checks if all nodes in the DAG are completed."""
        return all(
            node_states.get(n.id, {}).get("state") == NodeState.COMPLETED.value
            for n in self.workflow.dag.nodes
        )

    async def trigger_response(self, node_id: str, response: dict, success: bool):
        """Handles the response for a triggered node."""
        try:
            logger.info(
                f"[{self.execution_id}] Processing response for node {node_id}..."
            )
            node_state = await redis_client.get_node_state(
                get_node_key(self.execution_id, node_id)
            )
            if node_state["state"] != NodeState.RUNNING:
                raise ValueError(f"Node {node_id} is not in progress state.")

            # Update node state based on success flag
            new_state = NodeState.COMPLETED if success else NodeState.FAILED
            await redis_client.set_node_state(
                get_node_key(self.execution_id, node_id), new_state.value, response
            )

            # Dispatch ready nodes if the node was completed successfully
            if success:
                await self._dispatch_ready_nodes()
        except Exception as e:
            logger.error(f"Error in trigger_response: {e}")
            raise
