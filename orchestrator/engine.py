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
    OrchestrationEngine is responsible for managing the execution of a workflow DAG.
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
                    logger.info(f"[{self.execution_id}] Dispatching node: {node.id}")

                    try:
                        config = self._resolve_inputs(node, node_states, runtime_params)
                        logger.debug(
                            f"[{self.execution_id}] Node {node.id} config resolved: {config}"
                        )
                    except Exception as e:
                        logger.error(
                            f"[{self.execution_id}] Template resolution failed for node "
                            f"{node.id}: {e}"
                        )
                        await redis_client.set_node_state(
                            get_node_key(self.execution_id, node.id),
                            NodeState.FAILED.value,
                            output={"error": str(e)},
                        )
                        await redis_client.set_workflow_status(
                            self.meta_key, NodeState.FAILED.value
                        )
                        return

                    await redis_client.set_node_state(
                        get_node_key(self.execution_id, node.id),
                        NodeState.RUNNING.value,
                    )
                    logger.info(f"[{self.execution_id}] Node {node.id} marked RUNNING")

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
            logger.error(f"[{self.execution_id}] Error dispatching ready nodes: {e}")
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
            path = match.group(1).strip()
            parts = path.split(".")
            if len(parts) >= 2 and parts[1] == "output":
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

        prev_value = None
        while prev_value != value:
            prev_value = value
            value = re.sub(r"{{\s*(.*?)\s*}}", replace_match, value)

        return value

    def _aggregate_parent_outputs(
        self, node, node_states: dict[str, Any]
    ) -> dict[str, Any]:
        outputs = {
            dep_id: node_states.get(dep_id, {}).get("output", {})
            for dep_id in node.dependencies
        }
        logger.debug(
            f"[{self.execution_id}] Aggregated parent outputs for node {node.id}: {outputs}"
        )
        return outputs

    def _resolve_node_reference(self, parts, node_states):
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
        all_completed = all(
            node_states.get(n.id, {}).get("state") == NodeState.COMPLETED.value
            for n in self.workflow.dag.nodes
        )
        logger.debug(f"[{self.execution_id}] All nodes completed: {all_completed}")
        return all_completed
