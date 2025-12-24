import uuid
from typing import Any
import networkx as nx
from fastapi import HTTPException
from core import get_logger, get_node_key, redis_client
from orchestrator import OrchestrationEngine
from schemas import NodeState, NodeStatus, WorkflowSchema, WorkflowStatusResponse

logger = get_logger(__name__)


class WorkflowService:
    """Stateless service for managing workflow lifecycle operations.

    Handles workflow validation, execution triggering, status tracking,
    and result retrieval. Interacts with Redis for persistence and uses
    NetworkX for graph validation.
    """

    def __init__(self):
        """Initialize the workflow service with Redis client and logger."""
        self.redis = redis_client
        self.logger = logger

    async def submit(self, workflow: WorkflowSchema) -> str:
        """
        Handles the logic for creating/validating a new DAG.

        Args:
            workflow: The workflow schema to submit.

        Returns:
            str: The execution ID of the submitted workflow.

        Raises:
            ValueError: If workflow validation fails.
            RuntimeError: If workflow initialization fails.
        """
        try:
            execution_id = await self._validate_and_build(workflow)
            return execution_id
        except Exception as e:
            self.logger.error(f"Error submitting workflow: {e}")
            raise

    async def trigger(self, execution_id: str, params: dict[str, Any] | None = None):
        """
        Triggers the execution of a workflow given its execution ID.

        Args:
            execution_id: The unique identifier of the workflow execution.
            params: Optional runtime parameters for workflow execution.

        Raises:
            HTTPException: If the workflow execution is not found.
        """
        try:
            meta = await self.redis.get_workflow_meta(execution_id)
            if not meta:
                raise HTTPException(
                    status_code=404, detail=f"Execution {execution_id} not found"
                )

            engine = OrchestrationEngine(execution_id)
            await engine.trigger(params)
        except Exception as e:
            self.logger.error(f"Error triggering workflow: {e}")
            raise

    async def get_status(self, execution_id: str) -> WorkflowStatusResponse:
        """
        Aggregates the current state of the workflow and all its nodes.

        Args:
            execution_id: The unique identifier of the workflow execution.

        Returns:
            WorkflowStatusResponse: Current status of the workflow and its nodes.

        Raises:
            HTTPException: If the workflow execution is not found.
        """
        try:
            meta = await self.redis.get_workflow_meta(execution_id)
            if not meta:
                raise HTTPException(
                    status_code=404, detail=f"Execution {execution_id} not found"
                )

            engine = OrchestrationEngine(execution_id)
            await engine.initialize()
            node_states = await engine._get_all_node_states()

            return WorkflowStatusResponse(
                execution_id=execution_id,
                status=meta["status"],
                nodes={
                    nid: NodeStatus(state=data["state"], output=data.get("output"))
                    for nid, data in node_states.items()
                },
            )
        except Exception as e:
            self.logger.error(f"Error fetching workflow status: {e}")
            raise

    async def get_results(self, execution_id: str) -> dict[str, Any]:
        """
        Returns results only if the workflow is completed.

        Args:
            execution_id: The unique identifier of the workflow execution.

        Returns:
            dict: Dictionary containing execution status and results.

        Raises:
            HTTPException: If the workflow execution is not found.
        """
        meta = await self.redis.get_workflow_meta(execution_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Workflow not found")

        is_completed = meta["status"] == NodeState.COMPLETED.value
        results = {}

        if is_completed:
            engine = OrchestrationEngine(execution_id)
            await engine.initialize()
            node_states = await engine._get_all_node_states()

            # Only get the output node's results
            output_node = node_states.get("output", {})
            results = output_node.get("output", {})

        return {
            "execution_id": execution_id,
            "status": meta["status"],
            "completed": is_completed,
            "results": results,
            "message": (
                "Results retrieved successfully."
                if is_completed
                else "Results available after all nodes execution completed."
            ),
        }

    async def _validate_and_build(self, workflow: WorkflowSchema) -> str:
        """
        Orchestrates validation and persists the blueprint and initial state.

        Args:
            workflow: The workflow schema to validate and build.

        Returns:
            str: The execution ID of the validated workflow.

        Raises:
            ValueError: If validation fails (cycles, missing nodes, etc.).
            RuntimeError: If workflow initialization fails.
        """
        execution_id = str(uuid.uuid4())
        logger.info(
            f"[{execution_id}] Initializing Workflow DAG build: {workflow.name}"
        )

        try:
            # Extract workflow structure
            nodes = workflow.dag.nodes
            node_map = {node.id: node for node in nodes}

            # Build graph for validation
            graph = nx.DiGraph()
            for node in nodes:
                graph.add_node(node.id)
                for dep in node.dependencies:
                    graph.add_edge(dep, node.id)

            # 1. Structural Validation
            self._validate_workflow(nodes, node_map)
            self._check_cycles(graph)

            # 2. Persist Full Blueprint (WorkflowSchema)
            await redis_client.set_dag(execution_id, workflow.model_dump_json())

            # 3. Persist Workflow Metadata (Initial Status)
            await redis_client.set_workflow_meta(execution_id, NodeState.PENDING.value)

            # 4. Initialize individual node tracking keys
            await self._initialize_node_states(execution_id, nodes)

            logger.info(f"[{execution_id}] Workflow prepared for execution.")
            return execution_id

        except ValueError as ve:
            logger.error(f"[{execution_id}] Validation failed: {ve}")
            raise
        except Exception as e:
            logger.error(f"[{execution_id}] Setup failure: {e}", exc_info=True)
            raise RuntimeError(f"Workflow initialization failed: {str(e)}")

    def _validate_workflow(self, nodes: list, node_map: dict):
        """
        Validates the structural integrity of the workflow DAG.

        Args:
            nodes: List of workflow nodes.
            node_map: Mapping of node IDs to node objects.

        Raises:
            ValueError: If duplicate node IDs or missing dependencies are found.
        """
        if len(node_map) != len(nodes):
            raise ValueError("Integrity error: Duplicate Node IDs detected.")

        for node in nodes:
            for dep in node.dependencies:
                if dep not in node_map:
                    raise ValueError(
                        f"Reference error: Node '{node.id}' depends on missing node '{dep}'."
                    )

    def _check_cycles(self, graph: nx.DiGraph):
        """
        Detects cycles in the DAG using NetworkX.

        Args:
            graph: Directed graph representing the workflow DAG.

        Raises:
            ValueError: If a cycle is detected in the graph.
        """
        if not nx.is_directed_acyclic_graph(graph):
            cycle = nx.find_cycle(graph)
            cycle_str = " -> ".join([str(u) for u, v in cycle]) + f" -> {cycle[0][0]}"
            raise ValueError(f"Structural error: Circular dependency: {cycle_str}")

    async def _initialize_node_states(self, execution_id: str, nodes: list):
        """
        Sets all nodes to PENDING state at the start of execution.

        Args:
            execution_id: The unique identifier of the workflow execution.
            nodes: List of workflow nodes to initialize.
        """
        for node in nodes:
            node_key = get_node_key(execution_id, node.id)
            await redis_client.set_node_state(
                node_key, NodeState.PENDING.value, output={}
            )
