import uuid
from typing import Any
import networkx as nx
from fastapi import HTTPException
from core import get_logger, get_node_key, redis_client
from orchestrator import OrchestrationEngine
from schemas import NodeState, NodeStatus, WorkflowSchema, WorkflowStatusResponse

logger = get_logger(__name__)


class WorkflowService:
    def __init__(self):
        # In a more advanced setup, you could inject these via FastAPI Depends
        self.redis = redis_client
        self.logger = logger
        self.workflow: WorkflowSchema | None = None

    def initialize_workflow(self, workflow: WorkflowSchema):
        self.workflow = workflow
        self.nodes = workflow.dag.nodes
        self.node_map = {node.id: node for node in self.nodes}

        # Build graph for cycle detection
        self.graph = nx.DiGraph()
        for node in self.nodes:
            self.graph.add_node(node.id)
            for dep in node.dependencies:
                self.graph.add_edge(dep, node.id)

    async def submit(self, workflow: WorkflowSchema) -> str:
        """Handles the logic for creating/validating a new DAG."""
        execution_id = await self.validate_and_build(workflow)
        return execution_id

    async def trigger(self, execution_id: str, params: dict[str, Any] | None = None):
        """Triggers the execution of a workflow given its execution ID."""
        meta = await self.redis.get_workflow_meta(execution_id)
        if not meta:
            raise HTTPException(
                status_code=404, detail=f"Execution {execution_id} not found"
            )

        engine = OrchestrationEngine(execution_id)
        await engine.trigger(params)

    async def get_status(self, execution_id: str) -> WorkflowStatusResponse:
        """Aggregates the current state of the workflow and all its nodes."""
        meta = await self.redis.get_workflow_meta(execution_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Workflow not found")

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

    async def get_results(self, execution_id: str) -> dict[str, Any]:
        """Returns results only if the workflow is completed."""
        meta = await self.redis.get_workflow_meta(execution_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Workflow not found")

        is_completed = meta["status"] == NodeState.COMPLETED.value
        results = {}

        if is_completed:
            engine = OrchestrationEngine(execution_id)
            await engine.initialize()
            node_states = await engine._get_all_node_states()
            results = {nid: data.get("output") for nid, data in node_states.items()}

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

    async def validate_and_build(self, workflow: WorkflowSchema) -> str:
        """
        Orchestrates validation and persists the blueprint and initial state.
        """

        if not self.workflow:
            self.initialize_workflow(workflow)

        execution_id = str(uuid.uuid4())
        logger.info(
            f"[{execution_id}] Initializing Workflow DAG build: {workflow.name}"
        )

        try:
            # 1. Structural Validation
            self._validate_workflow()
            self._check_cycles()

            # 2. Persist Full Blueprint (WorkflowSchema)
            # This stores the 'name' and the 'dag' (nodes/dependencies)
            await redis_client.set_dag(execution_id, self.workflow.model_dump_json())

            # 3. Persist Workflow Metadata (Initial Status)
            await redis_client.set_workflow_meta(execution_id, NodeState.PENDING.value)

            # 4. Initialize individual node tracking keys
            await self._initialize_node_states(execution_id)

            logger.info(f"[{execution_id}] Workflow prepared for execution.")
            return execution_id

        except ValueError as ve:
            logger.error(f"[{execution_id}] Validation failed: {ve}")
            raise
        except Exception as e:
            logger.error(f"[{execution_id}] Setup failure: {e}", exc_info=True)
            raise RuntimeError(f"Workflow initialization failed: {str(e)}")

    def _validate_workflow(self):
        if len(self.node_map) != len(self.nodes):
            raise ValueError("Integrity error: Duplicate Node IDs detected.")

        for node in self.nodes:
            for dep in node.dependencies:
                if dep not in self.node_map:
                    raise ValueError(
                        f"Reference error: Node '{node.id}' depends on missing node '{dep}'."
                    )

    def _check_cycles(self):
        """Detects cycles in the DAG using NetworkX."""
        if not nx.is_directed_acyclic_graph(self.graph):
            cycle = nx.find_cycle(self.graph)
            cycle_str = " -> ".join([str(u) for u, v in cycle]) + f" -> {cycle[0][0]}"
            raise ValueError(f"Structural error: Circular dependency: {cycle_str}")

    async def _initialize_node_states(self, execution_id: str):
        """Sets all nodes to PENDING state at the start of execution."""
        for node in self.nodes:
            node_key = get_node_key(execution_id, node.id)
            await redis_client.set_node_state(
                node_key, NodeState.PENDING.value, output={}
            )
