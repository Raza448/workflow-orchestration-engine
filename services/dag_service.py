import uuid
import networkx as nx
from fastapi import HTTPException
from schemas import WorkflowSchema, NodeState
from core import redis_client, get_logger, get_node_key

logger = get_logger(__name__)


class DAGService:
    """
    Handles workflow validation and Redis initialization.
    """

    def __init__(self, workflow: WorkflowSchema):
        self.workflow = workflow
        self.nodes = workflow.dag.nodes
        self.node_map = {node.id: node for node in self.nodes}

        # Build graph for cycle detection
        self.graph = nx.DiGraph()
        for node in self.nodes:
            self.graph.add_node(node.id)
            for dep in node.dependencies:
                self.graph.add_edge(dep, node.id)

    async def validate_and_build(self) -> str:
        """
        Orchestrates validation and persists the blueprint and initial state.
        """
        execution_id = str(uuid.uuid4())
        logger.info(f"[{execution_id}] Initializing DAG build: {self.workflow.name}")

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

    @staticmethod
    async def get_dag_by_execution_id(execution_id: str) -> WorkflowSchema:
        """Fetches and reconstructs the DAG blueprint from Redis."""
        raw_json = await redis_client.get_dag(execution_id)
        if not raw_json:
            raise HTTPException(
                status_code=404,
                detail="Workflow DAG structure not found",
            )
        return WorkflowSchema.model_validate_json(raw_json)
