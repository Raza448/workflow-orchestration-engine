import uuid
import networkx as nx
from schemas import WorkflowSchema
from core import redis_client, get_logger

logger = get_logger(__name__)

WORKFLOW_KEY = "workflow"


class DAGService:
    """
    This class handles workflow validation, DAG construction using NetworkX, and persistence
    of workflow metadata and node states to Redis.

    Attributes:
        graph (nx.DiGraph | None): NetworkX directed graph representation of the workflow DAG.
            Initially None until a workflow is built.

    Error Handling:
        The service performs comprehensive validation and raises ValueError with descriptive
        messages for:
        - Duplicate node IDs: When multiple nodes share the same identifier
        - Missing dependencies: When a node references a non-existent dependency
        - Circular dependencies: When the workflow contains cycles that prevent execution

        All operations are logged at appropriate levels (INFO, DEBUG, ERROR) for monitoring
        and debugging.
    """

    def __init__(self):
        """Initialize DAGService with empty graph."""
        self.graph = None

    def validate_and_build(self, workflow: WorkflowSchema) -> str:
        """
        Validate workflow structure and build DAG, then persist to Redis.

        This is the main entry point for workflow initialization. It performs validation,
        builds the graph structure, and sets up initial state in Redis.

        Process:
            1. Validates the workflow structure
            2. Builds the DAG graph
            3. Persists workflow metadata to Redis
            4. Initializes all node states to "PENDING"

        Args:
            workflow (WorkflowSchema): The workflow schema containing DAG structure and metadata.

        Returns:
            str: Unique execution ID (UUID) for tracking this workflow instance.

        Raises:
            ValueError: If validation fails due to duplicate IDs, missing dependencies, or circular dependencies.

        Example:
            >>> dag_service = DAGService()
            >>> execution_id = dag_service.validate_and_build(workflow)
            >>> print(f"Workflow initialized with ID: {execution_id}")
        """
        logger.info(f"Starting validation and DAG build for workflow: {workflow.name}")
        self._validate_workflow(workflow)
        self.graph = self._build_dag(workflow)
        execution_id = self._persist_workflow_metadata(workflow)
        self._initialize_node_states(execution_id, workflow)
        logger.info(
            f"Workflow {workflow.name} successfully validated and persisted with execution ID: {execution_id}"
        )
        return execution_id

    def _validate_workflow(self, workflow: WorkflowSchema):
        """
        Validate workflow structure and dependencies.

        Performs the following validations:
        - Checks for duplicate node IDs across all nodes
        - Verifies that all node dependencies reference existing nodes

        Args:
            workflow (WorkflowSchema): The workflow to validate.

        Raises:
            ValueError: If duplicate node IDs are found or dependencies reference non-existent nodes.
        """
        logger.debug(f"Validating workflow: {workflow.name}")
        nodes = workflow.dag.nodes
        node_ids = {node.id for node in nodes}

        # Check for duplicate IDs
        if len(node_ids) != len(nodes):
            logger.error(
                f"Validation failed: Duplicate node IDs found in workflow '{workflow.name}'"
            )
            raise ValueError("Node IDs must be unique")

        # Validate dependencies
        for node in nodes:
            for dep in node.dependencies:
                if dep not in node_ids:
                    logger.error(
                        f"Validation failed: Dependency '{dep}' does not exist for node '{node.id}' in workflow '{workflow.name}'",
                    )
                    raise ValueError(
                        f"Dependency '{dep}' does not exist for node '{node.id}'"
                    )
        logger.debug(f"Workflow '{workflow.name}' validation successful")

    def _build_dag(self, workflow: WorkflowSchema) -> nx.DiGraph:
        """
        Construct a NetworkX directed graph from the workflow schema.

        Creates a directed graph where:
        - Each node ID becomes a graph node
        - Dependencies create directed edges: dependency -> node

        Args:
            workflow (WorkflowSchema): The workflow schema to build into a graph.

        Returns:
            nx.DiGraph: NetworkX directed graph with nodes and dependency edges.

        Raises:
            ValueError: If the resulting graph contains cycles (not a valid DAG).
        """
        logger.debug(f"Building DAG for workflow: {workflow.name}")
        graph = nx.DiGraph()
        for node in workflow.dag.nodes:
            graph.add_node(node.id)
            for dep in node.dependencies:
                graph.add_edge(dep, node.id)

        if not nx.is_directed_acyclic_graph(graph):
            logger.error(
                f"DAG build failed: Circular dependency detected in workflow '{workflow.name}'"
            )
            raise ValueError("Workflow contains a circular dependency")

        logger.debug(f"DAG successfully built for workflow: {workflow.name}")
        return graph

    def _persist_workflow_metadata(self, workflow: WorkflowSchema) -> str:
        """
        Save workflow metadata to Redis.

        Redis storage format:
            Key: workflow:{execution_id}
            Type: Hash
            Fields:
                - name: <workflow_name>
                - status: PENDING

        Args:
            workflow (WorkflowSchema): The workflow whose metadata should be persisted.

        Returns:
            str: Generated execution ID (UUID).
        """
        logger.debug(f"Persisting workflow metadata for workflow: {workflow.name}")
        execution_id = str(uuid.uuid4())
        redis_client.hset(
            f"{WORKFLOW_KEY}:{execution_id}",
            {
                "name": workflow.name,
                "status": "PENDING",
            },
        )
        logger.info(f"Workflow metadata persisted with execution ID: {execution_id}")
        return execution_id

    def _initialize_node_states(self, execution_id: str, workflow: WorkflowSchema):
        """
        Initialize the state of all workflow nodes in Redis.

        Redis storage format:
            Key: workflow:{execution_id}:nodes
            Type: Hash
            Fields: Hash map where each node ID maps to "PENDING"

        Args:
            execution_id (str): The execution ID for this workflow instance.
            workflow (WorkflowSchema): The workflow containing nodes to initialize.
        """
        logger.debug(f"Initializing node states for execution ID: {execution_id}")
        node_states_key = f"{WORKFLOW_KEY}:{execution_id}:nodes"
        node_states = {node.id: "PENDING" for node in workflow.dag.nodes}
        redis_client.hset(node_states_key, mapping=node_states)
