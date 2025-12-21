from pydantic import BaseModel, Field


class NodeConfig(BaseModel):
    """
    Configuration for a workflow node.

    Attributes:
        url (str | None): Optional URL for nodes that call external services.
    """

    url: str | None = None


class NodeSchema(BaseModel):
    """
    Represents a single node in the workflow DAG.

    Attributes:
        id (str): Unique identifier for the node.
        handler (str): Type of operation this node performs (e.g., input, call_external_service, output).
        dependencies (List[str]): List of node IDs that must complete before this node runs.
        config (NodeConfig | None): Optional configuration specific to the node.
    """

    id: str
    handler: str
    dependencies: list[str] = Field(default_factory=list)
    config: NodeConfig | None = None


class DAGSchema(BaseModel):
    """
    Directed Acyclic Graph (DAG) containing all nodes for the workflow.

    Attributes:
        nodes (List[NodeSchema]): List of nodes in the workflow.
    """

    nodes: list[NodeSchema]


class WorkflowSchema(BaseModel):
    """
    Top-level schema representing a workflow definition.

    Attributes:
        name (str): Human-readable name of the workflow.
        dag (DAGSchema): DAG containing all workflow nodes and dependencies.
    """

    name: str
    dag: DAGSchema
