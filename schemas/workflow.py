from enum import Enum
from pydantic import BaseModel, Field, model_validator


class NodeConfig(BaseModel):
    """
    Configuration for a workflow node.
    """

    url: str | None = None
    prompt: str | None = None


class HandlerType(str, Enum):
    INPUT = "input"
    EXTERNAL_SERVICE = "call_external_service"
    LLM_SERVICE = "call_llm_service"
    OUTPUT = "output"


class NodeSchema(BaseModel):
    """
    Schema representing a single node in the workflow DAG.
    """

    id: str
    handler: HandlerType
    dependencies: list[str] = Field(default_factory=list)
    config: NodeConfig | None = None

    @model_validator(mode="after")
    def validate_handler_config_contract(self):
        if (
            self.handler == HandlerType.EXTERNAL_SERVICE
            or self.handler == HandlerType.LLM_SERVICE
        ) and self.config is None:
            raise ValueError(
                f"Node '{self.id}' with handler {self.handler} requires config"
            )

        if self.handler == HandlerType.EXTERNAL_SERVICE and self.config.url is None:
            raise ValueError(
                f"Node '{self.id}' with handler EXTERNAL_SERVICE requires config url"
            )

        if self.handler == HandlerType.LLM_SERVICE and self.config.prompt is None:
            raise ValueError(
                f"Node '{self.id}' with handler LLM_SERVICE requires config prompt"
            )

        return self


class DAGSchema(BaseModel):
    """
    Directed Acyclic Graph (DAG) containing all nodes for the workflow.
    """

    nodes: list[NodeSchema]


class WorkflowSchema(BaseModel):
    """
    Top-level schema representing a workflow definition.
    """

    name: str
    dag: DAGSchema


class NodeState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class NodeExecution(BaseModel):
    node_id: str
    state: NodeState = NodeState.PENDING
    output: dict | None = None


class WorkflowExecution(BaseModel):
    execution_id: str
    status: NodeState = NodeState.PENDING
    nodes: dict[str, NodeExecution]


class WorkflowSubmitResponse(BaseModel):
    execution_id: str
    status: NodeState


class WorkflowTriggerResponse(BaseModel):
    execution_id: str
    status: NodeState


class NodeStatus(BaseModel):
    state: NodeState
    output: dict | None = None


class WorkflowStatusResponse(BaseModel):
    execution_id: str
    status: NodeState
    nodes: dict[str, NodeStatus]
