WORKFLOW_PREFIX = "workflows"


def get_workflow_meta_key(execution_id: str) -> str:
    """Returns the key for workflow-level metadata."""
    return f"{WORKFLOW_PREFIX}:{execution_id}:meta"


def get_dispatched_set_key(execution_id: str) -> str:
    """Returns the key for the atomic dispatch tracking set."""
    return f"{WORKFLOW_PREFIX}:{execution_id}:dispatched"


def get_node_key(execution_id: str, node_id: str) -> str:
    """Returns the key for an individual node's state and output."""
    return f"{WORKFLOW_PREFIX}:{execution_id}:node:{node_id}"


def get_runtime_params_key(execution_id: str) -> str:
    """Returns the Redis key for workflow runtime parameters."""
    return f"{WORKFLOW_PREFIX}:{execution_id}:params"
