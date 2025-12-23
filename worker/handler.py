import json
from schemas import NodeState
from core import redis_client
from services import DAGService
from orchestrator import OrchestrationEngine


def on_node_complete(
    execution_id: str,
    node_id: str,
    output: dict,
    success: bool = True,
):
    node_key = f"execution:{execution_id}:node:{node_id}"
    execution_key = f"execution:{execution_id}"

    node = redis_client.hget(node_key)
    if node["state"] != NodeState.RUNNING:
        return  # idempotent

    if success:
        redis_client.hset(
            node_key,
            mapping={
                "state": NodeState.COMPLETED,
                "output": json.dumps(output),
            },
        )
    else:
        redis_client.hset(node_key, mapping={"state": NodeState.FAILED})
        redis_client.hset(execution_key, mapping={"status": NodeState.FAILED})
        return

    OrchestrationEngine._dispatch_ready_nodes(execution_id)

    # Final workflow completion check
    execution = redis_client.hget(execution_key)
    workflow_id = execution["workflow_id"]
    dag = DAGService.get_dag(workflow_id)

    if all(
        redis_client.hget(f"execution:{execution_id}:node:{n}")["state"]
        == NodeState.COMPLETED
        for n in dag.nodes
    ):
        redis_client.hset(execution_key, mapping={"status": NodeState.COMPLETED})
