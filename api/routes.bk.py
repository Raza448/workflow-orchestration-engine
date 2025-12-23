import json
from fastapi import APIRouter, HTTPException
from schemas import (
    WorkflowSchema,
    WorkflowSubmitResponse,
    WorkflowTriggerResponse,
    WorkflowStatusResponse,
    NodeStatus,
    NodeState,
    DAGSchema,
)
from services import DAGService
from orchestrator import OrchestrationEngine
from core import redis_client

router = APIRouter()


@router.post("/workflow", response_model=WorkflowSubmitResponse)
async def submit_workflow(workflow: WorkflowSchema):
    dag_service = DAGService(workflow)
    execution_id = await dag_service.validate_and_build()

    return WorkflowSubmitResponse(
        execution_id=execution_id,
        status=NodeState.PENDING,
    )


# @router.post(
#     "/workflow/trigger/:execution_id",
#     response_model=WorkflowTriggerResponse,
# )
# def trigger_workflow(execution_id: str, params: DAGSchema | None = None):
#     orch_engine = OrchestrationEngine(execution_id, params=params)
#     orch_engine.trigger()

#     return WorkflowTriggerResponse(
#         execution_id=execution_id,
#         status=NodeState.RUNNING,
#     )


# @router.get(
#     "/workflow/:id",
#     response_model=WorkflowStatusResponse,
# )
# def get_workflow_status(execution_id: str):
#     execution_key = f"execution:{execution_id}"

#     if not redis_client.exists(execution_key):
#         raise HTTPException(status_code=404, detail="Execution not found")

#     execution = redis_client.hget(execution_key)
#     workflow_id = execution["workflow_id"]

#     dag = DAGService.get_dag(workflow_id)

#     nodes = {}
#     for node_id in dag.nodes:
#         node = redis_client.hget(f"execution:{execution_id}:node:{node_id}")
#         nodes[node_id] = NodeStatus(
#             state=node["state"],
#             output=json.loads(node["output"]) if node["output"] else None,
#         )

#     return WorkflowStatusResponse(
#         execution_id=execution_id,
#         workflow_id=workflow_id,
#         status=execution["status"],
#         nodes=nodes,
#     )


# @router.get("/workflows/:id/results")
# def get_workflow_results(execution_id: str):
#     """
#     Return final aggregated output of the workflow.
#     Only include COMPLETED nodes.
#     """
#     meta = redis_client.get_workflow_meta(execution_id)
#     if not meta:
#         raise HTTPException(status_code=404, detail="Execution not found")

#     workflow_id = meta.get("workflow_id")
#     dag = DAGService.get_dag(workflow_id)

#     results = {}
#     for node_id in dag.nodes.keys():

#         node_state = redis_client.get_node_state(execution_id, node_id)
#         if node_state["state"] == "COMPLETED":
#             results[node_id] = node_state.get("output")

#     return {
#         "execution_id": execution_id,
#         "workflow_id": workflow_id,
#         "results": results,
#     }
