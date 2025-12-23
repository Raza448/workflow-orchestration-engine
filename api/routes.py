from fastapi import APIRouter, HTTPException
from schemas import (
    WorkflowSchema,
    WorkflowSubmitResponse,
    WorkflowTriggerResponse,
    WorkflowStatusResponse,
    NodeState,
    NodeStatus,
)
from services.dag_service import DAGService
from orchestrator import OrchestrationEngine
from core import redis_client, get_workflow_meta_key, get_node_key, get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.post("/workflow", response_model=WorkflowSubmitResponse)
async def submit_workflow(workflow: WorkflowSchema):
    try:
        """
        Validates the DAG, saves it to Redis, and returns an execution ID.
        """
        dag_service = DAGService(workflow)
        execution_id = await dag_service.validate_and_build()

        return WorkflowSubmitResponse(
            execution_id=execution_id,
            status=NodeState.PENDING,
        )

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to submit workflow")
        raise HTTPException(
            status_code=500,
            detail="Failed to submit workflow",
        )


@router.post("/workflow/trigger/{execution_id}", response_model=WorkflowTriggerResponse)
async def trigger_workflow(execution_id: str):
    """
    Starts the execution of a submitted workflow.
    """
    try:
        engine = OrchestrationEngine(execution_id)
        await engine.initialize()
        await engine.trigger()

        return WorkflowTriggerResponse(
            execution_id=execution_id,
            status=NodeState.RUNNING,
        )

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to trigger workflow {execution_id}")
        raise HTTPException(
            status_code=500,
            detail="Failed to trigger workflow",
        )


@router.get("/workflows/{execution_id}", response_model=WorkflowStatusResponse)
async def get_workflow_status(execution_id: str):
    """
    Retrieves the overall status and individual node states.
    """
    try:
        # 1. Fetch metadata
        meta = await redis_client.get_workflow_meta(execution_id)
        if not meta:
            raise HTTPException(status_code=404, detail="Workflow meta not found")

        # 2. Fetch DAG
        workflow = await DAGService.get_dag_by_execution_id(execution_id)
        if not workflow:
            raise HTTPException(
                status_code=404,
                detail="Workflow DAG structure not found",
            )

        # 3. Fetch node states
        engine = OrchestrationEngine(execution_id)
        engine.workflow = workflow
        node_states = await engine._get_all_node_states()

        logger.info(f"Node states for {execution_id}: {node_states}")

        return WorkflowStatusResponse(
            execution_id=execution_id,
            status=meta["status"],
            nodes={
                node_id: NodeStatus(
                    state=data["state"],
                    output=data.get("output"),
                )
                for node_id, data in node_states.items()
            },
        )

    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to get workflow status for {execution_id}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get workflow status",
        )


@router.get("/workflows/{execution_id}/results")
async def get_workflow_results(execution_id: str):
    try:
        """
        Retrieves the final aggregated outputs of the workflow.
        """
        meta = await redis_client.get_workflow_meta(execution_id)

        if not meta:
            raise HTTPException(status_code=404, detail="Workflow execution not found")

        if meta["status"] != NodeState.COMPLETED.value:
            return {
                "execution_id": execution_id,
                "status": meta["status"],
                "message": "Results are only available once the workflow is COMPLETED.",
                "results": {},
            }

        engine = OrchestrationEngine(execution_id)
        await engine.initialize()
        node_states = await engine._get_all_node_states()

        return {
            "execution_id": execution_id,
            "status": meta["status"],
            "results": {nid: data.get("output") for nid, data in node_states.items()},
        }

    except HTTPException:
        raise
    except Exception:
        logger.exception(f"Failed to get workflow results for {execution_id}")
        raise HTTPException(
            status_code=500,
            detail="Failed to get workflow results",
        )
