from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Body

from core import get_logger
from schemas import (
    NodeState,
    WorkflowSchema,
    WorkflowStatusResponse,
    WorkflowSubmitResponse,
    WorkflowTriggerResponse,
    WorkflowResultsResponse,
)
from services.workflow_service import WorkflowService

logger = get_logger(__name__)
router = APIRouter()


@router.post("/workflow", response_model=WorkflowSubmitResponse, status_code=201)
async def submit_workflow(
    workflow: WorkflowSchema, service: WorkflowService = Depends(WorkflowService)
):
    try:
        execution_id = await service.submit(workflow)
        return WorkflowSubmitResponse(
            execution_id=execution_id, status=NodeState.PENDING
        )
    except ValueError as e:
        logger.warning(f"Workflow validation failed: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error submitting workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.post(
    "/workflow/trigger/{execution_id}",
    response_model=WorkflowTriggerResponse,
    status_code=202,
)
async def trigger_workflow(
    execution_id: str,
    params: dict[str, Any] | None = Body(None),
    service: WorkflowService = Depends(WorkflowService),
):
    try:
        await service.trigger(execution_id, params)
        return WorkflowTriggerResponse(
            execution_id=execution_id, status=NodeState.RUNNING
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error triggering workflow: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get(
    "/workflows/{execution_id}", response_model=WorkflowStatusResponse, status_code=200
)
async def get_workflow_status(
    execution_id: str, service: WorkflowService = Depends(WorkflowService)
):
    try:
        return await service.get_status(execution_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching workflow status: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")


@router.get(
    "/workflows/{execution_id}/results",
    response_model=WorkflowResultsResponse,
    status_code=200,
)
async def get_workflow_results(
    execution_id: str, service: WorkflowService = Depends(WorkflowService)
):
    try:
        return await service.get_results(execution_id)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error fetching workflow results: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal Server Error")
