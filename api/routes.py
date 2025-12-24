from typing import Any

from fastapi import APIRouter, Depends

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


# Dependency provider to get a service instance
def get_workflow_service():
    return WorkflowService()


@router.post("/workflow", response_model=WorkflowSubmitResponse)
async def submit_workflow(
    workflow: WorkflowSchema, service: WorkflowService = Depends(get_workflow_service)
):
    execution_id = await service.submit(workflow)
    return WorkflowSubmitResponse(execution_id=execution_id, status=NodeState.PENDING)


@router.post("/workflow/trigger/{execution_id}", response_model=WorkflowTriggerResponse)
async def trigger_workflow(
    execution_id: str,
    params: dict[str, Any] | None = None,
    service: WorkflowService = Depends(get_workflow_service),
):
    await service.trigger(execution_id, params)
    return WorkflowTriggerResponse(execution_id=execution_id, status=NodeState.RUNNING)


@router.get("/workflows/{execution_id}", response_model=WorkflowStatusResponse)
async def get_workflow_status(
    execution_id: str, service: WorkflowService = Depends(get_workflow_service)
):
    return await service.get_status(execution_id)


@router.get("/workflows/{execution_id}/results", response_model=WorkflowResultsResponse)
async def get_workflow_results(
    execution_id: str, service: WorkflowService = Depends(get_workflow_service)
):
    return await service.get_results(execution_id)
