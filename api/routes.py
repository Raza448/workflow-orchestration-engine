from fastapi import APIRouter, HTTPException
from schemas import WorkflowSchema
from services import DAGService
from core import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.get("/health")
async def health():
    """
    Health check endpoint for the API.
    Returns a simple welcome message indicating that the service is running.
    """
    return {"message": "Welcome! to the Workflow Orchestration Engine!"}


@router.post("/workflow")
def submit_workflow(workflow: WorkflowSchema):
    """
    Accepts a JSON workflow definition and validates it.
    Responsibilities:
      - Validate workflow structure and dependencies
      - Detect circular dependencies
      - Construct DAG
      - Persist workflow metadata in Redis
      - Initialize node states to PENDING

    Returns:
      - execution_id: Unique ID for this workflow
      - status: Current workflow state (PENDING/stored)

    Raises:
      - HTTPException 400 if workflow validation fails
    """
    logger.info(f"Received workflow submission: {workflow}")
    try:
        dag_service = DAGService()
        execution_id = dag_service.validate_and_build(workflow)
        logger.info(f"Workflow stored successfully with execution ID: {execution_id}")
        return {"execution_id": execution_id, "status": "stored"}
    except ValueError as e:
        logger.error(f"Workflow validation failed: {e}")
        raise HTTPException(
            status_code=400,
            detail="Workflow validation failed. Please check your input.",
        )
