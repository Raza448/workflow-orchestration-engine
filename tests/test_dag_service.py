import pytest
from schemas import WorkflowSchema, DAGSchema, NodeSchema
from services import DAGService
from unittest.mock import MagicMock, patch


@pytest.fixture
def dag_service():
    return DAGService()


def test_validate_and_build_success(dag_service):
    # Use actual schema instead of mock
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="input", dependencies=[]),
                NodeSchema(id="B", handler="process", dependencies=["A"]),
                NodeSchema(id="C", handler="output", dependencies=["A", "B"]),
            ]
        ),
    )

    with patch("services.dag_service.redis_client") as mock_redis:
        mock_redis.hset = MagicMock()
        execution_id = dag_service.validate_and_build(workflow)

        assert execution_id is not None
        mock_redis.hset.assert_called()


def test_validate_workflow_duplicate_ids(dag_service):
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="input", dependencies=[]),
                NodeSchema(id="A", handler="process", dependencies=[]),
            ]
        ),
    )

    with pytest.raises(ValueError, match="Node IDs must be unique"):
        dag_service._validate_workflow(workflow)


def test_validate_workflow_missing_dependency(dag_service):
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="input", dependencies=[]),
                NodeSchema(id="B", handler="process", dependencies=["C"]),
            ]
        ),
    )

    with pytest.raises(ValueError, match="Dependency 'C' does not exist for node 'B'"):
        dag_service._validate_workflow(workflow)


def test_build_dag_circular_dependency(dag_service):
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="process", dependencies=["B"]),
                NodeSchema(id="B", handler="process", dependencies=["C"]),
                NodeSchema(id="C", handler="process", dependencies=["A"]),
            ]
        ),
    )

    with pytest.raises(ValueError, match="Workflow contains a circular dependency"):
        dag_service._build_dag(workflow)


def test_initialize_node_states(dag_service):
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="input", dependencies=[]),
                NodeSchema(id="B", handler="process", dependencies=["A"]),
            ]
        ),
    )

    with patch("services.dag_service.redis_client") as mock_redis:
        mock_redis.hset = MagicMock()
        execution_id = "test_execution_id"
        dag_service._initialize_node_states(execution_id, workflow)

        mock_redis.hset.assert_called_with(
            "workflow:test_execution_id:nodes",
            mapping={"A": "PENDING", "B": "PENDING"},
        )
