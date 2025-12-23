import pytest
from schemas import WorkflowSchema, DAGSchema, NodeSchema
from services.dag_service import DAGService
from unittest.mock import MagicMock, patch


@pytest.fixture
def dag_service():
    workflow = WorkflowSchema(
        name="TestWorkflow",
        dag=DAGSchema(
            nodes=[
                NodeSchema(id="A", handler="input", dependencies=[]),
                NodeSchema(id="B", handler="call_external_service", dependencies=["A"]),
                NodeSchema(id="C", handler="output", dependencies=["A", "B"]),
            ]
        ),
    )
    return DAGService(workflow)


def test_validate_and_build_success(dag_service):
    with patch("core.redis_client.set_workflow_meta") as mock_set_meta, patch(
        "core.redis_client.set_node_state"
    ) as mock_set_node_state:
        execution_id = dag_service.validate_and_build()

        assert execution_id is not None
        mock_set_meta.assert_called()
        mock_set_node_state.assert_called()


def test_validate_workflow_duplicate_ids(dag_service):
    dag_service.workflow.dag.nodes.append(
        NodeSchema(id="A", handler="call_external_service", dependencies=[])
    )

    with pytest.raises(ValueError, match="Duplicate Node IDs detected"):
        dag_service._validate_workflow()


def test_validate_workflow_missing_dependency(dag_service):
    dag_service.workflow.dag.nodes.append(
        NodeSchema(
            id="D", handler="call_external_service", dependencies=["NonExistentNode"]
        )
    )

    # Reinitialize DAGService to rebuild internal node_map
    dag_service = DAGService(dag_service.workflow)

    with pytest.raises(ValueError, match="depends on non-existent node"):
        dag_service._validate_workflow()


def test_build_dag_circular_dependency(dag_service):
    dag_service.graph.add_edge("C", "A")  # Introduce a cycle

    with pytest.raises(ValueError, match="Circular dependency detected"):
        dag_service._check_cycles()


def test_initialize_node_states(dag_service):
    with patch("core.redis_client.set_node_state") as mock_set_node_state:
        execution_id = "test_execution_id"
        dag_service._initialize_node_states(execution_id)

        assert mock_set_node_state.call_count == len(dag_service.nodes)
