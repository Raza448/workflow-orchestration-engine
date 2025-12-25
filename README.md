# Workflow Orchestration Engine

This project is a workflow orchestration engine that allows users to define, submit, and manage complex workflows. It uses a combination of technologies to provide a robust and scalable solution for orchestrating tasks.

## Features

- **Workflow Definition**: Define workflows as Directed Acyclic Graphs (DAGs) using a simple and intuitive JSON schema.
- **Dynamic Templating**: Use Jinja2-style templating to pass outputs from one node to another.
- **Scalable Architecture**: The engine is designed to be scalable and can handle a large number of workflows.
- **Asynchronous Processing**: Workflows are processed asynchronously using Kafka as a message queue.
- **State Management**: Redis is used to maintain the state of each workflow, ensuring resilience.

## Technologies Used

- **Python**: The core language used for the project.
- **FastAPI**: A modern, fast (high-performance) web framework for building APIs.
- **Redis**: An in-memory data structure store, used for state management.
- **Kafka**: A distributed streaming platform for asynchronous task queuing.
- **Docker**: A platform for developing, shipping, and running applications in containers.
- **Pydantic**: Data validation and settings management.
- **NetworkX**: For graph operations and validation (e.g., cycle detection).

## Getting Started

To get started with the project, you will need to have Docker and Docker Compose installed on your machine.

1.  **Clone the repository**:

    ```bash
    git clone https://github.com/your-username/workflow-orchestration-engine.git
    ```

2.  **Create a `.env.docker` file**:

    Create a `.env.docker` file in the root of the project with the following content:

    ```
    APP_REDIS_HOST=redis
    APP_REDIS_PORT=6379
    APP_REDIS_DB=0
    APP_REDIS_PASSWORD=
    APP_KAFKA_BOOTSTRAP_SERVERS=kafka:9092
    APP_LOG_LEVEL=INFO
    APP_MAX_RETRIES=3
    APP_BASE_BACKOFF=1
    APP_RETRY_DELAY=1
    ```

3.  **Run the application**:

    ```bash
    docker-compose up -d
    ```

    This command starts the application and its dependencies in detached mode. It will use existing Docker images if they are already built.

    ```bash
    docker-compose up --build -d
    ```

    This command ensures that the Docker images are rebuilt before starting the containers. Use this if you have made changes to the Dockerfile or dependencies.

## Workflow Schema

A workflow is defined by a name and a Directed Acyclic Graph (DAG). The DAG contains a list of nodes, where each node represents a task.

- **`name`**: A string that gives a name to the workflow.
- **`dag`**: An object that contains the nodes of the workflow.
  - **`nodes`**: A list of `NodeSchema` objects.

### NodeSchema

Each node in the DAG is defined by the following attributes:

- **`id`**: A unique identifier for the node.
- **`handler`**: The type of task the node will execute. The available handlers are:
  - `input`: Represents an input to the workflow.
  - `call_external_service`: Calls an external service.
  - `call_llm_service`: Calls a large language model service.
  - `output`: Represents the output of the workflow.
- **`dependencies`**: A list of node IDs that must be completed before this node can be executed.
- **`config`**: A `NodeConfig` object that contains the configuration for the node. This is required for handlers like `call_external_service` and `call_llm_service`.
  - **`url`**: The URL of the external service to call.
  - **`prompt`**: The prompt to send to the LLM service.

### Example Workflow

Here is an example of a simple workflow that gets data from an external service, processes it with an LLM, and then returns the result.

```json
{
  "name": "Simple LLM Workflow",
  "dag": {
    "nodes": [
      {
        "id": "get_posts",
        "handler": "call_external_service",
        "config": {
          "url": "https://api.example.com/posts"
        }
      },
      {
        "id": "summarize_posts",
        "handler": "call_llm_service",
        "dependencies": ["get_posts"],
        "config": {
          "prompt": "Summarize the following posts: {{ get_posts.output.data }}"
        }
      },
      {
        "id": "output",
        "handler": "output",
        "dependencies": ["summarize_posts"]
      }
    ]
  }
}
```

## API Endpoints

The following API endpoints are available:

### `POST /workflow`

Submit a new workflow to the engine.

**Request Body:**

```json
{
  "name": "Simple LLM Workflow",
  "dag": {
    "nodes": [
      {
        "id": "get_posts",
        "handler": "call_external_service",
        "config": {
          "url": "https://api.example.com/posts"
        }
      },
      {
        "id": "summarize_posts",
        "handler": "call_llm_service",
        "dependencies": ["get_posts"],
        "config": {
          "prompt": "Summarize the following posts: {{ get_posts.output.data }}"
        }
      },
      {
        "id": "output",
        "handler": "output",
        "dependencies": ["summarize_posts"]
      }
    ]
  }
}
```

**Response Body:**

```json
{
  "execution_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  "status": "PENDING"
}
```

### `POST /workflow/trigger/{execution_id}`

Trigger the execution of a workflow. This endpoint accepts an optional JSON body with parameters for the workflow.

**Request Body (Optional):**

```json
{
  "summarize_posts": {
    "prompt": "Recent Posts: {{ get_posts.output.data }}. Task: Summarize this user activity"
   }
}
```
> **Note**: Ensure that the node ID used in the parameter matches the node ID defined in the workflow. In this example, `summarize_posts` is the node ID being referenced.

**Response Body:**

```json
{
  "execution_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  "status": "RUNNING"
}
```

### `GET /workflows/{execution_id}`

Get the status of a workflow.

**Response Body:**

```json
{
  "execution_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  "status": "RUNNING",
  "nodes": {
    "get_posts": {
      "state": "COMPLETED",
      "output": {
        "data": "Response from https://api.example.com/posts"
      }
    },
    "summarize_posts": {
      "state": "RUNNING",
      "output": {}
    },
    "output": {
      "state": "PENDING",
      "output": {}
    }
  }
}
```

### `GET /workflows/{execution_id}/results`

Get the results of a workflow.

**Response Body:**

```json
{
  "execution_id": "a1b2c3d4-e5f6-7890-1234-567890abcdef",
  "status": "COMPLETED",
  "completed": true,
  "results": {
    "final_status": "workflow_completed",
    "aggregated_results": {
      "summarize_posts": {
        "result": "LLM generated a summary for the prompt 'Summarize the following posts: Response from https://api.example.com/posts'"
      }
    }
  },
  "message": "Results retrieved successfully."
}
```

## API Documentation

The Swagger UI for the API documentation can be accessed at:

- **Swagger UI**: `http://localhost:8000/docs`

This assumes the application is running locally on port 8000. Adjust the port or host if different.

## Project Structure

The project is structured as follows:

- `api/`: Contains the FastAPI application and its routes.
- `core/`: Contains the core components of the application, such as the database and message queue clients.
- `orchestrator/`: Contains the workflow orchestration engine.
- `schemas/`: Contains the Pydantic schemas used for data validation.
- `services/`: Contains the business logic for the application.
- `tests/`: Contains the tests for the project.
- `worker/`: Contains the worker that processes workflows.

## How to Run Tests

To run the tests for the project, you can use the following command:

```bash
pytest -v
```
