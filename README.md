# Workflow Orchestration Engine

This project is a workflow orchestration engine that allows users to define, submit, and manage complex workflows. It uses a combination of technologies to provide a robust and scalable solution for orchestrating tasks.

## Features

- **Workflow Definition**: Define workflows using a simple and intuitive schema.
- **Workflow Submission**: Submit workflows to the engine for execution.
- **Workflow Management**: Track the status and results of workflows.
- **Scalable Architecture**: The engine is designed to be scalable and can handle a large number of workflows.
- **Asynchronous Processing**: Workflows are processed asynchronously using a message queue.

## Technologies Used

- **Python**: The core language used for the project.
- **FastAPI**: A modern, fast (high-performance) web framework for building APIs.
- **Redis**: An in-memory data structure store, used as a database and message broker.
- **Kafka**: A distributed streaming platform.
- **Docker**: A platform for developing, shipping, and running applications in containers.

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
    ```

3.  **Run the application**:

    ```bash
    docker-compose up -d
    ```

    This will start the application and all its dependencies in detached mode.

## API Endpoints

The following API endpoints are available:

- `POST /workflow`: Submit a new workflow to the engine.
- `POST /workflow/trigger/{execution_id}`: Trigger the execution of a workflow. This endpoint accepts an optional JSON body with parameters for the workflow.
  > **Note:** The `params` object contains node-specific configurations. The keys of the object are the node IDs, and the values are merged into the `config` of the corresponding node. For example, the following `params` object will update the `prompt` in the `config` of the `llm_service` node:
  >
  > ```json
  > {
  >   "llm_service": {
  >     "prompt": "Recent Posts: {{ get_posts.output.data }}. Task: Summarize this user activity"
  >    }
  > }
  > ```
- `GET /workflows/{execution_id}`: Get the status of a workflow.
- `GET /workflows/{execution_id}/results`: Get the results of a workflow.

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
pytest
```
