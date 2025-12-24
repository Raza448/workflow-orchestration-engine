# Design Document: Workflow Orchestration Engine

## 1. Introduction

This document outlines the design and architecture of the Workflow Orchestration Engine, a system for defining, executing, and monitoring workflows represented as Directed Acyclic Graphs (DAGs).

## 2. High-Level Architecture

The system is composed of several key components:

- **API Server**: A FastAPI application providing a RESTful API for workflow management.
- **Orchestration Engine**: The core component managing the lifecycle of workflows.
- **Worker**: A separate process that executes individual workflow tasks.
- **Redis**: An in-memory data store for state management.
- **Kafka**: A distributed streaming platform for task queuing and communication.

The interaction between these components is as follows:

1.  A user submits a workflow definition to the API Server.
2.  The API Server validates the workflow and passes it to the Orchestration Engine.
3.  The Orchestration Engine stores the workflow definition and its initial state in Redis.
4.  When a workflow is triggered, the Orchestration Engine determines the next task to be executed and sends a message to a Kafka topic.
5.  A Worker process consumes the message from the Kafka topic and executes the task.
6.  The Worker sends the result of the task to another Kafka topic.
7.  The Orchestration Engine consumes the result message, updates the workflow state in Redis, and determines the next task to be executed.
8.  This process continues until the workflow is complete.

## 3. Component Breakdown

### 3.1. API Server (`api/`)

The API Server is a FastAPI application with the following endpoints:

- `POST /workflow`: Submit a new workflow.
- `POST /workflow/trigger/{execution_id}`: Trigger a workflow execution.
- `GET /workflows/{execution_id}`: Get the status of a workflow.
- `GET /workflows/{execution_id}/results`: Get the results of a workflow.

The API Server validates user input and interacts with the Orchestration Engine.

### 3.2. Orchestration Engine (`orchestrator/`)

The Orchestration Engine manages the lifecycle of workflows, including:

- Parsing workflow definitions and representing them as DAGs.
- Determining the next task to be executed based on dependencies.
- Sending task execution messages to Kafka.
- Consuming task result messages from Kafka.
- Updating the workflow state in Redis.

### 3.3. Worker (`worker/`)

The Worker executes the individual tasks of a workflow. It is responsible for:

- Consuming task execution messages from Kafka.
- Executing the task logic.
- Sending task result messages to Kafka.

The Worker is designed for scalability, and multiple instances can be run in parallel.

### 3.4. Core Services (`core/`)

The `core/` directory contains shared components, such as:

- **Configuration management**: Loading and providing access to application settings.
- **Logging**: A centralized logging setup.
- **Redis client**: A client for interacting with Redis.
- **Kafka client**: A client for interacting with Kafka.

## 4. Data Models (`schemas/`)

The main data model is the `WorkflowSchema`, a Pydantic schema that defines the structure of a workflow. A workflow consists of a set of nodes and their dependencies, where each node represents a task to be executed.

## 5. Workflow Lifecycle

1.  **Submission**: A user submits a workflow definition to the `POST /workflow` endpoint. The Orchestration Engine validates the workflow and stores it in Redis with a `PENDING` status.
2.  **Triggering**: The user triggers the workflow using the `POST /workflow/trigger/{execution_id}` endpoint. The Orchestration Engine changes the workflow status to `RUNNING` and identifies the initial tasks to be executed.
3.  **Execution**: For each task, the Orchestration Engine sends a message to Kafka. A Worker consumes the message, executes the task, and sends the result back to Kafka.
4.  **State Update**: The Orchestration Engine consumes the task result, updates the task's state in Redis, and determines the next tasks to be executed based on the workflow's dependencies.
5.  **Completion**: This process continues until all tasks in the workflow have been executed. The Orchestration Engine then changes the workflow status to `COMPLETED`.

## 6. State Management

Redis is used to store the state of workflows and their tasks, including:

- The workflow definition.
- The status of the workflow (e.g., `PENDING`, `RUNNING`, `COMPLETED`).
- The status of each task (e.g., `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`).
- The results of each task.

This allows the system to be stateless, making it more resilient and scalable.

## 7. Task Communication

Kafka is used for communication between the Orchestration Engine and the Workers, providing:

- **Decoupling**: The Orchestration Engine and Workers can be developed, deployed, and scaled independently.
- **Asynchronous communication**: The system is fully asynchronous, improving performance and responsiveness.
- **Reliability**: Kafka provides at-least-once message delivery, ensuring that tasks are not lost.

## 8. Configuration

The application is configured using environment variables, which are loaded into a Pydantic `Settings` object. The following variables are available:

- `APP_REDIS_HOST`
- `APP_REDIS_PORT`
- `APP_REDIS_DB`
- `APP_REDIS_PASSWORD`
- `APP_KAFKA_BOOTSTRAP_SERVERS`
- `APP_LOG_LEVEL`
- `APP_MAX_RETRIES`
- `APP_BASE_BACKOFF`
- `APP_RETRY_DELAY`

## 9. Error Handling and Retries

The system includes a retry mechanism for failed tasks. When a task fails, the Worker will attempt to retry it up to `MAX_RETRIES` times with an exponential backoff strategy. If the task continues to fail, it will be marked as `FAILED`, and the workflow will be halted.

## 10. Future Considerations

- **Complex Node Types**: Add support for more complex node types, such as conditional nodes and loops.
- **Security**: Implement authentication and authorization to secure the API.
- **Improved Error Handling**: Enhance the error handling and reporting mechanisms to provide more detailed information about failures.