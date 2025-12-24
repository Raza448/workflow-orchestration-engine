# Design Document: Workflow Orchestration Engine

## 1. Introduction

This document outlines the design and architecture of the Workflow Orchestration Engine, a system for defining, executing, and monitoring workflows represented as Directed Acyclic Graphs (DAGs).

## 2. High-Level Architecture

The system is composed of several key components that work together to provide a robust and scalable workflow orchestration solution.

```
+-----------------+      +------------------+      +--------------------+
|   API Server    |----->| Workflow Service |----->| Orchestration Engine|
| (FastAPI)       |<-----| (Stateless)      |<-----| (Stateful)         |
+-----------------+      +------------------+      +--------------------+
       |                        |                             |
       v                        v                             v
+-------------------------------------------------------------------+
|                               Redis                               |
|        (State Management: DAGs, Node States, Workflow Status)     |
+-------------------------------------------------------------------+
       ^                        ^                             ^
       |                        |                             |
+-------------------------------------------------------------------+
|                               Kafka                               |
|                  (Task Queuing & Communication)                   |
+-------------------------------------------------------------------+
       ^                        ^                             ^
       |                        |                             |
+-----------------+      +------------------+      +--------------------+
|     Worker      |----->|  Task Handlers   |----->| Orchestration Engine|
|  (Concurrent)   |<-----|  (Business Logic)|<-----| (Callbacks)        |
+-----------------+      +------------------+      +--------------------+
```

The interaction between these components is as follows:

1.  A user submits a workflow definition to the **API Server**.
2.  The API Server forwards the request to the **Workflow Service**, which validates the workflow, creates a unique execution ID, and stores the workflow definition (DAG) and initial state in **Redis**.
3.  When a workflow is triggered, the Orchestration Engine is invoked. It identifies the initial tasks to be executed and sends them as messages to a **Kafka** topic.
4.  A **Worker** process consumes messages from the Kafka topic and executes the tasks using the appropriate **Task Handler**.
5.  After a task is completed, the Worker calls back to the **Orchestration Engine** to report the result.
6.  The Orchestration Engine updates the workflow state in **Redis** and determines the next tasks to be executed based on the DAG dependencies.
7.  This process continues until the workflow is complete or a failure occurs.

## 3. Component Breakdown

### 3.1. API Server (`api/`)

The API Server is a FastAPI application that provides a RESTful interface for interacting with the workflow engine. It is responsible for:

- Exposing endpoints for submitting, triggering, and monitoring workflows.
- Validating user input using Pydantic schemas.
- Delegating business logic to the `WorkflowService`.

### 3.2. Orchestration Engine (`orchestrator/`)

The Orchestration Engine is the core of the system, responsible for managing the lifecycle of each workflow execution. Its key responsibilities include:

- **DAG Traversal**: Determining the next nodes to execute based on their dependencies.
- **State Management**: Updating the state of nodes and the overall workflow in Redis.
- **Task Dispatch**: Publishing tasks to Kafka for execution by the Workers.
- **Template Resolution**: Resolving Jinja2-style templates in node configurations to enable dynamic data flow between nodes.

### 3.3. Worker (`worker/`)

The Worker is a separate process responsible for executing the actual tasks of a workflow. It is designed for scalability and can be run in multiple instances to process tasks in parallel. Its responsibilities include:

- Consuming task messages from Kafka.
- Executing task logic based on the specified handler (e.g., calling an external service, running a computation).
- Reporting task results back to the Orchestration Engine.
- Implementing retry logic with exponential backoff for handling transient failures.

## 4. State Management

Redis is used as the central state store for the orchestration engine, providing a fast and persistent way to track the progress of each workflow. The following data is stored in Redis:

- **Workflow DAG**: The complete definition of the workflow, stored as a JSON string.
- **Workflow Metadata**: The overall status of the workflow (e.g., `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`).
- **Node State**: The state of each individual node in the workflow, including its status and output.
- **Dispatched Set**: A Redis set is used to atomically track which nodes have been dispatched to prevent duplicate executions.

This approach allows the Orchestration Engine and Workers to be stateless, which simplifies scaling and improves resilience.

## 5. Task Communication

Kafka is used for asynchronous communication between the Orchestration Engine and the Workers. This provides several benefits:

- **Decoupling**: The Orchestration Engine and Workers can be developed, deployed, and scaled independently.
- **Asynchronous Processing**: The system is fully asynchronous, which improves performance and responsiveness.
- **Reliability**: Kafka's at-least-once delivery guarantee ensures that tasks are not lost in case of a failure.

The primary Kafka topic used is:

- **`workflow-tasks`**: This topic is used by the Orchestration Engine to send tasks to the Workers.

## 6. Error Handling and Retries

The system includes a robust error handling and retry mechanism to handle transient failures.

- **Worker Retries**: When a task fails, the Worker will automatically retry it up to a configurable number of times (`MAX_RETRIES`). An exponential backoff strategy is used to avoid overwhelming a failing service.
- **Workflow Failure**: If a task fails after all retries, it is marked as `FAILED`, and the entire workflow is halted to prevent further execution. The reason for the failure is recorded in the node's output for debugging purposes.

### Handling Failed Nodes

When a node fails due to template resolution errors or task execution failures, it is marked as FAILED and the workflow halts.

Failed nodes are not automatically retried; users can submit a new workflow with corrected inputs or parameters.

This approach ensures the workflow state accurately reflects real errors and prevents unintentional re-execution of tasks.

The design allows easy extension to support optional retry mechanisms for failed nodes in future versions if desired.

## 7. Configuration

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

## 8. Future Considerations

- **Complex Node Types**: Add support for more complex node types, such as conditional nodes  and loops (e.g., `for`).
- **Security**: Implement authentication and authorization to secure the API endpoints.
- **PostgreSQL**: Use PostgreSQL to store the graph JSON schema for workflows, enabling better querying, versioning, and persistence of workflow definitions.
- **Enhancements for Failed Node Retries**: Future versions could support optional retry of failed nodes without requiring new workflow submission. Include retry counts and history to prevent infinite loops and improve observability. Allow node-level or workflow-level retry control via optional parameters, giving users explicit choice over re-execution. Ensure all retries remain idempotent and safe, particularly for nodes with side effects in production.