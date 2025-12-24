from contextlib import asynccontextmanager
from fastapi import FastAPI
from core import get_logger, kafka_client, redis_client

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Orchestrates the startup and shutdown of all infrastructure dependencies.
    """

    try:
        logger.info("Initializing infrastructure...")

        # 1. Connect Kafka
        await kafka_client.connect()

        # 2. Connect Redis
        await redis_client.connect()

        logger.info("All systems go. Orchestration Engine is online.")
    except Exception as e:
        logger.error(f"Critical startup failure: {e}")
        raise e  # Fail-fast: prevent the app from starting

    yield

    logger.info("Shutting down: Flushing buffers and closing connections...")
    try:
        await kafka_client.disconnect()
        await redis_client.disconnect()
        logger.info("Graceful shutdown complete.")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
