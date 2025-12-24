import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from core import get_logger

logger = get_logger(__name__)


class KafkaClient:
    def __init__(self, bootstrap_servers=None):
        # Use environment variable or default to localhost for local dev
        if bootstrap_servers is None:
            bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.bootstrap_servers = bootstrap_servers
        self.producer = None

    async def connect(self):
        """Initialize and start the producer with retries for resilience."""
        if self.producer is not None:
            return

        max_retries = 10
        retry_delay = 3  # seconds

        for attempt in range(1, max_retries + 1):
            try:
                logger.info(
                    f"Attempting to connect to Kafka (Attempt {attempt}/{max_retries})..."
                )
                self.producer = AIOKafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    acks="all",
                    retry_backoff_ms=500,
                )
                await self.producer.start()
                logger.info(
                    f"Kafka Producer started successfully at {self.bootstrap_servers}"
                )
                return  # Success!
            except Exception as e:
                self.producer = None  # Reset for next attempt
                if attempt == max_retries:
                    logger.error("Could not connect to Kafka after maximum retries.")
                    raise e

                logger.warning(
                    f"Kafka connection failed: {e}. Retrying in {retry_delay}s..."
                )
                await asyncio.sleep(retry_delay)

    async def disconnect(self):
        """Flush and stop the producer on app shutdown."""
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka Producer disconnected.")

    async def publish(self, topic: str, message: dict):
        if not self.producer:
            raise RuntimeError("KafkaClient is not connected. Call .connect() first.")
        await self.producer.send_and_wait(topic, message)

    async def get_consumer(self, topic: str, group_id: str):
        """Create, start and return a Kafka consumer"""
        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=False,
        )
        await consumer.start()  # Start it here
        return consumer


kafka_client = KafkaClient()
