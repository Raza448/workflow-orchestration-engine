import redis.asyncio as redis
import json
from core import get_logger, settings
from core.utils import get_workflow_meta_key, get_runtime_params_key

logger = get_logger(__name__)


class RedisClient:
    def __init__(self):
        self.client = None
        logger.info("RedisClient initialized.")

    async def connect(self):
        """Standardized async connection logic."""
        if not self.client:
            try:
                self.client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    password=settings.redis_password,
                    decode_responses=True,
                )
                await self.client.ping()
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise
        return self.client

    async def set_dag(self, execution_id: str, dag_json: str, ttl: int = 86400):
        """Stores the DAG blueprint for the duration of the execution."""
        client = await self.connect()
        await client.set(f"dag_def:{execution_id}", dag_json, ex=ttl)

    async def get_dag(self, execution_id: str) -> str | None:
        client = await self.connect()
        return await client.get(f"dag_def:{execution_id}")

    # async def set_runtime_params(self, execution_id: str, params: dict):
    #     """Stores parameters provided during the /trigger call."""
    #     client = await self.connect()
    #     key = get_runtime_params_key(execution_id)
    #     await client.hset(key, mapping={k: json.dumps(v) for k, v in params.items()})
    #     await client.expire(key, 86400)

    async def get_runtime_params(self, execution_id: str) -> dict:
        client = await self.connect()
        key = get_runtime_params_key(execution_id)
        raw = await client.hgetall(key)
        return {k: json.loads(v) for k, v in raw.items()}

    async def set_workflow_meta(self, execution_id: str, status: str):
        key = get_workflow_meta_key(execution_id)
        await self.hset(key, {"status": status})
        client = await self.connect()
        await client.expire(key, 86400)

    async def get_workflow_meta(self, execution_id: str):
        key = get_workflow_meta_key(execution_id)
        return await self.hget(key)

    async def set_workflow_status(self, key: str, status: str):
        await self.hset(key, {"status": status})

    async def set_node_state(self, key: str, state: str, output: dict | None = None):
        """Sets the state and optional output for a node."""
        value = json.dumps({"state": state, "output": output or {}})
        client = await self.connect()
        await client.set(key, value, ex=86400)

    async def get_node_state(self, key: str):
        client = await self.connect()
        data = await client.get(key)
        if not data:
            return {"state": "PENDING", "output": {}}
        return json.loads(data)

    async def mget(self, keys: list):
        if not keys:
            return []
        client = await self.connect()
        return await client.mget(keys)

    async def hset(self, key: str, mapping: dict):
        client = await self.connect()
        return await client.hset(key, mapping=mapping)

    async def hget(self, key: str):
        client = await self.connect()
        return await client.hgetall(key)

    async def sadd(self, key: str, value: str) -> bool:
        """
        Uses SADD to ensure only one orchestrator/process
        dispatches this node. Returns True if successful.
        """
        client = await self.connect()
        result = await client.sadd(key, value)
        return result > 0

    async def close(self):
        if self.client:
            await self.client.close()


redis_client = RedisClient()
