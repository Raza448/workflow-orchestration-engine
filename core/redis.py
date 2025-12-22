import redis
from core import get_logger, settings

# Configure logging
logger = get_logger(__name__)


class RedisClient:
    """
    Thin wrapper around a redis.Redis client that provides lazy connection
    management, common convenience methods for hash and string operations, and
    uniform logging and error handling.

    Behavior
    - Lazily initializes a redis.Redis client on first use (via connect()).
    - Uses application-level settings (e.g. settings.redis_host, settings.redis_port,
        settings.redis_db, settings.redis_password, settings.redis_decode_responses)
        to configure the connection.
    - Logs key lifecycle events (initialization, connect attempts, operations,
        and close) and re-raises underlying exceptions to allow callers to handle
        failures.

    Attributes
    - client (redis.Redis | None): The underlying Redis connection object or None
        before the first connection.
    """

    def __init__(self):
        self.client = None
        logger.info("RedisClient initialized.")

    def connect(self):
        """Establish and return a Redis client.

        If a client is not already present on the instance (self.client), attempts to create
        and connect a new redis.Redis client.

        On successful connection the new client is stored on self.client and returned.
        If a client already exists, it is returned unchanged.

        Returns:
            redis.Redis: A connected Redis client instance.

        Raises:
            redis.ConnectionError: If the connection attempt fails.
        """
        if not self.client:
            logger.info("Connecting to Redis...")
            try:
                self.client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    db=settings.redis_db,
                    password=settings.redis_password,
                    decode_responses=settings.redis_decode_responses,
                )
                logger.info("Connected to Redis successfully.")
            except redis.ConnectionError as e:
                logger.error(f"Failed to connect to Redis: {e}")
                raise
        return self.client

    def hset(self, key: str, mapping: dict):
        """
        Set multiple fields in a Redis hash stored at the given key.

        This method obtains a Redis client via self.connect(), logs the operation, and
        invokes the client's hset method with the provided mapping.

        Args:
            key (str): Redis key under which the hash is stored.
            mapping (dict): A mapping of field names to values to set on the hash.

        Returns:
            Any: The raw result returned by the Redis client (commonly the number of fields added or updated).

        Raises:
            Exception: Propagates any exception encountered while connecting to Redis or executing the hset command.
        """
        try:
            logger.info(f"Setting hash for key: {key}")
            client = self.connect()
            result = client.hset(key, mapping=mapping)
            return result
        except Exception as e:
            logger.error(f"Failed to set hash for key {key}: {e}")
            raise

    def get(self, key: str):
        """
        Retrieve the value associated with the given key from the Redis database.

        Args:
            key (str): The key whose associated value is to be retrieved.

        Returns:
            The value associated with the specified key, or None if the key does not exist.

        Raises:
            Exception: If an error occurs while attempting to retrieve the value.
        """
        try:
            logger.info(f"Getting value for key: {key}")
            client = self.connect()
            value = client.get(key)
            return value
        except Exception as e:
            logger.error(f"Failed to get value for key {key}: {e}")
            raise

    def hget(self, key: str):
        """
        Retrieve all fields and values of a hash stored at the given key in Redis.

        This method connects to the Redis server, retrieves the hash stored at the
        specified key, and logs the operation. If the key does not exist, an empty
        dictionary is returned.

        Args:
            key (str): The key of the hash to retrieve.

        Returns:
            dict: A dictionary containing the fields and values of the hash.

        Raises:
            Exception: If there is an error during the operation, it is logged and
            re-raised.
        """
        try:
            logger.info(f"Getting hash for key: {key}")
            client = self.connect()
            value = client.hgetall(key)
            logger.info(f"Hash retrieved for key: {key}: {value}")
            return value
        except Exception as e:
            logger.error(f"Failed to get hash for key {key}: {e}")
            raise

    def exists(self, key: str) -> bool:
        """
        Check if a given key exists in the Redis database.

        Args:
            key (str): The key to check for existence.

        Returns:
            bool: True if the key exists, False otherwise.

        Logs:
            Logs the key being checked and any errors encountered during the operation.

        Raises:
            Exception: If an error occurs while checking the key's existence.
        """
        try:
            logger.info(f"Checking existence of key: {key}")
            client = self.connect()
            exists = client.exists(key) > 0
            return exists
        except Exception as e:
            logger.error(f"Failed to check existence of key {key}: {e}")
            raise

    def close(self):
        """
        Closes the Redis connection if it is open.

        This method attempts to close the Redis client connection and logs the process.
        If an error occurs during the closing operation, it is logged and re-raised.

        Raises:
            Exception: If an error occurs while closing the Redis connection.
        """
        try:
            logger.info("Closing Redis connection.")
            if self.client:
                self.client.close()
                logger.info("Redis connection closed.")
        except Exception as e:
            logger.error(f"Error while closing Redis connection: {e}")
            raise


redis_client = RedisClient()
