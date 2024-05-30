import redis
from redis.cluster import RedisCluster
from redis.commands.search.field import TagField, VectorField
from redis.commands.search.indexDefinition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from typing import Dict, Any, List
from pydantic import BaseModel, Field, ValidationError
import logging
import json
import numpy as np
from src.config.config import Config
from src.aws.dynamoDB.dynamo_db import DynamoDBManager, ProductMetaData
from src.aws.s3.s3 import S3Manager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define our Redis configuration using Pydantic for validation
class RedisConfig(BaseModel):
    host: str = Field(..., title="Redis Host")
    port: int = Field(..., title="Redis Port", ge=1, le=65535)
    index: str = Field(..., title="Redis Index Name")
    vector_dim: int = Field(default=1024, title="Vector Dimension")
    hash_prefix: str = Field(default="HASH:", title="HASH Prefix")
    ssl: bool = Field(default=True, title="SSL Usage")

class RedisDataBase:
    def __init__(self, config=Config(), section: str = 'redis'):
        redis_configuration = config.get_configurations(section=section)
        try:
            # Validate and parse configuration using Pydantic
            self.redis_config = RedisConfig(**redis_configuration)
        except ValidationError as e:
            logger.error(f"Configuration error: {e}")
            raise

        self.vector_dim = int(self.redis_config.vector_dim)
        self.host = self.redis_config.host
        self.port = int(self.redis_config.port)
        self.index_name = self.redis_config.index
        self.hash_prefix = self.redis_config.hash_prefix
        self.ssl = self.redis_config.ssl

        self.r = None
        self.connect_to_redis()


    def connect_to_redis(self):
        try:
            self.r = RedisCluster(
                host=self.host,
                port=self.port,
                ssl=self.ssl,
                decode_responses=False
            )

            # Check if index exists
            self.r.ft(self.index_name).info()
            logger.info(f'Found Redis Cluster: {self.index_name}')
        except Exception as e:
            logger.error(f"Error connecting to Redis or finding index: {e}")
            self.create_index()

    def create_index(self):
        try:
            schema = (
                TagField("tag"),
                VectorField("vector", "FLAT", {
                    "TYPE": "FLOAT32",
                    "DIM": self.vector_dim,
                    "DISTANCE_METRIC": "COSINE"
                }),
            )
            definition = IndexDefinition(
                prefix=[self.hash_prefix],
                index_type=IndexType.HASH
            )
            self.r.ft(self.index_name).create_index(fields=schema, definition=definition)
            logger.info(f'Index created with name: {self.index_name}')
        except Exception as e:
            logger.error(f"Error creating index: {e}")

    def upload_embeddings(self, hashes: List[str], embeddings: List[List[float]], image_urls: List[str], asins: List[str], image_titles: List[str], tag: str = 'amazon'):

        try:
            if len(hashes) != len(embeddings):
                logger.error("Lengths of hashes, embeddings, and tags should be the same.")
                return
            
            for hash_val, embedding, image_url, asin, image_title in zip(hashes, embeddings, image_urls, asins, image_titles):
                if self.r.exists(f"{self.hash_prefix}{hash_val}"):
                    logger.info(f"Hash {hash_val} already exists in the store.")
                else:                    
                    embedding_np = np.array(embedding, dtype=np.float16)
                    self.r.hset(f"{self.hash_prefix}{hash_val}", mapping={"tag": tag, "vector": embedding_np.tobytes()})
                    logger.info(f'Embedding uploaded to VectorDB for hash: {hash_val}')

            
            logger.info("All embeddings uploaded")
        except Exception as e:
            logger.error(f"Error uploading embeddings: {e}")

        

    def clear_index(self):
        try:
            # Flush all data and configurations from the index
            self.r.ft(self.index_name).flushdb()
            logger.info(f'Index {self.index_name} cleared')
        except Exception as e:
            logger.error(f"Error clearing index: {e}")


    def hash_prefix_exists(self, hash_prefix: str) -> bool:
        try:
            keys = self.r.keys(f"{hash_prefix}*")
            return len(keys) > 0
        except Exception as e:
            logger.error(f"HASH prefix existence: {e}")
            return False
    
