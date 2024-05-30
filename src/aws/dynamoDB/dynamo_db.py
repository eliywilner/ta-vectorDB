from typing import Dict, Any
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from pydantic import BaseModel, Field
import logging
from src.config.config import Config

logger = logging.getLogger(__name__)

class DynamoDBConfig(BaseModel):
    table_name: str = Field(..., title="DynamoDB Table Name")

class ProductMetaData(BaseModel):
    asin: str
    hash: str
    s3_url: str
    image_title: str

class DynamoDBManager:
    def __init__(self, section: str = 'dynamodb'):
        self.section = section
        self.table_name = Config().get_configurations(self.section)['table_name']
        self.dynamodb_client = boto3.client('dynamodb', region_name='us-west-2')

    def insert_metadata(self, product_metadata: ProductMetaData):
        try:
            response = self.dynamodb_client.put_item(
                TableName=self.table_name,
                Item={
                    'hash': {'S': product_metadata.hash}, 
                    'asin': {'S': product_metadata.asin},
                    's3_url': {'S': product_metadata.s3_url},
                    'image_title': {'S': product_metadata.image_title}
                }
            )
            logger.info(f"Metadata inserted successfully for HASH {product_metadata.hash}")
        except ClientError as e:
            logger.error(f"An error occurred while inserting metadata: {e}")

    def get_metadata(self, hash: str) -> Dict[str, Any]:
        try:
            response = self.dynamodb_client.get_item(
                TableName=self.table_name,
                Key={'hash': {'S': hash}}
            )
            item = response.get('Item')
            if item:
                metadata = {
                    'hash': item.get('hash').get('S'),
                    'asin': item.get('asin').get('S'),
                    's3_url': item.get('s3_url').get('S'),
                    'image_title': item.get('image_title').get('S')
                }
                logger.info(f"Metadata retrieved successfully for HASH {hash}")
                return metadata
            else:
                logger.error(f"No metadata found for HASH {hash}")
                return None
        except ClientError as e:
            logger.error(f"An error occurred while retrieving metadata: {e}")
            return None
