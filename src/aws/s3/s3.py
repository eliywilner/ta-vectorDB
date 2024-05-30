from src.config.config import Config
from typing import Dict, Any, Union, List
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from pydantic import BaseModel, Field
import logging
import os
import requests

logger = logging.getLogger(__name__)

class S3Config(BaseModel):
    bucket_name: str = Field(..., title="S3 Bucket Name")

class S3Manager:
    def __init__(self, section: str = 's3'):
        self.section = section
        self.bucket_name = Config().get_configurations(self.section)['bucket-name']        
        self.s3_client = boto3.client(self.section)


    def upload_image(self, image_url: str, hash_val: str):
        existing_url = self.get_existing_s3_url(hash_val)
        if existing_url:
            logger.info(f"Image with hash {hash_val} already exists. S3 URL: {existing_url}")
            return existing_url

        try:
            # Download image from URL
            response = requests.get(image_url)
            if response.status_code != 200:
                logger.error(f"Failed to download image from {image_url}. Status code: {response.status_code}")
                return None

            # Upload image to S3
            key = f"{hash_val}.jpg"
            self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=response.content)

            # Get the S3 URL
            s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
            logger.info(f"Image uploaded successfully to {s3_url}")
            return s3_url
        except NoCredentialsError:
            logger.error("Credentials not available")
            return None
        except ClientError as e:
            logger.error(f"An error occurred while uploading image: {e}")
            return None

    def upload_images(self, image_urls: Union[str, List[str]], hashes: Union[str, List[str]]):
        uploaded_urls = []

        # If single image and hash provided, convert to lists
        if not isinstance(image_urls, list):
            image_urls = [image_urls]
        if not isinstance(hashes, list):
            hashes = [hashes]

        for image_url, hash_val in zip(image_urls, hashes):
            try:
                existing_url = self.get_existing_s3_url(hash_val)
                if existing_url:
                    logger.info(f"Image with hash {hash_val} already exists. S3 URL: {existing_url}")
                    uploaded_urls.append(existing_url)
                else:
                    # Download image from URL
                    response = requests.get(image_url)
                    if response.status_code != 200:
                        logger.error(f"Failed to download image from {image_url}. Status code: {response.status_code}")
                        uploaded_urls.append(None)
                        continue

                    # Upload image to S3
                    key = f"{hash_val}.jpg"
                    self.s3_client.put_object(Bucket=self.bucket_name, Key=key, Body=response.content)

                    # Get the S3 URL
                    s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
                    logger.info(f"Image uploaded successfully to {s3_url}")
                    uploaded_urls.append(s3_url)
            except NoCredentialsError:
                logger.error("Credentials not available")
                uploaded_urls.append(None)
            except ClientError as e:
                logger.error(f"An error occurred while uploading image: {e}")
                uploaded_urls.append(None)

        return uploaded_urls

    def get_existing_s3_url(self, hash_val: str) -> str:
        key = f"{hash_val}.jpg"
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return f"https://{self.bucket_name}.s3.amazonaws.com/{key}"
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return None
            logger.error(f"An error occurred while checking S3 object existence: {e}")
            return None
