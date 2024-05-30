# from redis_db import RedisDataBase
import requests 
import pandas as pd
from typing import List
from src.aws.redisDB.redis_db import RedisDataBase
from src.aws.s3.s3 import S3Manager
from src.aws.dynamoDB.dynamo_db import DynamoDBManager, ProductMetaData

data_file_path = '/home/ec2-user/ta-vectorDB/src/test_data/raw_data.csv'
data = pd.read_csv(data_file_path)
data = data.drop(data.index[-1])

image_col = 'amazon_product_images_url'
text_col = 'amazon_product_title'
url = "http://ec2-35-92-155-213.us-west-2.compute.amazonaws.com:8000/embed/"

def embed_api(image_urls: List[str], texts: List[str], url: str = url):
    payload = {
    "image_urls": image_urls,
    "texts": texts
    }

    headers = {
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=payload, headers=headers)
    return response.json()


for index, row in data.iterrows():
    product_image_url = [row[image_col]]
    product_title = [row[text_col]]
    result = embed_api(image_urls=product_image_url, texts=product_title)['results'][0]

    embeddings = result['embedding']
    image_hash = result['image_hash']
    asin = row['ASIN']
    RedisDataBase().upload_embeddings(hashes=[image_hash],
                                       embeddings=[embeddings],
                                       image_urls=product_image_url, 
                                       asins=[asin],
                                       image_titles=product_title)
    s3_url = S3Manager().upload_images(image_urls=product_image_url, hashes=image_hash)[0]
    product_metadata = ProductMetaData(
        asin=asin,
        hash=image_hash,
        s3_url=s3_url,
        image_title=row[image_col]
    )

    DynamoDBManager().insert_metadata(product_metadata=product_metadata)




    

# r = RedisDataBase()

