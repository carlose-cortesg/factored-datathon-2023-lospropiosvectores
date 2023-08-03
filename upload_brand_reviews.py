import argparse, sys
import pandas as pd
import numpy as np
from scipy.spatial import distance
from collections import defaultdict
from typing import List, Tuple
import spacy
from google.cloud import bigquery
import os
from fast_sentence_transformers import FastSentenceTransformer as SentenceTransformer
from tqdm import tqdm

from vectorDB.classes import VectorDatabase

## SET AUTH SERVICE ACCOUNT
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"


### GET BRAND FROM CMD
parser = argparse.ArgumentParser()

parser.add_argument("--brand", help="Brand to upload reviews")

args = parser.parse_args()

brand = args.brand

## UPLOAD DATA TO VECTOR DATABASE
model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu", quantize=True)


nlp = spacy.load("en_core_web_lg")



questions = {
'Fit' : 'Does it fit well?',
'Comfortable' : 'Is it comfortable?',
'Material_Quality' : '''How is the material's quality?''',
'Price_and_Value' : 'How is the price',
'Fiability':'Does it look like the pictures?',
'Ease_of_use':'Is it easy to use?',
'Durability':'How is the durability?',
'Functionality':'Does it work as expected?'
}

vector_db = VectorDatabase(nlp, model)
print('uploading vectors to DB')

for i in questions:
    vector_db.insert(questions[i],i)


## GET REVIEWS FROM BRAND

client = bigquery.Client()


sql = f"""
SELECT asin, reviewText, overall,summary,reviewerID
FROM `factored.raw_reviews`
inner join `factored.metadata` using(asin)
where brand = '{brand}'
order by asin
"""

df = client.query(sql).result().to_dataframe()

## WHERE THE MAGIC HAPPENS
# We iterate through the reviews
# On each review we try to identify the topic i.e (quality, longevity, luxury): the set of topics depends on the sample_reviews file (it depends on the brand)
# Every Review Topic (+ overall) creates a row in the database

all_reviews = []

df = df.sample(2000)
for index, row in tqdm(df.iterrows(), total=df.shape[0]):
    if (row["reviewText"] is not None) & (row["reviewText"] != ""):
        topics_score = vector_db.long_search(row["reviewText"])
        
        

        reviews = {'asin':row["asin"],
                   'reviewText': row["reviewText"],
                   'overall': row["overall"],
                   "summary": row["summary"], 
                   "reviewerID":row["reviewerID"]}
        
        reviews = pd.DataFrame(reviews, index = [0])
        reviews = pd.concat([reviews,topics_score],axis=1)
        all_reviews.append(reviews)

reviews = pd.concat(all_reviews)


## UPLOAD THE RESULTING TABLE OF REVIEWS TO BIGQUERY

job_config = bigquery.LoadJobConfig(
    write_disposition="WRITE_TRUNCATE",
)

job = client.load_table_from_dataframe(
    reviews, f"factored.{brand}_reviews_by_topic", job_config=job_config
)  # Make an API request.

job.result()  # Wait for the job to complete.

print("data uploaded to {}".format(f"factored.{brand}_reviews_by_topic"))


#python upload_brand_reviews.py --brand=Casio
#python upload_brand_reviews.py --brand="Michael Kors"
#python upload_brand_reviews.py --brand="The North Face"

