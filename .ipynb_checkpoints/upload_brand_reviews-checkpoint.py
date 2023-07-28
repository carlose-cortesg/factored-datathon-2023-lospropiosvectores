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


from vectorDB.classes import VectorDatabase

## SET AUTH SERVICE ACCOUNT
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"


### GET BRAND FROM CMD
parser = argparse.ArgumentParser()

parser.add_argument("--brand", help="Brand to upload reviews")


parser.add_argument("--topics", type=str)




args = parser.parse_args()

brand = args.brand
topics = args.topics

if brand is None:
    raise ("Please make sure to use the brand parameter")


if topics is None:
    print('No selected Topics... we will use our entire database')
else:
    topics = topics.split(',')
    print('we will look for these: {}'.format(','.join(topics)))


## LOAD SYNTHETIC REVIEWS
reviews = pd.read_csv("Syntetic_reviews/reviews_all.csv".format(brand))


## UPLOAD DATA TO VECTOR DATABASE
model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu", quantize=True)


nlp = spacy.load("en_core_web_lg")
vector_db = VectorDatabase(nlp, model)

for index, row in reviews.iterrows():
    vector_db.insert(row["Review"], row["Polarity"], row["Topic"])


vector_db.set_th()
    
## GET REVIEWS FROM BRAND

client = bigquery.Client()


sql = f"""
SELECT asin, reviewText, overall
FROM `factored.raw_reviews`
inner join `factored.metadata` using(asin)
where brand = '{brand}'
"""

df = client.query(sql).result().to_dataframe()

## WHERE THE MAGIC HAPPENS
# We iterate through the reviews
# On each review we try to identify the topic i.e (quality, longevity, luxury): the set of topics depends on the sample_reviews file (it depends on the brand)
# Every Review Topic (+ overall) creates a row in the database

all_reviews = []
for index, row in df.iterrows():
    if (row["reviewText"] is not None) & (row["reviewText"] != ""):
        reviews = vector_db.long_search(row["reviewText"])
        if reviews is not None:
            reviews = list(reviews.topic.unique())
            reviews.append("Overall")
            reviews = pd.DataFrame(reviews)
            reviews.columns = ["topic"]
            reviews["score"] = row["overall"]
            reviews["asin"] = row["asin"]
            reviews["review"] = row["reviewText"]
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
