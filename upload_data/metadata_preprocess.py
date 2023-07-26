from utils import main_cat_pre_pro, upload_table

from google.cloud import storage
import os
from google.cloud import bigquery
import json
import gzip
import pandas as pd


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"

storage_client = storage.Client()
bq_client = bigquery.Client()

# Note: Client.list_blobs requires at least package version 1.17.0.
blobs = storage_client.list_blobs("factored_reviews")
i = 0
j = 0
for blob in blobs:
    if blob.name.startswith("amazon_reviews") & blob.name.endswith(".gz"):
        # here we run the following query:
        # LOAD DATA OVERWRITE factored.raw_reviews
        # FROM FILES (
        # format = 'JSON',
        # uris = ['gs://factored_reviews/amazon_reviews/*.json.gz']);
        #
        # blob.download_to_filename('temp.json.gz')
        reviews = []
        # with gzip.open("temp.json.gz", "rb") as f:
        #    for line in f:
        #        reviews.append(json.loads(line))

        # aux = pd.DataFrame(reviews)

        # aux['batch'] = j
        # aux['batch_file'] = blob.name

        # upload_table(aux,table_name = 'factored.reviews', i = j)
        # j+=1
        # print('reviews {}, metadata {}'.format(j,i))

    elif blob.name.startswith("amazon_metadata") & blob.name.endswith(".gz"):
        blob.download_to_filename("temp.json.gz")

        metadata = []
        with gzip.open("temp.json.gz", "rb") as f:
            for line in f:
                metadata.append(json.loads(line))
        aux = pd.DataFrame(metadata)
        aux.main_cat = [main_cat_pre_pro(str) for str in aux.main_cat]

        aux = aux.groupby("asin").first().reset_index()

        aux["batch"] = i
        aux["batch_file"] = blob.name

        upload_table(aux, table_name="factored.metadata2", i=i)
        i += 1
        print("reviews {}, metadata {}".format(j, i))

    # if ((j>10) or (i>10)):
    #    break
