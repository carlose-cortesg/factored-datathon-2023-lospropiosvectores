from google.cloud import storage
import os
from google.cloud import bigquery
import json
import gzip
import pandas as pd


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"

storage_client = storage.Client()
bq_client = bigquery.Client()


def main_cat_pre_pro(str):
    """If the str is a image with alt text this function extacts the Alt text"""
    if str.startswith("<img"):
        return str.split("alt=")[1].split('"')[1]
    else:
        return str


def upload_table(df, table_name="factored.metadata", i=0):
    if i == 0:
        print("deleted {}".format(table_name))
        bq_client.delete_table(table_name, not_found_ok=True)

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
    )

    job = bq_client.load_table_from_dataframe(
        df, table_name, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.
    return "ok"
