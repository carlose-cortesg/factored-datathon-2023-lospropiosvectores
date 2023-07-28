import pandas as pd
import numpy as np
from scipy.spatial import distance
import spacy
from google.cloud import bigquery
import os
from fast_sentence_transformers import FastSentenceTransformer as SentenceTransformer
from azure.eventhub import EventHubConsumerClient
import json


from vectorDB.classes import VectorDatabase

## SET AUTH SERVICE ACCOUNT
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "sa.json"


## LOAD SYNTHETIC REVIEWS
reviews = pd.read_csv("Syntetic_reviews/reviews_all.csv")


## UPLOAD DATA TO VECTOR DATABASE
model = SentenceTransformer("all-MiniLM-L6-v2", device="cpu", quantize=True)


nlp = spacy.load("en_core_web_lg")
vector_db = VectorDatabase(nlp, model)

for index, row in reviews.iterrows():
    vector_db.insert(row["Review"], row["Polarity"], row["Topic"])

## SET THRESHOLDS
vector_db.set_th()


client = bigquery.Client()

# Set up Event Hub connection parameters
connection_str = "Endpoint=sb://factored-datathon.servicebus.windows.net/;SharedAccessKeyName=datathon_group_4;SharedAccessKey=zkZkK6UnK6PpFAOGbgcBfnHUZsXPZpuwW+AEhEH24uc=;EntityPath=factored_datathon_amazon_reviews_4"
consumer_group = "team_name"
eventhub_name = "factored_datathon_amazon_reviews_4"


# Define the event processing function
def on_event(partition_context, event):
    """
    This function is simple the steps are the following
    1. Upload the streaming data to bigquery
    2. Get the review, apply the topic detection and upload the topics to a differnt table
    """

    offset = event.offset
    print("Event Offset:", offset)

    # SELECT THE INFO THAT WE WANT TO UPLOAD
    data = json.loads(event.body_as_str())
    data = {
        key: data[key]
        for key in ["asin", "reviewText", "summary", "reviewerID", "overall"]
    }

    # ADD OFFSET FOR PERSISTENCY
    data["offset_number"] = offset

    # DETECT TOPICS
    if (data["reviewText"] is not None) & (data["reviewText"] != ""):
        topics = vector_db.long_search(data["reviewText"])
        if topics is not None:
            topics = list(topics.topic.unique())
        else:
            topics = []
    else:
        topics = []
    topics.append("Overall")
    data_with_topics = data.copy()
    data_with_topics["topics"] = topics

    # UPLOAD REVIEWS
    errors = client.insert_rows_json("factored.raw_reviews_streaming", [data])
    if not errors:
        print("Data successfully uploaded to BigQuery.")
    else:
        print("Errors occurred while uploading data:")
        for error in errors:
            print(error)

    # UPLOAD REVIEWS WITH TOPICS
    errors = client.insert_rows_json(
        "factored.reviews_with_topics_streaming", [data_with_topics]
    )
    if not errors:
        print("Data successfully uploaded to BigQuery.")
    else:
        print("Errors occurred while uploading data:")
        for error in errors:
            print(error)


# Set up the Event Hub Consumer Client
consumer_client = EventHubConsumerClient.from_connection_string(
    connection_str, consumer_group, event_hub_name=eventhub_name
)


# Check which is the last event that we effectively processed

sql = """SELECT coalesce(max(offset_number),-1) start_at FROM `factored.raw_reviews_streaming`"""

start_at = str(client.query(sql).result().to_dataframe().start_at.values[0])

print("starting streaming from {}".format(start_at))

# Start the consumer
with consumer_client:
    consumer_client.receive(
        on_event=on_event,
        starting_position=start_at,  # Start reading from the end of the stream
    )
