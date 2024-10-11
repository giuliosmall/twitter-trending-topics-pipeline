import streamlit as st
import pandas as pd
from minio import Minio
import os
from wordcloud import WordCloud
import matplotlib.pyplot as plt

MINIO_URL = "http://minio:9000"
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "tweets-bucket"
FILE_PATH = "output/"

# Initialize the MinIO client
minio_client = Minio(
    MINIO_URL.replace("http://", ""),  # Remove protocol for MinIO client
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# List the objects
objects = minio_client.list_objects(BUCKET_NAME, prefix=FILE_PATH, recursive=True)

# Fetch the first CSV file from the bucket
csv_file = None
for obj in objects:
    if obj.object_name.endswith(".csv"):
        csv_file = obj.object_name
        break

if csv_file:
    # Download the CSV file from MinIO
    response = minio_client.get_object(BUCKET_NAME, csv_file)

    # Assign column names manually since the CSV doesn't have headers
    column_names = ['timestamp_start', 'timestamp_end', 'topic', 'count']
    data = pd.read_csv(response, header=None, names=column_names)

    # Generate the word cloud
    text = ' '.join(data['topic'])
    wordcloud = WordCloud(width=800, height=400, background_color='black').generate(text)

    # Display the word cloud using matplotlib
    st.title("Trending Twitter Topics")
    st.write("This word cloud shows the most common topics trending on Twitter.")
    plt.figure(figsize=(10, 5))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    st.pyplot(plt)

else:
    st.error("No CSV file found in the MinIO bucket.")