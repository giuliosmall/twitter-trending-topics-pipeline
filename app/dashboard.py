import logging
import os

import matplotlib.pyplot as plt
import pandas as pd
import streamlit as st
from minio import Minio
from minio.error import S3Error
from urllib3.exceptions import MaxRetryError
from wordcloud import WordCloud

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

MINIO_URL = os.getenv("MINIO_URL", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME", "tweets-bucket")
FILE_PATH = "output/"


def create_minio_client() -> Minio:
    """Create and return a MinIO client."""
    return Minio(
        MINIO_URL.replace("http://", "").replace("https://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def fetch_csv_from_minio(client: Minio) -> pd.DataFrame | None:
    """
    Fetch the first CSV file from the MinIO bucket and return it as a DataFrame.

    Returns None if no CSV file is found.
    """
    objects = client.list_objects(BUCKET_NAME, prefix=FILE_PATH, recursive=True)

    csv_file = None
    for obj in objects:
        if obj.object_name.endswith(".csv"):
            csv_file = obj.object_name
            break

    if csv_file is None:
        return None

    logging.info(f"Fetching CSV file: {csv_file}")
    response = client.get_object(BUCKET_NAME, csv_file)
    column_names = ["timestamp_start", "timestamp_end", "topic", "count", "burst_score", "rank"]
    data = pd.read_csv(response, header=None, names=column_names)
    response.close()
    response.release_conn()
    return data


def render_dashboard(data: pd.DataFrame) -> None:
    """Render the word cloud dashboard from the given DataFrame."""
    st.title("Trending Twitter Topics")
    st.write("This word cloud shows the most common topics trending on Twitter.")

    topics = data["topic"].dropna().astype(str)
    if topics.empty:
        st.warning("No topic data available to generate a word cloud.")
        return

    text = " ".join(topics)
    wordcloud = WordCloud(width=800, height=400, background_color="black").generate(
        text
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)
    plt.close(fig)


def main():
    try:
        client = create_minio_client()
    except Exception as e:
        logging.error(f"Failed to create MinIO client: {e}")
        st.error(f"Could not connect to MinIO storage: {e}")
        return

    try:
        data = fetch_csv_from_minio(client)
    except (S3Error, MaxRetryError) as e:
        logging.error(f"MinIO error fetching data: {e}")
        st.error(
            "Could not fetch data from MinIO. "
            "Please check that MinIO is running and the bucket exists."
        )
        return
    except Exception as e:
        logging.error(f"Unexpected error fetching data: {e}")
        st.error(f"An unexpected error occurred while fetching data: {e}")
        return

    if data is None:
        st.warning(
            "No CSV file found in the MinIO bucket. "
            "Please run the Spark pipeline first to generate output."
        )
        return

    if data.empty:
        st.warning("The CSV file is empty. No data to display.")
        return

    render_dashboard(data)


if __name__ == "__main__":
    main()
