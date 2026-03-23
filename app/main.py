import logging
import os

from config import configure_environment, create_spark_session, load_language_models
from processing import load_and_preprocess_data, run_pipeline
from io_utils import write_results

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def main(
    input_file,
    output_path,
    N=5,
    time_interval="day",
    start_date=None,
    end_date=None,
    use_minio=False,
):
    """
    Load, preprocess, and analyze Twitter data to extract trending topics.

    Parameters
    ----------
    input_file : str
        The path to the input JSON file.
    output_path : str
        The path to save the output CSV file.
    N : int, optional
        The number of trending topics to extract per time window. Defaults to 5.
    time_interval : str, optional
        The time interval to group tweets ("hour", "day", "week"). Defaults to "day".
    start_date : str, optional
        The start date for filtering tweets.
    end_date : str, optional
        The end date for filtering tweets.
    use_minio : bool, optional
        Whether to use MinIO for storage. Defaults to False.
    """
    try:
        if input_file is None or not isinstance(input_file, str):
            raise ValueError("Input parameter 'input_file' must be a string")
        if output_path is None or not isinstance(output_path, str):
            raise ValueError("Input parameter 'output_path' must be a string")
        if not isinstance(N, int) or N < 1:
            raise ValueError("Input parameter 'N' must be a positive integer")
        if time_interval is None or not isinstance(time_interval, str):
            raise ValueError("Input parameter 'time_interval' must be a string")
        if start_date is not None and not isinstance(start_date, str):
            raise ValueError("Input parameter 'start_date' must be a string")
        if end_date is not None and not isinstance(end_date, str):
            raise ValueError("Input parameter 'end_date' must be a string")

        configure_environment()
        spark = create_spark_session()
        nlp_en, nlp_nl = load_language_models()

        tweets_df = load_and_preprocess_data(
            spark, input_file, start_date, end_date, use_minio
        )

        top_trends = run_pipeline(
            spark, tweets_df, nlp_en, nlp_nl, N=N, time_interval=time_interval,
            min_count=2,
        )

        top_trends.orderBy("start_time", "rank").show(100, truncate=False)

        write_results(top_trends, output_path, use_minio)

        spark.stop()
        logging.info("Spark session stopped.")

    except Exception as e:
        logging.error(f"An error occurred: {e}")


if __name__ == "__main__":
    input_file = "data/dataset.json"
    output_path = "output/"
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    print(f"Using MinIO: {use_minio}")
    main(
        input_file,
        output_path,
        N=5,
        time_interval="hour",
        start_date="2012-01-01",
        end_date="2012-12-31",
        use_minio=use_minio,
    )
