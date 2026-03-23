import logging
import os

from pyspark.sql import DataFrame, SparkSession


def read_data(spark: SparkSession, file_path: str, use_minio: bool) -> DataFrame:
    """
    Read data from either MinIO or local filesystem based on configuration.

    Args:
        spark: The active Spark session.
        file_path: The path to the file to read.
        use_minio: Flag indicating whether to read from MinIO.

    Returns:
        Loaded DataFrame.
    """
    if use_minio:
        minio_url = os.getenv("MINIO_SERVER", os.getenv("MINIO_URL", "http://minio:9000"))
        minio_bucket = os.getenv("MINIO_BUCKET_NAME", "tweets-bucket")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        sc = spark.sparkContext

        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_url)
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        minio_file_path = f"s3a://{minio_bucket}/{file_path}"
        logging.info(f"Reading data from MinIO: {minio_file_path}")
        return spark.read.json(minio_file_path)
    else:
        logging.info(f"Reading data from local filesystem: {file_path}")
        return spark.read.json(file_path)


def write_results(df: DataFrame, output_path: str, use_minio: bool) -> None:
    """
    Write results to either MinIO or local filesystem.

    Args:
        df: The DataFrame to write.
        output_path: The output path.
        use_minio: Flag indicating whether to write to MinIO.
    """
    if use_minio:
        output_path = f"s3a://{os.getenv('MINIO_BUCKET_NAME', 'tweets-bucket')}/{output_path}"
        logging.info(f"Writing data to MinIO: {output_path}")

    df.write.mode("overwrite").csv(output_path)
    logging.info(f"Results saved successfully to: {output_path}")
