import logging
import os

import spacy
from pyspark.sql import SparkSession
from spacy.cli import download

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def is_docker_env() -> bool:
    """Detect if the application is running inside a Docker container."""
    return os.path.exists("/.dockerenv")


def configure_environment() -> None:
    """
    Configure the environment variables required for Spark to run using Python 3.11.

    If not running in Docker, sets PYSPARK_PYTHON and PYSPARK_DRIVER_PYTHON
    to the default homebrew Python 3.11 path (if not already set).
    """
    if not is_docker_env():
        default_python_path: str = "/opt/homebrew/bin/python3.11"
        if "PYSPARK_PYTHON" not in os.environ:
            os.environ["PYSPARK_PYTHON"] = default_python_path
        if "PYSPARK_DRIVER_PYTHON" not in os.environ:
            os.environ["PYSPARK_DRIVER_PYTHON"] = default_python_path
        logging.info(
            f"Configured environment variables for Spark using Python 3.11 at {default_python_path}."
        )


def create_spark_session(app_name: str = "TwitterTrendingTopics") -> SparkSession:
    """
    Creates a Spark session with the given app name.

    Args:
        app_name: The name of the Spark application.

    Returns:
        A SparkSession object.

    Raises:
        Exception: If there is an error creating the Spark session.
    """
    try:
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .config("spark.default.parallelism", "100")
            .master("local[*]")
            .getOrCreate()
        )
        logging.info(f"Successfully created Spark session: {app_name}")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise


def download_spacy_models() -> None:
    """
    Downloads spaCy models for English and Dutch if not already present.

    Raises:
        Exception: If there is an error downloading the models.
    """
    try:
        download("en_core_web_md")
        download("nl_core_news_md")
        logging.info("Successfully downloaded spaCy models.")
    except Exception as e:
        logging.error(f"Error downloading spaCy models: {e}")
        raise


def load_language_models() -> tuple[spacy.Language, spacy.Language]:
    """
    Load spaCy language models for English and Dutch.

    Returns:
        A tuple containing the loaded language models for English and Dutch.

    Raises:
        Exception: If there is an error loading the language models.
    """
    try:
        nlp_en: spacy.Language = spacy.load("en_core_web_md")
        nlp_nl: spacy.Language = spacy.load("nl_core_news_md")
        logging.info("Successfully loaded spaCy language models for English and Dutch.")
        return nlp_en, nlp_nl
    except Exception as e:
        logging.error(f"Error loading spaCy language models: {e}")
        raise
