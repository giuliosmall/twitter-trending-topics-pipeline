import re
import logging
from typing import List, Set
import spacy
from pyspark.sql import SparkSession
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, NGram
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    col,
    to_timestamp,
    window,
    size,
    desc,
    length,
    explode,
    udf,
)
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType
import os
from spacy.cli import download
from stop_words import get_stop_words


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

def is_docker_env() -> bool:
    """Detect if the application is running inside a Docker container."""
    return os.path.exists("/.dockerenv")

# Set Python version for both driver and worker
def configure_environment() -> None:
    """
    Configure the environment variables required for Spark to run using Python 3.11.

    The environment variables are:
    - PYSPARK_PYTHON: The path to the Python 3.11 executable for the Spark workers.
    - PYSPARK_DRIVER_PYTHON: The path to the Python 3.11 executable for the Spark driver.

    If the environment variables are not set, the function sets them to the default values.
    If the environment variables are already set, the function does not modify them.

    Args:
        None

    Returns:
        None
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


# Create and configure Spark session
def create_spark_session(app_name: str = "TwitterTrendingTopics") -> SparkSession:
    """
    Creates a Spark session with the given app name.

    The Spark session is configured with the legacy time parser policy and
    the master is set to "local[*]".

    Args:
        app_name: The name of the Spark application.

    Returns:
        A SparkSession object.

    Raises:
        Exception: If there is an error creating the Spark session.
    """
    try:
        # Create a Spark session with the given app name
        spark = (
            SparkSession.builder.appName(app_name)
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") # time parset to ensure compatibility with legacy Spark
            .config(
                "spark.sql.shuffle.partitions", "200"
            )  # 200 shuffle partitions for aggregated operations
            .config(
                "spark.sql.execution.arrow.pyspark.enabled", "true"
            )  # updated Arrow configuration
            .config(
                "spark.default.parallelism", "100"
            )  # set default parallelism for operations that don't specify a partition count
            .master("local[*]")
            .getOrCreate()
        )
        logging.info(f"Successfully created Spark session: {app_name}")
        return spark
    except Exception as e:
        logging.error(f"Error creating Spark session: {e}")
        raise

# Function to download and set up spaCy models if not available
def download_spacy_models() -> None:
    """
    Downloads and sets up spaCy models for English and Dutch.

    This function is idempotent and does not do anything if the models are already
    downloaded. If the download fails, an error is logged and the exception is re-raised.

    Args:
        None

    Returns:
        None
    """
    try:
        download("en_core_web_sm")
        download("nl_core_news_sm")
        logging.info("Successfully downloaded spaCy models.")
    except Exception as e:
        logging.error(f"Error downloading spaCy models: {e}")
        raise

# Load language models for English and Dutch
def load_language_models() -> tuple[spacy.Language, spacy.Language]:
    """
    Load language models for English and Dutch using spaCy.

    The language models are loaded using the `spacy.load()` function. The
    models are loaded in the "sm" (small) version, which is the smallest
    version of the model that still contains all of the features.

    Args:
        None

    Returns:
        A tuple containing the loaded language models for English and Dutch.

    Raises:
        Exception: If there is an error loading the language models.
    """
    try:
        nlp_en: spacy.Language = spacy.load("en_core_web_sm")
        nlp_nl: spacy.Language = spacy.load("nl_core_news_sm")
        logging.info("Successfully loaded spaCy language models for English and Dutch.")
        return nlp_en, nlp_nl
    except Exception as e:
        logging.error(f"Error loading spaCy language models: {e}")
        raise


# Preprocessing function to clean text
def clean_text(text: str) -> str:
    """
    Clean the input text by removing URLs, special characters, mentions, and unwanted punctuation.

    This function takes a string as input and returns a cleaned version of the text. It removes URLs,
    special characters, mentions, and punctuation. It also converts the text to lowercase and
    removes any whitespace from the beginning and end of the string.

    Parameters
    text : str
        The input text to be cleaned.

    Returns
    str
        The cleaned text.

    Examples
    --------
    >>> clean_text("   This is a test!  ")
    'this is a test'
    >>> clean_text("This is a test with a URL https://example.com")
    'this is a test with a url'
    >>> clean_text("This is a test with a mention @user")
    'this is a test with a mention'
    >>> clean_text("This is a test with special characters !@#$%^&*()")
    'this is a test with special characters'
    >>> clean_text("This is a test with punctuation.?")
    'this is a test with punctuation'
    >>> clean_text("This is a test with a hashtag #test")
    'this is a test with a hashtag #test'
    """
    if text is None:
        raise ValueError("Input text cannot be null")

    try:
        # remove URLs, special characters, mentions, and punctuation
        text = re.sub(r"http\S+|@\w+", "", text)  # Remove URLs and mentions
        text = re.sub(
            r"[!?:;()<>[\]{}|/\\]", "", text
        )  # remove specified special characters
        text = re.sub(
            r"[^A-Za-z0-9\s#]", "", text
        )  # keep only alphanumeric and hashtags
        return text.lower().strip()
    except Exception as e:
        raise ValueError(f"Error cleaning text: {e}") from e


# Function to remove stopwords and filter out single characters
def remove_stopwords_and_single_char(
    words: List[str], stop_words: Set[str]
) -> List[str]:
    """
    Remove stopwords and filter out single character tokens.

    This function takes a list of words and a set of stopwords as input and returns a filtered list of words.
    The function removes stopwords from the list of words and filters out single character tokens.

    Parameters
    words : List[str]
        The list of words to be filtered.
    stop_words : Set[str]
        The set of stopwords to be removed.

    Returns
    List[str]
        The filtered list of words.
    """
    if words is None or stop_words is None:
        raise ValueError("Input parameters cannot be None.")
    if not isinstance(words, list):
        raise TypeError("Input parameter 'words' must be a list.")
    if not isinstance(stop_words, set):
        raise TypeError("Input parameter 'stop_words' must be a set.")
    # Remove stopwords and filter out single character tokens
    filtered_words: List[str] = [
        word for word in words if word not in stop_words and len(word) > 1
    ]
    return filtered_words


# Function to prioritize hashtags
def prioritize_hashtags(text):
    """
    Extract hashtags from the text and return as a list.

    This function takes a text string as input and returns a list of hashtags.
    The function uses regular expression to find all substrings that start with
    '#' and are followed by one or more alphanumeric characters.

    Parameters
    text : str
        The input text string.

    Returns
    List[str]
        A list of hashtags.
    """
    if text is None:
        raise ValueError("Input parameter 'text' cannot be None")
    if not isinstance(text, str):
        raise TypeError("Input parameter 'text' must be a string")
    try:
        hashtags = re.findall(r"#\w+", text)
        return hashtags
    except re.error as e:
        raise ValueError(f"Error extracting hashtags: {e}") from e


# Named Entity Recognition based on language
def process_text_by_language(text, lang, nlp_en, nlp_nl):
    """
    Process text and extract named entities based on the detected language.

    Parameters
    text : str
        The text to process.
    lang : str
        The language of the text ("en" or "nl").
    nlp_en : spacy.Language
        The Spacy model for English.
    nlp_nl : spacy.Language
        The Spacy model for Dutch.

    Returns:
    str
        The named entities extracted from the text as a single string with spaces.
    """
    if text is None:
        raise ValueError("Input parameter 'text' cannot be None")
    if lang is None:
        raise ValueError("Input parameter 'lang' cannot be None")
    if lang not in ("en", "nl", "unknown"):
        raise ValueError("Input parameter 'lang' must be 'en', 'nl', or 'unknown'")

    if lang == "unknown":
        return ""

    if lang == "en":
        doc = nlp_en(text)
    elif lang == "nl":
        doc = nlp_nl(text)

    # extract named entities with labels PERSON, ORG, or GPE
    named_entities = [
        ent.text for ent in doc.ents if ent.label_ in ("PERSON", "ORG", "GPE")
    ]

    # join the named entities with spaces
    return " ".join(named_entities)

def read_data(spark: SparkSession, file_path: str, use_minio: bool) -> F.DataFrame:
    """
    Read data from either MinIO or local filesystem based on configuration.

    Args:
        spark (SparkSession): The active Spark session.
        file_path (str): The path to the file to read.
        use_minio (bool): Flag indicating whether to read from MinIO.

    Returns:
        DataFrame: Loaded DataFrame.
    """
    if use_minio:
        # MinIO configuration
        minio_url = os.getenv("MINIO_URL", "http://host.docker.internal:9000")
        minio_bucket = os.getenv("MINIO_BUCKET_NAME", "tweets-bucket")
        access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        sc = spark.sparkContext

        # configure Spark to access MinIO
        sc._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)
        sc._jsc.hadoopConfiguration().set("fs.s3a.endpoint", minio_url)
        sc._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")
        sc._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")

        # Construct the MinIO file path
        minio_file_path = f"s3a://{minio_bucket}/{file_path}"
        logging.info(f"Reading data from MinIO: {minio_file_path}")
        return spark.read.json(minio_file_path)
    else:
        logging.info(f"Reading data from local filesystem: {file_path}")
        return spark.read.json(file_path)

# Load and preprocess data
def load_and_preprocess_data(
    spark: SparkSession, input_file: str, start_date: str = None, end_date: str = None, use_minio: bool = False):
    """
    Load JSON data, extract relevant fields, and preprocess the text.

    Parameters
    spark : SparkSession
        The Spark session to use for loading the data.
    input_file : str
        The path to the input JSON file.

    Returns
    DataFrame
        The preprocessed DataFrame containing the text, language, and created_at fields.
    """
    logging.info(f"Loading data from: {input_file}")
    tweets_df = read_data(spark, input_file, use_minio)

    # extract the relevant fields from the JSON data
    tweets_df = tweets_df.select(
        col("interaction.content").alias("text"),
        col("language.tag").alias("lang"),
        col("twitter.created_at").alias("created_at"),
    )

    # convert created_at to timestamp and filter by date range if provided
    tweets_df = tweets_df.withColumn(
        "created_at", to_timestamp(col("created_at"), "EEE, dd MMM yyyy HH:mm:ss +0000")
    )

    # optional filtering by date range to reduce the data size a.k.a predicate pushdown
    if start_date:
        tweets_df = tweets_df.filter(col("created_at") >= start_date)
    if end_date:
        tweets_df = tweets_df.filter(col("created_at") <= end_date)

    # apply text cleaning using the clean_text UDF
    clean_text_udf = udf(
        lambda text: (
            clean_text(text) if (text is not None and isinstance(text, str)) else None
        ),
        StringType(),
    )
    tweets_df = tweets_df.withColumn("cleaned_text", clean_text_udf(col("text")))

    # check for nulls
    tweets_df = tweets_df.fillna({"cleaned_text": ""})

    # check for nulls in the created_at column and drop them. Only few cases.
    tweets_df = tweets_df.filter(col("created_at").isNotNull())

    return tweets_df


def main(
    input_file, output_path, N=5, time_interval="day", start_date=None, end_date=None, use_minio=False
):
    """
    The main function to load, preprocess, and analyze the Twitter data.

    Parameters:
    input_file : str
        The path to the input JSON file.
    output_path : str
        The path to save the output CSV file.
    N : int, optional
        The number of trending topics to extract. Defaults to 5.
    time_interval : str, optional
        The time interval to group tweets. Defaults to "day".
    start_date : str, optional
        The start date for filtering the tweets. Defaults to None.
    end_date : str, optional
        The end date for filtering the tweets. Defaults to None.
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

        # load multilingual stop words and broadcast them
        stop_words = set(get_stop_words("en") + get_stop_words("nl"))
        # broadcast stop words to all workers
        broadcast_stopwords = spark.sparkContext.broadcast(stop_words)

        # define UDFs
        remove_stopwords_udf = F.udf(
            lambda words: remove_stopwords_and_single_char(
                words, broadcast_stopwords.value
            ),
            ArrayType(StringType()),
        )
        prioritize_hashtags_udf = F.udf(prioritize_hashtags, ArrayType(StringType()))
        process_text_udf = F.udf(
            lambda text, lang: process_text_by_language(text, lang, nlp_en, nlp_nl)
        )

        # load and preprocess data with date range filtering
        tweets_df = load_and_preprocess_data(spark, input_file, start_date, end_date, use_minio)

        # apply named entity extraction based on the detected language
        tweets_df = tweets_df.withColumn(
            "cleaned_text", process_text_udf(col("cleaned_text"), col("lang"))
        )

        # tokenize, remove stop words, and extract hashtags
        tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
        words_data = tokenizer.transform(tweets_df)
        filtered_data = words_data.withColumn(
            "words", remove_stopwords_udf(col("words"))
        )
        filtered_data = filtered_data.withColumn(
            "hashtags", prioritize_hashtags_udf(col("cleaned_text"))
        )
        filtered_data = filtered_data.withColumn(
            "combined_words", F.concat(col("words"), col("hashtags"))
        )

        # generate bigrams and trigrams
        ngram2 = NGram(n=2, inputCol="combined_words", outputCol="bigrams")
        ngram3 = NGram(n=3, inputCol="combined_words", outputCol="trigrams")
        bigram_data = ngram2.transform(filtered_data)
        trigram_data = ngram3.transform(bigram_data)

        # apply HashingTF and IDF
        hashing_tf = HashingTF(
            inputCol="combined_words", outputCol="raw_features", numFeatures=10000
        )
        featurized_data = hashing_tf.transform(trigram_data)
        idf = IDF(inputCol="raw_features", outputCol="features")
        idf_model = idf.fit(featurized_data)
        rescaled_data = idf_model.transform(featurized_data)

        # filter out empty word lists
        rescaled_data = rescaled_data.filter(size(col("combined_words")) > 0)

        # determine window size and group tweets by the specified time window
        window_duration = {"hour": "1 hour", "week": "1 week"}.get(
            time_interval, "1 day"
        )
        # group by time window and combined words
        windowed_data = rescaled_data.groupBy(
            window(col("created_at"), window_duration).alias("time_window"),
            "combined_words",
        ).count()

        # rank and extract top N trending topics within each time window
        windowed_data = windowed_data.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy("time_window").orderBy(desc("count"))
            ),
        )
        top_trends = windowed_data.filter(col("rank") <= N)

        # flatten the time_window struct for CSV compatibility
        top_trends_flattened = top_trends.withColumn(
            "start_time", col("time_window.start")
        ).withColumn("end_time", col("time_window.end"))
        top_trends_flattened = top_trends_flattened.select(
            "start_time", "end_time", explode("combined_words").alias("word"), "count"
        )

        top_trends_flattened.show(N, truncate=False)

        # write to either local or MinIO storage
        if use_minio:
            output_path = f"s3a://{os.getenv('MINIO_BUCKET_NAME', 'tweets-bucket')}/{output_path}"
            logging.info(f"Writing data to MinIO: {output_path}")

        # save
        top_trends_flattened.write.mode("overwrite").csv(output_path)
        logging.info(f"Results saved successfully to: {output_path}")
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
        time_interval="day",
        start_date="2012-01-01",
        end_date="2012-12-31",
        use_minio=use_minio
    )
