import pytest
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, NGram
from pyspark.sql.window import Window
from pyspark.sql.functions import col, explode
from pyspark.sql.types import StringType, ArrayType
from main import (
    create_spark_session,
    clean_text,
    remove_stopwords_and_single_char,
    prioritize_hashtags,
    is_docker_env,
    load_and_preprocess_data,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

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


configure_environment()


@pytest.fixture(scope="module")
def spark_session():
    """
    Create a fixture for the SparkSession object.

    The SparkSession object is created with the app name "TwitterTrendingTopicsTest".

    Yields:
        SparkSession: The SparkSession object.
    """
    spark = create_spark_session("TwitterTrendingTopicsTest")
    try:
        yield spark
    finally:
        if spark is not None:
            # Stop the SparkSession when the fixture is torn down
            spark.stop()


@pytest.fixture(scope="module")
def test_data(spark_session):
    """
    Create a fixture for the test data loaded from test.json.

    This fixture is used to load the test data from the test.json file into a DataFrame.
    The test data is a sample of the real data and is used to test the data processing pipeline.

    Args:
        spark_session (SparkSession): The SparkSession object.

    Returns:
        DataFrame: The test data loaded into a DataFrame.
    """
    if spark_session is None:
        raise ValueError("spark_session is None")

    input_file = "data/test_sample.json"
    use_minio = os.getenv("USE_MINIO", "false").lower() == "true"
    if not use_minio and not os.path.exists(input_file):
        raise FileNotFoundError(f"The file {input_file} does not exist.")

    try:
        return load_and_preprocess_data(spark_session, input_file, use_minio=use_minio)
    except Exception as e:
        raise ValueError(f"Error loading test data: {e}") from e


def test_clean_text():
    """
    Test cleaning of text.

    The clean_text function removes URLs, special characters, mentions, and punctuation
    from the input text. It also converts the text to lowercase and removes any
    whitespace from the beginning and end of the string.
    """
    # null input
    with pytest.raises(ValueError):
        clean_text(None)

    # empty string input
    assert clean_text("") == ""

    # input with a URL
    assert (
        clean_text("This is a test with a URL https://example.com")
        == "this is a test with a url"
    )

    # input with special characters
    assert clean_text("Special characters !@$%^&*()") == "special characters"

    # input with a mention
    assert clean_text("Mentions @user") == "mentions"

    # input with a hashtag
    assert clean_text("Hashtags #test") == "hashtags #test"


def test_remove_stopwords_and_single_char():
    """
    Test removal of stopwords and single character tokens.

    This function takes a list of words and a set of stopwords as input and
    returns a filtered list of words. The function removes stopwords from the
    list of words and filters out single character tokens.
    """
    stop_words = {"the", "a", "an", "to", "with"}
    # removal of stopwords
    assert remove_stopwords_and_single_char(
        ["this", "is", "a", "test"], stop_words
    ) == ["this", "is", "test"]
    # removal of single character tokens
    assert remove_stopwords_and_single_char(
        ["#hello", "a", "to", "world"], stop_words
    ) == ["#hello", "world"]
    # empty list input
    assert remove_stopwords_and_single_char([], stop_words) == []


def test_prioritize_hashtags():
    """Test extraction of hashtags from text.

    The function takes a text string as input and returns a list of hashtags.
    """
    # single hashtag
    assert prioritize_hashtags("Hello #world") == ["#world"]

    # no hashtags
    assert prioritize_hashtags("No hashtags here") == []

    # multiple hashtags
    assert prioritize_hashtags("#One #Two #Three") == ["#One", "#Two", "#Three"]

    # empty string input
    assert prioritize_hashtags("") == []


def test_load_and_preprocess_data(test_data):
    """Test data loading and preprocessing function using test.json.

    This test checks the following:
    1. The number of rows in the DataFrame.
    2. The presence of the "cleaned_text" column in the DataFrame.
    3. The content of the first row in the DataFrame to ensure it is valid.
    """
    # Print the DataFrame
    print("\nTest Data DataFrame:")
    test_data.show(truncate=False)
    
    # check schema and row count
    assert test_data.count() == 2
    assert "cleaned_text" in test_data.columns

    # validate some sample content
    first_row = test_data.collect()[0]
    assert first_row["lang"] == "nl"
    assert "sneeuwpopje" in first_row["cleaned_text"]


def test_tokenization_and_hashtags_extraction(spark_session, test_data):
    """
    Test tokenization and hashtag extraction on the preprocessed data.

    This test checks the following:
    1. Tokenization is working as expected.
    2. The "words" column is generated correctly.
    3. The hashtag extraction UDF is working as expected.
    4. The "hashtags" column is generated correctly.
    """
    # apply tokenization
    tokenizer = Tokenizer(inputCol="cleaned_text", outputCol="words")
    tokenized_data = tokenizer.transform(test_data)

    # check if tokenization is working as expected
    assert "words" in tokenized_data.columns
    assert tokenized_data.count() > 0

    # apply a sample transformation to ensure the pipeline works
    hashtags_udf = F.udf(prioritize_hashtags, ArrayType(StringType()))
    processed_df = tokenized_data.withColumn(
        "hashtags", hashtags_udf(col("cleaned_text"))
    )

    # check if the hashtags column is generated correctly
    assert "hashtags" in processed_df.columns

    # check some sample values for correctness
    hashtags_list = processed_df.select("hashtags").collect()[1][0]
    assert "#kepzininvanavond" in hashtags_list
