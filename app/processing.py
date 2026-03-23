import logging

import spacy
from pyspark.ml.feature import NGram
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, desc, explode, udf, window
from pyspark.sql.types import ArrayType, StringType
from pyspark.sql.window import Window
from stop_words import get_stop_words

from preprocessing import clean_text, extract_topical_tokens
from io_utils import read_data


def build_stop_words(nlp_en: spacy.Language, nlp_nl: spacy.Language) -> set[str]:
    """
    Build a stop-word set by combining the stop-words library
    with spaCy's built-in stop words for both English and Dutch.
    """
    sw = set(get_stop_words("en") + get_stop_words("nl"))
    sw.update(nlp_en.Defaults.stop_words)
    sw.update(nlp_nl.Defaults.stop_words)
    return sw


def load_and_preprocess_data(
    spark: SparkSession,
    input_file: str,
    start_date: str = None,
    end_date: str = None,
    use_minio: bool = False,
) -> DataFrame:
    """
    Load JSON data, extract relevant fields, and preprocess the text.

    Parameters
    ----------
    spark : SparkSession
        The Spark session to use for loading the data.
    input_file : str
        The path to the input JSON file.
    start_date : str, optional
        The start date for filtering tweets.
    end_date : str, optional
        The end date for filtering tweets.
    use_minio : bool
        Whether to read from MinIO.

    Returns
    -------
    DataFrame
        The preprocessed DataFrame containing the text, language, and created_at fields.
    """
    logging.info(f"Loading data from: {input_file}")
    tweets_df = read_data(spark, input_file, use_minio)

    tweets_df = tweets_df.select(
        col("interaction.content").alias("text"),
        col("language.tag").alias("lang"),
        col("twitter.created_at").alias("created_at"),
    )

    tweets_df = tweets_df.withColumn(
        "created_at",
        to_timestamp(col("created_at"), "EEE, dd MMM yyyy HH:mm:ss +0000"),
    )

    if start_date:
        tweets_df = tweets_df.filter(col("created_at") >= start_date)
    if end_date:
        tweets_df = tweets_df.filter(col("created_at") <= end_date)

    clean_text_udf = udf(
        lambda text: (
            clean_text(text) if (text is not None and isinstance(text, str)) else None
        ),
        StringType(),
    )
    tweets_df = tweets_df.withColumn("cleaned_text", clean_text_udf(col("text")))
    tweets_df = tweets_df.fillna({"cleaned_text": ""})
    tweets_df = tweets_df.filter(col("created_at").isNotNull())

    return tweets_df


def _extract_terms(
    spark: SparkSession,
    tweets_df: DataFrame,
    nlp_en,
    nlp_nl,
) -> DataFrame:
    """
    Extract topical terms (unigrams + bigrams) from tweets.

    Returns a DataFrame with (created_at, topic) rows.
    """
    stop_words = build_stop_words(nlp_en, nlp_nl)
    broadcast_stopwords = spark.sparkContext.broadcast(stop_words)

    extract_tokens_udf = F.udf(
        lambda text, lang: extract_topical_tokens(
            text, lang, stop_words=broadcast_stopwords.value
        ),
        ArrayType(StringType()),
    )

    filtered_data = tweets_df.withColumn(
        "tokens", extract_tokens_udf(col("cleaned_text"), col("lang"))
    )

    ngram2 = NGram(n=2, inputCol="tokens", outputCol="bigrams")
    with_bigrams = ngram2.transform(filtered_data)

    unigrams = with_bigrams.select(
        col("created_at"), explode(col("tokens")).alias("topic"),
    )
    bigrams = with_bigrams.select(
        col("created_at"), explode(col("bigrams")).alias("topic"),
    )
    all_terms = unigrams.union(bigrams)

    all_terms = all_terms.filter(
        (col("topic").isNotNull()) & (F.length(F.trim(col("topic"))) > 0)
    )

    return all_terms


def run_pipeline(
    spark: SparkSession,
    tweets_df: DataFrame,
    nlp_en,
    nlp_nl,
    N: int = 5,
    time_interval: str = "hour",
    min_count: int = 2,
) -> DataFrame:
    """
    Run the trending topics pipeline using burst detection.

    Instead of ranking by raw frequency, this detects terms whose usage
    **spikes** in a given time window relative to their average across all
    windows — similar to how Twitter's real trending algorithm works.

    burst_score = count_in_window / avg_count_across_all_windows

    A term that goes from 2 mentions/hour to 40 mentions/hour scores 5x,
    while a consistently popular term at 100/hour scores ~1x and won't trend.

    Steps:
    1. POS-based token extraction (nouns, proper nouns, hashtags)
    2. Bigram generation
    3. Time-windowed term frequency
    4. Compute burst score per (window, term)
    5. Apply minimum volume floor
    6. Rank by burst score within each window
    7. Return top N per window

    Parameters
    ----------
    spark : SparkSession
        The active Spark session.
    tweets_df : DataFrame
        Preprocessed DataFrame with cleaned_text, lang, and created_at columns.
    nlp_en : spacy.Language
        English spaCy model.
    nlp_nl : spacy.Language
        Dutch spaCy model.
    N : int
        Number of top trending topics per time window.
    time_interval : str
        Time interval for grouping: "hour", "day", or "week".
    min_count : int
        Minimum mentions in a window for a term to be considered trending.
        Prevents low-volume noise from having artificially high burst scores.

    Returns
    -------
    DataFrame
        DataFrame with start_time, end_time, topic, count, burst_score, and rank.
    """
    all_terms = _extract_terms(spark, tweets_df, nlp_en, nlp_nl)

    # Time-windowed term frequency
    window_duration = {"hour": "1 hour", "week": "1 week"}.get(
        time_interval, "1 day"
    )
    term_counts = all_terms.groupBy(
        window(col("created_at"), window_duration).alias("time_window"),
        "topic",
    ).count()

    # Compute average count per term across all windows
    avg_counts = term_counts.groupBy("topic").agg(
        F.avg("count").alias("avg_count"),
    )

    # Join back to get burst score
    term_counts = term_counts.join(avg_counts, on="topic")
    term_counts = term_counts.withColumn(
        "burst_score",
        F.round(col("count") / col("avg_count"), 2),
    )

    # Apply minimum volume floor — terms with too few mentions in a window
    # can have artificially high burst scores from a single tweet
    term_counts = term_counts.filter(col("count") >= min_count)

    # Rank by burst score within each window (break ties by raw count)
    term_counts = term_counts.withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("time_window").orderBy(
                desc("burst_score"), desc("count")
            )
        ),
    )
    top_trends = term_counts.filter(col("rank") <= N)

    result = top_trends.select(
        col("time_window.start").alias("start_time"),
        col("time_window.end").alias("end_time"),
        "topic",
        "count",
        "burst_score",
        "rank",
    )

    return result
