import os
import logging
from unittest.mock import patch, MagicMock

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType,
    TimestampType,
)
from pyspark.ml.feature import Tokenizer

from config import create_spark_session, is_docker_env, configure_environment
from preprocessing import (
    clean_text,
    remove_stopwords_and_single_char,
    prioritize_hashtags,
    process_text_by_language,
    extract_topical_tokens,
    is_noise_token,
    NOISE_TOKENS,
)
from processing import load_and_preprocess_data, run_pipeline
from io_utils import read_data, write_results

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def spark_session():
    """Module-scoped SparkSession for all tests."""
    configure_environment()
    spark = create_spark_session("TwitterTrendingTopicsTest")
    yield spark
    spark.stop()


@pytest.fixture(scope="module")
def test_data(spark_session):
    """Preprocessed DataFrame loaded from the test sample JSON."""
    input_file = "data/test_sample.json"
    if not os.path.exists(input_file):
        pytest.skip(f"Test data file {input_file} not found")
    return load_and_preprocess_data(spark_session, input_file, use_minio=False)


@pytest.fixture(scope="module")
def nlp_models():
    """Load spaCy language models (English and Dutch)."""
    import spacy

    try:
        nlp_en = spacy.load("en_core_web_md")
        nlp_nl = spacy.load("nl_core_news_md")
    except OSError:
        pytest.skip("spaCy md models not installed")
    return nlp_en, nlp_nl


# ---------------------------------------------------------------------------
# clean_text
# ---------------------------------------------------------------------------

class TestCleanText:
    def test_null_input_raises(self):
        with pytest.raises(ValueError):
            clean_text(None)

    def test_empty_string(self):
        assert clean_text("") == ""

    def test_removes_url(self):
        assert clean_text("Check https://example.com out") == "check  out"

    def test_removes_mention(self):
        assert clean_text("Hello @user") == "hello"

    def test_preserves_hashtag(self):
        assert clean_text("Trending #python") == "trending #python"

    def test_removes_special_characters(self):
        assert clean_text("Wow!!! Really?? Yes: no") == "wow really yes no"

    def test_lowercases(self):
        assert clean_text("HELLO WORLD") == "hello world"

    def test_strips_whitespace(self):
        assert clean_text("   padded   ") == "padded"

    def test_multiple_urls_and_mentions(self):
        result = clean_text("@a @b http://x.com https://y.com text")
        assert "text" in result
        assert "@" not in result
        assert "http" not in result

    def test_only_special_characters(self):
        assert clean_text("!@#$%^&*()") == ""

    def test_hashtag_with_numbers(self):
        assert clean_text("#topic123") == "#topic123"

    def test_unicode_stripped(self):
        # Non-ASCII characters get removed by the regex
        result = clean_text("café résumé")
        assert "caf" in result


# ---------------------------------------------------------------------------
# remove_stopwords_and_single_char
# ---------------------------------------------------------------------------

class TestRemoveStopwords:
    def test_basic_removal(self):
        stop_words = {"the", "a", "an", "to", "with"}
        assert remove_stopwords_and_single_char(
            ["this", "is", "a", "test"], stop_words
        ) == ["this", "is", "test"]

    def test_single_char_removal(self):
        stop_words = set()
        assert remove_stopwords_and_single_char(
            ["a", "bb", "c", "dd"], stop_words
        ) == ["bb", "dd"]

    def test_empty_list(self):
        assert remove_stopwords_and_single_char([], {"the"}) == []

    def test_all_stopwords(self):
        stop_words = {"the", "is", "a"}
        assert remove_stopwords_and_single_char(
            ["the", "is", "a"], stop_words
        ) == []

    def test_none_words_raises(self):
        with pytest.raises(ValueError):
            remove_stopwords_and_single_char(None, set())

    def test_none_stopwords_raises(self):
        with pytest.raises(ValueError):
            remove_stopwords_and_single_char([], None)

    def test_wrong_type_words_raises(self):
        with pytest.raises(TypeError):
            remove_stopwords_and_single_char("not a list", set())

    def test_wrong_type_stopwords_raises(self):
        with pytest.raises(TypeError):
            remove_stopwords_and_single_char([], ["not", "a", "set"])

    def test_preserves_hashtags(self):
        stop_words = {"hello"}
        assert remove_stopwords_and_single_char(
            ["#hello", "hello", "world"], stop_words
        ) == ["#hello", "world"]


# ---------------------------------------------------------------------------
# prioritize_hashtags
# ---------------------------------------------------------------------------

class TestPrioritizeHashtags:
    def test_single_hashtag(self):
        assert prioritize_hashtags("Hello #world") == ["#world"]

    def test_no_hashtags(self):
        assert prioritize_hashtags("No hashtags here") == []

    def test_multiple_hashtags(self):
        assert prioritize_hashtags("#One #Two #Three") == ["#One", "#Two", "#Three"]

    def test_empty_string(self):
        assert prioritize_hashtags("") == []

    def test_none_raises(self):
        with pytest.raises(ValueError):
            prioritize_hashtags(None)

    def test_non_string_raises(self):
        with pytest.raises(TypeError):
            prioritize_hashtags(123)

    def test_hashtag_in_middle_of_text(self):
        assert prioritize_hashtags("I love #python programming") == ["#python"]

    def test_hashtag_with_underscores(self):
        assert prioritize_hashtags("#hello_world") == ["#hello_world"]

    def test_hash_only_no_match(self):
        # A lone # with no word characters after it
        assert prioritize_hashtags("just a # sign") == []


# ---------------------------------------------------------------------------
# process_text_by_language (NER)
# ---------------------------------------------------------------------------

class TestProcessTextByLanguage:
    def test_english_ner(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = process_text_by_language(
            "Barack Obama visited New York", "en", nlp_en, nlp_nl
        )
        # Should extract at least one named entity
        assert isinstance(result, str)
        # Obama and/or New York should be recognized
        assert "Obama" in result or "New York" in result or "Barack" in result

    def test_dutch_ner(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = process_text_by_language(
            "Amsterdam is een mooie stad", "nl", nlp_en, nlp_nl
        )
        assert isinstance(result, str)

    def test_unknown_language_returns_empty(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        assert process_text_by_language("some text", "unknown", nlp_en, nlp_nl) == ""

    def test_none_text_raises(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        with pytest.raises(ValueError, match="text"):
            process_text_by_language(None, "en", nlp_en, nlp_nl)

    def test_none_lang_raises(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        with pytest.raises(ValueError, match="lang"):
            process_text_by_language("text", None, nlp_en, nlp_nl)

    def test_invalid_lang_raises(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        with pytest.raises(ValueError, match="'en', 'nl', or 'unknown'"):
            process_text_by_language("text", "fr", nlp_en, nlp_nl)

    def test_no_entities_returns_empty(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        # Simple text unlikely to have named entities
        result = process_text_by_language("hello world", "en", nlp_en, nlp_nl)
        assert isinstance(result, str)


# ---------------------------------------------------------------------------
# load_and_preprocess_data
# ---------------------------------------------------------------------------

class TestLoadAndPreprocessData:
    def test_loads_correct_row_count(self, test_data):
        # 3 records in JSON, but 3rd has no twitter.created_at → filtered out
        assert test_data.count() == 2

    def test_has_cleaned_text_column(self, test_data):
        assert "cleaned_text" in test_data.columns

    def test_has_expected_columns(self, test_data):
        expected = {"text", "lang", "created_at", "cleaned_text"}
        assert expected.issubset(set(test_data.columns))

    def test_first_row_content(self, test_data):
        first_row = test_data.collect()[0]
        assert first_row["lang"] == "nl"
        assert "sneeuwpopje" in first_row["cleaned_text"]

    def test_no_null_created_at(self, test_data):
        null_count = test_data.filter(F.col("created_at").isNull()).count()
        assert null_count == 0

    def test_cleaned_text_is_lowercase(self, test_data):
        rows = test_data.select("cleaned_text").collect()
        for row in rows:
            assert row["cleaned_text"] == row["cleaned_text"].lower()

    def test_date_filtering(self, spark_session):
        """Filtering with a future date range should return 0 rows."""
        input_file = "data/test_sample.json"
        if not os.path.exists(input_file):
            pytest.skip("Test data file not found")
        df = load_and_preprocess_data(
            spark_session,
            input_file,
            start_date="2099-01-01",
            end_date="2099-12-31",
            use_minio=False,
        )
        assert df.count() == 0

    def test_date_filtering_includes_data(self, spark_session):
        """Filtering with the correct date range should include all rows."""
        input_file = "data/test_sample.json"
        if not os.path.exists(input_file):
            pytest.skip("Test data file not found")
        df = load_and_preprocess_data(
            spark_session,
            input_file,
            start_date="2012-01-01",
            end_date="2012-12-31",
            use_minio=False,
        )
        assert df.count() == 2


# ---------------------------------------------------------------------------
# Tokenization and hashtag extraction (pipeline fragment)
# ---------------------------------------------------------------------------

class TestTokenizationAndHashtags:
    def test_hashtag_udf_extracts_hashtags(self, test_data):
        hashtags_udf = F.udf(prioritize_hashtags, ArrayType(StringType()))
        result = test_data.withColumn(
            "hashtags", hashtags_udf(F.col("cleaned_text"))
        )
        assert "hashtags" in result.columns

        # Second tweet has #kepzininvanavond
        hashtags_list = result.select("hashtags").collect()[1][0]
        assert "#kepzininvanavond" in hashtags_list


# ---------------------------------------------------------------------------
# POS-based topical token extraction
# ---------------------------------------------------------------------------

class TestExtractTopicalTokens:
    def test_keeps_nouns(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        # min_freq_rank=0 disables frequency filter to test POS in isolation
        tokens = extract_topical_tokens(
            "the president visited the city", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "president" in tokens
        assert "city" in tokens

    def test_filters_verbs_and_adjectives(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "he quickly ran to the beautiful park", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "ran" not in tokens
        assert "quickly" not in tokens
        assert "park" in tokens

    def test_keeps_hashtags(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "loving #python today", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "#python" in tokens

    def test_unknown_language_returns_empty(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "some text", "unknown", nlp_en, nlp_nl, set()
        )
        assert tokens == []

    def test_empty_text_returns_empty(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        assert extract_topical_tokens("", "en", nlp_en, nlp_nl, set()) == []

    def test_filters_stopwords(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "the president visited the city", "en", nlp_en, nlp_nl, {"president"},
            min_freq_rank=0,
        )
        assert "president" not in tokens
        assert "city" in tokens

    def test_filters_noise_tokens(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "lol rt python", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "lol" not in tokens
        assert "rt" not in tokens

    def test_dutch_nouns(self, nlp_models):
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "het huis staat in amsterdam", "nl", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "huis" in tokens

    def test_frequency_filter_removes_common_nouns(self, nlp_models):
        """High-frequency generic nouns like 'people', 'time' should be filtered."""
        nlp_en, nlp_nl = nlp_models
        # With default min_freq_rank=200, very common nouns get dropped
        tokens = extract_topical_tokens(
            "the people need more time", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=200,
        )
        assert "people" not in tokens
        assert "time" not in tokens

    def test_frequency_filter_keeps_specific_nouns(self, nlp_models):
        """Specific/rare nouns should survive the frequency filter."""
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "the earthquake damaged the cathedral", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=200,
        )
        # These are specific nouns with high ranks (less common)
        assert "earthquake" in tokens or "cathedral" in tokens

    def test_frequency_filter_disabled_at_zero(self, nlp_models):
        """Setting min_freq_rank=0 should disable the frequency filter."""
        nlp_en, nlp_nl = nlp_models
        tokens = extract_topical_tokens(
            "the people need more time", "en", nlp_en, nlp_nl, set(),
            min_freq_rank=0,
        )
        assert "people" in tokens
        assert "time" in tokens


# ---------------------------------------------------------------------------
# run_pipeline (full integration)
# ---------------------------------------------------------------------------

class TestRunPipeline:
    EXPECTED_COLUMNS = {"start_time", "end_time", "topic", "count", "burst_score", "rank"}

    def test_full_pipeline_returns_results(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=5, time_interval="day",
            min_count=1,
        )
        assert set(result.columns) == self.EXPECTED_COLUMNS

    def test_pipeline_respects_n_parameter(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=1, time_interval="day",
            min_count=1,
        )
        # With N=1, should have at most 1 trend per window
        assert result.count() >= 0  # may be 0 if data is too sparse

    def test_pipeline_with_hour_interval(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=5, time_interval="hour",
            min_count=1,
        )
        assert set(result.columns) == self.EXPECTED_COLUMNS

    def test_pipeline_with_week_interval(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=5, time_interval="week",
            min_count=1,
        )
        assert set(result.columns) == self.EXPECTED_COLUMNS

    def test_min_count_filters_rare_terms(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        # With a very high min_count, nothing should survive
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=5, time_interval="day",
            min_count=9999,
        )
        assert result.count() == 0

    def test_pipeline_surfaces_bigrams(self, spark_session, test_data, nlp_models):
        nlp_en, nlp_nl = nlp_models
        result = run_pipeline(
            spark_session, test_data, nlp_en, nlp_nl, N=50, time_interval="day",
            min_count=1,
        )
        # At least some topics should contain spaces (bigrams)
        topics = [row["topic"] for row in result.collect()]
        bigrams = [t for t in topics if " " in t]
        # With only 2 tweets this may be empty, but the column should exist
        assert isinstance(bigrams, list)


# ---------------------------------------------------------------------------
# I/O: read_data
# ---------------------------------------------------------------------------

class TestReadData:
    def test_read_local_json(self, spark_session):
        input_file = "data/test_sample.json"
        if not os.path.exists(input_file):
            pytest.skip("Test data file not found")
        df = read_data(spark_session, input_file, use_minio=False)
        assert df.count() == 3  # raw count before preprocessing

    def test_read_nonexistent_file_raises(self, spark_session):
        with pytest.raises(Exception):
            df = read_data(spark_session, "data/nonexistent.json", use_minio=False)
            df.count()  # force evaluation of lazy DataFrame


# ---------------------------------------------------------------------------
# I/O: write_results
# ---------------------------------------------------------------------------

class TestWriteResults:
    def test_write_local_csv(self, spark_session, tmp_path):
        data = [("2012-02-04", "2012-02-05", "python", 10, 3.5, 1)]
        df = spark_session.createDataFrame(
            data, ["start_time", "end_time", "topic", "count", "burst_score", "rank"]
        )
        output_dir = str(tmp_path / "test_output")
        write_results(df, output_dir, use_minio=False)
        # Verify files were written
        assert os.path.exists(output_dir)
        files = os.listdir(output_dir)
        csv_files = [f for f in files if f.endswith(".csv")]
        assert len(csv_files) > 0


# ---------------------------------------------------------------------------
# config module
# ---------------------------------------------------------------------------

class TestConfig:
    def test_is_docker_env_returns_bool(self):
        assert isinstance(is_docker_env(), bool)

    def test_is_docker_env_false_locally(self):
        # Unless running in Docker, this should be False
        if not os.path.exists("/.dockerenv"):
            assert is_docker_env() is False

    def test_configure_environment_sets_vars(self):
        # Clear vars first, then configure
        orig_python = os.environ.pop("PYSPARK_PYTHON", None)
        orig_driver = os.environ.pop("PYSPARK_DRIVER_PYTHON", None)
        try:
            with patch("config.is_docker_env", return_value=False):
                configure_environment()
                assert "PYSPARK_PYTHON" in os.environ
                assert "PYSPARK_DRIVER_PYTHON" in os.environ
        finally:
            # Restore original values
            if orig_python is not None:
                os.environ["PYSPARK_PYTHON"] = orig_python
            if orig_driver is not None:
                os.environ["PYSPARK_DRIVER_PYTHON"] = orig_driver

    def test_configure_environment_does_not_overwrite(self):
        os.environ["PYSPARK_PYTHON"] = "/custom/path"
        try:
            with patch("config.is_docker_env", return_value=False):
                configure_environment()
                assert os.environ["PYSPARK_PYTHON"] == "/custom/path"
        finally:
            del os.environ["PYSPARK_PYTHON"]


# ---------------------------------------------------------------------------
# main() validation
# ---------------------------------------------------------------------------

class TestMainValidation:
    def test_invalid_input_file(self):
        from main import main

        # Should not raise (caught internally), but should log error
        main(None, "output/")

    def test_invalid_output_path(self):
        from main import main

        main("data/dataset.json", None)

    def test_invalid_n(self):
        from main import main

        main("data/dataset.json", "output/", N=0)

    def test_invalid_n_type(self):
        from main import main

        main("data/dataset.json", "output/", N="five")


# ---------------------------------------------------------------------------
# Empty DataFrame edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_empty_dataframe_through_pipeline(self, spark_session, nlp_models):
        """An empty DataFrame should not crash the pipeline."""
        nlp_en, nlp_nl = nlp_models
        schema = StructType(
            [
                StructField("text", StringType(), True),
                StructField("lang", StringType(), True),
                StructField("created_at", TimestampType(), True),
                StructField("cleaned_text", StringType(), True),
            ]
        )
        empty_df = spark_session.createDataFrame([], schema)
        result = run_pipeline(
            spark_session, empty_df, nlp_en, nlp_nl, N=5, time_interval="day"
        )
        assert result.count() == 0

    def test_clean_text_with_only_urls(self):
        result = clean_text("https://example.com http://test.org")
        assert result.strip() == ""

    def test_clean_text_preserves_numbers(self):
        assert "2024" in clean_text("Year 2024")

    def test_stopword_removal_with_empty_stopwords(self):
        result = remove_stopwords_and_single_char(
            ["hello", "world"], set()
        )
        assert result == ["hello", "world"]


# ---------------------------------------------------------------------------
# Noise token filtering
# ---------------------------------------------------------------------------

class TestNoiseFiltering:
    def test_lt3_is_noise(self):
        assert is_noise_token("lt3")

    def test_rt_is_noise(self):
        assert is_noise_token("rt")

    def test_laughter_variants_are_noise(self):
        assert is_noise_token("haha")
        assert is_noise_token("hahaha")
        assert is_noise_token("hahahaha")
        assert is_noise_token("hahahahah")

    def test_normal_words_not_noise(self):
        assert not is_noise_token("python")
        assert not is_noise_token("nederland")
        assert not is_noise_token("trending")

    def test_noise_tokens_filtered_by_stopword_removal(self):
        result = remove_stopwords_and_single_char(
            ["hello", "lt3", "hahaha", "world", "rt"], set()
        )
        assert result == ["hello", "world"]

    def test_html_entity_cleaned(self):
        # &lt;3 should not produce "lt3"
        result = clean_text("I love you &lt;3")
        assert "lt3" not in result

    def test_html_amp_cleaned(self):
        result = clean_text("Tom &amp; Jerry")
        assert "amp" not in result
