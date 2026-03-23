import re
import logging
from typing import List, Set

# Common noise tokens that survive text cleaning but carry no topical meaning.
# Includes emoticon fragments, laughter variants, and common internet slang.
NOISE_TOKENS = frozenset({
    "rt", "lt3", "lt", "gt", "amp",          # HTML entity remnants / retweet marker
    "lol", "lmao", "rofl", "lmfao", "omg",   # internet slang
    "haha", "hahah", "hahaha", "hahahaha",    # laughter variants
    "hihi", "hehe", "hehehehe",
    "xx", "xxx", "xo", "xoxo",               # affection filler
    "ja", "nee", "oh", "ah", "uh", "hm",     # interjections
    "dm", "ff", "ns", "fb",                   # abbreviations
})


def is_noise_token(word: str) -> bool:
    """Return True if the word is a known noise token or a laughter/repetition variant."""
    if word in NOISE_TOKENS:
        return True
    # Catch laughter variants of any length (hahaha..., hihihi...)
    if re.fullmatch(r"(ha){2,}h?", word) or re.fullmatch(r"(hi){2,}", word):
        return True
    return False


def clean_text(text: str) -> str:
    """
    Clean the input text by removing URLs, HTML entities, special characters,
    mentions, and unwanted punctuation.

    Preserves hashtags. Converts to lowercase and strips whitespace.

    Parameters
    ----------
    text : str
        The input text to be cleaned.

    Returns
    -------
    str
        The cleaned text.

    Examples
    --------
    >>> clean_text("This is a test with a URL https://example.com")
    'this is a test with a url'
    >>> clean_text("This is a test with a hashtag #test")
    'this is a test with a hashtag #test'
    """
    if text is None:
        raise ValueError("Input text cannot be null")

    try:
        text = re.sub(r"&\w+;", " ", text)    # Strip HTML entities (&lt; &amp; etc.)
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


def remove_stopwords_and_single_char(
    words: List[str], stop_words: Set[str]
) -> List[str]:
    """
    Remove stopwords and filter out single character tokens.

    Parameters
    ----------
    words : List[str]
        The list of words to be filtered.
    stop_words : Set[str]
        The set of stopwords to be removed.

    Returns
    -------
    List[str]
        The filtered list of words.
    """
    if words is None or stop_words is None:
        raise ValueError("Input parameters cannot be None.")
    if not isinstance(words, list):
        raise TypeError("Input parameter 'words' must be a list.")
    if not isinstance(stop_words, set):
        raise TypeError("Input parameter 'stop_words' must be a set.")
    filtered_words: List[str] = [
        word for word in words
        if word not in stop_words and len(word) > 1 and not is_noise_token(word)
    ]
    return filtered_words


def prioritize_hashtags(text):
    """
    Extract hashtags from the text and return as a list.

    Parameters
    ----------
    text : str
        The input text string.

    Returns
    -------
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


def process_text_by_language(text, lang, nlp_en, nlp_nl):
    """
    Process text and extract named entities based on the detected language.

    Parameters
    ----------
    text : str
        The text to process.
    lang : str
        The language of the text ("en", "nl", or "unknown").
    nlp_en : spacy.Language
        The spaCy model for English.
    nlp_nl : spacy.Language
        The spaCy model for Dutch.

    Returns
    -------
    str
        The named entities extracted from the text as a single space-joined string.
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

    named_entities = [
        ent.text for ent in doc.ents if ent.label_ in ("PERSON", "ORG", "GPE")
    ]

    return " ".join(named_entities)


# POS tags that carry topical meaning
_TOPICAL_POS = {"NOUN", "PROPN"}

# Lazy-loaded spaCy models for use inside Spark workers.
# Workers can't receive spaCy models via closure (they're not picklable),
# so we load them once per worker process on first use.
_worker_nlp_en = None
_worker_nlp_nl = None


def _get_worker_nlp(lang: str):
    """Get a spaCy model for the given language, loading lazily on first call."""
    global _worker_nlp_en, _worker_nlp_nl
    if lang == "en":
        if _worker_nlp_en is None:
            import spacy
            _worker_nlp_en = spacy.load("en_core_web_md")
        return _worker_nlp_en
    else:
        if _worker_nlp_nl is None:
            import spacy
            _worker_nlp_nl = spacy.load("nl_core_news_md")
        return _worker_nlp_nl

# Words with a frequency rank below this threshold (i.e. among the most common
# words in the language) are considered too generic to be trending topics.
# spaCy's token.rank is 0-based: rank 0 = most common word, rank 1 = second, etc.
# A value of 0 means the token is out-of-vocabulary (OOV), which we keep since
# OOV words are likely specific/topical (slang, names, hashtags).
_DEFAULT_MIN_FREQ_RANK = 700


def extract_topical_tokens(
    text, lang, nlp_en=None, nlp_nl=None, stop_words=None, min_freq_rank=_DEFAULT_MIN_FREQ_RANK
):
    """
    Use spaCy POS tagging and word frequency to extract topically meaningful tokens.

    Keeps nouns and proper nouns that are not among the most common words in the
    language. Also keeps hashtags unconditionally.

    When nlp_en/nlp_nl are None, models are loaded lazily via _get_worker_nlp().
    This is the intended path for Spark UDFs (models can't be pickled into closures).
    Pass models explicitly for direct calls (e.g. in tests).

    Filtering layers:
    1. POS filter — only NOUN and PROPN survive.
    2. Stop-word / noise filter — known filler removed.
    3. Frequency filter — nouns that rank among the top `min_freq_rank` most
       common words in the spaCy vocabulary are dropped (e.g. "people", "time",
       "uur", "mensen"). Out-of-vocabulary words (rank == 0) are kept, since
       they tend to be specific/topical.

    Parameters
    ----------
    text : str
        Cleaned tweet text.
    lang : str
        Language tag ("en", "nl", or "unknown").
    nlp_en : spacy.Language, optional
        English spaCy model. If None, loaded lazily.
    nlp_nl : spacy.Language, optional
        Dutch spaCy model. If None, loaded lazily.
    stop_words : set[str], optional
        Stop words to exclude. Defaults to empty set.
    min_freq_rank : int
        Minimum frequency rank a token must have to be kept. Tokens ranked
        below this (i.e. more common) are filtered out. Default 200.

    Returns
    -------
    list[str]
        List of topical tokens (nouns, proper nouns, hashtags).
    """
    if not text or lang == "unknown":
        return []

    if stop_words is None:
        stop_words = set()

    # Use provided models or fall back to lazy-loaded worker models
    if lang == "en":
        nlp = nlp_en if nlp_en is not None else _get_worker_nlp("en")
    else:
        nlp = nlp_nl if nlp_nl is not None else _get_worker_nlp("nl")
    doc = nlp(text)

    tokens = []
    for token in doc:
        word = token.text.lower().strip()
        if len(word) <= 1:
            continue
        if word in stop_words or is_noise_token(word):
            continue
        if token.pos_ not in _TOPICAL_POS:
            continue
        # Filter high-frequency generic nouns.
        # rank == 0 means OOV in spaCy — keep those (likely specific/topical).
        # rank > 0 and < min_freq_rank means it's among the most common words.
        if token.rank != 0 and token.rank < min_freq_rank:
            continue
        tokens.append(word)

    # Always include hashtags (spaCy may not tag them as nouns)
    for ht in re.findall(r"#\w+", text):
        ht_lower = ht.lower()
        if ht_lower not in tokens:
            tokens.append(ht_lower)

    return tokens
