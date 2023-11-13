"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.14
"""
import logging
import pandas as pd
import spacy

from typing import Any, Callable, Dict
from itertools import product


def make_corpus(clean_data: Dict[str, Callable[[], Any]]) -> Dict[str, pd.DataFrame]:
    """Returns a dictionary with clean Corpus Dataframes

    Args:
        clean_data (Dict[str, Callable[[], Any]]): Dictionary containing clean dataframes in a Partitioned Dataset of Feather files

    Returns:
        Dict[str, pd.DataFrame]: Dictionary containing dataframes with the original text and corpus
    """
    corpus_data = {}

    for filename, load_data_function in clean_data.items():
        new_filename = filename.replace("data_clean", "corpus")

        clean_data = load_data_function()
        clean_data.drop(["index"], axis=1, inplace=True)

        df_corpus = clean_data[
            ["id", "created_at", "newspaper", "text", "text_clean"]
        ].reset_index()

        df_corpus.rename(columns={"text_clean": "corpus"}, inplace=True)

        corpus_data[new_filename] = df_corpus

    return corpus_data


def make_data_dtm(corpus_data: Dict[str, Callable[[], Any]]) -> Dict[str, pd.DataFrame]:
    """Returns a dictionary of data to make Document-Term Matrices per week.

    Args:
        corpus_data (Dict[str, Callable[[], Any]]): Data from a Partitiones Dataset with files containtin corpus information

    Returns:
        Dict[str, pd.DataFrame]: Dictionary of Dataframes to be pickled. Contains NLP Spacy data.
    """
    nlp = spacy.load("es_core_news_sm")
    data_dtm_data = {}

    for filename, load_data_function in corpus_data.items():
        new_filename = filename.replace("corpus", "data_dtm")

        data_dtm = load_data_function()

        data_dtm["doc"] = data_dtm["corpus"].apply(lambda x: nlp(x))

        data_dtm["token"] = data_dtm["doc"].apply(
            lambda doc: [
                t.orth_ for t in doc if not t.is_punct | t.is_stop | t.is_space
            ]
        )
        data_dtm["lemma"] = data_dtm["doc"].apply(
            lambda doc: [
                t.lemma_ for t in doc if not t.is_punct | t.is_stop | t.is_space
            ]
        )

        data_dtm = data_dtm.loc[data_dtm["corpus"] != ""]
        data_dtm = data_dtm.loc[data_dtm["token"].map(lambda d: len(d)) > 0]

        data_dtm_data[new_filename] = data_dtm


def make_dtm(data_dtm_data: Dict[str, Callable[[], Any]]) -> Dict[str, pd.DataFrame]:
    """Returns a dictionary of Document-Term Matrices per week.

    Args:
        data_dtm_data (Dict[str, Callable[[], Any]]): Dictionary with Spacy data to make a Document Term Matrix

    Returns:
        Dict[str, pd.DataFrame]: Dictionary of Document Term Matrices per week
    """
    dtm_data = {}

    for filename, load_data_function in data_dtm_data.items():
        new_filename = filename.replace("data_dtm", "dtm")

        data_dtm = load_data_function()

        dtm = pd.DataFrame(
            data_dtm[["id", "lemma"]].explode("lemma").groupby(by="id").value_counts()
        )
        dtm.rename({0: "count"}, axis=1, inplace=True)
        dtm = dtm.reset_index()

        dtm = dtm.pivot(index="id", columns="lemma", values="count")
        dtm.fillna(0.00, inplace=True)

        dtm_data[new_filename] = dtm

    return dtm_data


def make_dtm_newspaper(
    corpus_data: Dict[str, Callable[[], Any]], dtm_data: Dict[str, Callable[[], Any]]
) -> Dict[str, pd.DataFrame]:
    """Returns a dictionary of DTM per newspaper per week.

    Args:
        corpus_data (Dict[str, Callable[[], Any]]): Dictionary with Corpus data
        dtm_data (Dict[str, Callable[[], Any]]): Dictionary with DTM data

    Returns:
        Dict[str, pd.DataFrame]: Dictionary with Dataframes with the Top 30 words per Newspaper.
    """
    dtm_newspaper_dict = {}
    logger = logging.getLogger(__name__)

    for (filename, corpus_data_function), dtm_data_function in zip(
        corpus_data.items(), dtm_data.values()
    ):
        new_filename = filename.replace("corpus", "dtm_newspaper")

        corpus = corpus_data_function()
        dtm = dtm_data_function()

        corpus.set_index("index", inplace=True)
        dtm.set_index("id.", inplace=True)

        newspapers = corpus["newspaper"].unique()

        year_weeks = corpus["created_at"].dt.isocalendar()[["year", "week"]]
        year_weeks.drop_duplicates(inplace=True)
        year_weeks = year_weeks.to_numpy()

        dtm_newspaper = pd.DataFrame(index=dtm.columns)

        corpus["year"] = corpus["created_at"].dt.isocalendar().year
        corpus["week"] = corpus["created_at"].dt.isocalendar().week

        dtm_newspaper = pd.DataFrame(index=dtm.columns)

        for year_week, newspaper in product(year_weeks, newspapers):
            data_ids = corpus.loc[
                (corpus["newspaper"] == newspaper)
                & (corpus["year"] == year_week[0])
                & (corpus["week"] == year_week[1]),
                ["id"],
            ]
            filtered_data = dtm.filter(items=data_ids["id"], axis=0)
            dtm_newspaper[
                f"{newspaper}-{year_week[0]}_{year_week[1]}"
            ] = filtered_data.sum(axis=0)

        dtm_newspaper_dict[new_filename] = dtm_newspaper

        logger.info(f"DTM Newspaper -> {new_filename}")

    return dtm_newspaper_dict
