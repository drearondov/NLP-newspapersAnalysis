import logging
import os
import pandas as pd
import spacy
import sys

from itertools import product
from dotenv import load_dotenv


def make_stats(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Creates a data frame with the stats associated to a tweet

    Args:
        df_clean (pd.DataFrame): Dataframe containing the stats associated with a tweet and other columns

    Returns:
        pd.DataFrame: A data frame with only the stats
    """
    return df_clean.drop(["text", "text_clean"], axis=1)


def make_corpus(df_clean: pd.DataFrame) -> pd.DataFrame:
    """Creates a dataframe with a corpus for analysis

    Args:
        clean_data (pd.DataFrame): Data frame containing a column with clean data, and a tokenized set

    Returns:
        pd.DataFrame: Data frame containing just que necesary column for analysis as a corpus
    """
    df_corpus = df_clean[["id", "created_at", "newspaper", "text_clean"]].reset_index()
    df_corpus.rename(columns={"text_clean": "corpus"}, inplace=True)

    return df_corpus


def make_dtm(data_dtm: pd.DataFrame) -> pd.DataFrame:
    """Makes a Document - Term Matrix out of doc

    Args:
        data_dtm (pd.DataFrame): Dataframe containing normalised data

    Returns:
        pd.DataFrame: Document-Term matrix
    """
    dtm = pd.DataFrame(data_dtm[["id", "lemma"]].explode("lemma").groupby(by="id").value_counts())
    dtm.rename({0: "count"}, axis=1, inplace=True)
    dtm = dtm.reset_index()

    dtm = dtm.pivot(index="id", columns="lemma", values="count")
    dtm.fillna(0.00, inplace=True)

    return dtm


if __name__ == "__main__":
    load_dotenv()

    BASE_DIR = os.environ.get("BASE_DIR")
    BEARER_TOKEN = os.environ.get("BEARER_TOKEN")

    gruvbox_colors = [
        "#458588",
        "#FABD2F",
        "#B8BB26",
        "#CC241D",
        "#B16286",
        "#8EC07C",
        "#FE8019",
    ]

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(stream=sys.stdout, format=log_format, level=logging.INFO)

    logger = logging.getLogger()

    TIME_STAMPS = [(2023, 23)]

    nlp = spacy.load("es_core_news_sm")

    for time_stamp in TIME_STAMPS:

        clean_data = pd.read_feather(f"{BASE_DIR}/data/interim/data_clean-{time_stamp}.feather")
        clean_data.drop(["index"], axis=1, inplace=True)

        stats = make_stats(clean_data)

        stats.to_feather(
            f"{BASE_DIR}/data/processed/stats-{time_stamp}.feather"
        )

        df_corpus = clean_data[
            ["id", "created_at", "newspaper", "text", "text_clean"]
        ].reset_index()

        df_corpus.rename(columns={"text_clean": "corpus"}, inplace=True)

        df_corpus.to_feather(
            f"{BASE_DIR}/data/processed/corpus-{time_stamp}.feather"
        )

        logger.info(f"Corpus saved!: {time_stamp[0]}-{time_stamp[1]}")

        data_dtm = df_corpus

        data_dtm["doc"] = data_dtm["corpus"].apply(lambda x: nlp(x))

        data_dtm["token"] = data_dtm["doc"].apply(lambda doc: [t.orth_ for t in doc if not t.is_punct | t.is_stop | t.is_space])
        data_dtm["lemma"] = data_dtm["doc"].apply(lambda doc: [t.lemma_ for t in doc if not t.is_punct | t.is_stop | t.is_space])
        
        data_dtm = data_dtm.loc[data_dtm["corpus"] != ""]
        data_dtm = data_dtm.loc[data_dtm["token"].map(lambda d: len(d)) > 0]

        data_dtm.to_pickle(
            f"{BASE_DIR}/data/processed/data-dtm-{time_stamp}.pkl"
        )

        dtm = make_dtm(data_dtm)

        dtm.reset_index(names="id.").to_feather(
            f"{BASE_DIR}/data/processed/dtm-{time_stamp}.feather"
        )

        logger.info(f"DTM saved!: {time_stamp[0]}-{time_stamp[1]}")