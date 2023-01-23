import emoji
import json
import logging
import os
import re
import spacy
import string
import sys

import pandas as pd
import numpy as np

from collections import Counter
from dotenv import load_dotenv
from itertools import product
from pathlib import Path


def load_from_s3():
    pass


def load_raw_data(time_stamps: list[tuple]) -> pd.DataFrame:
    """Loads and consolidates data into one data frame.

    Args:
        time_stamps (list[tuple]): List of (year, week) tuples to load for analysis.

    Returns:
        pd.DataFrame: Data frame ready for cleaning.
    """
    if Path(f"{BASE_DIR}/data/interim/data_raw-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather").is_file():
        data_raw = pd.read_feather(f"{BASE_DIR}/data/processed/data_raw-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")
        data_raw.set_index("index", inplace=True)
    else:
        newspaper_df_list = []

        for newspaper, (year, week) in product(newspapers_id, time_stamps):

            with open(f'{BASE_DIR}/data/raw/{year}w{week}_data_{newspaper}.json', 'r') as read_file:
                json_file = json.load(read_file)

            json_data = json_file["data"]

            newspaper_df = pd.json_normalize(json_data)
            newspaper_df["newspaper"] = newspaper

            newspaper_df_list.append(newspaper_df)

        data_raw = pd.concat(newspaper_df_list)
        data_raw["created_at"] = pd.to_datetime(data_raw["created_at"], infer_datetime_format=True)
        data_raw.columns = data_raw.columns.str.removeprefix("public_metrics.")

        data_raw.reset_index().to_feather(f"{BASE_DIR}/data/interim/data_raw-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")

    return data_raw

def drop_non_relevant(data: pd.DataFrame) -> pd.DataFrame:
    """Removing the tweets that don't add to the sentiment of the newspaper.

    Args:
        raw_data (pd.DataFrame): Consolidated raw data, with all tweets.

    Returns:
        pd.DataFrame: Data without non-relevant tweets.
    """
    data.drop(data[data["text"].str.contains('horóscopo diario', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('horóscopo de', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('horóscopo hoy', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('horóscopo y tarot', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('horóscopo', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('Buenos días', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('caricatura de', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('las caricaturas de', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('portada impresa', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('portada de hoy', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('en portada', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('trome gol', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('no te pierdas las chiquitas de hoy', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data[data["text"].str.contains('esta es la portada', flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data.loc[data["text"].str.contains("Aquí la portada del", flags=re.IGNORECASE, regex=True)].index, inplace=True)
    data.drop(data.loc[data["text"].str.contains("yapaza", flags=re.IGNORECASE, regex=True)].index, inplace=True)

    return data

def clean_text_first_pass(text):
    """Get rid of other punctuation and non-sensical text identified.

    Args:
        text (string): text to be processed.
    """
    text = text.lower()
    text = re.sub("http[s]?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+", "", text) # Eliminates links
    text = re.sub("\w*\d\w*", "", text) # Eliminates numbers
    text = re.sub("[%s]" % re.escape(string.punctuation), "", text) # Eliminates punctuarion
    text = re.sub("[‘’“”…«»►¿¡|│]", "", text)
    text = re.sub("\n", " ", text)

    return text


def clean_text_second_pass(text):
    """Get rid of other punctuation and non-sensical text identified.

    Args:
        text (string): text to be processed.
    """
    text = re.sub("click aquí", "", text)
    text = re.sub("opinión", "", text)
    text = re.sub("rt ", "", text)
    text = re.sub('lee aquí el blog de', '', text)
    text = re.sub('vía gestionpe', '', text)
    text = re.sub('entrevista exclusiva', '', text)
    text = re.sub('en vivo', '', text)
    text = re.sub('entérate más aquí', '', text)
    text = re.sub('lee la columna de', '', text)
    text = re.sub('lee y comenta', '', text)
    text = re.sub('lea hoy la columna de', '', text)
    text = re.sub('escrito por', '', text)
    text = re.sub('lee la nota aquí', '', text)
    text = re.sub('una nota de', '', text)
    text = re.sub('aquí la nota', '', text)
    text = re.sub('nota completa aquí', '', text)
    text = re.sub('nota completa', '', text)
    text = re.sub('lee más', '', text)
    text = re.sub('lee aquí', '', text)

    text = re.sub("  ", " ", text)
    text = re.sub(" \w ", " ", text)
    text = re.sub("^(plusg)", "", text)
    text = re.sub("( video )$", "", text)
    text = re.sub("( lee )$", "", text)
    text = re.sub("( lee la )$", "", text)

    return text

def normalize_text(text):
    doc = nlp(text)
    words = [t.orth_ for t in doc if not t.is_punct | t.is_stop | t.is_space]

    return words


def make_dtm(data_dtm: pd.DataFrame) -> pd.DataFrame:
    """Makes a Document - Term Matrix out of doc

    Args:
        data_dtm (pd.DataFrame): Dataframe containing normalised data

    Returns:
        pd.DataFrame: Document-Term matrix
    """
    tweet_count = {}

    for tweet in data_dtm.itertuples(index=False, name="Tweet"):
        tweet_count[tweet.id] = dict(Counter(tweet.doc))

    dtm = pd.DataFrame.from_dict(tweet_count, orient="index")
    dtm.fillna(0.00, inplace=True)

    return dtm


if __name__ == "__main__":

    load_dotenv()

    BASE_DIR = os.environ.get("BASE_DIR")
    BEARER_TOKEN = os.environ.get("BEARER_TOKEN")

    gruvbox_colors = ["#458588", "#FABD2F", "#B8BB26", "#CC241D", "#B16286", "#8EC07C", "#FE8019"]

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    logging.basicConfig(
        stream = sys.stdout, 
        format = log_format, 
        level = logging.INFO
    )

    logger = logging.getLogger()

    TIME_STAMPS = [(2022, 35), (2022, 40), (2022, 45), (2022, 50), (2023, 3)]

    with open(f'{BASE_DIR}/data/raw/newspapers_id.json', 'r') as read_file:
        newspapers_id = json.load(read_file)

    raw_data = load_raw_data(TIME_STAMPS)

    logger.info("Raw data loaded")

    data = drop_non_relevant(raw_data)

    first_pass = lambda x: clean_text_first_pass(x)
    replace_emojis = lambda x: emoji.replace_emoji(x, "")
    second_pass = lambda x: clean_text_second_pass(x)

    data["text_clean"] = data.text.apply(first_pass)
    data["text_clean"] = data["text_clean"].apply(replace_emojis)
    data["text_clean"] = data.text_clean.apply(second_pass)

    logger.info("Data cleanned")

    data.reset_index().to_feather(f"{BASE_DIR}/data/interim/data_clean-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")

    df_corpus = data[["id", "created_at", "newspaper", "text_clean"]].reset_index()
    df_corpus.rename(columns={"text_clean": "corpus"}, inplace=True)

    df_corpus.to_feather(f"{BASE_DIR}/data/processed/corpus-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")

    nlp = spacy.load('es_core_news_sm')

    normalize = lambda x: normalize_text(x)

    data_dtm = df_corpus

    data_dtm["doc"] = data_dtm["corpus"].apply(normalize)

    data_dtm.to_feather(f"{BASE_DIR}/data/processed/data-dtm-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")

    dtm = make_dtm(data_dtm)

    dtm.reset_index().to_feather(f"{BASE_DIR}/data/processed/dtm-{TIME_STAMPS[0]}-{TIME_STAMPS[-1]}.feather")