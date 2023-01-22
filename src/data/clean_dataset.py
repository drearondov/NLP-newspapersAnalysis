import json
import os
import re
import string

import pandas as pd
import numpy as np

from dotenv import load_dotenv
from itertools import product


load_dotenv()

BASE_DIR = os.environ.get("BASE_DIR")
BEARER_TOKEN = os.environ.get("BEARER_TOKEN")

gruvbox_colors = ["#458588", "#FABD2F", "#B8BB26", "#CC241D", "#B16286", "#8EC07C", "#FE8019"]


def load_from_s3():
    pass


def load_raw_data(time_stamps: list[tuple]) -> pd.DataFrame:
    """Loads and consolidates data into one data frame.

    Args:
        time_stamps (list[tuple]): List of (year, week) tuples to load for analysis.

    Returns:
        pd.DataFrame: Data frame ready for cleaning.
    """
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

def clear_emojis():
    pass

def clean_text_second_pass():
    pass


if __name__ == "__main__":

    with open(f'{BASE_DIR}/data/raw/newspapers_id.json', 'r') as read_file:
        newspapers_id = json.load(read_file)

    raw_data = load_raw_data()

    data = drop_non_relevant(raw_data)

    data = clean_text_first_pass(data)