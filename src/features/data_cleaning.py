import emoji
import json
import logging
import os
import re
import string
import sys

import pandas as pd

from dotenv import load_dotenv
from itertools import product
from pathlib import Path


def load_from_s3():
    pass


def load_raw_data(time_stamp: tuple[int, int], newspapers_id) -> pd.DataFrame:
    """Loads and consolidates data from a specific (year,week) combination into one data frame.

    Args:
        time_stamp tuple[int, int]: (year, week) tuple to load for analysis.

    Returns:
        pd.DataFrame: Data frame ready for cleaning.
    """
    if Path(
        f"{BASE_DIR}/data/interim/data_raw-{time_stamp}.feather"
    ).is_file():
        data_raw = pd.read_feather(
            f"{BASE_DIR}/data/interim/data_raw-{time_stamp}.feather"
        )
        data_raw.set_index("index", inplace=True)
    else:
        newspaper_df_list = []

        for newspaper in newspapers_id:
            try:
                with open(
                    f"{BASE_DIR}/data/raw/{time_stamp[0]}w{time_stamp[1]}_data_{newspaper}.json", "r"
                ) as read_file:
                    json_file = json.load(read_file)

                json_data = json_file["data"]

                newspaper_df = pd.json_normalize(json_data)
                newspaper_df["newspaper"] = newspaper

                newspaper_df_list.append(newspaper_df)
            except FileNotFoundError:
                continue

        data_raw = pd.concat(newspaper_df_list)
        data_raw["created_at"] = pd.to_datetime(
            data_raw["created_at"], infer_datetime_format=True
        )
        data_raw.columns = data_raw.columns.str.removeprefix("public_metrics.")

        data_raw.reset_index().to_feather(
            f"{BASE_DIR}/data/interim/data_raw-{time_stamp}.feather"
        )

    return data_raw


def drop_non_relevant(data: pd.DataFrame) -> pd.DataFrame:
    """Removing the tweets that don't add to the sentiment of the newspaper.

    Args:
        raw_data (pd.DataFrame): Consolidated raw data, with all tweets.

    Returns:
        pd.DataFrame: Data without non-relevant tweets.
    """
    non_relevant_strings = [
        "horóscopo diario",
        "horóscopo de",
        "horóscopo hoy",
        "horóscopo y tarot",
        "horóscopo",
        "Buenos días",
        "caricatura de",
        "las caricaturas de",
        "portada impresa",
        "portada de hoy",
        "en portada",
        "trome gol",
        "no te pierdas las chiquitas de hoy",
        "esta es la portada",
        "Aquí la portada del",
        "yapaza"
    ]

    for non_relevant in non_relevant_strings:
        data.drop(
            data[
                data["text"].str.contains(
                    non_relevant, flags=re.IGNORECASE, regex=True
                )
            ].index,
            inplace=True,
        )

    return data


def clean_text_first_pass(text):
    """Get rid of other punctuation and non-sensical text identified.

    Args:
        text (string): text to be processed.
    """
    text = text.lower()
    text = re.sub(
        "http[s]?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",
        "",
        text,
    )  # Eliminates links
    text = re.sub("\w*\d\w*", "", text)  # Eliminates numbers
    text = re.sub(
        "[%s]" % re.escape(string.punctuation), "", text
    )  # Eliminates punctuarion
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
    text = re.sub("lee aquí el blog de", "", text)
    text = re.sub("vía gestionpe", "", text)
    text = re.sub("entrevista exclusiva", "", text)
    text = re.sub("en vivo", "", text)
    text = re.sub("entérate más aquí", "", text)
    text = re.sub("lee la columna de", "", text)
    text = re.sub("lee y comenta", "", text)
    text = re.sub("lea hoy la columna de", "", text)
    text = re.sub("escrito por", "", text)
    text = re.sub("lee la nota aquí", "", text)
    text = re.sub("una nota de", "", text)
    text = re.sub("aquí la nota", "", text)
    text = re.sub("nota completa aquí", "", text)
    text = re.sub("nota completa", "", text)
    text = re.sub("lee más", "", text)
    text = re.sub("lee aquí", "", text)

    text = re.sub("  ", " ", text)
    text = re.sub(" \w ", " ", text)
    text = re.sub("^(plusg)", "", text)
    text = re.sub("( video )$", "", text)
    text = re.sub("( lee )$", "", text)
    text = re.sub("( lee la )$", "", text)

    return text


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

    TIME_STAMPS = [(2023,23)]

    with open(f"{BASE_DIR}/data/raw/newspapers_id.json", "r") as read_file:
        newspapers_id = json.load(read_file)

    for time_stamp in TIME_STAMPS:
        raw_data = load_raw_data(time_stamp, newspapers_id)

        logger.info(f"Raw data loaded: {time_stamp[0]}-{time_stamp[1]}")

        data = drop_non_relevant(raw_data)

        data["text_clean"] = data.text.apply(lambda x: clean_text_first_pass(x))
        data["text_clean"] = data["text_clean"].apply(lambda x: emoji.replace_emoji(x, ""))
        data["text_clean"] = data.text_clean.apply(lambda x: clean_text_second_pass(x))

        logger.info(f"Data cleanned: {time_stamp[0]}-{time_stamp[1]}")

        data.reset_index().to_feather(
            f"{BASE_DIR}/data/interim/data_clean-{time_stamp}.feather"
        )

        logger.info(f"Data saved: {time_stamp[0]}-{time_stamp[1]}")
