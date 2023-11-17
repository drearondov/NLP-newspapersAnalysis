"""
This is a boilerplate pipeline 'cleaning_and_preprocessing'
generated using Kedro 0.18.14
"""
import emoji
import logging
import pandas as pd
import re
import string

from collections import defaultdict
from num2words import num2words
from typing import Any, Callable, Dict

logger = logging.getLogger(__name__)


def _separate_weeks(raw_tweet_data: pd.DataFrame) -> defaultdict:
    """Separates a file by week and returns a dict where the keys are year_week to be appended into the year week list.

    Args:
        raw_tweet_data (pd.DataFrame): Dataframe with raw tweet data to separate weeks

    Returns:
        defaultdict: Dictionary with year_week as keys
    """
    weeks = defaultdict(list)

    raw_tweet_data["created_at"] = pd.to_datetime(raw_tweet_data["created_at"])

    raw_tweet_data["year"] = raw_tweet_data["created_at"].dt.isocalendar()["year"]
    raw_tweet_data["week"] = raw_tweet_data["created_at"].dt.isocalendar()["week"]

    raw_tweet_data["year_week"] = raw_tweet_data.apply(
        lambda x: f"{x['year']}_{x['week']}", axis=1
    )

    unique_year_week = raw_tweet_data["year_week"].unique()

    for year_week in unique_year_week:
        weeks[year_week].append(
            raw_tweet_data.loc[raw_tweet_data["year_week"] == year_week]
        )

    return weeks


def compile_raw_data(
    newspaper_raw_tweets: Dict[str, Callable[[], Any]]
) -> Dict[str, Any]:
    """Node to process and compile raw tweet data into dataframes and then save as feather files.

    Args:
        newspaper_raw_json_tweets (Dict[str, Callable[[], Any]]): Dictionary containing the part of a Partitioned Dataset of JSON files.

    Returns:
        Dict[str, Any]: Dictionary containing the Dataframes to save as feather files. File name: data_raw-(timestamp tuple).feather
    """
    compiled_data = defaultdict(list)
    data_raw = {}

    for filename, load_data_function in newspaper_raw_tweets.items():
        newspaper_name = filename.replace(".json", "").split("_data_")[-1]

        json_data = load_data_function()
        raw_data = json_data["data"]

        newspaper_df = pd.json_normalize(raw_data)

        newspaper_df["newspaper"] = newspaper_name

        newspaper_df.columns = newspaper_df.columns.str.removeprefix("public_metrics.")

        newspaper_weeks = _separate_weeks(newspaper_df)

        logger.debug(f"{newspaper_weeks}")

        for year_week, data_frame in newspaper_weeks.items():
            year_week = year_week.split("_")
            compiled_data[f"data_raw-({year_week[0]}, {year_week[1]})"] += data_frame

        logger.debug(compiled_data.keys())

        for filename, df_list in compiled_data.items():
            data_raw[f"{filename}.feather"] = pd.concat(df_list).reset_index()

    return data_raw


def _drop_non_relevant(data: pd.DataFrame) -> pd.DataFrame:
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
        "yapaza",
    ]

    for non_relevant in non_relevant_strings:
        data.drop(
            data[
                data["text"].str.contains(non_relevant, flags=re.IGNORECASE, regex=True)
            ].index,
            inplace=True,
        )

    return data


def _number_processing(text: str) -> str:
    """Takes a string, finds numbers on it, converts numbers to words and returns string with numbers replaced

    Args:
        text (str): text string to be processed

    Returns:
        str: string with numbers processed
    """
    numbers = re.findall(r"\b\d+\b", text)

    if numbers is []:
        return text

    for number in numbers:
        word_number = num2words(float(number), lang="es")
        text = re.sub(number, word_number, text)

    return text


def _clean_text_first_pass(text: str) -> str:
    """Get rid of other punctuation and non-sensical text identified.

    Args:
        text (string): text to be processed.
    Returns:
        str: Processed string
    """
    text = text.lower()

    string_remove = [
        "http[s]?(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+",  # Eliminates links
        "\w*\d\w*",  # Eliminates numbers
        "[%s]" % re.escape(string.punctuation),  # Eliminates punctuarion
        "[‘’“”…«»►¿¡|│]",
    ]

    for noise in string_remove:
        text = re.sub(noise, "", text)

    text = re.sub("\n", " ", text)

    return text


def _clean_text_second_pass(text: str) -> str:
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


def clean_data(raw_data: Dict[str, Callable[[], Any]]) -> Dict[str, Any]:
    """_summary_

    Args:
        raw_data (Dict[str, Callable[[], Any]]): _description_

    Returns:
        Dict[str, Any]: _description_
    """
    clean_data = {}

    for filename, load_data_function in raw_data.items():
        new_filename = filename.replace("raw", "clean")
        data = _drop_non_relevant(load_data_function())

        data["mentions"] = data["text"].apply(lambda x: re.findall("@(\w+)", x))
        data["hasthags"] = data["text"].apply(lambda x: re.findall("#(\w+)", x))

        data["text_clean"] = data["text"].apply(_number_processing)
        data["text_clean"] = data["text_clean"].apply(
            lambda x: _clean_text_first_pass(x)
        )
        data["text_clean"] = data["text_clean"].apply(
            lambda x: emoji.replace_emoji(x, "")
        )
        data["text_clean"] = data["text_clean"].apply(
            lambda x: _clean_text_second_pass(x)
        )
        data["text_clean"] = data["text_clean"].str.strip()

        clean_data[new_filename] = data

    return clean_data
