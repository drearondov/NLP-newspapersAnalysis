"""
This is a boilerplate pipeline 'eda'
generated using Kedro 0.18.14
"""
import pandas as pd

from typing import Any, Callable, Dict


def create_stats_summary(
    clean_data_dataset: Dict[str, Callable[[], Any]]
) -> Dict[str, pd.DataFrame]:
    """Transforms stats data into long format.

    Args:
        data (pd.DataFrame): Statistics DataFrame clean and compiled

    Returs:
        pd.DataFrame: Stats data in long form
    """
    stats_summary_results = {}

    for filename, clean_data_function in clean_data_dataset.items():
        new_filename = filename.replace(".feather", "").replace(
            "data_clean", "stats_summary"
        )

        data = clean_data_function()
        data_stats = pd.DataFrame()

        data_stats["tweet_count"] = data["newspaper"].value_counts()

        data_stats = data_stats.merge(
            data.loc[data["referenced_tweets"].notna(), "newspaper"].value_counts(),
            how="left",
            left_index=True,
            right_index=True,
        )
        data_stats.rename(columns={"count": "referenced_tweet_count"}, inplace=True)

        data_stats = data_stats.merge(
            data.loc[data["possibly_sensitive"], "newspaper"].value_counts(),
            how="left",
            left_index=True,
            right_index=True,
        )
        data_stats.rename(columns={"count": "possibly_sensitive_count"}, inplace=True)

        data_stats = data_stats.merge(
            data.groupby("newspaper")
            .sum(numeric_only=True)
            .drop("possibly_sensitive", axis=1),
            how="left",
            left_index=True,
            right_index=True,
        )

        data_stats["reference_to_tweets_ratio"] = (
            data_stats["referenced_tweet_count"] / data_stats["tweet_count"]
        )
        data_stats["sensitive_to_tweets_ratio"] = (
            data_stats["possibly_sensitive_count"] / data_stats["tweet_count"]
        )
        data_stats["retweet_to_tweets_ratio"] = (
            data_stats["retweet_count"] / data_stats["tweet_count"]
        )
        data_stats["reply_to_tweets_ratio"] = (
            data_stats["reply_count"] / data_stats["tweet_count"]
        )
        data_stats["like_to_tweets_ratio"] = (
            data_stats["like_count"] / data_stats["tweet_count"]
        )
        data_stats["quote_to_tweets_ratio"] = (
            data_stats["quote_count"] / data_stats["tweet_count"]
        )

        stats_summary_results[new_filename] = data_stats.T.reset_index(names=["Stat"])

    return stats_summary_results


def create_top30(
    dtm_newspaper_data: Dict[str, Callable[[], Any]]
) -> Dict[str, Callable[[], Any]]:
    """Returns a Dictionary of Dataframes with the Top 30 words form every newspaper by week.

    Args:
        dtm_newspaper_data (Dict[str, Callable[[], Any]]): Dictionary of DTM per newspaper per week

    Returns:
        Dict[str, Callable[[], Any]]: Dictionary of Top30 words per newspaper per week
    """
    top30_results = {}

    for filename, dtm_newspaper_load in dtm_newspaper_data.items():
        new_filename = filename.replace("dtm_newspaper", "top30")

        dtm_newspaper = dtm_newspaper_load()

        top30_dict = {}

        for newspaper in dtm_newspaper.columns:
            top = dtm_newspaper[newspaper].sort_values(ascending=False).head(30)
            top30_dict[newspaper] = list(zip(top.index, top.values))

        top30_df = pd.DataFrame.from_records(top30_dict)

        top30_df = top30_df.melt(
            value_vars=top30_df.columns,
            var_name="newspaper_date",
            value_name="word_count",
        )

        top30_df[["newspaper", "year_week"]] = top30_df["newspaper_date"].str.split(
            r"-", expand=True
        )
        top30_df[["year", "week"]] = top30_df["year_week"].str.split(r"_", expand=True)
        top30_df[["word", "count"]] = pd.DataFrame(
            top30_df["word_count"].to_list(), index=top30_df.index
        )

        top30_df.drop(
            ["word_count", "newspaper_date", "year_week"], axis=1, inplace=True
        )

        top30_df["year"] = pd.to_numeric(top30_df["year"])
        top30_df["week"] = pd.to_numeric(top30_df["week"])

        top30_df["hot_topics"] = ""

        top30_results[new_filename] = top30_df

    return top30_results


def create_unique_words(
    dtm_newspaper_data: Dict[str, Callable[[], Any]],
    corpus_data: Dict[str, Callable[[], Any]],
) -> Dict[str, Callable[[], Any]]:
    """Returns a dictionary with Dataframes containting data about number of unique words and the ratio words to Tweets.

    Args:
        dtm_newspaper_data (Dict[str, Callable[[], Any]]): Dictionary of DTM per newspaper data
        corpus_data (Dict[str, Callable[[], Any]]): Dictionary of corpus data

    Returns:
        Dict[str, Callable[[], Any]]: Dictionary with Pandas Dataframes with data about unique words
    """
    unique_words_dict = {}

    for (filename, dtm_newspaper_data_load), corpus_data_load in zip(
        dtm_newspaper_data.items(), corpus_data.values()
    ):
        new_filename = filename.replace("dtm_newspaper", "unique_words")

        dtm_newspaper = dtm_newspaper_data_load()
        corpus = corpus_data_load()

        unique_list = []

        # Identify the non-zero items in the document-term matrix
        for newspaper in dtm_newspaper.columns:
            uniques = dtm_newspaper[newspaper].to_numpy().nonzero()[0].size
            unique_list.append(uniques)

        # Create a new datafra,e that contains this unique word count
        data_words = pd.DataFrame(
            list(zip(dtm_newspaper.columns, unique_list)),
            columns=["newspaper", "unique_words"],
        )
        data_words.set_index("newspaper", inplace=True)
        data_words.sort_values(by="unique_words", ascending=False)
        data_words.reset_index(inplace=True)

        data_words[["newspaper", "year_week"]] = data_words["newspaper"].str.split(
            r"-", expand=True
        )
        data_words[["year", "week"]] = data_words["year_week"].str.split(
            r"_", expand=True
        )

        data_words.drop(["year_week"], axis=1, inplace=True)

        data_words["year"] = pd.to_numeric(data_words["year"])
        data_words["week"] = pd.to_numeric(data_words["week"])

        tweet_number = pd.DataFrame(
            corpus.groupby(by=["newspaper", "year", "week"]).count()["id"]
        )
        tweet_number.rename(columns={"id": "tweet_number"}, inplace=True)
        tweet_number.reset_index(inplace=True)

        data_words = data_words.merge(tweet_number)

        data_words["word_tweet_ratio"] = (
            data_words["unique_words"] / data_words["tweet_number"]
        )
        data_words.sort_values(by="word_tweet_ratio", ascending=False)

        unique_words_dict[new_filename] = data_words

    return unique_words_dict
