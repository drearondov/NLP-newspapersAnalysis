"""
This is a boilerplate pipeline 'topic_modeling'
generated using Kedro 0.18.14
"""
import pandas as pd

from gensim import matutils, models
from scipy import sparse
from typing import Dict, Any, Callable


def topic_modeling(
    data_dtm_data: Dict[str, Callable[[], Any]], dtm_data: Dict[str, Callable[[], Any]]
) -> Dict[str, Callable[[], Any]]:
    """Returns a Dict with Dataframes after Topic Modeling

    Args:
        data_dtm (Dict[str, Callable[[], Any]]): Dictionary with Partitioned Dataset with Data DTM per week.
        dtm (Dict[str, Callable[[], Any]]): Dictionary with Partitioned Dataset with DTM per week.

    Returns:
        Dict[str, Callable[[], Any]]: Dictionary with Partitioned Dataset with the results of Topic Modeling
    """
    topic_modeling_results = {}

    for (filename, data_dtm_data_function), dtm_data_function in zip(
        data_dtm_data.items(), dtm_data.values()
    ):
        new_filename = filename.replace("data_dtm", "topic_modeling")

        data_dtm = data_dtm_data_function()
        dtm = dtm_data_function()

        data_dtm["year"] = data_dtm["created_at"].dt.isocalendar().year
        data_dtm["week"] = data_dtm["created_at"].dt.isocalendar().week
        data_dtm["year_week"] = (
            data_dtm["year"].astype("str") + "w" + data_dtm["week"].astype("str")
        )

        newspapers = data_dtm["newspaper"].unique()

        newspapers_dtm = {}

        newspapers_models = {}

        for newspaper in newspapers:
            ids = data_dtm.loc[
                data_dtm["newspaper"] == newspaper,
                "id",
            ].tolist()

            newspaper_tweets = dtm.T[ids]

            newspapers_dtm[newspaper] = newspaper_tweets

            sparse_dtm = sparse.csc_matrix(newspaper_tweets)
            tweet_corpus = matutils.Sparse2Corpus(sparse_dtm)

            id2word = {}

            for index, word in enumerate(newspaper_tweets.T.columns):
                id2word[index] = word

            newspapers_models[newspaper] = models.LdaModel(
                corpus=tweet_corpus, id2word=id2word, num_topics=3, passes=80
            )

        newspaper_lda = pd.DataFrame.from_dict(newspapers_models, orient="index")
        newspaper_lda.reset_index(inplace=True)
        newspaper_lda.rename({0: "lda_model"}, axis=1, inplace=True)

        newspaper_lda["topics"] = newspaper_lda["lda_model"].apply(
            lambda lda: lda.print_topics()
        )

        newspaper_lda["topic_1"] = newspaper_lda["topics"].apply(lambda x: x[0])
        newspaper_lda["topic_2"] = newspaper_lda["topics"].apply(lambda x: x[1])
        newspaper_lda["topic_3"] = newspaper_lda["topics"].apply(lambda x: x[2])

        topic_modeling_results[new_filename] = newspaper_lda

    return topic_modeling_results
