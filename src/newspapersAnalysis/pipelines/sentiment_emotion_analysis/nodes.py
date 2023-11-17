"""
This is a boilerplate pipeline 'sentiment_emotion_analysis'
generated using Kedro 0.18.14
"""
import logging

from pysentimiento.analyzer import AnalyzerForSequenceClassification
from typing import Dict, Any, Callable


logger = logging.getLogger("nlp-newpapersAnalysis")


def sentiment_emotion_analysis(
    emotion_analyzer: AnalyzerForSequenceClassification,
    sentiment_analyzer: AnalyzerForSequenceClassification,
    corpus: Dict[str, Callable[[], Any]],
) -> Dict[str, Callable[[], Any]]:
    """Returns a dict with Corpus Dataframes with the results of sentiment and emotion analysis.

    Args:
        emotion_analyzer (AnalyzerForSequenceClassification): Model from PySentimiento Emotion Analyzer
        sentiment_analyzer (AnalyzerForSequenceClassification): Model from Pysentimiento Sentiment Analyzer
        corpus (Dict[str, Callable[[], Any]]): Dictionary with Corpus Dataframes to be analyzed

    Returns:
        Dict[str, Callable[[], Any]]: Dicttionary of Dataframes with the Emotion and Sentiment Analyzer
    """
    sentiment_emotion = {}

    for filename, corpus_data_load in corpus.items():
        new_filename = filename.replace("corpus", "corpus_emotion")

        logger.info(
            f"[bold blue]Sentiment - Emotion Analysis ->[/bold blue] {new_filename} starts",
            extra={"markup": True},
        )

        corpus_df = corpus_data_load()

        corpus_df["sentiment"] = corpus_df["corpus"].apply(
            lambda x: sentiment_analyzer.predict(x)
        )
        corpus_df["emotion"] = corpus_df["corpus"].apply(
            lambda x: emotion_analyzer.predict(x)
        )

        corpus_df["sentiment_output"] = corpus_df["sentiment"].apply(lambda x: x.output)
        corpus_df["sentiment_prob_NEG"] = corpus_df["sentiment"].apply(
            lambda x: x.probas["NEG"]
        )
        corpus_df["sentiment_prob_NEU"] = corpus_df["sentiment"].apply(
            lambda x: x.probas["NEU"]
        )
        corpus_df["sentiment_prob_POS"] = corpus_df["sentiment"].apply(
            lambda x: x.probas["POS"]
        )

        corpus_df["emotion_output"] = corpus_df["emotion"].apply(lambda x: x.output)
        corpus_df["emotion_probas_others"] = corpus_df["emotion"].apply(
            lambda x: x.probas["others"]
        )
        corpus_df["emotion_probas_joy"] = corpus_df["emotion"].apply(
            lambda x: x.probas["joy"]
        )
        corpus_df["emotion_probas_surprise"] = corpus_df["emotion"].apply(
            lambda x: x.probas["surprise"]
        )
        corpus_df["emotion_probas_sadness"] = corpus_df["emotion"].apply(
            lambda x: x.probas["sadness"]
        )
        corpus_df["emotion_probas_fear"] = corpus_df["emotion"].apply(
            lambda x: x.probas["fear"]
        )
        corpus_df["emotion_probas_anger"] = corpus_df["emotion"].apply(
            lambda x: x.probas["anger"]
        )
        corpus_df["emotion_probas_disgust"] = corpus_df["emotion"].apply(
            lambda x: x.probas["disgust"]
        )

        corpus_df["year"] = corpus_df["created_at"].dt.isocalendar().year
        corpus_df["week"] = corpus_df["created_at"].dt.isocalendar().week
        corpus_df["year_week"] = (
            corpus_df["year"].astype("str") + "w" + corpus_df["week"].astype("str")
        )

        sentiment_emotion[new_filename] = corpus_df

        logger.info(
            f"[bold blue]Sentiment - Emotion Analysis ->[/bold blue] {new_filename} finish",
            extra={"markup": True},
        )

    return sentiment_emotion
