"""Project pipelines."""
from typing import Dict

from kedro.pipeline import Pipeline

from newspapersAnalysis.pipelines import (
    cleaning_and_preprocessing,
    eda,
    feature_engineering,
    sentiment_emotion_analysis,
    topic_modeling,
)


def register_pipelines() -> Dict[str, Pipeline]:
    """Register the project's pipelines.
    Returns:
        (Dict[str, Pipeline]: A mapping from a pipeline name to a ``Pipeline`` object.
    """
    cleaning_and_preprocessing_pipeline = cleaning_and_preprocessing.create_pipeline()
    eda_pipeline = eda.create_pipeline()
    feature_engineering_pipeline = feature_engineering.create_pipeline()
    sentiment_emotion_analysis_pipeline = sentiment_emotion_analysis.create_pipeline()
    topic_modeling_pipeline = topic_modeling.create_pipeline()

    return {
        "cleaning_preprocessing": cleaning_and_preprocessing_pipeline,
        "eda": eda_pipeline,
        "feature_engineering": feature_engineering_pipeline,
        "sentiment_emotion": sentiment_emotion_analysis_pipeline,
        "topic_modeling": topic_modeling_pipeline,
        "__default__": cleaning_and_preprocessing_pipeline
        + eda_pipeline
        + feature_engineering_pipeline
        + sentiment_emotion_analysis_pipeline
        + topic_modeling_pipeline,
    }
