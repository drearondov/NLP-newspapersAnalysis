"""
This is a boilerplate pipeline 'sentiment_emotion_analysis'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import sentiment_emotion_analysis


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=sentiment_emotion_analysis,
                inputs=["emotion_analyzer", "sentiment_analyzer", "corpus"],
                outputs="corpus_sentiment-emotion",
                name="sentiment_emotion_analysis_node",
            )
        ]
    )
