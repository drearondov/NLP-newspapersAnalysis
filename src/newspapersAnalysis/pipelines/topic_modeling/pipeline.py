"""
This is a boilerplate pipeline 'topic_modeling'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import topic_modeling


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=topic_modeling,
                inputs=["data_dtm", "dtm"],
                outputs="corpus_topic",
                name="topic_modeling_node",
            )
        ]
    )
