"""
This is a boilerplate pipeline 'feature_engineering'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import make_corpus, make_data_dtm, make_dtm, make_dtm_newspaper


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=make_corpus,
                inputs="clean_data",
                outputs="corpus",
                name="make_corpus_node",
            ),
            node(
                func=make_data_dtm,
                inputs="corpus",
                outputs="data_dtm",
                name="make_data_dtm_node",
            ),
            node(func=make_dtm, inputs="data_dtm", outputs="dtm", name="make_dtm_node"),
            node(
                func=make_dtm_newspaper,
                inputs=["corpus", "dtm"],
                outputs="dtm_newspaper",
            ),
        ]
    )
