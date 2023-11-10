"""
This is a boilerplate pipeline 'cleaning_and_preprocessing'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import compile_raw_data, clean_data


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=compile_raw_data,
                inputs="newspapers_raw_tweets",
                outputs="raw_data",
                name="compile_raw_data_node",
            ),
            node(
                func=clean_data,
                inputs="raw_data",
                outputs="clean_data",
                name="clean_data_node",
            ),
        ]
    )
