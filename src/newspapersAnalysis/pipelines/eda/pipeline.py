"""
This is a boilerplate pipeline 'eda'
generated using Kedro 0.18.14
"""

from kedro.pipeline import Pipeline, pipeline, node

from .nodes import create_stats_summary, create_top30, create_unique_words


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=create_stats_summary,
                inputs="clean_data",
                outputs="stats_summary",
                name="create_stats_summary_node",
            ),
            node(
                func=create_top30,
                inputs="dtm_newspaper",
                outputs="top30_df",
                name="create_top30_df_node",
            ),
            node(
                func=create_unique_words,
                inputs=["dtm_newspaper", "corpus"],
                outputs="unique_words",
                name="create_unique_words_node",
            ),
        ]
    )
