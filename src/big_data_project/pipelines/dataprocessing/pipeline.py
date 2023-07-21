"""
This is a boilerplate pipeline 'dataprocessing'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func=top_crimes,
            inputs="chicago_crimes",
            outputs="crimes_count_output",
            name="crimes_count_node",
        )
        ,
        node(
            func=replace_columns_names,
            inputs=["chicago_crimes"],
            outputs="chicago_crimes_renamed_output",
            name="chicago_crimes_renamed_node",
        ),
        node(
            func=replace_columns_names,
            inputs=["crimes_count_output"],
            outputs="crimes_count_renamed_output",
            name="crimes_count_renamed_node",
        ),
        node(
            func=filter_top_crimes_inner_join,
            inputs=["chicago_crimes_renamed_output","crimes_count_renamed_output"],
            outputs="filter_top_crimes_output",
            name="filter_top_crimes_node",
        ),
        node(
            func= group_crimes_by_year,
            inputs="filter_top_crimes_output",
            outputs="group_crimes_by_year_output",
            name="group_crimes_by_year_node"
        ),
        node(
            func=location_crimes,
            inputs="chicago_crimes_renamed_output",
            outputs="location_crimes_output",
            name="location_crimes_node"
        ),
        node(
            func=arrest_overtime,
            inputs="chicago_crimes_renamed_output",
            outputs="arrest_overtime_output",
            name="arrest_overtime_node"
        ),
        node(
            func=criminal_locations,
            inputs="chicago_crimes_renamed_output",
            outputs="criminal_locations_output",
            name="criminal_locations_node"
        ),


    ])
