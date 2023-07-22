"""
This is a boilerplate pipeline 'clustering'
generated using Kedro 0.18.11
"""

from kedro.pipeline import Pipeline, node, pipeline
from.nodes import *


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(func=features_selection, inputs="chicago_crimes", outputs="features_procesing_output", name="features_processing_node"),
        node(func=feature_transformation, inputs="features_procesing_output", outputs="feature_transformation_output",name="feature_transformation_node"),
        node(func=convert_to_vector, inputs="feature_transformation_output", outputs="convert_to_vector_output",name="convert_to_vector_node"),
        #node(func=optimize_k, inputs="convert_to_vector_output", outputs="optimize_k_output",name="optimize_k_node"),
        node(func= clustering_output, inputs="feature_transformation_output", outputs="clustering_model_output", name="model_output_node"),
        node(func=plotcluster, inputs="clustering_model_output", outputs="plotcluster_output",name="plotcluster_node")

    ])
