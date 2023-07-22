"""
This is a boilerplate pipeline 'clustering'
generated using Kedro 0.18.11
"""

from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import numpy as np
import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import geopandas as gpd
from pyspark.sql.functions import col, countDistinct, expr, to_timestamp, month, to_date, date_format, year




def features_selection(crimes_data_set):
    column_names = crimes_data_set.columns
    new_column_names = [col_name.replace(" ", "_") for col_name in column_names]
    crimes_data_set = crimes_data_set.select([col(old).alias(new) for old, new in zip(column_names, new_column_names)])
    #crimes_data_set.filter(col("Year").between(2015,2017))
    crimes_data_set = crimes_data_set.dropna(subset=["Case_Number","Community_Area","Primary_Type"])
    crimes_data_set=crimes_data_set.groupby("Community_Area","Primary_Type").agg(countDistinct("Case_Number").alias("Crimes_Qty"))
    crimes_data_set_transposed = crimes_data_set.groupBy("Community_Area").pivot("Primary_Type").sum("Crimes_Qty")
    crimes_data_set_transposed=crimes_data_set_transposed.fillna(0)

    return crimes_data_set_transposed

def feature_transformation(crimes_data_set):
    feature_columns=crimes_data_set.columns[1:]
    vec_assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
    crimes_data_set = vec_assembler.transform(crimes_data_set)
    standard_scaler = StandardScaler(inputCol="features", outputCol="scaled_features",withMean=True, withStd=True)
    df_standard_scaled = standard_scaler.fit(crimes_data_set).transform(crimes_data_set)

    return df_standard_scaled


def convert_to_vector(df):
    print(df.limit(4).toPandas())
    return df

def optimize_k(df_kmeans):
    kvalues = range(2, 10)
    cost_values=[]
    for k in kvalues:
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans)
        cost = model.summary.trainingCost  # Manually calculate the cost (inertia)
        cost_values.append(cost)

    plt.plot(kvalues, cost_values, marker='o')
    plt.xlabel("Number of Clusters (k)")
    plt.ylabel("Cost (Inertia)")
    plt.title("Elbow Method to Find Optimal k in K-means")

    return cost_values


def clustering_output (df_kmeans):
        k = 3
        kmeans = KMeans().setK(k).setSeed(1).setFeaturesCol("features")
        model = kmeans.fit(df_kmeans)
        predictions = model.transform(df_kmeans)
        evaluator = ClusteringEvaluator()

        silhouette = evaluator.evaluate(predictions)
        print("Silhouette with squared euclidean distance = " + str(silhouette))
        print(" ")
        centers = model.clusterCenters()
        clustered_df = model.transform(df_kmeans)
        clustered_df=clustered_df.toPandas()


        return clustered_df



def plotcluster(crimes):

    gdf = gpd.read_file("/home/david/Documents/Pyspark_Projects/big-data-project/data/01_raw/Boundaries - Community Areas (current).geojson")

    gdf['area_numbe'] = gdf['area_numbe'].astype(str)
    crimes['Community_Area'] = crimes['Community_Area'].astype(str)

    gdf = gdf.merge(crimes, left_on='area_numbe',right_on="Community_Area", how='left')

    gdf.plot(column='prediction', cmap='coolwarm', legend=True)
    plt.savefig("data/08_reporting/areas_map_with_clusters.png")

    return gdf