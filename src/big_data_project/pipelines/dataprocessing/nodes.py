"""
This is a boilerplate pipeline 'dataprocessing'
generated using Kedro 0.18.11
"""
from matplotlib import pyplot
from matplotlib.dates import DateFormatter
from pyspark.sql.functions import col, countDistinct, expr, to_timestamp, month, to_date, date_format, year
import matplotlib.pyplot as plt

import folium
from shapely.geometry import Polygon
from selenium import webdriver
import geopandas as gpd
import pandas as pd


def top_crimes(df):
    crime_type_count=df.groupby("Primary Type").count()
    sorted_counts=crime_type_count.sort(col("count").desc())
    top_10_values = sorted_counts.limit(10)
    return top_10_values


def replace_columns_names(df):
    column_names = df.columns
    new_column_names = [col_name.replace(" ", "_") for col_name in column_names]
    df = df.select([col(old).alias(new) for old, new in zip(column_names, new_column_names)])
    return df


def filter_top_crimes_inner_join(crimes_df, top_10_crimes):
    filtered_crimes= crimes_df.join(top_10_crimes,["Primary_Type"],"inner")
    return filtered_crimes

def group_crimes_by_year(filtered_crimes_new):
    filtered_crimes_grouped=filtered_crimes_new.groupby("Primary_Type","Year").count().orderBy(["Primary_Type","Year"], ascending=False)
    filtered_crimes_grouped=filtered_crimes_grouped.toPandas()

    primary_types = filtered_crimes_grouped["Primary_Type"].unique()

    # Create a for loop to iterate over the primary types
    for primary_type in primary_types:
        # Create a subset of the dataframe for the current primary type
        df_subset = filtered_crimes_grouped[filtered_crimes_grouped["Primary_Type"] == primary_type]

        # Create a line plot for the current primary type
        plt.plot(df_subset["Year"], df_subset["count"], label=primary_type)
        plt.legend(loc="upper right", bbox_to_anchor=(1.05, 1))

    plt.show()



    return filtered_crimes_grouped

def group_crimes_by_month(df):

    df = df.withColumn("timestamp_column", to_date(col("Date"), "dd/MM/yyyy hh:mm:ss a"))
    df = df.withColumn("MonthYear", date_format(col("timestamp_column"), "MM/yyyy"))
    df=df.groupby("MonthYear").agg(countDistinct("Case_Number"))
    df=df.toPandas()
    df["MonthYear"] = pd.to_datetime(df["MonthYear"] , format='%m/%Y')
    df.dropna(inplace=True)
    df=df.sort_values(by='MonthYear')

    plt.plot(df["MonthYear"], df["count(Case_Number)"], marker='o', linestyle='-')

    date_formatting = DateFormatter("%b %Y")
    plt.gca().xaxis.set_major_formatter(date_formatting)
    plt.xticks(rotation=45, ha='right')

    # Add labels and title
    plt.xlabel('Date')
    plt.ylabel('Value')
    plt.title('Date with Format "Month Year" on X-axis')
    plt.show()

    return df



def location_crimes(crimes_df):

    crimes_df=crimes_df.filter(crimes_df.Location_Description.isNotNull())

    location_description_crimes=crimes_df.groupby("Location_Description").agg(countDistinct("Case_Number"))
    location_description_crimes=location_description_crimes.toPandas()

    return location_description_crimes


def arrest_overtime(df):
    df=df.groupby("Primary_Type","Year","Arrest").agg(countDistinct("Case_Number")).orderBy(["Primary_Type","Year"], ascending=True)
    df = df.toPandas()

    return df


def chicago_crime_cluster():



    return None



def criminal_locations(crimes,Comm_area):
    Comm_area=Comm_area.select("Community_Area","COMMUNITY","the_geom")

    crimes = crimes.filter(crimes.Community_Area.isNotNull())

    crimes = crimes.groupby("Primary_Type", "Year","Location_Description","Community_Area","Location").agg(countDistinct("Case_Number"))
    crimes=crimes.join(Comm_area,["Community_Area"],"left")


    return crimes



def draw_map(crimes):

    crimes=crimes.groupby("Community_Area").agg(countDistinct("Case_Number"))
    crimes=crimes.toPandas()
    crimes=crimes.dropna()
    crimes['Community_Area']=crimes['Community_Area'].astype(int)
    crimes['Danger_Level'] = pd.qcut(crimes["count(Case_Number)"], q=4, labels=['Low danger', 'Low mid danger', 'High mid danger', 'High danger'])
    print(crimes)

    crimes=crimes.rename(columns={"Community_Area":"area_numbe"})


    gdf = gpd.read_file("/home/david/Documents/Pyspark_Projects/big-data-project/data/01_raw/Boundaries - Community Areas (current).geojson")

    gdf['area_numbe'] = gdf['area_numbe'].astype(str)
    crimes['area_numbe'] = crimes['area_numbe'].astype(str)

    gdf=gdf.merge(crimes, on='area_numbe', how='left')

    gdf.plot(column='Danger_Level',cmap='coolwarm',legend=True)
    plt.show()

    return gdf




