"""
This is a boilerplate pipeline 'dataprocessing'
generated using Kedro 0.18.11
"""
from pyspark.sql.functions import col,countDistinct,expr
import matplotlib.pyplot as plt


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

    # Show the chart
    plt.savefig("data/08_reporting/crime_timeline.png")


    return filtered_crimes_grouped



def location_crimes(crimes_df):

    crimes_df=crimes_df.filter(crimes_df.Location_Description.isNotNull())

    location_description_crimes=crimes_df.groupby("Location_Description").agg(countDistinct("Case_Number"))
    location_description_crimes=location_description_crimes.toPandas()

    return location_description_crimes


def arrest_overtime(df):
    df=df.groupby("Primary_Type","Year","Arrest").agg(countDistinct("Case_Number")).orderBy(["Primary_Type","Year"], ascending=True)
    df = df.toPandas()

    return df

def criminal_locations(df):
    df = df.groupby("Primary_Type", "Year","Location_Description" ,"Latitude","Longitude").agg(countDistinct("Case_Number"))

    return df


