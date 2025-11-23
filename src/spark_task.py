"""Spark Task"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, BooleanType
from pyspark.sql.functions import col, expr, datediff, lag, when
from pyspark.sql.window import Window

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_PATH = os.path.join(CURRENT_DIR, '..', 'input', 'enroll.csv')
OUTPUT_PATH = os.path.join(CURRENT_DIR, '..', 'output', 'result.csv')
END_DATE = "2016-09-30"
DAYS_OF_YEAR = 365
TARGET_DATE_FORMAT = "MMddyyyy"


def create_spark_session(app_name="Spark Assignment"):
    """
    Create a SparkSession with the specified app name.
    """
    spark_session = (SparkSession.
               builder.
               appName(app_name).getOrCreate())
    return spark_session


def convert_date_column(input_df, column_name):
    """
    Convert a column in DataFrame to date type.
    """
    converted_dates = input_df.withColumn(column_name, expr(f"to_date({column_name}, '{TARGET_DATE_FORMAT}')"))
    return converted_dates


def filter_records_by_date(df_converted, column_name, end_date, days):
    """
    Filter out records older than a certain number of days from the end date.
    """
    df_filter = df_converted.filter((col(column_name) >= expr(f"date_sub(to_date('{end_date}'), {days})"))
                                    & (col(column_name) <= expr(f"to_date('{end_date}')")))
    return df_filter


def calculate_days_since_last_visit(df_filter, partition_column, order_column):
    """
    Calculate the difference in days between consecutive visits for each partition.
    """
    window_spc = Window.partitionBy(partition_column).orderBy(order_column)
    df_dif_visits = df_filter.withColumn("days_since_last_visit",
                                         datediff(col(order_column), lag(col(order_column), 1).over(window_spc)))
    return df_dif_visits


def calculate_consecutive_visits(df_dif, column_name):
    """
    Calculate the number of consecutive visits for each partition.
    """
    df_count_visits = df_dif.withColumn("consecutive_visits", when(col(column_name) == 1, 0)
                               .otherwise(1)).withColumn("consecutive_visits", expr("sum(consecutive_visits) \
                                                        over (partition by patient_id \
                                                        order by effective_from_date)"))
    return df_count_visits


def calculate_consecutive_visit_conditions(df_count_visits, group_by_column, expression_dict):
    """
    Calculate consecutive visit conditions based on specified expressions.
    """
    df_visit_conditions = (df_count_visits.groupBy(group_by_column)
                           .agg(*[expr(expression)
                                .alias(alias)for expression, alias in expression_dict.items()]))
    return df_visit_conditions


def create_new_dataframe_with_schema(df_to_change_schema, desired_schema, spark):
    return spark.createDataFrame(df_to_change_schema.rdd, desired_schema)



if __name__ == "__main__":

    spark = create_spark_session()

    schema = StructType([
        StructField("effective_from_date", StringType(), True),
        StructField("patient_id", StringType(), True)
    ])

    window_spec = Window.partitionBy("patient_id").orderBy("effective_from_date")

    conditions = {
        "max(consecutive_visits >= 5)": "5months",
        "max(consecutive_visits >= 9)": "9months",
        "max(consecutive_visits >= 11)": "11months"
    }

    new_schema = StructType([
        StructField("patient_id", StringType(), True),
        StructField("5months", BooleanType(), False),
        StructField("9months", BooleanType(), False),
        StructField("11months", BooleanType(), False)
    ])

    df = spark.read.csv(FILE_PATH, schema=schema, header=True)

    df = convert_date_column(df, "effective_from_date")

    df_filtered = filter_records_by_date(df, "effective_from_date", END_DATE, DAYS_OF_YEAR)

    df_diff = calculate_days_since_last_visit(df_filtered, "patient_id", "effective_from_date")

    df_count = calculate_consecutive_visits(df_diff, "days_since_last_visit")

    df_result = calculate_consecutive_visit_conditions(df_count, "patient_id", conditions)

    df_result.show()

    df_new = create_new_dataframe_with_schema(df_result, new_schema, spark)

    df_new.printSchema()

    df_new.write.csv(OUTPUT_PATH, header=True, mode="overwrite")
