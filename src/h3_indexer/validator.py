"""Job config validation functions."""

import logging
from typing import List

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DecimalType, DoubleType, FloatType, IntegerType, LongType, ShortType

from h3_indexer.constants import GEOM_WKT_COL_NAME
from h3_indexer.data_model.job import Job, JobStatus
from h3_indexer.spark.spark import cache_df, read_s3_file_to_spark
from h3_indexer.utils.geospatial import fix_and_remove_invalid_geometries


def validate_unique_id(df: DataFrame, unique_id: list[str]) -> bool:
    """
    Validate that the specified unique ID in the DataFrame contains unique values.

    :param df: The input DataFrame
    :param unique_id: The column name of the unique identifier
    :return: True if the unique_id is a unique identifier, False otherwise
    :raises AssertionError: if the unique_id is non-unique
    """
    # Count the total number of rows in the DataFrame
    total_rows = df.count()

    # Count the number of distinct combinations of the id_columns
    distinct_count = df.select(unique_id).distinct().count()

    # If the counts match, the id_columns form a unique identifier
    is_unique = total_rows == distinct_count

    if is_unique:
        return is_unique
    else:
        raise AssertionError(f"The column {unique_id} is not a unique identifier.")


def validate_input_columns_are_numeric(df: DataFrame, input_columns: List) -> bool:
    """
    Validate that all specified columns in the DataFrame are numeric.

    :param df: The input PySpark DataFrame
    :param input_columns: List of column names to check
    :return: True if all specified columns are numeric, False otherwise
    :raises AssertionError: If the columns do not exist in the dataframe or are not numeric.
    """
    numeric_types = (IntegerType, LongType, FloatType, DoubleType, DecimalType, ShortType)

    for column in input_columns:
        if column.name not in df.columns:
            raise AssertionError(f"Column '{column.name}' does not exist in the input DataFrame.")

        column_type = df.schema[column.name].dataType
        if not isinstance(column_type, numeric_types):
            raise AssertionError(
                f"Column '{column.name}' is not numeric. Its type is {column_type}. H3 Indexer does not currently support this data type."
            )

    logging.info("All specified input columns are numeric.")
    return True


def validate_config(job: Job, spark: SparkSession) -> Job:
    """
    Validate the config.

    :param job: Job config.
    :param spark: Spark session.
    :return: Job config.
    """
    for input_name, input_config in job.inputs.items():
        if input_config.s3_path:
            input_df = read_s3_file_to_spark(spark, input_config.s3_path)
        elif input_config.glue_catalog_database_name and input_config.glue_catalog_table_name:
            if input_config.where_clause:
                where_clause = f"WHERE {input_config.where_clause}"
            else:
                where_clause = ""
            input_df = (
                spark.read.format("jdbc")
                .option("driver", "com.simba.athena.jdbc.Driver")
                .option(
                    "AwsCredentialsProviderClass",
                    "com.simba.athena.amazonaws.auth.DefaultAWSCredentialsProviderChain",
                )
                .option("url", "jdbc:awsathena://athena.us-east-1.amazonaws.com:443")
                .option("WorkGroup", "ReadOnlyWorkGroup")
                .option(
                    "query",
                    f'SELECT * FROM {input_config.glue_catalog_database_name}."{input_config.glue_catalog_table_name}" {where_clause}',
                )
                .load()
            )

        # ensure that the unique ID is in fact unique
        validate_unique_id(input_df, input_config.unique_id)

        # ensure that the input columns are numeric (categorical/string type not yet supported)
        validate_input_columns_are_numeric(input_df, input_config.input_columns)

        # initialize the input df to the input config data model
        input_config.initialize_input_df(input_df)

        # remove nulls and invalid geometries
        input_config.df = fix_and_remove_invalid_geometries(
            input_config.df, GEOM_WKT_COL_NAME, input_name
        )

        # cache df to make future ops run faster
        input_config.df = cache_df(input_config.df)

    job.update_status(JobStatus.VALIDATED)

    return job
