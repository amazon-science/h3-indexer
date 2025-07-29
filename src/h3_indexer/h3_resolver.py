"""H3 Resolver functions."""

from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, sum

from h3_indexer.constants import (
    DEFAULT_H3_RES,
    H3_AREA_COL_NAME,
    H3_INDEX_COL_NAME,
    H3_PARENT_COL_NAME,
    H3_RES_COL_NAME,
)
from h3_indexer.data_model.job import Job
from h3_indexer.data_model.vector import VectorTable
from h3_indexer.spark.spark import read_s3_file_to_spark
from h3_indexer.spark.spark_udfs import get_parent_h3_res3
from h3_indexer.utils.geospatial import add_h3_area_column


def group_and_sum(df: DataFrame, group_by_col: str) -> DataFrame:
    """
    Group by and sum the columns in a pyspark dataframe.

    :param df: PySpark DataFrame to perform group by operation on.
    :param group_by_col: Column to group on.
    :return: PySpark DataFrame following the group by sum operation.
    """
    # Get all column names
    all_columns = df.columns

    # Remove the group_by_col from the list of columns to sum
    columns_to_sum = [col_name for col_name in all_columns if col_name != group_by_col]

    # Create a list of sum expressions for each column to be summed
    sum_expressions = [sum(col(col_name)).alias(f"sum_{col_name}") for col_name in columns_to_sum]

    # Perform the groupBy and sum operations
    result_df = df.groupBy(group_by_col).agg(*sum_expressions)

    return result_df


def h3_resolver_spark(
    spark: SparkSession,
    job: Job,
) -> DataFrame:
    """
    Resolve attributes of multiple DataFrames to their corresponding ratios and join together.

    :param spark: Spark Session.
    :param job: Job config.
    :return: PySpark DataFrame with resolved data.
    """
    subset_columns = [H3_INDEX_COL_NAME, H3_RES_COL_NAME, H3_PARENT_COL_NAME, H3_AREA_COL_NAME]

    list_resolved_dfs = []
    for input_name, input_config in job.inputs.items():

        # add input columns to subset columns
        subset_columns += [f"sum_{col.name}" for col in input_config.input_columns]

        input_df = read_s3_file_to_spark(spark, f"s3a://{input_config.s3_path}")
        h3_indexed_df = input_config.h3_indexed_df
        h3_resolved_df = h3_resolver_single_input_spark(
            input_df, h3_indexed_df, input_config, job.h3_resolution
        )
        list_resolved_dfs.append(h3_resolved_df)

    def join_dfs(df1: DataFrame, df2: DataFrame) -> DataFrame:
        """
        Full outer join 2 data frames together.

        :param df1: First PySpark DataFrame.
        :param df2: Second PySpark DataFrame.
        :return: Joined PySpark DataFrame.
        """
        return df1.join(df2, on=H3_INDEX_COL_NAME, how="full_outer")

    # Reduce all DataFrames to a single joined DataFrame
    joined_df = reduce(join_dfs, list_resolved_dfs)

    # add H3 hexagon area to the dataframe
    h3_resolved_df = add_h3_area_column(joined_df, h3_index_col=H3_INDEX_COL_NAME)

    # add parent H3 column
    h3_resolved_df = h3_resolved_df.withColumn(
        H3_PARENT_COL_NAME, get_parent_h3_res3(col(H3_INDEX_COL_NAME))
    )

    # add H3 resolution column
    h3_resolved_df = h3_resolved_df.withColumn(H3_RES_COL_NAME, lit(job.h3_resolution))

    # filter out all unnecessary cols from the h3 resolved output
    h3_resolved_df = h3_resolved_df.select(*subset_columns)

    return h3_resolved_df


def h3_resolver_single_input_spark(
    input_df: DataFrame,
    h3_indexed_df: DataFrame,
    input: VectorTable,
    h3_resolution: int = DEFAULT_H3_RES,
    true_single_input: bool = False,
) -> DataFrame:
    """
    Run the H3 Resolver using PySpark.

    :param input_df: Input table pyspark dataframe.
    :param h3_indexed_df: H3 indexed pyspark dataframe.
    :param input: The input VectorTable BaseModel.
    :param h3_resolution: H3 resolution. Defaults to DEFAULT_H3_RES.
    :param true_single_input: Indicates whether or not the resolver is running this function within a loop for multiple inputs, or truly as a single input. False by default.
    :return: PySpark DataFrame with resolved data.
    """
    # Iterate through input columns and add joins
    for column in input.input_columns:
        input_col_name = column.name
        h3_indexed_df = h3_indexed_df.join(
            input_df.select(input.unique_id, f"{input_col_name}"), on=input.unique_id, how="left"
        )

    for column in input.input_columns:
        input_col_name = column.name
        h3_indexed_df = h3_indexed_df.withColumn(input_col_name, col(input_col_name) * col("ratio"))

    # filter out all unnecessary cols from the indexer before running the group and sum
    subset_columns = [H3_INDEX_COL_NAME, H3_RES_COL_NAME, H3_PARENT_COL_NAME] + [
        col.name for col in input.input_columns
    ]
    h3_indexed_df = h3_indexed_df.select(*subset_columns)

    h3_resolved_df = group_and_sum(h3_indexed_df, H3_INDEX_COL_NAME)

    # basically, if the single input is run in isolation, there are columns we need to add and then filter out
    # otherwise, the filter will happen later after the join
    if true_single_input:
        # add H3 hexagon area to the dataframe
        h3_resolved_df = add_h3_area_column(h3_resolved_df, h3_index_col=H3_INDEX_COL_NAME)
        # filter out all unnecessary cols from the h3 resolved output
        subset_columns = [
            H3_INDEX_COL_NAME,
            H3_RES_COL_NAME,
            H3_PARENT_COL_NAME,
            H3_AREA_COL_NAME,
        ] + [f"sum_{col.name}" for col in input.input_columns]
        # add parent H3 column
        h3_resolved_df = h3_resolved_df.withColumn(
            H3_PARENT_COL_NAME, get_parent_h3_res3(col(H3_INDEX_COL_NAME))
        )

        # add H3 resolution column
        h3_resolved_df = h3_resolved_df.withColumn(H3_RES_COL_NAME, lit(h3_resolution))

        # filter out all unnecessary cols from the h3 resolved output
        h3_resolved_df = h3_resolved_df.select(*subset_columns)

    return h3_resolved_df
