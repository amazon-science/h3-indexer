"""Pyspark utilities."""

import logging
import os
from typing import Optional

import boto3
from awsglue.context import GlueContext
from pyspark import SparkContext
from pyspark.sql import DataFrame, SparkSession
from sedona.register import SedonaRegistrator
from sedona.utils import KryoSerializer, SedonaKryoRegistrator

PARTITION_MAPPING = {
    3: "100",
    4: "200",
    5: "400",
    6: "800",
    7: "1200",
    8: "2000",
    9: "4000",
    10: "8000",
}


def read_s3_file_to_spark(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Read S3 file to PySpark DataFrame.

    :param spark: Spark Session.
    :param s3_path: Path to S3 file. If no file extension, assumes parquet.
    :return: PySpark DataFrame.
    """
    file_ext = s3_path.split(".")[-1]

    if file_ext == "shp":
        df = read_s3_shapefile_to_spark(spark, s3_path)
        return df
    # otherwise, assume parquet
    else:
        df = read_s3_parquet_to_spark(spark, s3_path)
        return df


def read_s3_parquet_to_spark(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Read a parquet file in S3 into PySpark DataFrame.

    :param spark: Spark Session.
    :param s3_path: S3 path.
    :return: PySpark DataFrame.
    """
    spark_df = spark.read.parquet(s3_path)

    return spark_df


def read_s3_shapefile_to_spark(spark: SparkSession, s3_path: str) -> DataFrame:
    """
    Read a shapefile in S3 into PySpark DataFrame.

    :param spark: Spark Session.
    :param s3_path: S3 path.
    :return: PySpark DataFrame.
    """
    spark_df = (
        spark.read.format("shapefile")
        .option("delimiter", "|")
        .option("header", "true")
        .load(s3_path)
    )

    return spark_df


def get_spark_session(h3_resolution: int) -> tuple[SparkSession, SparkContext]:
    """
    Create a SparkSession.

    :param h3_resolution: H3 resolution of the job config.
    :return: tuple(SparkSession, SparkContext).
    """
    spark = (
        SparkSession.builder.appName("Python Spark Apache Sedona")
        .master("local[*]")
        .config("spark.serializer", KryoSerializer.getName)
        .config("spark.kryo.registrator", SedonaKryoRegistrator.getName)
        .config(
            "spark.jars.packages",
            "org.apache.sedona:sedona-spark-shaded-3.3_2.12:1.7.1,"
            "org.datasyslab:geotools-wrapper:1.7.1-28.5,"
            "org.apache.hadoop:hadoop-aws:3.4.1",
        )
        .config("spark.jars", os.environ.get("GLUE_JARS", ""))
        .config(
            "spark.jars.repositories",
            "https://artifacts.unidata.ucar.edu/repository/unidata-all",
        )
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )
        # this fixes a known problem in apache sedona re: non-noded intersections
        # https://github.com/apache/sedona/issues/1612
        .config("spark.driver.extraJavaOptions", "-Djts.overlay=ng")
        .config("spark.executor.extraJavaOptions", "-Djts.overlay=ng")
        # these are local configurations that should be removed and reconfigured in glue job later
        # if you run into an EOFError or "py4j.protocol.Py4JError: py4j.reflection.TypeUtil does not exist in the JVM"
        # error, then you may need to increase
        .config("spark.driver.memory", "32g")
        .config("spark.executor.memory", "32g")
        .config("spark.driver.maxResultSize", "20g")
        .config("spark.executor.cores", "6")
        .config("spark.sql.shuffle.partitions", PARTITION_MAPPING[h3_resolution])
        .config("spark.executor.extraClassPath", os.environ["GLUE_JARS"])
        .config("spark.driver.extraClassPath", os.environ["GLUE_JARS"])
        .config("spark.sql.files.maxRecordsPerFile", 500000)
        # enable adaptive query execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # optimize for large datasets
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
        .config("spark.sql.files.maxPartitionBytes", "128MB")
        # optimize for geospatial operations
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .getOrCreate()
    )

    spark_context = spark.sparkContext

    SedonaRegistrator.registerAll(spark)

    return spark, spark_context


def get_glue_context(spark_context: SparkContext) -> GlueContext:
    """Establish a Glue Context from the Spark Context.

    :param spark_context: SparkContext.
    :return: GlueContext.
    """
    glue_context = GlueContext(spark_context)
    glue = boto3.client("glue")
    databases = glue.get_databases()
    database_names = [db["Name"] for db in databases["DatabaseList"]]
    logging.info(f"Found the following databases: {database_names}")

    return glue_context


def write_df_to_s3(
    df: DataFrame,
    s3_path: str,
    format: str = "parquet",
    mode: str = "overwrite",
    compression: str = "snappy",
    partition_by: Optional[str] = None,
) -> None:
    """Write PySpark DataFrame to S3.

    :param df: PySpark DataFrame.
    :param s3_path: S3 path (e.g., 's3a://bucket/path/').
    :param format: Output format ('parquet', 'csv', 'json', etc.).
    :param mode: Write mode ('overwrite', 'append', 'ignore', 'error').
    :param compression: Compression codec ('snappy', 'gzip', 'none').
    :param partition_by: Column(s) to partition by.
    :raises Exception: When error is raised writing to S3.
    """
    # starting with basic write configuration
    writer = df.write.format(format).mode(mode).option("compression", compression)

    # add S3-specific configuration
    writer = writer.option("fs.s3a.fast.upload", "true")

    # handle partitioning
    partition_columns = []
    if partition_by:
        if isinstance(partition_by, str):
            partition_columns.append(partition_by)
        elif isinstance(partition_by, (list, tuple)):
            partition_columns.extend(partition_by)

    if partition_columns:
        writer = writer.partitionBy(*partition_columns)

    # remove trailing slash if present
    s3_path = s3_path.rstrip("/")

    try:
        writer.save(s3_path)
    except Exception as e:
        logging.error(f"Error writing to S3: {str(e)}")
        raise


def cache_df(df: DataFrame) -> DataFrame:
    """Cache the pyspark dataframe.

    :param df: PySpark DataFrame
    :return: Cached PySpark DataFrame
    """
    df = df.cache()
    df.count()

    return df
