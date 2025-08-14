"""Geospatial utility functions."""

import logging

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, expr
from pyspark.sql.types import BinaryType, StringType
from sedona.sql import ST_AsGeoJSON, ST_AsText, ST_GeomFromGeoJSON, ST_GeomFromWKB, ST_GeomFromWKT
from sedona.sql.types import GeometryType

from h3_indexer.constants import GEOJSON_COL_NAME, GEOM_WKT_COL_NAME, H3_AREA_COL_NAME
from h3_indexer.data_model.shared import GeomColDataTypeEnum
from h3_indexer.spark.spark_udfs import h3_to_wkt

logging.basicConfig(level=logging.INFO)


def get_geom_col_data_type(df: DataFrame, geometry_col: str) -> GeomColDataTypeEnum:
    """
    Determine the data type of a geometry column.

    :param df: PySpark DataFrame.
    :param geometry_col: Name of geometry column.
    :return: GeomColDataTypeEnum
    :raises ValueError: If first geometry in dataframe returns Null.
    """
    data_type = df.schema[geometry_col].dataType

    # if geometry is string dtype
    if isinstance(data_type, StringType):
        first_geom = df.select(geometry_col).first()

        if not first_geom:
            raise ValueError

        if first_geom[0][0].isnumeric():
            return GeomColDataTypeEnum.STRING_WKB
        if first_geom[0][0] == "{":
            return GeomColDataTypeEnum.GEOJSON
        else:
            return GeomColDataTypeEnum.WKT

    # if geometry is binary dtype
    elif isinstance(data_type, BinaryType):
        return GeomColDataTypeEnum.BINARY_WKB

    # if geometry is geometry type
    elif isinstance(data_type, GeometryType):
        return GeomColDataTypeEnum.GEOMETRY

    else:
        raise ValueError(f"Unrecognized geometry data type: {data_type}")


def add_geojson_geometry(
    input_df: DataFrame, geometry_col: str, geom_col_data_type: GeomColDataTypeEnum
) -> DataFrame:
    """
    Add a GeoJSON geometry column to a PySpark DataFrame.

    :param input_df: PySpark DataFrame.
    :param geometry_col: Name of the geometry column.
    :param geom_col_data_type: Geometry column data type.
    :return: PySpark DataFrame with GeoJSON geometry column.
    """
    # if geometry is already in GeoJSON format, no need for a new column
    if geom_col_data_type == GeomColDataTypeEnum.GEOJSON:
        return input_df.withColumnRenamed(geometry_col, GEOJSON_COL_NAME)

    elif geom_col_data_type == GeomColDataTypeEnum.WKT:
        df_with_geojson = input_df.withColumn(
            GEOJSON_COL_NAME, ST_AsGeoJSON(ST_GeomFromWKT(col(geometry_col)))
        )

    elif geom_col_data_type in (GeomColDataTypeEnum.STRING_WKB, GeomColDataTypeEnum.BINARY_WKB):
        df_with_geojson = input_df.withColumn(
            GEOJSON_COL_NAME, ST_AsGeoJSON(ST_GeomFromWKB(col(geometry_col)))
        )

    elif geom_col_data_type == GeomColDataTypeEnum.GEOMETRY:
        df_with_geojson = input_df.withColumn(GEOJSON_COL_NAME, ST_AsGeoJSON(col(geometry_col)))

    return df_with_geojson


def add_wkt_geometry(
    input_df: DataFrame, geometry_col: str, geom_col_data_type: GeomColDataTypeEnum
) -> DataFrame:
    """
    Add a WKT geometry column to a PySpark DataFrame.

    :param input_df: PySpark DataFrame.
    :param geometry_col: Name of the geometry column.
    :param geom_col_data_type: Geometry column data type.
    :return: PySpark DataFrame with WKT geometry column.
    """
    # if geometry is already in WKT format, no need for a new column
    if geom_col_data_type == GeomColDataTypeEnum.WKT:
        return input_df.withColumnRenamed(geometry_col, GEOM_WKT_COL_NAME)

    elif geom_col_data_type == GeomColDataTypeEnum.GEOJSON:
        df_with_wkt = input_df.withColumn(
            GEOM_WKT_COL_NAME, ST_AsText(ST_GeomFromGeoJSON(GEOJSON_COL_NAME))
        )

    elif geom_col_data_type in (GeomColDataTypeEnum.STRING_WKB, GeomColDataTypeEnum.BINARY_WKB):
        df_with_wkt = input_df.withColumn(
            GEOM_WKT_COL_NAME, ST_AsText(ST_GeomFromWKB(col(geometry_col)))
        )

    elif geom_col_data_type == GeomColDataTypeEnum.GEOMETRY:
        df_with_wkt = input_df.withColumn(GEOM_WKT_COL_NAME, ST_AsText(col(geometry_col)))

    return df_with_wkt


def add_h3_area_column(
    df: DataFrame, h3_geom_col: str | None = None, h3_index_col: str | None = None
) -> DataFrame:
    """
    Add H3 hexagon area as a column to the PySpark DataFrame.

    :param df: PySpark DataFrame.
    :param h3_geom_col: Column name containing the H3 geometry.
    :param h3_index_col: Column name containing the H3 index.
    :return: PySpark DataFrame with area column added.
    """
    if h3_geom_col:
        # add H3 area as a column (units: sq km)
        df = df.withColumn(H3_AREA_COL_NAME, expr(f"ST_AreaSpheroid({h3_geom_col})/1000000"))
    elif h3_index_col:
        # add H3 area as a column (units: sq km)
        df = df.withColumn("h3_wkt_geom", h3_to_wkt(col(h3_index_col))).withColumn(
            H3_AREA_COL_NAME, expr("ST_AreaSpheroid(ST_GeomFromWKT(h3_wkt_geom))/1000000")
        )

    return df


def fix_and_remove_invalid_geometries(df: DataFrame, wkt_geom_col: str, name: str) -> DataFrame:
    """
    Fix invalid geometries, then remove rows with null or invalid geometries from the DataFrame.

    :param df: PySpark DataFrame containing a 'geom_wkt' column.
    :param wkt_geom_col: Name of WKT geometry column.
    :param name: Name of the input for logging purposes.
    :return: DataFrame with only valid geometries.
    """
    # First remove nulls
    df_no_nulls = df.filter(col(wkt_geom_col).isNotNull())

    # fix invalid geometries
    df_valid = df_no_nulls.withColumn(
        wkt_geom_col, expr(f"ST_AsText(ST_MakeValid(ST_GeomFromWKT({wkt_geom_col})))")
    ).filter(expr(f"ST_IsValid(ST_GeomFromWKT({wkt_geom_col}))"))

    # Log the number of invalid geometries
    total_count = df.count()
    valid_count = df_valid.count()
    invalid_count = total_count - valid_count

    logging.info(f"Total rows in input {name}: {total_count}")
    logging.info(f"Valid geometries in input {name}: {valid_count}")
    logging.info(f"Removed invalid geometries in input {name}: {invalid_count}")

    return df_valid
