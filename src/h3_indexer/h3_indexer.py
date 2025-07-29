"""H3 Indexer functions."""

from h3_pyspark.indexing import index_shape
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, explode, lit
from sedona.sql import (
    ST_Area,
    ST_AreaSpheroid,
    ST_GeomFromWKT,
    ST_Intersection,
    ST_Length,
    ST_LengthSpheroid,
)

from h3_indexer.constants import (
    DEFAULT_H3_RES,
    GEOJSON_COL_NAME,
    GEOM_WKT_COL_NAME,
    H3_AREA_COL_NAME,
    H3_GEOM_COL_NAME,
    H3_INDEX_COL_NAME,
    H3_PARENT_COL_NAME,
    H3_RES_COL_NAME,
    RATIO_COL_NAME,
)
from h3_indexer.data_model.vector import VectorTable
from h3_indexer.spark.spark_udfs import get_parent_h3_res3, h3_to_wkt
from h3_indexer.utils.geospatial import add_h3_area_column


def h3_indexer_spark(
    input: VectorTable,
    h3_resolution: int = DEFAULT_H3_RES,
) -> DataFrame:
    """
    Run the H3 Indexer using PySpark.

    :param input: The input VectorTable BaseModel.
    :param h3_resolution: H3 resolution. Defaults to DEFAULT_H3_RES.
    :return: DataFrame with H3 indexed data.
    :raises ValueError: If the geometry type doesn't have an associated H3 Indexer function.
    """
    # these are the columns that will remain in the final indexer output
    subset_columns = [
        H3_INDEX_COL_NAME,
        H3_RES_COL_NAME,
        H3_PARENT_COL_NAME,
        H3_AREA_COL_NAME,
        input.unique_id,
        RATIO_COL_NAME,
    ]

    if input.geometry_type == "POINT":
        h3_indexed_df, subset_columns = point_router(input, subset_columns, h3_resolution)
    elif input.geometry_type == "LINE":
        h3_indexed_df, subset_columns = line_router(input, subset_columns, h3_resolution)
    elif input.geometry_type == "POLYGON":
        h3_indexed_df, subset_columns = polygon_router(input, subset_columns, h3_resolution)
    else:
        raise ValueError(
            f"geometry type: {input.geometry_type} doesn't have an associated H3 Indexer function."
        )

    # add H3 area as a column (units: sq km)
    h3_indexed_df = add_h3_area_column(h3_indexed_df, h3_geom_col=H3_GEOM_COL_NAME)

    # add parent H3 column
    h3_indexed_df = h3_indexed_df.withColumn(
        H3_PARENT_COL_NAME, get_parent_h3_res3(col(H3_INDEX_COL_NAME))
    )

    # add H3 resolution column
    h3_indexed_df = h3_indexed_df.withColumn(H3_RES_COL_NAME, lit(h3_resolution))

    # only keep the subset columns
    h3_indexed_df = h3_indexed_df.select(*subset_columns)

    # partition the indexed dataframe on H3 Parent index to make stuff run faster
    h3_indexed_df = h3_indexed_df.repartition(H3_PARENT_COL_NAME)

    return h3_indexed_df


def point_router(
    input: VectorTable,
    subset_columns: list,
    h3_resolution: int = DEFAULT_H3_RES,
) -> tuple:
    """
    Routes the input dataframe to the H3 indexer methods for point geometry types.

    :param input: The input VectorTable BaseModel.
    :param subset_columns: List of column names to keep in final indexer output.
    :param h3_resolution: H3 resolution. Defaults to DEFAULT_H3_RES.
    :return: Tuple containing:
        - DataFrame: Point geometry type input pyspark dataframe indexed to H3.
        - List: List of columns to keep in final indexed output.
    :raises ValueError: If the input DataFrame is None or empty.
    """
    if input.df is None:
        raise ValueError("DataFrame must be initialized before H3 indexing")

    # cache the input dataframe first
    input_df = input.df.cache()

    input_df = input_df.withColumn(
        H3_INDEX_COL_NAME, index_shape(GEOJSON_COL_NAME, lit(h3_resolution))
    )

    # explode on the H3 index and cache immediately
    exploded_df = input_df.select(
        input.unique_id, explode(H3_INDEX_COL_NAME).alias(H3_INDEX_COL_NAME)
    ).cache()

    # pre-compute H3 geometries and cache
    h3_geoms_df = (
        exploded_df.select(H3_INDEX_COL_NAME)
        .distinct()
        .withColumn(H3_GEOM_COL_NAME, ST_GeomFromWKT(h3_to_wkt(col(H3_INDEX_COL_NAME))))
        .cache()
    )

    # join back efficiently
    exploded_df = exploded_df.join(h3_geoms_df, on=H3_INDEX_COL_NAME, how="left")
    exploded_df = exploded_df.join(
        input_df.select(input.unique_id, GEOM_WKT_COL_NAME), on=input.unique_id, how="left"
    )

    # cache joins
    input_df_h3_indexed = exploded_df.cache()

    if input.method == "WITHIN":

        input_df_h3_indexed = input_df_h3_indexed.withColumn(
            RATIO_COL_NAME,
            lit(1.00),
        )

    # add total count column to indexer output
    input_df_h3_indexed = input_df_h3_indexed.withColumn("total_count", lit(1))
    subset_columns.append("total_count")
    return input_df_h3_indexed, subset_columns


def line_router(
    input: VectorTable,
    subset_columns: list,
    h3_resolution: int = DEFAULT_H3_RES,
) -> tuple:
    """
    Routes the input dataframe to the H3 indexer methods for line geometry types.

    :param input: The input VectorTable BaseModel.
    :param subset_columns: List of column names to keep in final indexer output.
    :param h3_resolution: H3 resolution. Defaults to DEFAULT_H3_RES.
    :return: Tuple containing:
        - DataFrame: Line geometry type input pyspark dataframe indexed to H3.
        - List: List of columns to keep in final indexed output.
    :raises ValueError: If the input DataFrame is None or empty.
    """
    if input.df is None:
        raise ValueError("DataFrame must be initialized before H3 indexing")

    # cache the input dataframe first
    input_df = input.df.cache()

    input_df = input_df.withColumn(
        H3_INDEX_COL_NAME, index_shape(GEOJSON_COL_NAME, lit(h3_resolution))
    )

    # explode on the H3 index and cache immediately
    exploded_df = input_df.select(
        input.unique_id, explode(H3_INDEX_COL_NAME).alias(H3_INDEX_COL_NAME)
    ).cache()

    # pre-compute H3 geometries and cache
    h3_geoms_df = (
        exploded_df.select(H3_INDEX_COL_NAME)
        .distinct()
        .withColumn(H3_GEOM_COL_NAME, ST_GeomFromWKT(h3_to_wkt(col(H3_INDEX_COL_NAME))))
        .cache()
    )

    # join back efficiently
    exploded_df = exploded_df.join(h3_geoms_df, on=H3_INDEX_COL_NAME, how="left")
    exploded_df = exploded_df.join(
        input_df.select(input.unique_id, GEOM_WKT_COL_NAME), on=input.unique_id, how="left"
    )

    # cache joins
    input_df_h3_indexed = exploded_df.cache()

    if input.method == "PCT_LENGTH":

        input_df_h3_indexed = input_df_h3_indexed.withColumn(
            RATIO_COL_NAME,
            ST_Length(ST_Intersection(ST_GeomFromWKT(GEOM_WKT_COL_NAME), H3_GEOM_COL_NAME))
            / ST_Length(ST_GeomFromWKT(GEOM_WKT_COL_NAME)),
        )

    # add total length column (in kilometers) to indexer output
    input_df_h3_indexed = input_df_h3_indexed.withColumn(
        "total_length_km", ST_LengthSpheroid(ST_GeomFromWKT(GEOM_WKT_COL_NAME)) / 1000
    )
    subset_columns.append("total_length_km")
    return input_df_h3_indexed, subset_columns


def polygon_router(
    input: VectorTable,
    subset_columns: list,
    h3_resolution: int = DEFAULT_H3_RES,
) -> tuple:
    """
    Routes the input dataframe to the H3 indexer methods for polygon geometry types.

    :param input: The input VectorTable BaseModel.
    :param subset_columns: List of column names to keep in final indexer output.
    :param h3_resolution: H3 resolution. Defaults to DEFAULT_H3_RES.
    :return: Tuple containing:
        - DataFrame: Polygon geometry type input pyspark dataframe indexed to H3.
        - List: List of columns to keep in final indexed output.
    :raises ValueError: If the input DataFrame is None or empty.
    """
    if input.df is None:
        raise ValueError("DataFrame must be initialized before H3 indexing")

    # cache the input dataframe first
    input_df = input.df.cache()

    input_df = input_df.withColumn(
        H3_INDEX_COL_NAME, index_shape(GEOJSON_COL_NAME, lit(h3_resolution))
    )

    # explode on the H3 index and cache immediately
    exploded_df = input_df.select(
        input.unique_id, explode(H3_INDEX_COL_NAME).alias(H3_INDEX_COL_NAME)
    ).cache()

    # Pre-compute H3 geometries and cache
    h3_geoms_df = (
        exploded_df.select(H3_INDEX_COL_NAME)
        .distinct()
        .withColumn(H3_GEOM_COL_NAME, ST_GeomFromWKT(h3_to_wkt(col(H3_INDEX_COL_NAME))))
        .cache()
    )

    # join back efficiently
    exploded_df = exploded_df.join(h3_geoms_df, on=H3_INDEX_COL_NAME, how="left")
    exploded_df = exploded_df.join(
        input_df.select(input.unique_id, GEOM_WKT_COL_NAME), on=input.unique_id, how="left"
    )

    # cache joins
    input_df_h3_indexed = exploded_df.cache()

    if input.method == "PCT_AREA":

        input_df_h3_indexed = input_df_h3_indexed.withColumn(
            RATIO_COL_NAME,
            ST_Area(ST_Intersection(ST_GeomFromWKT(GEOM_WKT_COL_NAME), H3_GEOM_COL_NAME))
            / ST_Area(ST_GeomFromWKT(GEOM_WKT_COL_NAME)),
        )

    # add total area column (in sq kilometers) to indexer output
    input_df_h3_indexed = input_df_h3_indexed.withColumn(
        "total_area_km2", ST_AreaSpheroid(ST_GeomFromWKT(GEOM_WKT_COL_NAME)) / 1000000
    )
    subset_columns.append("total_area_km2")
    return input_df_h3_indexed, subset_columns
