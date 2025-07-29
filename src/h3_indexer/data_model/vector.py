"""Vector base model definition."""

from enum import Enum
from typing import List, Literal, Optional, Type, TypeVar, Union

from pydantic import BaseModel, ValidationInfo, field_validator, model_validator
from pyspark.sql import DataFrame
from sedona.sql import ST_Point

from h3_indexer.utils.geospatial import (
    add_geojson_geometry,
    add_wkt_geometry,
    get_geom_col_data_type,
)

T = TypeVar("T", bound="VectorTable")


class GeomTypeEnum(str, Enum):
    """Enum class containing vector geometry types."""

    POINT = "POINT"
    LINE = "LINE"
    POLYGON = "POLYGON"


class PointMethodEnum(str, Enum):
    """Enum class containing H3 resolver methods for point geometry type."""

    WITHIN = "WITHIN"


class LineMethodEnum(str, Enum):
    """Enum class containing H3 resolver methods for line geometry type."""

    PCT_LENGTH = "PCT_LENGTH"
    PASS_THROUGH = "PASS_THROUGH"


class PolygonMethodEnum(str, Enum):
    """Enum class containing H3 resolver methods for polygon geometry type."""

    PCT_AREA = "PCT_AREA"
    CENTROID = "CENTROID"


class InputColumn(BaseModel):
    """
    Input column.

    :param name: Column name.
    :param type: Column data type.
    """

    name: str
    type: str | None = None


class VectorTable(BaseModel):
    """
    Represents a vector geospatial data table with detailed metadata.

    :param job_id: Unique identifier for the job processing the vector data. Inherited from the Job BaseModel.
    :param id: Unique identifier for the vector table.
    :param type: Type of data, constrained to "vector".
    :param glue_catalog_database_name: Glue Catalog database name.
    :param glue_catalog_table_name: Glue Catalog table name.
    :param where_clause: SQL where clause to filter the Glue catalog table. Only applicable with Glue catalog source.
    :param s3_path: Path to file in S3.
    :param unique_id: Unique identifier within the table.
    :param geometry_type: Type of geometric data (point, line, polygon).
    :param geometry_column_name: Name of the geometry column in the table.
    :param lat_column_name: Name of the latitude geometry column in the table.
    :param lon_column_name: Name of the latitude geometry column in the table.
    :param method: Processing method specific to the geometry type.
    :param input_columns: List of input columns in the table.
    :param df: PySpark DataFrame containing input data.
    :param h3_indexed_df: PySpark DataFrame containing H3 indexed data.
    :raises ValueError: If the method is incompatible with the geometry type.
    """

    job_id: str
    id: str
    type: Literal["vector"]
    glue_catalog_database_name: Optional[str] = None
    glue_catalog_table_name: Optional[str] = None
    where_clause: Optional[str] = None
    s3_path: Optional[str] = None
    unique_id: str
    geometry_type: GeomTypeEnum
    geometry_column_name: Optional[str] = None
    lat_column_name: Optional[str] = None
    lon_column_name: Optional[str] = None
    method: Union[PointMethodEnum, LineMethodEnum, PolygonMethodEnum]
    input_columns: List[InputColumn]
    df: DataFrame | None = None
    h3_indexed_df: DataFrame | None = None

    class Config:
        """Config class to allow storing PySpark DataFrames as attributes."""

        # Allow non-Pydantic types like PySpark DataFrame
        arbitrary_types_allowed = True

    def initialize_input_df(self: "VectorTable", df: DataFrame) -> "VectorTable":
        """
        Set the input DataFrame.

        :param df: PySpark DataFrame containing input data.
        :return: The VectorTable instance.
        """
        # if geometry column doesn't exist, it's a POINT with lat/lon cols
        if not self.geometry_column_name:
            df = df.withColumn(
                "geom_from_lat_lon", ST_Point(self.lon_column_name, self.lat_column_name)
            )
            self.geometry_column_name = "geom_from_lat_lon"

        geom_dtype = get_geom_col_data_type(df, self.geometry_column_name)

        # add geojson geometry - needed for h3-pyspark
        self.df = add_geojson_geometry(df, self.geometry_column_name, geom_dtype)
        # add WKT geometry - needed for other geospatial operations
        self.df = add_wkt_geometry(self.df, self.geometry_column_name, geom_dtype)

        return self

    def set_h3_indexed_df(self: "VectorTable", df: DataFrame) -> "VectorTable":
        """
        Set the H3 indexed DataFrame.

        :param df: PySpark DataFrame containing H3 indexed data.
        :return: The VectorTable instance.
        """
        self.h3_indexed_df = df
        return self

    @model_validator(mode="before")
    @classmethod
    def check_s3_path_or_glue_catalog(cls: Type[T], values: dict) -> dict:
        """
        Validate that either s3_path OR both glue_catalog fields are provided.
        Also validates that where_clause is only provided with glue catalog fields.

        :param values: Dict[str, Any], the dictionary of field values
        :return: Dict[str, Any], the validated dictionary of field values
        :raises ValueError: If the input combination is invalid
        """
        s3_path = values.get("s3_path")
        glue_db_name = values.get("glue_catalog_database_name")
        glue_table_name = values.get("glue_catalog_table_name")
        where_clause = values.get("where_clause")

        # Check if s3_path is provided
        has_s3_path = s3_path is not None

        # Check if both glue catalog fields are provided
        has_glue_catalog = glue_db_name is not None and glue_table_name is not None

        # Check if only one of the glue catalog fields is provided (incomplete)
        has_incomplete_glue = (
            (glue_db_name is not None and glue_table_name is None) or
            (glue_db_name is None and glue_table_name is not None)
        )

        # Check where_clause constraints
        if where_clause is not None and not has_glue_catalog:
            raise ValueError("'where_clause' can only be used with Glue catalog source, not with 's3_path'")

        if has_incomplete_glue:
            raise ValueError("Both 'glue_catalog_database_name' and 'glue_catalog_table_name' must be provided together")

        if has_s3_path and has_glue_catalog:
            raise ValueError("Provide either 's3_path' OR both glue catalog fields, not both")

        if not has_s3_path and not has_glue_catalog:
            raise ValueError("Either 's3_path' OR both glue catalog fields must be provided")

        return values

    @field_validator("s3_path")
    @classmethod
    def validate_s3_uri(cls: Type[T], v: str) -> str:
        """
        Validate that the S3 path starts with the correct prefix.

        :param v: The S3 URI to validate.
        :return: The validated S3 URI.
        """
        if v.startswith("s3a://"):
            return v

        # Remove the existing prefix
        elif any(v.startswith(prefix) for prefix in ("s3:/", "s3://", "s3a:/")):
            path = v[v.index("/") + 2 :]
            return f"s3a://{path}"

        # Add the 's3a://' prefix
        else:
            return f"s3a://{v}"

    @field_validator("glue_catalog_database_name")
    @classmethod
    def validate_glue_catalog_database_name(cls: Type[T], v: str) -> str:
        """
        Ensure that the Glue Catalog database name is lowercase.

        :param v: The Glue Catalog database name to validate.
        :return: The validated Glue Catalog database name.
        """
        return v.lower()

    @field_validator("glue_catalog_table_name")
    @classmethod
    def validate_glue_catalog_table_name(cls: Type[T], v: str) -> str:
        """
        Ensure that the Glue Catalog table name is lowercase.

        :param v: The Glue Catalog table name to validate.
        :return: The validated Glue Catalog table name.
        """
        return v.lower()

    @field_validator("method")
    @classmethod
    def validate_method_for_geometry(
        cls: Type[T],
        v: Union[PointMethodEnum, LineMethodEnum, PolygonMethodEnum],
        info: ValidationInfo,
    ) -> Union[PointMethodEnum, LineMethodEnum, PolygonMethodEnum]:
        """
        Validate that the method is compatible with the geometry type.

        :param v: The method to validate.
        :param info: Validation context information.
        :return: The validated method.
        :raises ValueError: If the method is not compatible with the geometry type.
        """
        geom_type = info.data.get("geometry_type")

        if geom_type == GeomTypeEnum.POINT:
            if not isinstance(v, PointMethodEnum):
                raise ValueError(
                    f"Point geometry only accepts methods: {[m.value for m in PointMethodEnum]}"
                )
        elif geom_type == GeomTypeEnum.LINE:
            if not isinstance(v, LineMethodEnum):
                raise ValueError(
                    f"Line geometry only accepts methods: {[m.value for m in LineMethodEnum]}"
                )
        elif geom_type == GeomTypeEnum.POLYGON:
            if not isinstance(v, PolygonMethodEnum):
                raise ValueError(
                    f"Polygon geometry only accepts methods: {[m.value for m in PolygonMethodEnum]}"
                )

        return v

    @field_validator("input_columns", mode="before")
    @classmethod
    def validate_input_columns(
        cls: Type[T], v: Union[List[str], List[InputColumn], List[Union[str, InputColumn]]]
    ) -> List[InputColumn]:
        """
        Convert a list of names into a list of InputColumn types.

        :param v: Input columns.
        :return: Input columns.
        """
        return [InputColumn(name=item) if isinstance(item, str) else item for item in v]

    @model_validator(mode="after")
    def validate_geometry_columns(self: "VectorTable") -> "VectorTable":
        """
        Validate geometry column requirements based on geometry type.

        :return: The validated VectorTable instance.
        :raises ValueError: If the geometry column requirements are not met.
        """
        if self.geometry_type in [GeomTypeEnum.LINE, GeomTypeEnum.POLYGON]:
            if not self.geometry_column_name:
                raise ValueError(
                    f"geometry_column_name is required for {self.geometry_type} geometry type."
                )
            if self.lat_column_name or self.lon_column_name:
                raise ValueError(
                    f"lat_column_name and lon_column_name should not be provided for {self.geometry_type} geometry type."
                )
        elif self.geometry_type == GeomTypeEnum.POINT:
            if self.geometry_column_name and (self.lat_column_name or self.lon_column_name):
                raise ValueError(
                    "For POINT geometry, either geometry_column_name or lat_column_name/lon_column_name should be provided, not both."
                )
            if self.lat_column_name and not self.lon_column_name:
                raise ValueError("lon_column_name must be provided when lat_column_name is used.")
            if self.lon_column_name and not self.lat_column_name:
                raise ValueError("lat_column_name must be provided when lon_column_name is used.")
        return self
