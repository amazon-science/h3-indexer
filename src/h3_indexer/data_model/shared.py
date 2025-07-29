"""Shared model definition."""

from enum import Enum


class GeomColDataTypeEnum(str, Enum):
    """Enum class containing all possible geometry column data types."""

    WKT = "WKT"
    BINARY_WKB = "BINARY_WKB"
    STRING_WKB = "STRING_WKB"
    GEOJSON = "GEOJSON"
    GEOMETRY = "GEOMETRY"
