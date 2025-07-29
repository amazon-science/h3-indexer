"""Spark DataFrame UDFs."""

import binascii

import h3
from pyspark.sql.functions import udf
from pyspark.sql.types import BinaryType, StringType
from shapely import wkt
from shapely.geometry import Polygon


@udf(returnType=StringType())
def get_parent_h3_res3(h3_index: str) -> str:
    """Get the parent of the H3 index at resolution 3.

    :param h3_index: H3 index - any resolution.
    :return: parent H3 index at resolution 3
    """
    if h3_index is None:
        return None
    return h3.h3_to_parent(h3_index, 3)


@udf(returnType=BinaryType())
def h3_to_wkb(h3_index: str) -> str:
    """
    Convert H3 index to WKB geometry bytes.

    :param h3_index: H3 cell index as string.
    :return: WKB representation of the H3 cell boundary as bytes.
    """
    # Get the boundary coordinates
    coords = h3.h3_to_geo_boundary(h3_index)

    # H3 returns [lat, lng] but Shapely expects [lng, lat]
    # Also need to close the polygon by repeating first point
    boundary = [[coord[1], coord[0]] for coord in coords]
    boundary.append(boundary[0])  # Close the polygon

    # Create Shapely polygon
    polygon = Polygon(boundary)

    # Convert to WKB and then to hexadecimal string
    wkb_bytes = polygon.wkb
    return "0x" + binascii.hexlify(wkb_bytes).decode("ascii")


@udf(returnType=BinaryType())
def h3_to_wkt(h3_index: str) -> str:
    """
    Convert H3 index to WKB geometry bytes.

    :param h3_index: H3 cell index as string.
    :return: WKB representation of the H3 cell boundary as bytes.
    """
    # Get the boundary coordinates
    coords = h3.h3_to_geo_boundary(h3_index)

    # H3 returns [lat, lng] but Shapely expects [lng, lat]
    # Also need to close the polygon by repeating first point
    boundary = [[coord[1], coord[0]] for coord in coords]
    boundary.append(boundary[0])  # Close the polygon

    # Create Shapely polygon
    polygon = Polygon(boundary)

    return polygon.wkt


@udf(returnType=StringType())
def clip_line_to_polygon(line_wkt: str, polygon_wkt: str) -> str | None:
    """
    Clips a LineString to a Polygon boundary.

    Args:
        line_wkt: WKT representation of LineString
        polygon_wkt: WKT representation of Polygon

    Returns:
        WKT string of clipped LineString
    """
    # Convert WKT to Shapely geometries
    line = wkt.loads(line_wkt)
    polygon = wkt.loads(polygon_wkt)

    # Perform the intersection
    clipped_line = line.intersection(polygon)

    # Handle different result types
    if clipped_line.is_empty:
        return None
    elif clipped_line.geom_type in ("LineString", "MultiLineString"):
        return clipped_line.wkt
    else:
        return None
