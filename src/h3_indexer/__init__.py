"""H3-Indexer: A tool for indexing geospatial data using H3 hierarchical spatial indexing."""

__version__ = "1.0.0"

# Import main functions from your existing modules
from .h3_indexer import (
    h3_indexer_spark,
)

from .h3_resolver import (
    h3_resolver_single_input_spark,
    h3_resolver_spark
)

from .constants import (
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

from .validator import (
    validate_config
)

# Make submodules available
from . import data_model
from . import utils
from . import spark