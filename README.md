# h3-indexer

The **h3-indexer** is an open source package for indexing geospatial data using PySpark, Apache Sedona and Uber's open source H3 hierarchical spatial indexing system. The h3-indexer maps any number of vector-type geospatial data sets to H3 grids for efficient spatial analysis and querying.

The h3-indexer contains 3 stages, and users can [provide command line arguments](#usage) to run the stages one at a time, or altogether. 
1. [Validator](#input-requirements)
2. [Indexer](#methodology-indexer)
3. [Resolver](#methodology-resolver)

## Features
- Supports vector point, line & polygon data types
- Supports the following inputs:
  - Parquet files & shapefiles in S3
  - Glue catalog tables
- Outputs are written to S3 in parquet format
- Configurable [H3 resolution](https://h3geo.org/docs/core-library/restable/) (3-10)
- PySpark for H3 indexing operations using Apache Sedona
- YAML & JSON-based configuration supported

## Developer Setup

### Versions:
This tool requires the following versions:
Python: 3.10
Glue: 4.0
Spark: 3.3.0
Apache Sedona: 1.7.1

### Setup

Run the following commands from inside the h3-indexer root directory to set up your run environment:

```
chmod +x scripts/env_setup.sh
source ./scripts/env_setup.sh
```

When this executable finishes running, it will print out the environment variable paths to set in your .env file - for example:
```
Your SPARK_HOME path is <path>
Your GLUE_JARS path is <path>
Your JAVA_HOME path is <path>
```

### Environment

You need to include the following environment variables in a .env file. You can convert the .sample.env file to a .env file and update it accordingly. You can get the paths for `SPARK_HOME`, `GLUE_JARS` and `JAVA_HOME` from the `env_setup.sh` file. You will need to add the Python interpreter path to the 3 Python environment variables.

- PYTHONPATH
- PYSPARK_PYTHON
- PYSPARK_DRIVER_PYTHON
- SPARK_HOME
- GLUE_JARS
- JAVA_HOME

### Python

Required version is Python 3.10. You will need to install the Python libraries in the requirements.txt file and update the Python environment variables to the path of your interpreter.

### S3
You'll need to have credentials and proper permissions set up for the files in the S3 bucket included in your config.

### Debugging

There is a sample yaml config in the sample_configs directory. Here's a sample launch.json file to use for debugging with a test config:

```
{
    "name": "main",
    "type": "debugpy",
    "request": "launch",
    "program": "/H3-Indexer/src/H3-Indexer/src/main.py",
    "console": "integratedTerminal",
    "args": ["--yaml-path", "/H3-Indexer/src/H3-Indexer/configs/test_job.yaml"],
    "envFile": "/H3-Indexer/src/H3-Indexer/.env"
}
```

## Usage
Run the H3 Indexer with a YAML configuration file:

```
python main.py --yaml-path configs/sample_job.yaml --run-all
```

### Args
The following args are available:

**Inputs:**  
`--yaml-path`: File path to the yaml.  
`--json-input`: JSON input.

**Run Type:**  
`--validate-only`: Only validate the input config. (Validate)  
`--index-only`: Only validate the input config and run the indexer. (Validate -> Index)  
`--run-all`: Optional flag, this is the defauly behavior. Run all. (Validate -> Index -> Resolve)

## Configuration
The YAML configuration file defines the job parameters including input data sources, H3 resolution, and output location.

### Required Fields
- `name`: Project name (string)
- `version`: Version number in semantic format #.#.# (string)
- `h3_resolution`: H3 resolution level, supported range 3-10 (integer)
- `output_s3_path`: S3 path for output storage (string)
- `inputs`: Dictionary of input data sources. Each input in the `inputs` dictionary can either be a Vector or Raster input type. See below for more details on required parameters for each input type.

##### Vector Data
For vector inputs (type: "vector"):
- `type`: Must be "vector"
- `glue_catalog_database_name`: Glue Catalog database name - must be provided with `glue_catalog_table_name`.
- `glue_catalog_table_name`: Glue Catalog table name - must be provided with `glue_catalog_database_name`.
- `where_clause`: SQL where clause to filter the Glue catalog table. Only applicable with Glue catalog source.
- `s3_path`: Path to input data in S3
- `unique_id`: Column name containing unique identifier
- `geometry_type`: Type of geometry ("POINT", "LINE", or "POLYGON")
- `geometry_column_name`: Name of the geometry column.
- `method`: Processing method ("PCT_LENGTH" for lines, "PCT_AREA" for polygons)
- `input_columns`: List of columns to include in output

When providing a Glue Catalog table, both `glue_catalog_database_name` and `glue_catalog_table_name` must be provided.  These variables are mutually exclusive to `s3_path`. So when providing an S3 path, you cannot also include Glue Catalog parameters. And when providing Glue Catalog parameters, you cannot also include an S3 path.

For POINT inputs only, the `geometry_column_name` parameter can be replaced with these 2 parameters:
- `lat_column_name`: Name of the latitude column.
- `lon_column_name`: Name of the longitude column.

##### Raster Data
Raster data types are not currently enabled in the service. Raster pixels can be converted to points/centroids and then provided as an input data set to the indexer.

##### Input Requirements
The Validator will fail if the following requirements are not met by the inputs:
- The `unique_id` column must, in fact, be unique or the validator will fail. The H3 indexer doesn't yet support an input that contains more than 1 column as the primary key.
- The geometry column (`geometry_column_name`) must be of the following data types: WKB, WKT, Geometry, GeoJSON.
- If your input is a POINT geometry type, and you elected to use the `lat_column_name` + `lon_column_name` parameters in place of the `geometry_column_name` parameter, those columns must be Float or Double type.
- The columns in `input_columns` must be numeric (Integer, Float, Double, etc). The H3 Indexer does not yet support categorical or label-based (String) column data types.
- Invalid or NULL geometries are dropped from the dataset.

### Example Configuration

As yaml:
```yaml
name: "project_name"
version: "1.0.0"
h3_resolution: 6
output_s3_path: "s3://my-bucket/output/"
inputs:
  mypoints:  
    type: "vector"
    s3_path: "s3://my-bucket/my_points.parquet"
    unique_id: "unique_id"
    geometry_type: "POINT"
    lat_column_name: "latitude"
    lon_column_name: "longitude"
    method: "WITHIN"
    input_columns:
      - "population"
  mylines:  
    type: "vector"
    s3_path: "s3://my-bucket/my_line_data.parquet"
    unique_id: "unique_id"
    geometry_type: "LINE"
    geometry_column_name: "geometry"
    method: "PCT_LENGTH"
    input_columns:
      - "emissions"
  mypolygons:  
    type: "vector"
    s3_path: "s3://my-bucket/my_polygon_data.parquet"
    unique_id: "unique_id"
    geometry_type: "POLYGON"
    geometry_column_name: "geom_wkt"
    method: "PCT_AREA"
    input_columns:
      - "population"
```

As JSON:

```json
{
  "name": "project_name",
  "version": "1.0.0",
  "h3_resolution": 6,
  "output_s3_path": "s3://my-bucket/output/",
  "inputs": {
    "mypoints": {
      "type": "vector",
      "s3_path": "s3://my-bucket/my_points.parquet",
      "unique_id": "unique_id",
      "geometry_type": "POINT",
      "lat_column_name": "latitude",
      "lon_column_name": "longitude",
      "method": "WITHIN",
      "input_columns": [
        "population"
      ]
    },
    "mylines": {
      "type": "vector",
      "s3_path": "s3://my-bucket/my_line_data.parquet",
      "unique_id": "unique_id",
      "geometry_type": "LINE",
      "geometry_column_name": "geometry",
      "method": "PCT_LENGTH",
      "input_columns": [
        "emissions"
      ]
    },
    "mypolygons": {
      "type": "vector",
      "s3_path": "s3://my-bucket/my_polygon_data.parquet",
      "unique_id": "unique_id",
      "geometry_type": "POLYGON",
      "geometry_column_name": "geom_wkt",
      "method": "PCT_AREA",
      "input_columns": [
        "population"
      ]
    }
  }
}
```

## Methodology: Indexer

### Points

#### Methods available to Point Inputs:
The following methods are enabled for point inputs:

- `WITHIN`: The attribution of the point is allocated to the H3 grid that the point lies within.

#### Indexer Output Columns for Points:
The indexer will produce an output with the following columns for point inputs:
- `h3_index`: The H3 index.
- `h3_resolution`: The H3 resolution of the index, as provided in the job config.
- `h3_r3_parent`: The Resolution=3 parent index of the H3 grid. This column is used for partitioning.
- `h3_area_km2`: The area of the H3 hexagon in km<sup>2</sup>.
- `<unique_id>`: The unique ID of the input, based on the **unique_id** for the input provided in the job config.
- `ratio`: The percentage of the point associated with the H3 grid. For method `WITHIN`, this will always be 1.00.
- `total_count`: The total count of the point (will always be 1).

The primary key of the output is the combination of `h3_index` and `<unique_id>`.

For example:

| h3_index        | h3_resolution   |h3_r3_parent    | h3_area_km2       | pixel_id | ratio | total_count |
|-----------------|-------------|-----------------|-------------------|----------|-------|-------------|
| 840e4d3ffffffff | 4 | 830e4dfffffffff | 2004.4344472440796 | 6834     | 1.0   | 1           |
| 840e4d7ffffffff | 4 | 830e4dfffffffff | 2011.5201608518523 | 13638    | 1.0   | 1           |

This example shows 2 H3 hexagons (2 rows) at resolution 4 that each intersected with 1 unique point (*pixel_id*). The area of both hexagons is ~2005km<sup>2</sup>. 100% of each point is associated with the hexagon that they are located within. The total count of each point is 1.

### Lines

#### Methods available to Line Inputs:
The following methods are enabled for line inputs:

- `PCT_LENGTH`: The attribution of the line is allocated to the H3 grids based on the percentage of the total length of the line that intersects with the H3 grids. For example, if 50% of the line falls within an H3 grid, then 50% of the line’s attributes get mapped to the H3 grid.

#### Indexer Output Columns for Lines:
The indexer will produce an output with the following columns for line inputs:
- `h3_index`: The H3 index.
- `h3_resolution`: The H3 resolution of the index, as provided in the job config.
- `h3_r3_parent`: The Resolution=3 parent index of the H3 grid. This column is used for partitioning.
- `h3_area_km2`: The area of the H3 hexagon in km<sup>2</sup>.
- `<unique_id>`: The unique ID of the input, based on the **unique_id** for the input provided in the job config.
- `ratio`: The percent length of the line associated with the H3 grid. This calculation is based on the **method** for the input provided in the job config.
- `total_length_km`: The total length in km of the line.

The primary key of the output is the combination of `h3_index` and `<unique_id>`.

For example:

| h3_index | h3_resolution | h3_r3_parent | h3_area_km2 | route_id | ratio | total_length_km |
|---------------|-------------|------------------|-------------------|---------------------|-------------------|-------------------|
| 86446cae7ffffff | 6 | 86446cfffffffff | 40.55609958082783 | route123 | 0.7927260401223691 | 10.74319262992267 |
| 86446ca57ffffff | 6 | 86446cfffffffff | 40.58272492454886 | route123 | 0.2072739598776309 | 10.74319262992267 |

This example table depicts 2 H3 hexagons (2 rows) at resolution 6 that intersected with a single line (*route123*). The area of both hexagons is ~40km<sup>2</sup>. 79.3% of the line intersected with the first hexagon and 20.7% of the line intersected with the second hexagon. The total length of line *route123* is 10.7 km.

### Polygons

#### Methods available to Polygon Inputs:
The following methods are enabled for polygon inputs:

- `PCT_AREA`: The attribution of the polygon is allocated to the H3 grids based on the percentage of the total area of the polygon that overlaps with the H3 grids. For example, if 50% of the polygon overlaps with an H3 grid, then 50% of the polygon’s attributes get mapped to the H3 grid.

#### Indexer Output Columns for Polygons:
The indexer will product an output with the following columns for polygon inputs:
- `h3_index`: The H3 index.
- `h3_resolution`: The H3 resolution of the index, as provided in the job config.
- `h3_r3_parent`: The Resolution=3 parent index of the H3 grid. This column is used for partitioning.
- `h3_area_km2`: The area of the H3 hexagon in km<sup>2</sup>.
- `<unique_id>`: The unique ID of the input, based on the **unique_id** for the input provided in the job config.
- `ratio`: The percent area of the polygon associated with the H3 grid. This calculation is based on the **method** for the input provided by the job config.
- `total_area_km2`: The total area in km<sup>2</sup> of the polygon.

The primary key of the output is the combination of `h3_index` and `<unique_id>`.

For example:

| h3_index | h3_resolution | h3_r3_parent | h3_area_km2 | GEOID20 | ratio | total_area_km2 |
|---------------|-----------|------------------|--------------|---------------------|-------------------|-------------------|
| 8644697b7ffffff | 6 | 864469fffffffff | 40.12018482559633 | geoid123 | 0.6830630172222252 | 0.7395279788133455 |
| 86446945fffffff | 6 | 86446ffffffffff | 40.145249906240224 | geoid123 | 0.3169369827778003 | 0.7395279788133455 |

This example table depicts 2 H3 hexagons (2 rows) at resolution 6 that intersected with a single polygon (*geoid123*). The area of both hexagons is ~40km<sup>2</sup>. 68.3% of the polygon intersected with the first hexagon and 31.7% of the polygon intersected with the second hexagon. The total area of polygon *geoid123* is ~0.74 km<sup>2</sup>.

## Methodology: Resolver
The resolver takes all of the `input_columns` of all of the inputs in the Job config and resolves them to H3 grids. It does this by taking the ratios associated with each H3 grid, then summing the attributes up in a `GROUP BY h3_index`. This operation handles for known overlaps on H3 grids, for example: when several routes pass through the same H3 grid, or when several blocks intersect with the same H3 grid.

### Resolver Output Columns:
The resolver will produce an output with the following columns:
- `h3_index`: The H3 index.
- `h3_resolution`: The H3 resolution of the index, as provided in the job config.
- `h3_area_km2`: The area of the H3 hexagon in km<sup>2</sup>.
- `sum_<input_column_name>`: There will be one of these columns from all of the `input_columns` for each input from the job config. The values are multiplied by their corresponding ratios, then summed and grouped by the H3 indices that they intersected with. 

The primary key of the output is `h3_index`.

For example:

| h3_index | h3_resolution | h3_area_km2 | sum_census_value | sum_route_value |
|---------------|---------|------------------|-------------------|-------------------|
| 86446cae7ffffff | 6 | 40.55609958082783 | 100.43 | 7.8728 |
| 86446ca57ffffff | 6 | 40.58272492454886 | 9433.12 | 0.007726 |

This example table depicts 2 H3 hexagons (2 rows) at resolution 6. The area of both hexagons is ~40km<sup>2</sup>. This example resolver output would have had 2 inputs:
1) A polygon input table with the column: census_value
2) A line input table with the column: route_value

For each hexagon, the column named `sum_census_value` is calculated by taking the sum of the `census_value` x `ratio` for all intersecting polygons. The column named `sum_route_value` is calculated by taking the sum of the `route_value` x `ratio` for all intersecting lines. 

## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This project is licensed under the Apache-2.0 License.

