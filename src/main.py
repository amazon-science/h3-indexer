"""H3-Indexer main executable."""

import argparse
import logging
import json
from h3_indexer.validator import validate_config
from h3_indexer.utils.config import read_yaml_config, read_json_config
from h3_indexer.h3_indexer import h3_indexer_spark
from h3_indexer.h3_resolver import h3_resolver_spark, h3_resolver_single_input_spark
from h3_indexer.spark.spark import get_spark_session, write_df_to_s3, cache_df
from h3_indexer.data_model.job import JobStatus, Job
from pyspark.sql import SparkSession
from h3_indexer.constants import H3_PARENT_COL_NAME

logging.basicConfig(level=logging.INFO)


def arg_parser() -> argparse.Namespace:
    """
    Parse command line arguments.

    :return: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description='Argument parser.'
    )

    parser.add_argument(
        '--yaml-path',
        dest="yaml_path",
        type=str,
        required=True,
        help='Path to the input yaml config file.'
    )

    args = parser.parse_args()

    return args


def index_job(job: Job, spark: SparkSession) -> None:
    """
    Run H3 Indexer on the Job.

    :param job: Job config.
    :param spark: Spark session.
    """
    job.update_status(JobStatus.RUNNING_INDEXER)

    for input_name, input_config in job.inputs.items():

        if input_config.type == 'vector':

            h3_indexed_df = h3_indexer_spark(input_config, job.h3_resolution)
            # cache df to make future ops run faster
            h3_indexed_df = cache_df(h3_indexed_df)
            input_config.set_h3_indexed_df(h3_indexed_df)

            output_path = (
                f"s3a://{job.output_s3_path}/{job.name}/indexer/{input_name}"
            )
            logging.info(f'Exporting to S3: {output_path.replace("s3a", "s3")}')
            write_df_to_s3(h3_indexed_df, output_path, "parquet",
                           partition_by=['h3_resolution', H3_PARENT_COL_NAME])

    job.update_status(JobStatus.COMPLETED_INDEXER)


def resolve_job(job: Job, spark: SparkSession) -> None:
    """
    Run H3 Resolver on the Job.

    :param job: Job config.
    :param spark: Spark session.
    """
    job.update_status(JobStatus.RUNNING_RESOLVER)

    if len(job.inputs.items()) == 1:

        # there's only one input config, so set the variable equal to the first value in the dict
        input_config = next(iter(job.inputs.values()))
        input_df = input_config.df
        h3_indexed_df = input_config.h3_indexed_df

        h3_resolved_df = h3_resolver_single_input_spark(input_df, h3_indexed_df, input_config, job.h3_resolution, True)
    else:
        h3_resolved_df = h3_resolver_spark(spark, job)

    # cache df to make future ops run faster
    h3_resolved_df = cache_df(h3_resolved_df)
    job.set_h3_resolved_df(h3_resolved_df)

    output_path = f"s3a://{job.output_s3_path}/{job.name}/resolver"
    logging.info(f'Exporting to S3: {output_path.replace("s3a", "s3")}')
    write_df_to_s3(h3_resolved_df, output_path, "parquet",
                   partition_by=['h3_resolution', H3_PARENT_COL_NAME])

    job.update_status(JobStatus.COMPLETED_RESOLVER)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Argument parser.'
    )

    # Create a mutually exclusive group for the input job config options
    input_group = parser.add_mutually_exclusive_group(required=True)

    input_group.add_argument(
        '--yaml-path',
        dest="yaml_path",
        type=str,
        help='Path to the input yaml config file.'
    )

    input_group.add_argument(
        '--json-input',
        dest="json_input",
        type=str,
        help='Input JSON job config.'
    )

    # Create a mutually exclusive group for the run options
    run_group = parser.add_mutually_exclusive_group()
    # Add the mutually exclusive arguments
    run_group.add_argument('--validate-only', action='store_true', help='Run validation only')
    run_group.add_argument('--index-only', action='store_true', help='Run indexing only')
    run_group.add_argument('--run-all', action='store_true', help='Run all steps (default behavior)')
    # Set the default for the group
    parser.set_defaults(run_all=True)

    args = parser.parse_args()

    if args.yaml_path:
        job_config = read_yaml_config(args.yaml_path)
    else:
        job_config = read_json_config(json.loads(args.json_input))

    # validator runs for all steps no matter what
    spark, _ = get_spark_session(job_config.h3_resolution)

    # validate the job config
    logging.info('Validating config...')
    job = validate_config(job_config, spark)
    logging.info('Config validated.')

    if args.index_only or args.run_all:
        # run the indexer
        logging.info("Running indexer...")
        index_job(job, spark)

    if args.run_all:
        # run the resolver
        logging.info("Running resolver...")
        resolve_job(job, spark)
