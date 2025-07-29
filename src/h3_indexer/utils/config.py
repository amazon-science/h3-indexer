"""Config & data model utilities."""

import os
from pathlib import Path
from typing import Union

import yaml

from h3_indexer.data_model.job import Job


def read_yaml_config(config_path: Union[str, Path]) -> Job:
    """
    Read and validate a YAML configuration file into a Job model.

    :param config_path: Path to the YAML configuration file.
    :return: A validated Job model instance.
    :raises FileNotFoundError: If the config file doesn't exist.
    """
    # Convert string path to Path object if needed
    config_path = Path(config_path) if isinstance(config_path, str) else config_path

    # Check if file exists
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    # Read and parse YAML file
    with open(config_path, "r") as f:
        config_dict = yaml.safe_load(f)

    # Validate and create Job instance
    job = Job(**config_dict)
    return job


def read_json_config(json_input: dict) -> Job:
    """
    Read and validate a JSON dict into a Job model.

    :param json_input: Parsed JSON dict.
    :return: A validated Job model instance.
    """
    # Validate and create Job instance
    job = Job(**json_input)
    return job


if __name__ == "__main__":
    # Or using a Path object
    config_path = Path("configs/sample_job.yaml")
    job = read_yaml_config(config_path)
