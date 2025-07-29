"""Job config base model definition."""

import hashlib
import re
import time
from datetime import datetime
from enum import Enum
from typing import Any, Type, TypeVar, Union

from pydantic import BaseModel, Field, field_validator, model_validator
from pyspark.sql import DataFrame

from h3_indexer.data_model.raster import RasterFile
from h3_indexer.data_model.vector import VectorTable

T = TypeVar("T", bound="Job")


class JobStatus(str, Enum):
    """Job status."""

    PENDING = "PENDING"
    VALIDATED = "VALIDATED"
    RUNNING_INDEXER = "RUNNING_INDEXER"
    COMPLETED_INDEXER = "COMPLETED_INDEXER"
    RUNNING_RESOLVER = "RUNNING_RESOLVER"
    COMPLETED_RESOLVER = "COMPLETED_RESOLVER"
    FAILED = "FAILED"


def create_unique_id() -> str:
    """
    Create a unique hash based on the timestamp.

    :return: Unique ID as a string.
    """
    timestamp = time.time()
    timestamp_str = str(timestamp).encode("utf-8")
    hash_object = hashlib.sha256(timestamp_str)
    return hash_object.hexdigest()[:12]


class Job(BaseModel):
    """
    Represents a processing job with configuration details.

    A Job instance contains metadata and configuration required for processing,
    including identifiers, input parameters, and database connection details.

    :param name: Name of the job.
    :param version: Version.
    :param h3_resolution: H3 resolution.
    :param output_s3_path: Path to S3 bucket where outputs will be stored.
    :param inputs: Dictionary containing input parameters and values.
    :param h3_resolved_df: PySpark DataFrame containing H3 resolved data.
    :param id: Unique identifier for the job. Defaults to "8d9e8cffddcc" to use for debug.
    :param status: Current status of the job.
    :param error_message: Error message if any.
    :param created_at: Timestamp when the job was created.
    :param updated_at: Timestamp when the job was last updated.
    """

    name: str
    version: str
    h3_resolution: int
    output_s3_path: str
    inputs: dict[str, Any]
    h3_resolved_df: DataFrame | None = None

    # metadata fields
    # for testing, always use same job id, remove this later
    # Field(default_factory=create_unique_id)
    id: str = "8d9e8cffddcc"
    status: JobStatus = JobStatus.PENDING
    error_message: str | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime | None = Field(default=None)

    class Config:
        """Config class."""

        # Allow non-Pydantic types like PySpark DataFrame
        arbitrary_types_allowed = True
        # This tells Pydantic how to serialize datetime objects when converting the model to JSON
        json_encoders = {datetime: lambda v: v.isoformat()}

    def set_h3_resolved_df(self: "Job", df: DataFrame) -> "Job":
        """
        Set the H3 indexed DataFrame.

        :param df: PySpark DataFrame containing H3 indexed data.
        :return: The Job instance.
        """
        self.h3_resolved_df = df
        return self

    def update_status(self: "Job", status: JobStatus, error: str | None = None) -> "Job":
        """
        Update the job status and error message.

        :param status: The new status of the job.
        :param error: Error message if any. Defaults to None.
        :return: The updated Job instance.
        """
        self.status = status
        self.error_message = error
        self.updated_at = datetime.utcnow()
        return self

    @field_validator("version")
    @classmethod
    def validate_version(cls: Type[T], v: str) -> str:
        """
        Validate the version string format.

        Ensures the version follows semantic versioning format: #.#.#

        :param v: The version string to validate.
        :return: The validated version string.
        :raises ValueError: If version string doesn't match format #.#.#
        """
        pattern = r"^\d+\.\d+\.\d+$"
        if not re.match(pattern, v):
            raise ValueError('Version must be in format #.#.# (e.g. "1.0.0")')
        return v

    @field_validator("h3_resolution")
    @classmethod
    def validate_h3_resolution(cls: Type[T], v: int) -> int:
        """
        Validate the H3 resolution value.

        Ensures the H3 resolution is within the supported range (3-10).

        :param v: The H3 resolution value to validate.
        :return: The validated H3 resolution value.
        :raises AssertionError: If H3 resolution is not between 3 and 10 inclusive.
        """
        assert (
            3 <= v <= 10
        ), f"Currently, only H3 resolutions 3-10 are supported. Please change the H3 resolution: {v}"
        return v

    @model_validator(mode="after")
    def validate_and_convert_inputs(self: "Job") -> "Job":
        """
        Validate and convert input data to appropriate types.

        Processes each input in the inputs dictionary and converts them to either
        VectorTable or RasterFile objects based on their type.

        :return: The validated Job instance with converted inputs.
        :raises ValueError: If an input type is neither 'vector' nor 'raster'.
        """
        validated_inputs: dict[str, Union[VectorTable, RasterFile]] = {}

        for input_name, input_data in self.inputs.items():
            # Add the id to the input data
            input_data["id"] = input_name
            input_data["job_id"] = self.id  # Now we can access self.id

            # Validate and convert based on type
            if input_data.get("type") == "vector":
                validated_inputs[input_name] = VectorTable(**input_data)
            elif input_data.get("type") == "raster":
                validated_inputs[input_name] = RasterFile(**input_data)
            else:
                raise ValueError(
                    f"Input type must be either 'vector' or 'raster' for input: {input_name}"
                )

        self.inputs = validated_inputs
        return self
