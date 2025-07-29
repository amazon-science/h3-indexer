"""Raster base model definition."""

from typing import Literal, Type, TypeVar

from pydantic import BaseModel, field_validator

T = TypeVar("T", bound="RasterFile")


class RasterFile(BaseModel):
    """
    Represents a raster file with associated metadata and S3 storage information.

    :param job_id: Unique identifier for the job associated with the raster file. Inherited from the Job BaseModel.
    :param id: Unique identifier for the raster file.
    :param type: Type of the file, constrained to "raster".
    :param s3_uri: S3 URI location of the raster file, validated to start with "s3://".
    :raises ValueError: If the s3_uri does not start with "s3://".
    """

    job_id: str
    id: str
    type: Literal["raster"]
    s3_uri: str

    @field_validator("s3_uri")
    @classmethod
    def validate_s3_uri(cls: Type[T], v: str) -> str:
        """
        Validate that the S3 URI starts with the correct prefix.

        :param v: The S3 URI to validate.
        :return: The validated S3 URI.
        :raises ValueError: If the URI does not start with "s3://".
        """
        if not v.startswith("s3://"):
            raise ValueError(f"S3 URI must start with s3://: {v}")
        return v
