"""A collection of functions that simplify working with S3. 

This module defines functions used to
    * create an S3 path
    * write a Spark DataFrame to S3 in various data formats

Most of these functions use the `AWS SDK for Python (Boto3)`_ to complete their
tasks.  Refer to this documentation to understand the data sent to and returned
from it.  

.. _AWS SDK for Python (Boto3):
   https://boto3.amazonaws.com/v1/documentation/api/latest/index.html
"""
def create_s3_path_from_params(bucket, key):
    """Returns a string formatted as an S3 path URL.

    Args:
        bucket (str): An S3 bucket name.
        key (str): An S3 key name.

    Returns:
        str: A string formatted as an S3 path URL.
    """
    s3_path = 's3://{0}/{1}'.format(bucket, key)
    return s3_path

def write_dataframe_as_parquet_to_s3(dataframe, s3_path, mode, file_format, data_partition_metadata=None):
    """Writes a Spark DataFrame to S3 in Parquet format.

    Args:
        dataframe (DataFrame): The DataFrame to be written.
        s3_path (str): The S3 Path to write the DataFrame to.
        mode (str): Specifies the behavior when data or table already exists:
            * append: Append contents of this DataFrame to existing data.
            * overwrite: Overwrite existing data.
            * error or errorifexists: Throw an exception if data already
                exists.
            * ignore: Silently ignore this operation if data already exists.
        file_format (str): Specifies the underlying output data source (json,
            parquet)
        data_partition_metadata ([type]): [description]
    """
    if data_partition_metadata and len(data_partition_metadata) > 0:
        data_partition_metadata_names = list(map(lambda v : v.get('name'), data_partition_metadata))
        dataframe.write \
            .mode(mode) \
            .format(file_format) \
            .partitionBy(*data_partition_metadata_names) \
            .parquet(s3_path)
    else:
        dataframe.write \
        .mode(mode) \
        .format(file_format) \
        .parquet(s3_path)

        
def write_dataframe_as_json_to_s3(dataframe, s3_path, mode, file_format, compression=None):
    """Writes a Spark DataFrame to S3 in JSON format.

    Args:
        dataframe (DataFrame): The DataFrame to be written.
        s3_path (str): The S3 Path to write the DataFrame to.
        mode (str): Specifies the behavior when data or table already exists:
            * append: Append contents of this DataFrame to existing data.
            * overwrite: Overwrite existing data.
            * error or errorifexists: Throw an exception if data already
                exists.
            * ignore: Silently ignore this operation if data already exists.
        file_format (str): Specifies the underlying output data source (json,
            parquet)
        compression ([type], optional): Compression codec to use when saving to
            file. This can be one of the known case-insensitive shorten names
            (none, bzip2, gzip, lz4, snappy and deflate). Defaults to None.
    """
    dataframe.write \
        .mode(mode) \
        .format(file_format) \
        .json(s3_path, compression=compression)

def write_dataframe_as_csv_to_s3(dataframe, s3_path, mode):
    """Writes a Spark DataFrame to S3 in CSV format.

    Args:
        dataframe (DataFrame): The DataFrame to be written.
        s3_path (str): The S3 Path to write the DataFrame to.
        mode (str): Specifies the behavior when data or table already exists:
            * append: Append contents of this DataFrame to existing data.
            * overwrite: Overwrite existing data.
            * error or errorifexists: Throw an exception if data already
                exists.
            * ignore: Silently ignore this operation if data already exists.
    """
    dataframe.write \
        .csv(s3_path, mode=mode, header=True)