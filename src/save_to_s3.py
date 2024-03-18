import boto3
import io
import logging
import polars as pl

from botocore.config import Config
from typing import Union, List, Dict, Any


logger = logging.getLogger(__name__)


def save_df_to_s3(
    df: Union[pl.DataFrame, List[Dict[str, Any]]],
    bucket: str,
    key: str,
    format: str = "csv",
    max_attempts: int = 3,
):
    """
    Save a Polars dataframe to an S3 bucket.

    Parameters
    ----------
    df : pl.DataFrame
        The dataframe to save.
    bucket : str
        The S3 bucket to save to.
    key_prefix : str
        The prefix to use for the S3 key.
    max_attempts : int, optional
        The maximum number of times to retry the upload, by default 3
    """
    try:
        if isinstance(df, list):
            try:
                df = pl.DataFrame(df)
            except Exception as e:
                logger.error(
                    f'Failed to convert data to Polars DataFrame: {e}',
                    exc_info=e
                )
                raise e

        # Check if the dataframe is empty
        if not isinstance(df, pl.DataFrame):
            logger.error("save_df_to_s3: Data should be a Polars DataFrame")
            return

        number_of_records = df.shape[0]
        if number_of_records == 0:
            logger.error("save_df_to_s3: Dataframe is empty")
            return

        # Convert the dataframe to a csv buffer
        buffer_df = io.BytesIO()
        if format == "csv":
            df.write_csv(buffer_df)
        elif format == "parquet":
            df.write_parquet(buffer_df)
        else:
            logger.error(f"save_df_to_s3: Unsupported format: {format}")
            return

        buffer_df.seek(0)

        logger.info(f"save_df_to_s3: Start archiving to {bucket} S3 bucket")
        # Create a boto3 client with retries
        config = Config(
            retries={
                'max_attempts': max_attempts,
                'mode': 'standard'
            }
        )
        # Upload the dataframe to S3
        s3_client = boto3.client("s3", config=config)
        s3_client.upload_fileobj(buffer_df, bucket, key)
        logger.info(
            "save_df_to_s3: End archiving {} records to {} S3 bucket".format(
                number_of_records,
                bucket
            )
        )
    except Exception as e:
        logger.critical(
            "Exception in save_df_to_s3 in {} bucket: {}".format(bucket, e),
            exc_info=e
        )
        raise e
