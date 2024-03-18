import urllib.parse
import boto3

import datetime as dt
import logging
import orjson

from save_to_s3 import save_df_to_s3
from utils import clean_and_transform, _now_ts

logger = logging.getLogger(__name__)

s3 = boto3.client('s3')


S3_KEY_PREFIX: str = "test_vcxt_trade"
S3_BUCKET: str = "stage-trades-archive"
FORMAT: str = "parquet"


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'],
        encoding='utf-8'
    )
    print(f"Bucket: {bucket}")
    print(f"Key: {key}")

    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        trades = orjson.loads(response['Body'].read())
        vcxt_trades_process_data(
            trades,
            s3_key_prefix=S3_KEY_PREFIX,
            s3_bucket=S3_BUCKET,
            format=FORMAT
        )
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}.'.format(key, bucket))
        raise e


def vcxt_trades_process_data(
    trades: list,
    s3_key_prefix: str = "vcxt_trades",
    s3_bucket: str = "vcxt-archive",
    format: str = "parquet"
):
    """
    This function will process the trades data,
    clean and transform it, and save it to S3.

    Args:
        trades (list): The list of trades to process.
        s3_key_prefix (str, optional): The prefix of the keys in the s3 bucket.
                                       Defaults to "vcxt_trades".
        s3_bucket (str, optional): The name of the s3 bucket.
                                   Defaults to "vcxt-archive".
        format (str, optional): The format of the file to save in s3.
                                Defaults to "parquet".
    """
    start_time_ts = _now_ts()
    batch_data = list()
    for trade_str in trades:
        try:
            json_data = orjson.loads(trade_str)[0]
            cleaned_data = clean_and_transform(json_data)
            batch_data.append(cleaned_data)
        except Exception as e:
            logger.error(
                "vcxt_trades_process_data: "
                "Exception raised while parsing json_data",
                exc_info=e
            )
            continue
    duration = _now_ts() - start_time_ts
    logger.info(
        f"vcxt_trades_process_data: "
        f"Time elapsed in transform step: {duration} seconds"
    )

    if not batch_data:
        logger.info("No data to insert in vcxt_trades table.")
        return

    try:
        time_suffix = (
            dt.datetime.now(tz=dt.timezone.utc)
              .isoformat()
              .replace("+00:00", "Z")
        )
        s3_key = f"{s3_key_prefix}_{time_suffix}.{format}"

        start_time_ts = _now_ts()
        save_df_to_s3(
            df=batch_data,
            bucket=s3_bucket,
            key=s3_key,
            format=format
        )
        duration = _now_ts() - start_time_ts
        logger.info(
            f"vcxt_trades_process_data: "
            f"Time elapsed in save_df_to_s3: {duration} seconds"
        )
        logger.info(
            "vcxt_trades_process_data: "
            "Successfully inserted data in S3"
        )
    except Exception as e:
        # If insertion failed, we retreive the fields that are older
        # than 1H and remove them from the hashmap
        logger.critical(
            "vcxt_trades_process_data: "
            "Exception raised while saving data to S3: ",
            exc_info=e
        )


if __name__ == "__main__":
    lambda_handler({
        "Records": [
            {
                "s3": {
                    "bucket": {
                        "name": "stage-trades-archive"
                    },
                    "object": {
                        "key": "vcxt_trades/2021-09-01T00:00:00Z.parquet"
                    }
                }
            }
        ]
    }, None)
