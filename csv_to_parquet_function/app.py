import datetime as dt
import urllib.parse

import boto3

from cvs_to_parquet_converter import CsvToParquetConverter


def lambda_handler(event, context):
    """Sample pure Lambda function

    Parameters
    ----------
    event: dict, required
        Lambda event

        Event doc: https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html#api-gateway-simple-proxy-for-lambda-input-format

    context: object, required
        Lambda Context runtime methods and attributes

        Context doc: https://docs.aws.amazon.com/lambda/latest/dg/python-context-object.html
   """

    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    new_key = f"titanic_{dt.datetime.now().strftime('%s')}.parquet"
    print(f"Converting {key} from {bucket} to {new_key}...")
    converter = CsvToParquetConverter(_s3_client=boto3.client('s3'))
    converter.convert(bucket=bucket,
                      csv_key=key,
                      parquet_key=new_key,
                      separator=";",
                      header=0,
                      na_filter=False)
    print(f"Converted {key} from {bucket} to {new_key} and pushed to {bucket}")
