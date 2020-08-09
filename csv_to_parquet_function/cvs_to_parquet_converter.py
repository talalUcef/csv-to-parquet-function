from io import StringIO, BytesIO

import pandas as pd
from pandas import DataFrame


class CsvToParquetConverter:

    def __init__(self, _s3_client):
        self.s3_client = _s3_client

    def convert(self,
                bucket: str,
                csv_key: str,
                parquet_key: str,
                separator: str = ",",
                header: int = None,
                na_filter: bool = False):
        """
        Get the csv file (csv_key) for S3 bucket (bucket),
        convert it to parquet file (parquet_key)
        and put it to S3

        Parameters
        ----------

        bucket: string, required
            The S3 bucket name

        csv_key: string, required
            The csv file name

        parquet_key: string, required
            The S3 bucket object name

        separator: int
            Separator character to use

        header: int
            Row to use to parse column labels

        na_filter: bool
            Detect missing value markers (empty strings and the value of na_values). In
            data without any NAs, passing na_filter=False can improve the performance
            of reading a large file.
        """

        csv_contents_buff = self._get_object(bucket=bucket, csv_key=csv_key)
        df = self._read_csv_to_df(contents_buff=csv_contents_buff,
                                  separator=separator,
                                  header=header,
                                  na_filter=na_filter)
        parquet_contents_buff = self._convert_df_to_parquet(dataframe=df)
        self._save_object(bucket=bucket,
                          parquet_key=parquet_key,
                          parquet_buff=parquet_contents_buff)

    def _get_object(self, bucket: str, csv_key: str) -> StringIO:
        """ Get the contents of a S3 object """

        try:
            obj = self.s3_client.get_object(Bucket=bucket, Key=csv_key)
            contents = obj['Body'].read().decode('utf-8')
            print(f"Object {csv_key} fetched from bucket {bucket}")
            return StringIO(contents)
        except Exception as e:
            print(f"Error getting object {csv_key} from bucket {bucket} : {e}")
            raise e

    def _read_csv_to_df(self,
                        contents_buff: StringIO,
                        separator: str = ",",
                        header: int = None,
                        na_filter: bool = False) -> DataFrame:
        """ Put the contents into a panda's dataframe """

        try:
            df = pd.read_csv(contents_buff,
                             delimiter=separator,
                             header=header,
                             encoding='utf-8',
                             na_filter=na_filter)
            # replaces spaces in column names like `sample column` with `sample_column`
            cols = df.columns.str.strip().str.replace(' ', '_')
            df.columns = cols
            print("Converted S3 object to pandas dataframe")
            return df
        except Exception as e:
            print(f"Can't create panda's dataframe from object : {e}")
            raise e

    def _convert_df_to_parquet(self, dataframe: DataFrame) -> BytesIO:
        """ convert the pandas dataframe to Parquet """

        try:
            parquet_obj = BytesIO()
            dataframe.to_parquet(parquet_obj, engine="pyarrow")
            parquet_obj.seek(0)
            print("Converted panda's dataframe to parquet data")
            return parquet_obj
        except Exception as e:
            print(f"Can't create parquet from dataframe : {e}")
            raise e

    def _save_object(self,
                     bucket: str,
                     parquet_key: str,
                     parquet_buff: BytesIO):
        """ Put the parquet data to S3 """

        try:
            self.s3_client.put_object(Bucket=bucket,
                                      Key=parquet_key,
                                      Body=parquet_buff.getvalue())
            print(f"Pushed parquet data to bucket {bucket} as {parquet_key}")
        except Exception as e:
            print(f"Error putting object {parquet_key} to bucket {bucket} : {e}")
            raise e
