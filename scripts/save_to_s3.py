import pandas as pd
import boto3
from pathlib import Path
from io import BytesIO

def save_to_s3(bucket_name, s3_key, input_csv="s3://nyc311-airflow-data-bucket/cleaned/nyc_311_cleaned.csv"):
    """
    Reads a CSV file, converts it to Parquet, and uploads to S3.

    input_csv   -> Path to local cleaned CSV file
    bucket_name -> S3 bucket name
    s3_key      -> Destination path in S3 (e.g., folder/file.parquet)
    """
    # Check if CSV exists
    if not Path(input_csv).is_file():
        raise FileNotFoundError(f"{input_csv} not found.")

    # Read CSV into DataFrame
    df = pd.read_csv(input_csv)

    # Convert to Parquet in memory
    buffer = BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)

    # Upload to S3
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=buffer.getvalue())

    print(f"Uploaded Parquet to s3://{bucket_name}/{s3_key}")