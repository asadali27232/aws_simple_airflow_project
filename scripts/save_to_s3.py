import boto3
import pandas as pd

def save_to_s3(bucket_name, s3_key, local_file="./data/nyc_311_first_1m.csv"):
    data = pd.read_csv(local_file)

    # 2. Create S3 client
    s3 = boto3.client("s3")

    # 3. Upload to S3
    s3.upload_file(data, bucket_name, s3_key)

    print(f"Uploaded to s3://{bucket_name}/{s3_key}")
