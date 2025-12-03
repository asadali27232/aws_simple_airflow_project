import boto3

def upload_df_to_s3(df, bucket_name, s3_key, local_file="temp_output.csv"):
    """
    Save DataFrame to CSV and upload to AWS S3.
    
    df          -> pandas DataFrame to upload
    bucket_name -> your S3 bucket name
    s3_key      -> full S3 path: folder/file.csv
    local_file  -> temporary CSV file saved locally
    """

    # 1. Save DataFrame locally
    df.to_csv(local_file, index=False)
    print(f"Saved temporary file: {local_file}")

    # 2. Create S3 client
    s3 = boto3.client("s3")

    # 3. Upload to S3
    s3.upload_file(local_file, bucket_name, s3_key)

    print(f"Uploaded to s3://{bucket_name}/{s3_key}")
