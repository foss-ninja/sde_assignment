import os

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path="../setup/.env")


# AWS S3 credentials and bucket info
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")


def get_boto_client(service_name):
    """
    Creates a boto3 client for the given service name.

    :param service_name: The name of the AWS service to create a client for
    :return: A boto3 client instance for the given service name
    """
    return boto3.client(
        service_name,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )


def upload_file_to_s3(s3_client, file_name):
    """
    Uploads the given file to the specified S3 bucket and returns the public URL of the uploaded file.

    :param s3_client: A boto3 client instance for the S3 service
    :param file_name: The name of the file to upload
    :return: A string representing the public URL of the uploaded file
    """
    try:
        # upload file
        print(f"Uploading file: {file_name} to S3 bucket '{S3_BUCKET_NAME}'")
        s3_client.upload_file(file_name, S3_BUCKET_NAME, file_name)
        print(f"File uploaded to S3 bucket '{S3_BUCKET_NAME}' as '{file_name}'")

        # Generate the public URL
        public_url = (
            f"https://{S3_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{file_name}"
        )
        print(f"Public URL for the file: {public_url}")
        return public_url

    except Exception as e:
        print(f"Error uploading file: {str(e)}")


def generate_csv_report(report_df: pd.DataFrame, file_name: str):
    """
    Generates a CSV report from given Pandas DataFrame and saves it to a given file_name.

    Parameters
    ----------
    report_df : pd.DataFrame
        DataFrame containing the report data
    file_name : str
        Name of the file to be saved
    """
    report_df.to_csv(file_name, index=False)
    print(f"Report generated successfully with name: {file_name}")
