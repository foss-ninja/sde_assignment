"""
Utils for interacting with AWS services like S3 and SES.

The functions:
    - get_boto_client() to create a boto3 client for the given service name
    - upload_file_to_s3() to upload the given file to the specified S3 bucket
    - send_email_via_ses() to send an email using AWS SES
"""

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import boto3
from config import settings


def get_boto_client(service_name: str):
    """
    Creates a boto3 client for the given service name.

    :param service_name: The name of the AWS service to create a client for
    :return: A boto3 client instance for the given service name
    """
    return boto3.client(
        service_name,
        aws_access_key_id=settings.aws_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.aws_secret_access_key.get_secret_value(),
        region_name=settings.aws_region,
    )


def upload_file_to_s3(s3_client, file_name) -> str | None:
    """
    Uploads the given file to the specified S3 bucket and returns the public URL of the uploaded file.

    :param s3_client: A boto3 client instance for the S3 service
    :param file_name: The name of the file to upload
    :return: A string representing the public URL of the uploaded file. In case of error returns None
    """
    try:
        # upload file
        bucket_name = settings.aws_s3_bucket_name
        print(f"Uploading file '{file_name}' to S3 bucket '{bucket_name}'...")
        s3_client.upload_file(file_name, bucket_name, file_name)
        print(f"File uploaded to S3 bucket '{bucket_name}' as '{file_name}'")

        # Generate the public URL
        public_url = (
            f"https://{bucket_name}.s3.{settings.aws_region}.amazonaws.com/{file_name}"
        )
        print(f"Public URL for the file: {public_url}")
        return public_url

    except Exception as e:
        print(f"Error uploading file: {str(e)}")


def send_email_via_ses(
    sender: str,
    recipients: list[str],
    subject: str,
    body_text: str,
    attachment_file: str,
) -> bool:
    """
    Sends an email via SES with the given sender, recipients, subject, body text, and attachment file.

    :param sender: The sender email address
    :param recipients: The recipient email addresses
    :param subject: The subject of the email
    :param body_text: The body text of the email
    :param attachment_file: The file to attach to the email
    :return: True if the email is sent successfully, False otherwise
    """
    try:
        print(f"Sending email to recipients: {recipients}...")
        # Create a multipart message
        msg = MIMEMultipart()
        msg["From"] = sender
        msg["To"] = recipients[0]
        msg["Subject"] = subject

        # Add the email body
        msg.attach(MIMEText(body_text))

        # Attach the file
        part = MIMEBase("application", "octet-stream")
        with open(attachment_file, "rb") as file:
            part.set_payload(file.read())

        encoders.encode_base64(part)
        part.add_header(
            "Content-Disposition", f'attachment; filename="{attachment_file}"'
        )

        # Attach the part to the message
        msg.attach(part)

        # Send the email via SES
        ses_client = get_boto_client("ses")
        response = ses_client.send_raw_email(
            Source=sender,
            Destinations=recipients,
            RawMessage={"Data": msg.as_string()},
        )

        print(
            f"Email sent! Recipients: {recipients}, Message ID: {response['MessageId']}"
        )
        return True
    except Exception as e:
        print(f"Error sending email: {str(e)}")
        return False
