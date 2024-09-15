"""
This module provides utility functions to interact with the report data.

The functions:
    - generate_csv_report() to generate a CSV report from given Pandas DataFrame
      and save it to a given file_name
    - send_report_on_mail() to send a CSV report to a recipient email address
      using AWS SES

"""

import pandas as pd
from config import settings
from utils.aws_utils import send_email_via_ses

email_subject = "MT | Daily Lessons Completed Report"
email_body = """
    Dear Recipients,
    Please find attached the daily lessons completed report for the last year. \n
    This report provides a summary of the number of lessons completed by users on a daily basis.

    The report is based on data extracted from our PostgreSQL and MySQL databases, \n
    and is intended to provide insights into user engagement and lesson completion trends.

    If you have any questions or would like to discuss the report in more detail, \n
    feel free to reach out to us.

    Best regards,
    Team MT
    """


def generate_csv_report(report_df: pd.DataFrame, file_name: str) -> None:
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


def send_report_on_mail(file_name: str) -> bool:
    """
    Sends a CSV report to a recipient email address using AWS SES.

    Parameters
    ----------
    file_name : str
        Name of the file containing the report data
    """
    is_sent = send_email_via_ses(
        sender=settings.sender_email,
        recipients=settings.recipient_emails.split(","),
        subject=email_subject,
        body_text=email_body,
        attachment_file=file_name,
    )
    if is_sent:
        print(f"Report sent successfully to recipients: {settings.recipient_emails}")
        return True
    else:
        print(f"Failed to send report with name: {file_name}")
        return False
