from pydantic import Field, SecretStr, EmailStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="../setup/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database Configs
    mysql_url: SecretStr = Field(..., alias="MYSQL_DATABASE_URI")
    postgres_url: SecretStr = Field(..., alias="POSTGRES_DATABASE_URI")

    # AWS Configs
    aws_access_key_id: SecretStr
    aws_secret_access_key: SecretStr
    aws_region: str = "ap-south-1"
    aws_s3_bucket_name: str = Field(..., alias="S3_BUCKET_NAME")

    # SES configs
    sender_email: EmailStr
    recipient_emails: str
    report_file_name: str = "daily_lessons_report"
    email_subject: str = "Daily Lessons Report"
    email_body_text: str = "Please find the attached report."


settings = Settings()
