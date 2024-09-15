import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv(dotenv_path="../setup/.env")

# PostgreSQL connection details
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DATABASE_URI = os.getenv("POSTGRES_DATABASE_URI")

# MySQL connection details
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")
MYSQL_DB = os.getenv("MYSQL_DATABASE")
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = os.getenv("MYSQL_PORT")
MYSQL_DATABASE_URI = os.getenv("MYSQL_DATABASE_URI")

# SQL Queries
POSTGRES_QUERY = """
    SELECT user_id, user_name
    FROM mindtickle_users
    WHERE active_status = 'active';
"""

MYSQL_QUERY = """
    SELECT user_id, completion_date, COUNT(lesson_id) AS daily_lessons
    FROM lesson_completion
    WHERE completion_date >= NOW() - INTERVAL 1 YEAR
    GROUP BY user_id, completion_date;
"""


def connect_to_postgres():
    """
    Create PostgreSQL connection using SQLAlchemy.

    Returns:
        connection: A database connection object.
    """
    engine = create_engine(POSTGRES_DATABASE_URI)
    connection = engine.connect()
    return connection.connection


def connect_to_mysql():
    """
    Create MySQL connection using SQLAlchemy.

    Returns:
        connection: A database connection object.
    """
    engine = create_engine(MYSQL_DATABASE_URI)
    connection = engine.connect()
    return connection.connection


def extract_postgres_data() -> pd.DataFrame:
    """
    Extract user data from PostgreSQL.

    Returns:
        pd.DataFrame: A DataFrame containing "user_id" and "user_name" of active users.
    """
    conn = connect_to_postgres()
    users_df = pd.read_sql(sql=POSTGRES_QUERY, con=conn)
    conn.close()
    return users_df


def extract_mysql_data() -> pd.DataFrame:
    """
    Extract lessons data from MySQL.

    Returns:
        pd.DataFrame: A DataFrame containing daily lessons per user
                      with columns "user_id", "completion_date", "daily_lessons".
    """
    conn = connect_to_mysql()
    lessons_df = pd.read_sql(MYSQL_QUERY, conn)
    conn.close()
    return lessons_df
