"""
This module provides utility functions to extract data from different sources.

Extract data from databases:
    - extract_postgres_data() to extract data from PostgreSQL
    - extract_mysql_data() to extract data from MySQL
"""

import pandas as pd
from config import settings
from sqlalchemy import create_engine

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
    engine = create_engine(settings.postgres_url.get_secret_value())
    connection = engine.connect()
    return connection.connection


def connect_to_mysql():
    """
    Create MySQL connection using SQLAlchemy.

    Returns:
        connection: A database connection object.
    """
    engine = create_engine(settings.mysql_url.get_secret_value())
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
