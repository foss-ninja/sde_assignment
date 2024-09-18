"""
This module provides utility functions to transform the extracted data to the required format.

The transformation functions:
    - transform_data() to transform the given user and lessons data into the required format.

"""

import pandas as pd


def transform_data(users_data: pd.DataFrame, lessons_data: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the given user and lessons data into a report.

    Args:
        users_data (pd.DataFrame): The user data.
        lessons_data (pd.DataFrame): The lessons data.

    Returns:
        pd.DataFrame: The transformed data.
    """
    # drop duplicate rows
    users_data.drop_duplicates(inplace=True)
    lessons_data.drop_duplicates(inplace=True)

    # fill missing values - replace missing usernames with "Unknown"
    users_data.loc[:, "user_name"] = users_data["user_name"].fillna("Unknown")

    # drop rows where completion_date is not available
    lessons_data.dropna(subset=["completion_date"], inplace=True)

    # ensure completion_date is in correct format
    # this will replace invalid dates with NaT
    lessons_data["completion_date"] = pd.to_datetime(
        lessons_data["completion_date"], errors="coerce"
    )

    # merge data for final report
    df = pd.merge(users_data, lessons_data, on="user_id", how="inner")

    # rename columns to required format
    df.rename(
        columns={
            "user_id": "User ID",
            "user_name": "Name",
            "completion_date": "Date of completion",
            "daily_lessons": "Number of lessons completed",
        },
        inplace=True,
    )
    return df
