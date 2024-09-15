import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.core_utils import generate_csv_report, upload_file_to_s3, get_boto_client
from utils.extract_data_utils import extract_mysql_data, extract_postgres_data
from utils.transform_data_utils import transform_data

# Define default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 9, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Create the DAG
with DAG(
    dag_id="daily_lessions_report_dag",
    default_args=default_args,
    description="DAG to extract users and lessons data from PostgreSQL and MySQL, "
    "transform the data, and generate a custom report with the number "
    "of daily lessons completed by users in the last year, and save "
    "the report to a CSV file",
    schedule_interval=timedelta(days=1),  # Run every day
    catchup=False,
) as dag:
    # Task 1: Extract users data from PostgreSQL
    def extract_users_data_task():
        return extract_postgres_data()

    extract_users_data = PythonOperator(
        task_id="extract_users_data",
        python_callable=extract_users_data_task,
    )

    # Task 2: Extract lessons data from MySQL
    def extract_lessons_data_task():
        return extract_mysql_data()

    extract_lessons_data = PythonOperator(
        task_id="extract_lessons_data",
        python_callable=extract_lessons_data_task,
    )

    # Task 3: Transform data
    def transform_data_task(ti):
        # Here ti is a TaskInstance object, which provides access to the
        # context of the task instance, including the task_id, dag_id,
        # and execution_date. The xcom_pull method allows us to pull
        # XCOM values from other tasks in the same DAG.
        users_data = ti.xcom_pull(task_ids="extract_users_data")
        lessons_data = ti.xcom_pull(task_ids="extract_lessons_data")
        return transform_data(users_data=users_data, lessons_data=lessons_data)

    transform_data_op = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data_task,
    )

    # Task 4: Generate the CSV report
    def generate_report_task(ti):
        report_data = ti.xcom_pull(task_ids="transform_data")
        generate_csv_report(
            report_df=report_data, file_name=f"{os.getenv('REPORT_FILE_NAME')}.csv"
        )

    generate_csv_report_op = PythonOperator(
        task_id="generate_csv_report",
        python_callable=generate_report_task,
    )

    # Task 5: Upload report to S3
    def upload_file_to_s3_task():
        upload_file_to_s3(
            s3_client=get_boto_client("s3"),
            file_name=f"{os.getenv('REPORT_FILE_NAME')}.csv",
        )

    upload_report_op = PythonOperator(
        task_id="upload_report",
        python_callable=upload_file_to_s3_task,
    )

    # Define task dependencies
    (
        [extract_users_data, extract_lessons_data]
        >> transform_data_op
        >> generate_csv_report_op
        >> upload_report_op
    )
