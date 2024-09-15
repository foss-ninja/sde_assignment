# MT Daily Report Generator

This project generates a report with the number of daily lessons completed by users in the last year.
The report is generated in CSV format having  following columns:
- `User ID`: The unique identifier for the user
- `Name`: The name of the user
- `Date of completion`: The date the user completed the lessons
- `Number of lessons completed`: The number of lessons completed by the user on that date

This report is finally uploaded to S3 bucket and a public URL is generated.

Sample report created and uploaded to S3.
https://mt-daily-lessons-report.s3.ap-south-1.amazonaws.com/daily_lessons_report.csv

## How to run?

**Prerequisites:**
    - *You should have a aws account with s3 bucket created.*

**This project can be run in multiple ways.**

1. Run using Docker.
2. Run with Python Environment.
3. Run with existing Airflow setup.

### 1. Run using Docker

This is the recommended way to run the project which does not require any additional setup.


1. Install [Docker](https://www.docker.com/get-docker) and [Docker Compose](https://docs.docker.com/compose/install/).
2. Clone the repository.
```bash
git clone https://github.com/nileshverma054/mt-reports.git
```
3. Change directory to setup.
```bash
cd setup
```
4. Copy `.env.example` file and populate the required variables. 
```bash
cp .env.example .env
```
5. Run application.
```bash
docker compose up --build
```
6. Visit http://localhost:8082 in your browser and login to using below credentials.
```bash
username: admin
password: admin
```

---

### 2. Run with Python Environment

You can also run the project using Python Environment. Follow the setp 1-4 from `Run using Docker` section.

1. Create and activate python virtual environment.

```bash
python3 -m venv venv && source venv/bin/activate
```

2. Install dependencies.

```bash
pip install -r requirements.txt
```

3. Go to `report_generator` directory

```bash
cd report_generator
```

4. Run report generator.

```bash
python main.py
```

---

### 3. Run with existing Airflow setup

If you have an existing Airflow setup you can the dag with existng airflow. Follow the setp 1-4 from `Run using Docker` section.

1. Copy dag source code

```bash
cp -r ../report_generator /opt/airflow/dags
```

2. Go to airflow console and run the dag
