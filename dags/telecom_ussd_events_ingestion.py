from airflow import DAG
from datetime import timedelta
import pendulum
from airflow.operators.python import PythonOperator
from airflow.providers.smtp.operators.smtp import EmailOperator
from scripts.telecom_functions import *


with DAG (
    dag_id="telecom_ussd_events_ingestion",
    dag_display_name="ussd_events",
    description="telecom_ussd_events_ingestion",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026,10,15,tz="Africa/Cairo"),
    end_date=None,
    catchup=False,
    default_args={"owner": "data_engineering","retries": 2,"retry_delay": timedelta(minutes=2),"depends_on_past": False,},
    tags=["etl","data-engineering","production"],

) as dag:

    detect_new_data=PythonOperator(
        task_id="detect_data",
        python_callable=detect_new_file
    )

    count_records = PythonOperator(
        task_id="count",
        python_callable=count_csv_records,
    )

    send_email = EmailOperator(
        task_id="send_test_email",
        conn_id="test_stmp",
        to="YOUR_EMAIL@gmail.com",
        subject="New USSD File Arrived",
        html_content="""
        <h2>Airflow detected CSV files.</h2>
        <p>Detected files:</p>
        <p>{{ ti.xcom_pull(task_ids='detect_data') }}</p>
        <p><b>Total CSV records:</b></p>
        <h3>{{ ti.xcom_pull(task_ids='count') }}</h3>
        """,
    )

    load_data = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load__csv_to_postgres,
)

    detect_new_data >> count_records >> send_email >> load_data


