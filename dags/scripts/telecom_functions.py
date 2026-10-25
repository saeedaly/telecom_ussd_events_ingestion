from airflow.providers.postgres.hooks.postgres import PostgresHook
from pathlib import Path
import csv

INCOMING_DIR = Path("/opt/airflow/data/incoming")



def detect_new_file():
    if not INCOMING_DIR.is_dir():
        raise FileNotFoundError(f"Directory not found: {INCOMING_DIR}")
    csv_files = sorted(INCOMING_DIR.glob("*.csv"))
    if not csv_files:
        print("No CSV files found.")
        raise AirflowSkipException("No CSV files to process.")
    file_names = [file.name for file in csv_files]
    print(f"CSV files detected: {file_names}")
    # Return filenames through XCom
    return file_names


def count_csv_records(ti):
    # Retrieve filenames from the detection task
    file_names = ti.xcom_pull(task_ids="detect_data")
    if not file_names:
        raise AirflowSkipException("No files to count.")
    total_records = 0
    for file_name in file_names:
        file_path = INCOMING_DIR / file_name
        with file_path.open("r", newline="", encoding="utf-8-sig") as file:
            reader = csv.reader(file)
            next(reader, None)  # Skip header
            count = sum(1 for row in reader if row)
        print(f"{file_name}: {count} records")
        total_records += count
    print(f"Total CSV records: {total_records}")
    # Return counts through XCom
    return total_records

def load__csv_to_postgres(ti):
    file_names = ti.xcom_pull(task_ids="detect_data")
    if not file_names:
        raise AirflowSkipException("No files to load.")
    columns = ["event_time", "msisdn", "service_id_name", "service", "status", "source_file"]
    hook = PostgresHook(postgres_conn_id="postgres_conn")
    for file_name in file_names:
        file_path = INCOMING_DIR / file_name
        with file_path.open("r", newline="",encoding="utf-8") as file:
            rows = list(csv.DictReader(file))
        values = []
        for row in rows:
            values.append((
                row["event_time"],
                row["msisdn"],
                row["service_id_name"],
                row["service"],
                row["status"],
                file_name,
            ))
        hook.insert_rows(table="ussd_events", rows=values, target_fields=columns)

