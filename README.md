# Telecom USSD Events Ingestion Pipeline

**An automated telecom data ingestion pipeline built with Apache Airflow, Python, PostgreSQL, and Docker.**

## Overview

The **Telecom USSD Events Ingestion Pipeline** is a simulated telecom data engineering project designed to automate the ingestion of USSD event records from CSV files into a centralized PostgreSQL database.

In a typical telecom environment, operational systems generate event files that need to be collected, processed, and stored centrally for monitoring, reporting, and further analysis.

This project demonstrates how a scheduled ETL workflow can be orchestrated using Apache Airflow instead of relying exclusively on standalone scripts or traditional cron jobs.

The pipeline runs in a Docker-based environment and uses Python for data processing, PostgreSQL for centralized storage, and Airflow for workflow scheduling, monitoring, and task execution.

## Project Objectives

- Automate the ingestion of USSD event data from CSV files.
- Orchestrate data processing tasks using Apache Airflow.
- Count source records before database ingestion.
- Load event data into a centralized PostgreSQL database.
- Configure email notifications for pipeline execution.
- Monitor task execution and troubleshoot failures through the Airflow UI.
- Demonstrate a containerized, reproducible data engineering workflow.

## Architecture

The source system is simulated using CSV files placed in a shared directory. Apache Airflow orchestrates the ingestion workflow, while PostgreSQL stores the resulting event records.

## Technology Stack

| Technology | Purpose |
|---|---|
| Python | Data processing and ingestion logic |
| Apache Airflow 3.3.1 | Workflow orchestration and scheduling |
| PostgreSQL 16 | Centralized telecom event storage |
| Docker Compose | Containerized deployment |
| SMTP / Gmail | Email notification configuration |
| SQL | Database schema and data validation |

## Workflow

The project contains an Airflow DAG named `telecom_ussd_events_ingestion`, configured to run every five minutes.

The workflow consists of the following stages:

1. **File detection:** Identify CSV files available in the incoming data directory.
2. **Record counting:** Count the records in the source CSV files.
3. **Email notification:** Send an email notification using the configured SMTP settings.
4. **Database ingestion:** Load the CSV records into the centralized PostgreSQL database.

Airflow provides visibility into task status, execution history, logs, and failures.

> Note: The current workflow is a learning and simulation project. File-level deduplication, comprehensive data quality validation, and production-grade recovery mechanisms should be added before using it in a production environment.

## Repository Structure

```text
telecom_ussd_events_ingestion/
│
├── dags/
│   ├── telecom_ussd_events_ingestion.py
│   ├── scripts/
│   │   └── telecom_functions.py
│   └── sql/
│       └── create_tables.sql
│
├── data/
│   └── incoming/
│       └── test_ussd_events.csv
│
├── docker-compose.yaml
├── .env.example
├── .gitignore
└── README.md
```

The tree above illustrates the intended organization. Actual filenames may differ as the project evolves.

## Prerequisites

Install the following before running the project:

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) with Docker Compose support.
- [Git](https://git-scm.com/downloads) to clone the repository.
- A web browser to access the Airflow UI.
- A Gmail account and App Password if Gmail notifications are required.

Ensure Docker Desktop is running before starting the containers.

## Getting Started

### 1. Clone the repository

```bash
git clone https://github.com/saeedaly/telecom_ussd_events_ingestion.git
cd telecom_ussd_events_ingestion
```

### 2. Configure environment variables

Create a local `.env` file using the project's environment template.

If `.env.example` is available:

```bash
cp .env.example .env
```

On Windows PowerShell, you can use:

```powershell
Copy-Item .env.example .env
```

If the template does not exist yet, create `.env` based on the configuration required by `docker-compose.yaml`.

Configure the required Airflow settings and SMTP values. Use your own Gmail address and Gmail App Password for email notifications.

**Security:** Never commit actual passwords, API keys, or other secrets to GitHub. Keep `.env` local and commit only a sanitized `.env.example`.

### 3. Validate the Docker Compose configuration

```bash
docker compose config --quiet
```

If the command completes without errors, the Compose configuration is syntactically valid.

### 4. Start the services

```bash
docker compose up -d
```

The initial startup may take several minutes while Docker downloads images and Airflow initializes its metadata database.

### 5. Verify the containers

```bash
docker compose ps
```

Check that the required services are running and that their health checks pass.

If a service fails, inspect its logs:

```bash
docker compose logs --tail=100 airflow-scheduler
docker compose logs --tail=100 airflow-dag-processor
docker compose logs --tail=100 telecom-db
```

## Access the Airflow UI

Open:

**http://localhost:8080**

Sign in using the Airflow credentials configured in the Compose environment.

Then:

1. Locate `telecom_ussd_events_ingestion`.
2. Unpause the DAG if it is paused.
3. Trigger a manual run.
4. Open the Grid view to monitor task execution.
5. Inspect individual task logs if a task fails.

The DAG is configured for a five-minute schedule, but scheduled execution depends on the Airflow scheduler being healthy and the DAG being unpaused.

## Prepare Test Data

Place a CSV file in the local incoming directory, which is mounted into the Airflow containers at:

```text
/opt/airflow/data/incoming
```

For example, create `data/incoming/test_ussd_events.csv`:

```csv
event_time,msisdn,service_id_name,service,status
2026-10-25 12:00:00,201000000001,USSD_BALANCE,BALANCE,SUCCESS
2026-10-25 12:01:00,201000000002,USSD_RECHARGE,RECHARGE,SUCCESS
2026-10-25 12:02:00,201000000003,USSD_BALANCE,BALANCE,FAILED
2026-10-25 12:03:00,201000000004,USSD_BUNDLE,BUNDLE,SUCCESS
2026-10-25 12:04:00,201000000005,USSD_BALANCE,BALANCE,TIMEOUT
```

This sample contains five data records, excluding the header.

Ensure that the CSV columns match the schema and column mapping expected by the ingestion code.

## PostgreSQL Configuration

The Compose environment separates the Airflow metadata database from the telecom application database.

| Setting | Value |
|---|---|
| Database service | `telecom-db` |
| Database name | `telecom` |
| Database user | `telecom` |
| Internal PostgreSQL port | `5432` |
| Host-accessible port | `5433` |
| Expected table | `ussd_events` |

The values above describe the repository's current development configuration. Replace default credentials before any deployment beyond local testing.

### Configure the Airflow connection

In the Airflow UI, navigate to **Admin → Connections** and configure a PostgreSQL connection with:

| Field | Value |
|---|---|
| Connection ID | `postgres_conn` |
| Connection Type | Postgres |
| Host | `telecom-db` |
| Database | `telecom` |
| Username | `telecom` |
| Password | `telecom` |
| Port | `5432` |

The hostname `telecom-db` is used for connections between Docker Compose services. From the Windows host, use `localhost:5433` instead.

### Verify ingested data

Connect to PostgreSQL from PowerShell:

```powershell
docker compose exec telecom-db psql -U telecom -d telecom
```

Run the following queries:

```sql
SELECT COUNT(*)
FROM ussd_events;

SELECT *
FROM ussd_events
ORDER BY event_time DESC
LIMIT 10;
```

For a clean initial test, if the table starts empty and the sample file is successfully loaded once, the expected row count is five.

Repeated runs may insert duplicates unless deduplication is implemented.

## Email Notifications

The project supports SMTP configuration for email notifications.

For Gmail, configure the SMTP settings in the local environment:

```dotenv
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=YOUR_EMAIL@gmail.com
AIRFLOW__SMTP__SMTP_PASSWORD=YOUR_GMAIL_APP_PASSWORD
AIRFLOW__SMTP__SMTP_MAIL_FROM=YOUR_EMAIL@gmail.com
```

Use a Gmail App Password where required. Do not use your regular Gmail password.

After changing environment variables, recreate or restart the relevant Airflow services so the new configuration is applied.

Email delivery also depends on the notification task's implementation and its configured recipient address.

## Useful Docker Commands

| Command | Purpose |
|---|---|
| `docker compose up -d` | Start the project |
| `docker compose ps` | Check container status |
| `docker compose logs -f airflow-scheduler` | Follow scheduler logs |
| `docker compose logs -f telecom-db` | Follow telecom database logs |
| `docker compose restart airflow-scheduler` | Restart the scheduler |
| `docker compose down` | Stop and remove containers |
| `docker compose down -v` | Remove containers and named volumes |

**Warning:** `docker compose down -v` deletes the project's named database volumes, including stored telecom data and Airflow metadata. Use it only when you intentionally want to reset the environment.

## Troubleshooting

### DAG does not appear in Airflow

- Check that the DAG Python file is in the mounted `dags` directory.
- Inspect the DAG processor logs.
- Resolve Python import errors and syntax errors.

### CSV files are not detected

- Confirm that files exist in the local `data/incoming` directory.
- Verify the directory is mounted to `/opt/airflow/data`.
- Confirm that the code uses the same directory and file extension.

### PostgreSQL connection fails

- Verify that `telecom-db` is healthy.
- Check the `postgres_conn` connection ID and credentials.
- Use port `5432` from other containers and `5433` from the host.

### Email notifications fail

- Verify SMTP credentials and Gmail App Password.
- Confirm that the SMTP settings are loaded into Airflow.
- Inspect the notification task logs.

### Database table is missing

- Verify that `dags/sql/create_tables.sql` exists and defines the expected table.
- Remember that PostgreSQL initialization scripts run automatically only when the database data directory is initialized for the first time.
- Apply schema changes explicitly when using an existing database volume.

## Future Improvements

Potential extensions to make the pipeline more robust include:

- Detect only newly arrived files and maintain a processed-file registry.
- Validate required columns, data types, and mandatory fields.
- Compare source record counts with successfully inserted database rows.
- Add duplicate detection and idempotent ingestion.
- Move failed records into a dedicated error table.
- Archive processed files after successful ingestion.
- Add retry policies, failure alerts, and structured logging.
- Add automated tests and CI/CD checks.
- Integrate monitoring and data quality metrics.

## Learning Outcomes

This project demonstrates practical skills in:

- ETL and data pipeline design.
- Apache Airflow DAG development and scheduling.
- Python-based CSV processing.
- PostgreSQL schema design and data ingestion.
- Docker Compose orchestration.
- Service-to-service networking.
- SMTP notification configuration.
- Pipeline monitoring and troubleshooting.

## Author

**Saeed Ali**

GitHub: [@saeedaly](https://github.com/saeedaly)

## Disclaimer

This is a simulated telecom data engineering project intended for educational and demonstration purposes. The data is synthetic, and the project is not an official telecom operator system or a production-ready ingestion platform.
