import argparse
import csv
import random
import time
from datetime import datetime
from pathlib import Path


DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "incoming"

FIELDNAMES = [
    "event_time",
    "msisdn",
    "service_id_name",
    "service",
    "status",
]


def generate_events():
    return {
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "msisdn": f"201277{random.randint(10000000, 99999999)}",
        "service_id_name": random.choices(population=['#11#', '#22#', '#33#'],weights=[0.60, 0.30, 0.10],k=1,)[0],
        "service": 'USSD',
        "status": random.choices(population=['Succes', 'Failed', 'Timeout'],weights=[0.60, 0.30, 0.10],k=1,)[0],
    }


def generate_file(rows=1000):
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    final_path = DATA_DIR / f"charging_cdr_{timestamp}.csv"
    temporary_path = DATA_DIR / f".{final_path.name}.tmp"

    with temporary_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=FIELDNAMES)
        writer.writeheader()

        for _ in range(rows):
            writer.writerow(generate_events())

    # Publish the file only after it has been completely written.
    temporary_path.replace(final_path)

    print(f"Generated: {final_path}")
    print(f"Expected records: {rows}")

    return final_path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--rows",
        type=int,
        default=1000,
        help="Number of CDR records per file",
    )
    parser.add_argument(
        "--hourly",
        action="store_true",
        help="Generate a new file every hour",
    )

    args = parser.parse_args()

    if args.rows < 1:
        parser.error("--rows must be at least 1")

    if not args.hourly:
        generate_file(args.rows)
        return

    while True:
        generate_file(args.rows)
        print("Waiting 1 hour...")
        time.sleep(3600)


if __name__ == "__main__":
    main()
