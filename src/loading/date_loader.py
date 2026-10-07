import argparse
import os
from datetime import date

import psycopg
from dotenv import load_dotenv


load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")


def main(snapshot_date: date):

    with psycopg.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    ) as connection:

        with connection.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO dim_date (
                    date_id,
                    year,
                    month,
                    quarter,
                    day,
                    day_of_week
                )
                VALUES (%s, %s, %s, %s, %s, %s)

                ON CONFLICT (date_id)
                DO UPDATE SET
                    year = EXCLUDED.year,
                    month = EXCLUDED.month,
                    quarter = EXCLUDED.quarter,
                    day = EXCLUDED.day,
                    day_of_week = EXCLUDED.day_of_week;
                """,
                (
                    snapshot_date,
                    snapshot_date.year,
                    snapshot_date.month,
                    (snapshot_date.month - 1) // 3 + 1,
                    snapshot_date.day,
                    snapshot_date.isoweekday(),
                ),
            )

    print(
        "Date loaded:",
        snapshot_date,
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=date.today(),
    )

    args = parser.parse_args()

    main(
        snapshot_date=args.snapshot_date
    )