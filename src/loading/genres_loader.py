import argparse
import os
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.config.settings import PROCESSED_DATA_PATH


load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

JDBC_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:"
    f"{POSTGRES_PORT}/{POSTGRES_DB}"
)

JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}


def main(snapshot_date: date):

    movie_genres_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movie_genres"
    )

    if not movie_genres_path.exists():
        raise RuntimeError(
            f"Movie genres directory not found: {movie_genres_path}"
        )

    spark = (
        SparkSession.builder
        .appName("Load genres to DWH")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.8",
        )
        .getOrCreate()
    )

    movie_genres = spark.read.parquet(
        str(movie_genres_path)
    )

    genres = (
        movie_genres
        .select(
            "genre_id",
            "genre_name",
        )
        .dropDuplicates(
            ["genre_id"]
        )
    )

    genre_count = genres.count()

    # Загружаем актуальные данные во staging-таблицу

    genres.write.jdbc(
        url=JDBC_URL,
        table="stg_dim_genre",
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # Добавляем новые жанры и обновляем существующие

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
                INSERT INTO dim_genre (
                    genre_id,
                    genre_name
                )
                SELECT
                    genre_id,
                    genre_name
                FROM stg_dim_genre

                ON CONFLICT (genre_id)
                DO UPDATE SET
                    genre_name = EXCLUDED.genre_name;
                """
            )

            cursor.execute(
                """
                DROP TABLE stg_dim_genre;
                """
            )

    print(
        "Genres loaded:",
        snapshot_date,
        genre_count,
    )

    spark.stop()


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