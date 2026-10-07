import argparse
import os
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, when

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

    movies_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movies"
    )

    if not movies_path.exists():
        raise RuntimeError(
            f"Movies directory not found: {movies_path}"
        )

    spark = (
        SparkSession.builder
        .appName("Load movies to DWH")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.8",
        )
        .getOrCreate()
    )

    movies = spark.read.parquet(
        str(movies_path)
    )

    movies = movies.withColumn(
        "release_date",
        to_date(
            when(
                trim(col("release_date")) != "",
                trim(col("release_date")),
            ),
            "yyyy-MM-dd",
        ),
    )

    movies = movies.select(
        "movie_id",
        "title",
        "original_title",
        "original_language",
        "release_date",
        "runtime",
        "status",
        "imdb_id",
        "overview",
        "tagline",
        "poster_path",
        "backdrop_path",
        "adult",
        "video",
    )

    movie_count = movies.count()

    # Загружаем актуальные данные во staging-таблицу

    movies.write.jdbc(
        url=JDBC_URL,
        table="stg_dim_movie",
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # Добавляем новые фильмы и обновляем существующие

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
                INSERT INTO dim_movie (
                    movie_id,
                    title,
                    original_title,
                    original_language,
                    release_date,
                    runtime,
                    status,
                    imdb_id,
                    overview,
                    tagline,
                    poster_path,
                    backdrop_path,
                    adult,
                    video
                )
                SELECT
                    movie_id,
                    title,
                    original_title,
                    original_language,
                    release_date,
                    runtime,
                    status,
                    imdb_id,
                    overview,
                    tagline,
                    poster_path,
                    backdrop_path,
                    adult,
                    video
                FROM stg_dim_movie

                ON CONFLICT (movie_id)
                DO UPDATE SET
                    title = EXCLUDED.title,
                    original_title = EXCLUDED.original_title,
                    original_language = EXCLUDED.original_language,
                    release_date = EXCLUDED.release_date,
                    runtime = EXCLUDED.runtime,
                    status = EXCLUDED.status,
                    imdb_id = EXCLUDED.imdb_id,
                    overview = EXCLUDED.overview,
                    tagline = EXCLUDED.tagline,
                    poster_path = EXCLUDED.poster_path,
                    backdrop_path = EXCLUDED.backdrop_path,
                    adult = EXCLUDED.adult,
                    video = EXCLUDED.video;
                """
            )

            cursor.execute(
                """
                DROP TABLE stg_dim_movie;
                """
            )

    print(
        "Movies loaded:",
        snapshot_date,
        movie_count,
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