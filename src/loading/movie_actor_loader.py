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

    movie_actors_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movie_actors"
    )

    if not movie_actors_path.exists():
        raise RuntimeError(
            f"Movie actors directory not found: {movie_actors_path}"
        )

    spark = (
        SparkSession.builder
        .appName("Load movie actors to DWH")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.8",
        )
        .getOrCreate()
    )

    movie_actors = spark.read.parquet(
        str(movie_actors_path)
    )

    movie_actors = (
        movie_actors
        .select(
            "movie_id",
            "actor_id",
            "character",
            "cast_order",
        )
        .dropDuplicates(
            [
                "movie_id",
                "actor_id",
                "character",
                "cast_order",
            ]
        )
    )

    # Берем только существующие фильмы

    dim_movie = (
        spark.read.jdbc(
            url=JDBC_URL,
            table="dim_movie",
            properties=JDBC_PROPERTIES,
        )
        .select(
            "movie_id"
        )
    )

    # Берем только существующих актеров

    dim_actor = (
        spark.read.jdbc(
            url=JDBC_URL,
            table="dim_actor",
            properties=JDBC_PROPERTIES,
        )
        .select(
            "actor_id"
        )
    )

    # Убираем связи с фильмами и актерами,
    # которых нет в dimensions

    movie_actors = (
        movie_actors
        .join(
            dim_movie,
            on="movie_id",
            how="inner",
        )
        .join(
            dim_actor,
            on="actor_id",
            how="inner",
        )
    )

    relation_count = movie_actors.count()

    # Сохраняем актуальный набор связей
    # во staging-таблицу

    movie_actors.write.jdbc(
        url=JDBC_URL,
        table="stg_bridge_movie_actor",
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # Полностью синхронизируем bridge
    # с состоянием выбранного snapshot

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
                DELETE FROM bridge_movie_actor;
                """
            )

            cursor.execute(
                """
                INSERT INTO bridge_movie_actor (
                    movie_id,
                    actor_id,
                    character,
                    cast_order
                )
                SELECT
                    movie_id,
                    actor_id,
                    character,
                    cast_order
                FROM stg_bridge_movie_actor;
                """
            )

            cursor.execute(
                """
                DROP TABLE stg_bridge_movie_actor;
                """
            )

    print(
        "Movie-actor relations loaded:",
        snapshot_date,
        relation_count,
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