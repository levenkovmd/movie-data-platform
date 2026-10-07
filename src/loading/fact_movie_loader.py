import argparse
import os
from datetime import date
from pathlib import Path

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, row_number
from pyspark.sql.window import Window

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
        .appName("Load movie snapshot fact")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.postgresql:postgresql:42.7.8",
        )
        .getOrCreate()
    )

    movies = (
        spark.read
        .parquet(str(movies_path))
        .select(
            "movie_id",
            "popularity",
            "vote_average",
            "vote_count",
            "budget",
        )
    )

    dim_movie = (
        spark.read.jdbc(
            url=JDBC_URL,
            table="dim_movie",
            properties=JDBC_PROPERTIES,
        )
        .select(
            "movie_id",
            "title",
            "original_language",
        )
    )

    dim_actor = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_actor",
        properties=JDBC_PROPERTIES,
    )

    dim_genre = spark.read.jdbc(
        url=JDBC_URL,
        table="dim_genre",
        properties=JDBC_PROPERTIES,
    )

    bridge_movie_actor = spark.read.jdbc(
        url=JDBC_URL,
        table="bridge_movie_actor",
        properties=JDBC_PROPERTIES,
    )

    bridge_movie_genre = spark.read.jdbc(
        url=JDBC_URL,
        table="bridge_movie_genre",
        properties=JDBC_PROPERTIES,
    )

    actor_window = (
        Window
        .partitionBy("movie_id")
        .orderBy(col("cast_order"))
    )

    main_actor = (
        bridge_movie_actor
        .withColumn(
            "rn",
            row_number().over(actor_window),
        )
        .filter(
            col("rn") == 1
        )
        .select(
            "movie_id",
            "actor_id",
        )
    )

    genre_window = (
        Window
        .partitionBy("movie_id")
        .orderBy(col("genre_id"))
    )

    main_genre = (
        bridge_movie_genre
        .withColumn(
            "rn",
            row_number().over(genre_window),
        )
        .filter(
            col("rn") == 1
        )
        .select(
            "movie_id",
            "genre_id",
        )
    )

    fact = (
        movies
        .join(
            dim_movie,
            on="movie_id",
            how="inner",
        )
        .join(
            main_actor,
            on="movie_id",
            how="left",
        )
        .join(
            dim_actor,
            on="actor_id",
            how="left",
        )
        .join(
            main_genre,
            on="movie_id",
            how="left",
        )
        .join(
            dim_genre,
            on="genre_id",
            how="left",
        )
    )

    fact = fact.withColumn(
        "snapshot_date",
        lit(snapshot_date.isoformat()).cast("date"),
    )

    fact = fact.select(
        "snapshot_date",
        "movie_id",
        "title",
        "popularity",
        "vote_average",
        "vote_count",
        "budget",
        col("actor_id").alias("main_actor_id"),
        col("actor_name").alias("main_actor_name"),
        "genre_id",
        "genre_name",
        "original_language",
    )

    fact_count = fact.count()

    # Сначала загружаем snapshot во staging

    fact.write.jdbc(
        url=JDBC_URL,
        table="stg_fact_movie_snapshot",
        mode="overwrite",
        properties=JDBC_PROPERTIES,
    )

    # Затем атомарно заменяем snapshot нужной даты

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
                DELETE FROM fact_movie_snapshot
                WHERE snapshot_date = %s;
                """,
                (snapshot_date,),
            )

            cursor.execute(
                """
                INSERT INTO fact_movie_snapshot (
                    snapshot_date,
                    movie_id,
                    title,
                    popularity,
                    vote_average,
                    vote_count,
                    budget,
                    main_actor_id,
                    main_actor_name,
                    genre_id,
                    genre_name,
                    original_language
                )
                SELECT
                    snapshot_date,
                    movie_id,
                    title,
                    popularity,
                    vote_average,
                    vote_count,
                    budget,
                    main_actor_id,
                    main_actor_name,
                    genre_id,
                    genre_name,
                    original_language
                FROM stg_fact_movie_snapshot
                WHERE snapshot_date = %s;
                """,
                (snapshot_date,),
            )

            cursor.execute(
                """
                DROP TABLE stg_fact_movie_snapshot;
                """
            )

    print(
        "Movie snapshot loaded:",
        snapshot_date,
        fact_count,
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