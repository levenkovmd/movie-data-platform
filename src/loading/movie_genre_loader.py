import os

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession


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


spark = (
    SparkSession.builder
    .appName("Load movie genres to DWH")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)


movie_genres = spark.read.parquet(
    "data/processed/tmdb/movie_genres"
)


movie_genres = (
    movie_genres
    .select(
        "movie_id",
        "genre_id",
    )
    .dropDuplicates(
        ["movie_id", "genre_id"]
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


# Берем только существующие жанры

dim_genre = (
    spark.read.jdbc(
        url=JDBC_URL,
        table="dim_genre",
        properties=JDBC_PROPERTIES,
    )
    .select(
        "genre_id"
    )
)


# Убираем связи с фильмами и жанрами,
# которых нет в dimensions

movie_genres = (
    movie_genres
    .join(
        dim_movie,
        on="movie_id",
        how="inner",
    )
    .join(
        dim_genre,
        on="genre_id",
        how="inner",
    )
)


relation_count = movie_genres.count()


# Загружаем актуальный набор связей
# в staging-таблицу

movie_genres.write.jdbc(
    url=JDBC_URL,
    table="stg_bridge_movie_genre",
    mode="overwrite",
    properties=JDBC_PROPERTIES,
)


# Полностью синхронизируем bridge-таблицу

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
            DELETE FROM bridge_movie_genre;
            """
        )

        cursor.execute(
            """
            INSERT INTO bridge_movie_genre (
                movie_id,
                genre_id
            )
            SELECT
                movie_id,
                genre_id
            FROM stg_bridge_movie_genre;
            """
        )

        cursor.execute(
            """
            DROP TABLE stg_bridge_movie_genre;
            """
        )


print(
    "Movie-genre relations loaded:",
    relation_count,
)


spark.stop()