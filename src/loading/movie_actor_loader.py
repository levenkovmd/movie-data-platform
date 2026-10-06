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
    .appName("Load movie actors to DWH")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)


movie_actors = spark.read.parquet(
    "data/processed/tmdb/movie_actors"
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


# Сначала сохраняем актуальный набор связей
# во staging-таблицу

movie_actors.write.jdbc(
    url=JDBC_URL,
    table="stg_bridge_movie_actor",
    mode="overwrite",
    properties=JDBC_PROPERTIES,
)


# Полностью синхронизируем bridge с текущими данными

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
    relation_count,
)


spark.stop()