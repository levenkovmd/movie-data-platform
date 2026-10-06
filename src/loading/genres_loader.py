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
    .appName("Load genres to DWH")
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


# Загружаем во staging-таблицу

genres.write.jdbc(
    url=JDBC_URL,
    table="stg_dim_genre",
    mode="overwrite",
    properties=JDBC_PROPERTIES,
)


# Переносим в основную dimension

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
    genre_count,
)


spark.stop()