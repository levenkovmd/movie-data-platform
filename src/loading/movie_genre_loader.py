import os
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

dim_movie = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_movie",
    properties=JDBC_PROPERTIES,
).select(
    "movie_id"
)

dim_genre = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_genre",
    properties=JDBC_PROPERTIES,
).select(
    "genre_id"
)

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

movie_genres.write.jdbc(
    url=JDBC_URL,
    table="bridge_movie_genre",
    mode="append",
    properties=JDBC_PROPERTIES,
)

print(
    "Movie-genre relations loaded:",
    movie_genres.count(),
)

spark.stop()