import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

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
    .appName("Load movie snapshot fact")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)

movies = spark.read.parquet(
    "data/processed/tmdb/movies"
)

movies = movies.select(
    "movie_id",
    "popularity",
    "budget",
)

dim_movie = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_movie",
    properties=JDBC_PROPERTIES,
).select(
    "movie_id",
    "title",
    "original_language",
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

dim_date = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_date",
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

snapshot_date = dim_date.select(
    col("date_id").alias("snapshot_date")
)

fact = fact.crossJoin(
    snapshot_date
)

fact = fact.select(
    "snapshot_date",
    "movie_id",
    "title",
    "popularity",
    "budget",
    col("actor_id").alias("main_actor_id"),
    col("actor_name").alias("main_actor_name"),
    col("genre_id"),
    col("genre_name"),
    "original_language",
)

fact.write.jdbc(
    url=JDBC_URL,
    table="fact_movie_snapshot",
    mode="append",
    properties=JDBC_PROPERTIES,
)

print(
    "Movie snapshot loaded:",
    fact.count(),
)

spark.stop()