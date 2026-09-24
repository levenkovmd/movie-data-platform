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

movie_actors = movie_actors.select(
    "movie_id",
    "actor_id",
    "character",
    "cast_order",
).dropDuplicates(
    ["movie_id", "actor_id", "character", "cast_order"]
)

dim_movie = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_movie",
    properties=JDBC_PROPERTIES,
).select(
    "movie_id"
)

dim_actor = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_actor",
    properties=JDBC_PROPERTIES,
).select(
    "actor_id"
)

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

movie_actors.write.jdbc(
    url=JDBC_URL,
    table="bridge_movie_actor",
    mode="append",
    properties=JDBC_PROPERTIES,
)

print(
    "Movie-actor relations loaded:",
    movie_actors.count(),
)

spark.stop()