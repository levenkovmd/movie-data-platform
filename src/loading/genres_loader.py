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

genres = movie_genres.select(
    "genre_id",
    "genre_name",
).dropDuplicates(
    ["genre_id"]
)

genres.write.jdbc(
    url=JDBC_URL,
    table="dim_genre",
    mode="append",
    properties=JDBC_PROPERTIES,
)


print("Genres loaded:", genres.count())


spark.stop()