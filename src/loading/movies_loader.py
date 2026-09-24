import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, to_date, when

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
    .appName("Load movies to DWH")
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

movies.write.jdbc(
    url=JDBC_URL,
    table="dim_movie",
    mode="append",
    properties=JDBC_PROPERTIES,
)


print("Movies loaded:", movies.count())


spark.stop()