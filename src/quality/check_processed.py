from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("Check processed data")
    .master("local[*]")
    .getOrCreate()
)


movies = spark.read.parquet(
    "data/processed/tmdb/movies"
)

print("MOVIES")
movies.printSchema()
print("Количество фильмов:", movies.count())


genres = spark.read.parquet(
    "data/processed/tmdb/movie_genres"
)

print("\nMOVIE GENRES")
genres.printSchema()
print("Количество связей фильм-жанр:", genres.count())


companies = spark.read.parquet(
    "data/processed/tmdb/movie_companies"
)

print("\nMOVIE COMPANIES")
companies.printSchema()
companies.show(10, truncate=False)
print("Количество связей фильм-компания:", companies.count())


countries = spark.read.parquet(
    "data/processed/tmdb/movie_countries"
)

print("\nMOVIE COUNTRIES")
countries.printSchema()
countries.show(10, truncate=False)
print("Количество связей фильм-страна:", countries.count())


languages = spark.read.parquet(
    "data/processed/tmdb/movie_languages"
)

print("\nMOVIE LANGUAGES")
languages.printSchema()
languages.show(10, truncate=False)
print("Количество связей фильм-язык:", languages.count())

actors = spark.read.parquet(
    "data/processed/tmdb/movie_actors"
)

print("\nMOVIE ACTORS")

actors.printSchema()

actors.show(
    10,
    truncate=False
)

print(
    "Количество связей фильм-актер:",
    actors.count()
)


directors = spark.read.parquet(
    "data/processed/tmdb/movie_directors"
)

print("\nMOVIE DIRECTORS")

directors.printSchema()

directors.show(
    10,
    truncate=False
)

print(
    "Количество связей фильм-режиссер:",
    directors.count()
)

spark.stop()