import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession

from src.config.settings import PROCESSED_DATA_PATH


def main(snapshot_date: date):

    processed_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    if not processed_path.exists():
        raise RuntimeError(
            f"Processed directory not found: {processed_path}"
        )

    spark = (
        SparkSession.builder
        .appName("Check processed data")
        .master("local[*]")
        .getOrCreate()
    )

    movies = spark.read.parquet(
        str(processed_path / "movies")
    )

    print("MOVIES")
    movies.printSchema()
    print(
        "Количество фильмов:",
        movies.count(),
    )

    genres = spark.read.parquet(
        str(processed_path / "movie_genres")
    )

    print("\nMOVIE GENRES")
    genres.printSchema()
    print(
        "Количество связей фильм-жанр:",
        genres.count(),
    )

    companies = spark.read.parquet(
        str(processed_path / "movie_companies")
    )

    print("\nMOVIE COMPANIES")
    companies.printSchema()
    companies.show(
        10,
        truncate=False,
    )
    print(
        "Количество связей фильм-компания:",
        companies.count(),
    )

    countries = spark.read.parquet(
        str(processed_path / "movie_countries")
    )

    print("\nMOVIE COUNTRIES")
    countries.printSchema()
    countries.show(
        10,
        truncate=False,
    )
    print(
        "Количество связей фильм-страна:",
        countries.count(),
    )

    languages = spark.read.parquet(
        str(processed_path / "movie_languages")
    )

    print("\nMOVIE LANGUAGES")
    languages.printSchema()
    languages.show(
        10,
        truncate=False,
    )
    print(
        "Количество связей фильм-язык:",
        languages.count(),
    )

    actors = spark.read.parquet(
        str(processed_path / "movie_actors")
    )

    print("\nMOVIE ACTORS")
    actors.printSchema()
    actors.show(
        10,
        truncate=False,
    )
    print(
        "Количество связей фильм-актер:",
        actors.count(),
    )

    directors = spark.read.parquet(
        str(processed_path / "movie_directors")
    )

    print("\nMOVIE DIRECTORS")
    directors.printSchema()
    directors.show(
        10,
        truncate=False,
    )
    print(
        "Количество связей фильм-режиссер:",
        directors.count(),
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