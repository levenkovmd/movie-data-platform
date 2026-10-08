import argparse
import json
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode

from src.config.settings import (
    RAW_DATA_PATH,
    PROCESSED_DATA_PATH,
)


def load_movie_ids(
    snapshot_date: date,
) -> list[int]:

    manifest_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movie_ids.json"
    )

    if not manifest_path.exists():
        raise RuntimeError(
            f"Manifest not found: {manifest_path}"
        )

    with open(
        manifest_path,
        encoding="utf-8",
    ) as f:
        movie_ids = json.load(f)

    if not isinstance(movie_ids, list):
        raise RuntimeError(
            f"Invalid manifest: {manifest_path}"
        )

    if not movie_ids:
        raise RuntimeError(
            f"Manifest is empty: {manifest_path}"
        )

    if len(movie_ids) != len(set(movie_ids)):
        raise RuntimeError(
            "Manifest contains duplicate movie IDs"
        )

    return movie_ids


def main(snapshot_date: date):

    details_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "details"
    )

    processed_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    if not details_path.exists():
        raise RuntimeError(
            f"Details directory not found: {details_path}"
        )

    if not list(
        details_path.glob("movie_*.json")
    ):
        raise RuntimeError(
            f"No detail files found for snapshot "
            f"{snapshot_date}"
        )

    movie_ids = load_movie_ids(
        snapshot_date
    )

    print(
        f"Movies in manifest: {len(movie_ids)}"
    )

    spark = (
        SparkSession.builder
        .appName("TMDB Movies Transform")
        .master("local[*]")
        .getOrCreate()
    )

    details = (
        spark.read
        .option("multiLine", True)
        .json(str(details_path))
    )

    print("SOURCE SCHEMA")
    details.printSchema()

    manifest = spark.createDataFrame(
        [
            (movie_id,)
            for movie_id in movie_ids
        ],
        ["movie_id"],
    )

    details = (
        details
        .join(
            manifest,
            details["id"]
            == manifest["movie_id"],
            "inner",
        )
        .drop(
            manifest["movie_id"]
        )
    )

    print(
        "Details after manifest filter:",
        details.count(),
    )

    movies = details.select(
        col("id").alias("movie_id"),
        col("title"),
        col("original_title"),
        col("original_language"),
        col("release_date"),
        col("budget"),
        col("revenue"),
        col("runtime"),
        col("popularity"),
        col("vote_average"),
        col("vote_count"),
        col("status"),
        col("imdb_id"),
        col("overview"),
        col("tagline"),
        col("poster_path"),
        col("backdrop_path"),
        col("adult"),
        col("video"),
    )

    movies.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movies"
        )
    )

    movie_genres = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("genres").alias("genre"),
        )
        .select(
            col("movie_id"),
            col("genre.id").alias("genre_id"),
            col("genre.name").alias("genre_name"),
        )
    )

    movie_genres.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_genres"
        )
    )

    movie_companies = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("production_companies").alias(
                "company"
            ),
        )
        .select(
            col("movie_id"),
            col("company.id").alias("company_id"),
            col("company.name").alias(
                "company_name"
            ),
            col("company.origin_country").alias(
                "origin_country"
            ),
        )
        .dropDuplicates(
            ["movie_id", "company_id"]
        )
    )

    movie_companies.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_companies"
        )
    )

    movie_countries = (
        details
        .select(
            col("id").alias("movie_id"),
            explode(
                "production_countries"
            ).alias("country"),
        )
        .select(
            col("movie_id"),
            col("country.iso_3166_1").alias(
                "country_code"
            ),
            col("country.name").alias(
                "country_name"
            ),
        )
    )

    movie_countries.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_countries"
        )
    )

    movie_languages = (
        details
        .select(
            col("id").alias("movie_id"),
            explode(
                "spoken_languages"
            ).alias("language"),
        )
        .select(
            col("movie_id"),
            col("language.iso_639_1").alias(
                "language_code"
            ),
            col("language.name").alias(
                "language_name"
            ),
            col("language.english_name").alias(
                "english_name"
            ),
        )
    )

    movie_languages.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_languages"
        )
    )

    print("\nTRANSFORMATION FINISHED")
    print(
        f"Snapshot date: {snapshot_date}"
    )

    print("\nMovies:")
    movies.show(
        5,
        truncate=False,
    )

    print("\nMovie genres:")
    movie_genres.show(
        10,
        truncate=False,
    )

    print("\nMovie companies:")
    movie_companies.show(
        10,
        truncate=False,
    )

    print("\nMovie countries:")
    movie_countries.show(
        10,
        truncate=False,
    )

    print("\nMovie languages:")
    movie_languages.show(
        10,
        truncate=False,
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