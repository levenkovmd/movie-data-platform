import argparse
import json
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower

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


def main(
    snapshot_date: date,
):

    credits_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "credits"
    )

    processed_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    if not credits_path.exists():
        raise RuntimeError(
            f"Credits directory not found: {credits_path}"
        )

    if not list(
        credits_path.glob("movie_*.json")
    ):
        raise RuntimeError(
            f"No credits files found for snapshot "
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
        .appName("TMDB Credits Transform")
        .master("local[*]")
        .getOrCreate()
    )

    credits = (
        spark.read
        .option("multiLine", True)
        .json(str(credits_path))
    )

    print("SOURCE SCHEMA")
    credits.printSchema()

    manifest = spark.createDataFrame(
        [
            (movie_id,)
            for movie_id in movie_ids
        ],
        ["movie_id"],
    )

    credits = (
        credits
        .join(
            manifest,
            credits["id"]
            == manifest["movie_id"],
            "inner",
        )
        .drop(
            manifest["movie_id"]
        )
    )

    print(
        "Credits after manifest filter:",
        credits.count(),
    )

    movie_actors = (
        credits
        .select(
            col("id").alias("movie_id"),
            explode("cast").alias("actor"),
        )
        .select(
            col("movie_id"),
            col("actor.id").alias("actor_id"),
            col("actor.name").alias("actor_name"),
            col("actor.character").alias("character"),
            col("actor.order").alias("cast_order"),
            col("actor.gender").alias("gender"),
            col("actor.known_for_department").alias(
                "known_for_department"
            ),
        )
    )

    movie_actors.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_actors"
        )
    )

    movie_directors = (
        credits
        .select(
            col("id").alias("movie_id"),
            explode("crew").alias("crew_member"),
        )
        .filter(
            lower(
                col("crew_member.job")
            )
            == "director"
        )
        .select(
            col("movie_id"),
            col("crew_member.id").alias(
                "director_id"
            ),
            col("crew_member.name").alias(
                "director_name"
            ),
            col("crew_member.department").alias(
                "department"
            ),
            col("crew_member.job").alias(
                "job"
            ),
        )
    )

    movie_directors.write.mode(
        "overwrite"
    ).parquet(
        str(
            processed_path
            / "movie_directors"
        )
    )

    print("\nTRANSFORMATION FINISHED")
    print(
        f"Snapshot date: {snapshot_date}"
    )

    print("\nMovie actors:")
    movie_actors.show(
        20,
        truncate=False,
    )

    print("\nMovie directors:")
    movie_directors.show(
        20,
        truncate=False,
    )

    print("\nCounts:")

    print(
        "Actor relations:",
        movie_actors.count(),
    )

    print(
        "Director relations:",
        movie_directors.count(),
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