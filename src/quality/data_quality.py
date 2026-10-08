import argparse
from datetime import date
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from src.config.settings import PROCESSED_DATA_PATH


TARGET_MOVIES_COUNT = 6000


def check_nulls(df, table_name, columns):

    print(f"\n{table_name} — NULL checks")

    failed = False

    for column in columns:

        count = (
            df
            .filter(col(column).isNull())
            .count()
        )

        print(f"{column}: {count}")

        if count > 0:
            failed = True

    return failed


def check_duplicates(
    df,
    table_name,
    columns,
):

    print(
        f"\n{table_name} — duplicate checks"
    )

    duplicates = (
        df
        .groupBy(*columns)
        .count()
        .filter(col("count") > 1)
    )

    count = duplicates.count()

    print(
        f"Дубликатов по {columns}: {count}"
    )

    return count > 0


def check_movie_ids(
    df,
    table_name,
    movies,
):

    print(
        f"\n{table_name} — movie_id integrity"
    )

    invalid = (
        df
        .select("movie_id")
        .distinct()
        .join(
            movies
            .select("movie_id")
            .distinct(),
            on="movie_id",
            how="left_anti",
        )
    )

    count = invalid.count()

    print(
        f"movie_id отсутствует в movies: "
        f"{count}"
    )

    if count > 0:

        invalid.show(
            100,
            truncate=False,
        )

    return count > 0


def check_vote_average(
    movies,
):

    invalid_count = (
        movies
        .filter(
            (col("vote_average") < 0)
            | (col("vote_average") > 10)
        )
        .count()
    )

    print(
        f"\nMOVIES — invalid vote_average: "
        f"{invalid_count}"
    )

    return invalid_count > 0


def check_vote_count(
    movies,
):

    invalid_count = (
        movies
        .filter(
            col("vote_count") < 0
        )
        .count()
    )

    print(
        f"\nMOVIES — invalid vote_count: "
        f"{invalid_count}"
    )

    return invalid_count > 0


def check_movies_count(
    movies,
):

    actual_count = movies.count()

    print(
        f"\nMOVIES — row count: "
        f"{actual_count}"
    )

    print(
        f"MOVIES — expected row count: "
        f"{TARGET_MOVIES_COUNT}"
    )

    return (
        actual_count
        != TARGET_MOVIES_COUNT
    )


def main(
    snapshot_date: date,
):

    base_path = (
        Path(PROCESSED_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    if not base_path.exists():

        raise RuntimeError(
            f"Processed directory not found: "
            f"{base_path}"
        )

    spark = (
        SparkSession.builder
        .appName("TMDB Data Quality")
        .master("local[*]")
        .getOrCreate()
    )

    movies = spark.read.parquet(
        str(
            base_path
            / "movies"
        )
    )

    movie_genres = spark.read.parquet(
        str(
            base_path
            / "movie_genres"
        )
    )

    movie_companies = spark.read.parquet(
        str(
            base_path
            / "movie_companies"
        )
    )

    movie_countries = spark.read.parquet(
        str(
            base_path
            / "movie_countries"
        )
    )

    movie_languages = spark.read.parquet(
        str(
            base_path
            / "movie_languages"
        )
    )

    movie_actors = spark.read.parquet(
        str(
            base_path
            / "movie_actors"
        )
    )

    movie_directors = spark.read.parquet(
        str(
            base_path
            / "movie_directors"
        )
    )

    failed = False

    # MOVIES

    if check_movies_count(
        movies
    ):
        failed = True

    if check_nulls(
        movies,
        "MOVIES",
        [
            "movie_id",
            "title",
        ],
    ):
        failed = True

    if check_duplicates(
        movies,
        "MOVIES",
        [
            "movie_id",
        ],
    ):
        failed = True

    if check_vote_average(
        movies
    ):
        failed = True

    if check_vote_count(
        movies
    ):
        failed = True

    # MOVIE GENRES

    if check_nulls(
        movie_genres,
        "MOVIE GENRES",
        [
            "movie_id",
            "genre_id",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_genres,
        "MOVIE GENRES",
        [
            "movie_id",
            "genre_id",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_genres,
        "MOVIE GENRES",
        movies,
    ):
        failed = True

    # MOVIE COMPANIES

    if check_nulls(
        movie_companies,
        "MOVIE COMPANIES",
        [
            "movie_id",
            "company_id",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_companies,
        "MOVIE COMPANIES",
        [
            "movie_id",
            "company_id",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_companies,
        "MOVIE COMPANIES",
        movies,
    ):
        failed = True

    # MOVIE COUNTRIES

    if check_nulls(
        movie_countries,
        "MOVIE COUNTRIES",
        [
            "movie_id",
            "country_code",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_countries,
        "MOVIE COUNTRIES",
        [
            "movie_id",
            "country_code",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_countries,
        "MOVIE COUNTRIES",
        movies,
    ):
        failed = True

    # MOVIE LANGUAGES

    if check_nulls(
        movie_languages,
        "MOVIE LANGUAGES",
        [
            "movie_id",
            "language_code",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_languages,
        "MOVIE LANGUAGES",
        [
            "movie_id",
            "language_code",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_languages,
        "MOVIE LANGUAGES",
        movies,
    ):
        failed = True

    # MOVIE ACTORS

    if check_nulls(
        movie_actors,
        "MOVIE ACTORS",
        [
            "movie_id",
            "actor_id",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_actors,
        "MOVIE ACTORS",
        [
            "movie_id",
            "actor_id",
            "character",
            "cast_order",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_actors,
        "MOVIE ACTORS",
        movies,
    ):
        failed = True

    # MOVIE DIRECTORS

    if check_nulls(
        movie_directors,
        "MOVIE DIRECTORS",
        [
            "movie_id",
            "director_id",
        ],
    ):
        failed = True

    if check_duplicates(
        movie_directors,
        "MOVIE DIRECTORS",
        [
            "movie_id",
            "director_id",
        ],
    ):
        failed = True

    if check_movie_ids(
        movie_directors,
        "MOVIE DIRECTORS",
        movies,
    ):
        failed = True

    if failed:

        print(
            f"\nDATA QUALITY FAILED "
            f"for snapshot "
            f"{snapshot_date}"
        )

        spark.stop()

        raise SystemExit(1)

    print(
        f"\nDATA QUALITY PASSED "
        f"for snapshot "
        f"{snapshot_date}"
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