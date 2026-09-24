from pyspark.sql import SparkSession
from pyspark.sql.functions import col


BASE_PATH = "data/processed/tmdb"


def check_nulls(df, table_name, columns):
    print(f"\n{table_name} — NULL checks")

    failed = False

    for column in columns:
        count = df.filter(
            col(column).isNull()
        ).count()

        print(f"{column}: {count}")

        if count > 0:
            failed = True

    return failed


def check_duplicates(df, table_name, columns):
    print(f"\n{table_name} — duplicate checks")

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


def check_movie_ids(df, table_name, movies):
    print(f"\n{table_name} — movie_id integrity")

    invalid = (
        df
        .select("movie_id")
        .distinct()
        .join(
            movies
            .select("movie_id")
            .distinct(),
            on="movie_id",
            how="left_anti"
        )
    )

    count = invalid.count()

    print(
        f"movie_id отсутствует в movies: {count}"
    )

    if count > 0:
        invalid.show(
            100,
            truncate=False
        )

    return count > 0


def check_vote_average(movies):
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


def main():

    spark = (
        SparkSession.builder
        .appName("TMDB Data Quality")
        .master("local[*]")
        .getOrCreate()
    )

    movies = spark.read.parquet(
        f"{BASE_PATH}/movies"
    )

    movie_genres = spark.read.parquet(
        f"{BASE_PATH}/movie_genres"
    )

    movie_companies = spark.read.parquet(
        f"{BASE_PATH}/movie_companies"
    )

    movie_countries = spark.read.parquet(
        f"{BASE_PATH}/movie_countries"
    )

    movie_languages = spark.read.parquet(
        f"{BASE_PATH}/movie_languages"
    )

    movie_actors = spark.read.parquet(
        f"{BASE_PATH}/movie_actors"
    )

    movie_directors = spark.read.parquet(
        f"{BASE_PATH}/movie_directors"
    )

    failed = False

    if check_nulls(
        movies,
        "MOVIES",
        [
            "movie_id",
            "title"
        ]
    ):
        failed = True

    if check_duplicates(
        movies,
        "MOVIES",
        [
            "movie_id"
        ]
    ):
        failed = True

    if check_vote_average(movies):
        failed = True

    if check_nulls(
        movie_genres,
        "MOVIE GENRES",
        [
            "movie_id",
            "genre_id"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_genres,
        "MOVIE GENRES",
        [
            "movie_id",
            "genre_id"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_genres,
        "MOVIE GENRES",
        movies
    ):
        failed = True

    if check_nulls(
        movie_companies,
        "MOVIE COMPANIES",
        [
            "movie_id",
            "company_id"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_companies,
        "MOVIE COMPANIES",
        [
            "movie_id",
            "company_id"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_companies,
        "MOVIE COMPANIES",
        movies
    ):
        failed = True

    if check_nulls(
        movie_countries,
        "MOVIE COUNTRIES",
        [
            "movie_id",
            "country_code"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_countries,
        "MOVIE COUNTRIES",
        [
            "movie_id",
            "country_code"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_countries,
        "MOVIE COUNTRIES",
        movies
    ):
        failed = True

    if check_nulls(
        movie_languages,
        "MOVIE LANGUAGES",
        [
            "movie_id",
            "language_code"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_languages,
        "MOVIE LANGUAGES",
        [
            "movie_id",
            "language_code"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_languages,
        "MOVIE LANGUAGES",
        movies
    ):
        failed = True

    if check_nulls(
        movie_actors,
        "MOVIE ACTORS",
        [
            "movie_id",
            "actor_id"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_actors,
        "MOVIE ACTORS",
        [
            "movie_id",
            "actor_id",
            "character",
            "cast_order"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_actors,
        "MOVIE ACTORS",
        movies
    ):
        failed = True

    if check_nulls(
        movie_directors,
        "MOVIE DIRECTORS",
        [
            "movie_id",
            "director_id"
        ]
    ):
        failed = True

    if check_duplicates(
        movie_directors,
        "MOVIE DIRECTORS",
        [
            "movie_id",
            "director_id"
        ]
    ):
        failed = True

    if check_movie_ids(
        movie_directors,
        "MOVIE DIRECTORS",
        movies
    ):
        failed = True

    if failed:
        print("\nDATA QUALITY FAILED")

        spark.stop()

        raise SystemExit(1)

    print("\nDATA QUALITY PASSED")

    spark.stop()


if __name__ == "__main__":
    main()