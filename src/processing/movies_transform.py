from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode


DETAILS_PATH = "data/raw/tmdb/details"
PROCESSED_PATH = "data/processed/tmdb"


def main():
    spark = (
        SparkSession.builder
        .appName("TMDB Movies Transform")
        .master("local[*]")
        .getOrCreate()
    )

    details = (
        spark.read
        .option("multiLine", True)
        .json(DETAILS_PATH)
    )

    print("SOURCE SCHEMA")
    details.printSchema()

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
        col("video")
    )

    movies.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movies"
    )

    movie_genres = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("genres").alias("genre")
        )
        .select(
            col("movie_id"),
            col("genre.id").alias("genre_id"),
            col("genre.name").alias("genre_name")
        )
    )

    movie_genres.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_genres"
    )

    movie_companies = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("production_companies").alias("company")
        )
        .select(
            col("movie_id"),
            col("company.id").alias("company_id"),
            col("company.name").alias("company_name"),
            col("company.origin_country").alias(
                "origin_country"
            )
        )
    )

    movie_companies.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_companies"
    )

    movie_countries = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("production_countries").alias(
                "country"
            )
        )
        .select(
            col("movie_id"),
            col("country.iso_3166_1").alias(
                "country_code"
            ),
            col("country.name").alias(
                "country_name"
            )
        )
    )

    movie_countries.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_countries"
    )

    movie_languages = (
        details
        .select(
            col("id").alias("movie_id"),
            explode("spoken_languages").alias(
                "language"
            )
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
            )
        )
    )

    movie_languages.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_languages"
    )


    print("\nTRANSFORMATION FINISHED")

    print("\nMovies:")
    movies.show(5, truncate=False)

    print("\nMovie genres:")
    movie_genres.show(10, truncate=False)

    print("\nMovie companies:")
    movie_companies.show(10, truncate=False)

    print("\nMovie countries:")
    movie_countries.show(10, truncate=False)

    print("\nMovie languages:")
    movie_languages.show(10, truncate=False)


    spark.stop()


if __name__ == "__main__":
    main()