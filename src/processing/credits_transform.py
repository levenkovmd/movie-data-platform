from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lower


CREDITS_PATH = "data/raw/tmdb/credits"
PROCESSED_PATH = "data/processed/tmdb"


def main():
    spark = (
        SparkSession.builder
        .appName("TMDB Credits Transform")
        .master("local[*]")
        .getOrCreate()
    )

    credits = (
        spark.read
        .option("multiLine", True)
        .json(CREDITS_PATH)
    )

    print("SOURCE SCHEMA")
    credits.printSchema()

    movie_actors = (
        credits
        .select(
            col("id").alias("movie_id"),
            explode("cast").alias("actor")
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
            )
        )
    )

    movie_actors.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_actors"
    )

    movie_directors = (
        credits
        .select(
            col("id").alias("movie_id"),
            explode("crew").alias("crew_member")
        )
        .filter(
            lower(col("crew_member.job")) == "director"
        )
        .select(
            col("movie_id"),
            col("crew_member.id").alias("director_id"),
            col("crew_member.name").alias("director_name"),
            col("crew_member.department").alias(
                "department"
            ),
            col("crew_member.job").alias("job")
        )
    )

    movie_directors.write.mode("overwrite").parquet(
        f"{PROCESSED_PATH}/movie_directors"
    )


    print("\nTRANSFORMATION FINISHED")


    print("\nMovie actors:")
    movie_actors.show(20, truncate=False)

    print("\nMovie directors:")
    movie_directors.show(20, truncate=False)


    print("\nCounts:")

    print(
        "Actor relations:",
        movie_actors.count()
    )

    print(
        "Director relations:",
        movie_directors.count()
    )


    spark.stop()


if __name__ == "__main__":
    main()