from pyspark.sql import SparkSession

BASE_PATH = "data/processed/tmdb"

def deduplicate(
    spark,
    input_path,
    output_path,
    keys
):
    df = spark.read.parquet(input_path)

    before = df.count()

    df_clean = df.dropDuplicates(keys)

    after = df_clean.count()

    removed = before - after

    print(
        f"\n{input_path}"
    )
    print(f"До дедупликации: {before}")
    print(f"После дедупликации: {after}")
    print(f"Удалено дубликатов: {removed}")

    df_clean.write.mode("overwrite").parquet(
        output_path
    )


def main():

    spark = (
        SparkSession.builder
        .appName("TMDB Deduplication")
        .master("local[*]")
        .getOrCreate()
    )

    deduplicate(
        spark,
        f"{BASE_PATH}/movie_companies",
        f"{BASE_PATH}/movie_companies",
        [
            "movie_id",
            "company_id"
        ]
    )
    spark.stop()

if __name__ == "__main__":
    main()