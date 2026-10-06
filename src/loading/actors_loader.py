import os

import psycopg
from dotenv import load_dotenv
from pyspark.sql import SparkSession


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
    .appName("Load actors to DWH")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)


movie_actors = spark.read.parquet(
    "data/processed/tmdb/movie_actors"
)


actors = (
    movie_actors
    .select(
        "actor_id",
        "actor_name",
        "gender",
        "known_for_department",
    )
    .dropDuplicates(
        ["actor_id"]
    )
)


actor_count = actors.count()


# Загружаем актуальные данные во staging-таблицу

actors.write.jdbc(
    url=JDBC_URL,
    table="stg_dim_actor",
    mode="overwrite",
    properties=JDBC_PROPERTIES,
)


# Добавляем новых актеров и обновляем существующих

with psycopg.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
) as connection:

    with connection.cursor() as cursor:

        cursor.execute(
            """
            INSERT INTO dim_actor (
                actor_id,
                actor_name,
                gender,
                known_for_department
            )
            SELECT
                actor_id,
                actor_name,
                gender,
                known_for_department
            FROM stg_dim_actor

            ON CONFLICT (actor_id)
            DO UPDATE SET
                actor_name = EXCLUDED.actor_name,
                gender = EXCLUDED.gender,
                known_for_department = EXCLUDED.known_for_department;
            """
        )

        cursor.execute(
            """
            DROP TABLE stg_dim_actor;
            """
        )


print(
    "Actors loaded:",
    actor_count,
)


spark.stop()