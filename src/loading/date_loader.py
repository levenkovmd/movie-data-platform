import os
from datetime import date
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    dayofmonth,
    dayofweek,
    lit,
    month,
    quarter,
    year,
)

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

SNAPSHOT_DATE = date.today()

spark = (
    SparkSession.builder
    .appName("Load date to DWH")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.postgresql:postgresql:42.7.8",
    )
    .getOrCreate()
)

snapshot = spark.createDataFrame(
    [(SNAPSHOT_DATE,)],
    ["date_id"],
)

snapshot = (
    snapshot
    .withColumn("year", year("date_id"))
    .withColumn("month", month("date_id"))
    .withColumn("quarter", quarter("date_id"))
    .withColumn("day", dayofmonth("date_id"))
    .withColumn("day_of_week", dayofweek("date_id"))
)

snapshot.write.jdbc(
    url=JDBC_URL,
    table="dim_date",
    mode="append",
    properties=JDBC_PROPERTIES,
)

print("Snapshot date loaded:", SNAPSHOT_DATE)

spark.stop()