from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime


PROJECT_DIR = "/home/user/projects/movie-data-platform"

SNAPSHOT_DATE = "{{ params.snapshot_date }}"


def python_task(
    task_id,
    module,
):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"uv run python -m {module} "
            f'--snapshot-date "{SNAPSHOT_DATE}"'
        ),
    )


def sql_task(
    task_id,
    sql_file,
):
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "set -a && "
            "source .env && "
            "set +a && "
            'PGPASSWORD="$POSTGRES_PASSWORD" '
            "psql "
            '-h "$POSTGRES_HOST" '
            '-p "$POSTGRES_PORT" '
            '-U "$POSTGRES_USER" '
            '-d "$POSTGRES_DB" '
            "-v ON_ERROR_STOP=1 "
            f"-f {sql_file}"
        ),
    )


@dag(
    dag_id="movie_pipeline",
    schedule=None,
    start_date=datetime(
        2026,
        10,
        1,
        tz="UTC",
    ),
    catchup=False,
    params={
        "snapshot_date": "2026-10-07",
    },
)
def movie_pipeline():

    # INGESTION
    

    ingest_movies = python_task(
        task_id="ingest_movies",
        module="src.ingestion.tmdb_batch",
    )

    ingest_genres = python_task(
        task_id="ingest_genres",
        module="src.ingestion.tmdb_genres",
    )

    ingest_details = python_task(
        task_id="ingest_details",
        module="src.ingestion.tmdb_details",
    )

    ingest_credits = python_task(
        task_id="ingest_credits",
        module="src.ingestion.tmdb_credits",
    )

    check_raw = python_task(
        task_id="check_raw",
        module="src.quality.check_raw",
    )


    
    # PROCESSING
    

    transform_movies = python_task(
        task_id="transform_movies",
        module="src.processing.movies_transform",
    )

    transform_credits = python_task(
        task_id="transform_credits",
        module="src.processing.credits_transform",
    )

    check_processed = python_task(
        task_id="check_processed",
        module="src.quality.data_quality",
    )


    # DWH LOADING
    

    load_date = python_task(
        task_id="load_date",
        module="src.loading.date_loader",
    )

    load_movies = python_task(
        task_id="load_movies",
        module="src.loading.movies_loader",
    )

    load_genres = python_task(
        task_id="load_genres",
        module="src.loading.genres_loader",
    )

    load_actors = python_task(
        task_id="load_actors",
        module="src.loading.actors_loader",
    )

    load_movie_genres = python_task(
        task_id="load_movie_genres",
        module="src.loading.movie_genre_loader",
    )

    load_movie_actors = python_task(
        task_id="load_movie_actors",
        module="src.loading.movie_actor_loader",
    )

    load_fact = python_task(
        task_id="load_fact",
        module="src.loading.fact_movie_loader",
    )



    # DWH QUALITY


    check_dwh = sql_task(
        task_id="check_dwh",
        sql_file="src/quality/dwh_quality.sql",
    )



    # ANALYTICAL MARTS


    build_movie_performance = sql_task(
        task_id="build_movie_performance",
        sql_file="sql/marts/mart_movie_performance.sql",
    )

    build_genre_stats = sql_task(
        task_id="build_genre_stats",
        sql_file="sql/marts/mart_genre_stats.sql",
    )

    build_actor_stats = sql_task(
        task_id="build_actor_stats",
        sql_file="sql/marts/mart_actor_stats.sql",
    )

    build_movie_trends = sql_task(
        task_id="build_movie_trends",
        sql_file="sql/marts/mart_movie_trends.sql",
    )

    check_marts = sql_task(
        task_id="check_marts",
        sql_file="sql/marts/mart_quality.sql",
    )


    
    # DEPENDENCIES

    ingest_movies >> [
        ingest_details,
        ingest_credits,
    ]

    [
        ingest_details,
        ingest_credits,
        ingest_genres,
    ] >> check_raw


    check_raw >> [
        transform_movies,
        transform_credits,
    ]

    [
        transform_movies,
        transform_credits,
    ] >> check_processed


    check_processed >> [
        load_date,
        load_movies,
        load_genres,
        load_actors,
    ]


    [
        load_movies,
        load_genres,
    ] >> load_movie_genres

    [
        load_movies,
        load_actors,
    ] >> load_movie_actors


    [
        load_date,
        load_movie_genres,
        load_movie_actors,
    ] >> load_fact


    load_fact >> check_dwh


    check_dwh >> [
        build_movie_performance,
        build_genre_stats,
        build_actor_stats,
        build_movie_trends,
    ]


    [
        build_movie_performance,
        build_genre_stats,
        build_actor_stats,
        build_movie_trends,
    ] >> check_marts


movie_pipeline()