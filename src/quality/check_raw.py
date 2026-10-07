import argparse
import json
from datetime import date
from pathlib import Path

from src.config.settings import RAW_DATA_PATH


def get_movie_ids(movies_path):

    files = list(
        movies_path.glob("movies_page_*.json")
    )

    total_movies = 0
    movie_ids = []

    for file in files:

        with open(
            file,
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        movies = data.get("results", [])

        total_movies += len(movies)

        for movie in movies:
            movie_ids.append(movie["id"])

    return files, total_movies, movie_ids


def get_ids_from_files(path):

    ids = set()

    for file in path.glob("movie_*.json"):

        try:
            movie_id = int(
                file.stem.replace("movie_", "")
            )

            ids.add(movie_id)

        except ValueError:
            pass

    return ids


def check_genres(genres_path):

    file = genres_path / "genres.json"

    if not file.exists():
        return 0

    with open(
        file,
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    return len(
        data.get("genres", [])
    )


def check_raw(snapshot_date: date):

    raw_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    movies_path = raw_path / "movies"
    genres_path = raw_path / "genres"
    details_path = raw_path / "details"
    credits_path = raw_path / "credits"

    if not movies_path.exists():
        raise RuntimeError(
            f"Movies directory not found: {movies_path}"
        )

    files, total_movies, movie_ids = get_movie_ids(
        movies_path
    )

    unique_movie_ids = set(
        movie_ids
    )

    details_ids = get_ids_from_files(
        details_path
    )

    credits_ids = get_ids_from_files(
        credits_path
    )

    genre_count = check_genres(
        genres_path
    )

    missing_details = (
        unique_movie_ids - details_ids
    )

    missing_credits = (
        unique_movie_ids - credits_ids
    )

    print(
        f"\nSnapshot date: {snapshot_date}"
    )

    print("\nMOVIES")

    print(
        f"Файлов: {len(files)}"
    )

    print(
        f"Фильмов: {total_movies}"
    )

    print(
        f"Уникальных фильмов: "
        f"{len(unique_movie_ids)}"
    )

    print(
        f"Дубликатов: "
        f"{len(movie_ids) - len(unique_movie_ids)}"
    )

    print("\nGENRES")

    print(
        f"Жанров: {genre_count}"
    )

    print("\nDETAILS")

    print(
        f"Файлов: {len(details_ids)}"
    )

    print(
        f"Без details: "
        f"{len(missing_details)}"
    )

    print("\nCREDITS")

    print(
        f"Файлов: {len(credits_ids)}"
    )

    print(
        f"Без credits: "
        f"{len(missing_credits)}"
    )

    if missing_details:

        print("\nID без details:")

        print(
            sorted(missing_details)
        )

    if missing_credits:

        print("\nID без credits:")

        print(
            sorted(missing_credits)
        )

    failed = False

    if not files:
        print(
            "\nERROR: нет файлов movies"
        )
        failed = True

    if genre_count == 0:
        print(
            "\nERROR: нет данных genres"
        )
        failed = True

    if missing_details:
        failed = True

    if missing_credits:
        failed = True

    if failed:
        raise RuntimeError(
            f"RAW DATA QUALITY FAILED "
            f"for snapshot {snapshot_date}"
        )

    print(
        "\nRAW DATA QUALITY PASSED"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=date.today(),
    )

    args = parser.parse_args()

    check_raw(
        snapshot_date=args.snapshot_date
    )