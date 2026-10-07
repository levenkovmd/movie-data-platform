import argparse
import json
import time
from datetime import date
from pathlib import Path

import requests

from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES,
    RAW_DATA_PATH,
)


def get_movie_ids(snapshot_date: date):
    movies_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movies"
    )

    movie_ids = set()

    for file in movies_path.glob("movies_page_*.json"):

        with open(
            file,
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        for movie in data.get("results", []):
            movie_ids.add(movie["id"])

    return movie_ids


def get_movie_details(movie_id: int):

    url = f"{TMDB_BASE_URL}/movie/{movie_id}"

    params = {
        "api_key": TMDB_API_KEY,
    }

    response = requests.get(
        url,
        params=params,
        proxies=PROXIES,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def save_details(
    data: dict,
    snapshot_date: date,
):

    details_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "details"
    )

    details_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    movie_id = data["id"]

    file_path = (
        details_path
        / f"movie_{movie_id}.json"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8",
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=4,
        )


def run_ingestion(snapshot_date: date):

    movie_ids = get_movie_ids(
        snapshot_date
    )

    if not movie_ids:
        raise RuntimeError(
            f"No movies found for snapshot {snapshot_date}"
        )

    details_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "details"
    )

    print(
        f"Found movies: {len(movie_ids)}"
    )

    success = 0
    skipped = 0
    failed_movie_ids = []

    for index, movie_id in enumerate(
        sorted(movie_ids),
        1,
    ):

        file_path = (
            details_path
            / f"movie_{movie_id}.json"
        )

        if file_path.exists():
            skipped += 1
            continue

        try:

            data = get_movie_details(
                movie_id
            )

            save_details(
                data,
                snapshot_date,
            )

            success += 1

            print(
                f"{index}/{len(movie_ids)} "
                f"saved {movie_id}"
            )

        except Exception as e:

            failed_movie_ids.append(
                movie_id
            )

            print(
                f"ERROR {movie_id}: {e}"
            )

        time.sleep(0.3)

    print("\nFinished")
    print(f"Saved: {success}")
    print(f"Skipped: {skipped}")
    print(
        f"Errors: {len(failed_movie_ids)}"
    )

    if failed_movie_ids:
        raise RuntimeError(
            "Failed movie IDs: "
            f"{failed_movie_ids}"
        )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=date.today(),
    )

    args = parser.parse_args()

    run_ingestion(
        snapshot_date=args.snapshot_date
    )