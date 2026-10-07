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


def get_movie_credits(movie_id: int):

    url = (
        f"{TMDB_BASE_URL}/movie/{movie_id}/credits"
    )

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


def save_credits(
    data: dict,
    snapshot_date: date,
):

    credits_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "credits"
    )

    credits_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    movie_id = data["id"]

    file_path = (
        credits_path
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

    credits_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "credits"
    )

    print(
        f"Found movies: {len(movie_ids)}"
    )

    saved = 0
    skipped = 0
    failed_movie_ids = []

    for index, movie_id in enumerate(
        sorted(movie_ids),
        1,
    ):

        file_path = (
            credits_path
            / f"movie_{movie_id}.json"
        )

        if file_path.exists():
            skipped += 1
            continue

        try:

            data = get_movie_credits(
                movie_id
            )

            save_credits(
                data,
                snapshot_date,
            )

            saved += 1

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
    print(f"Saved: {saved}")
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