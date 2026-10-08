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


def get_snapshot_path(
    snapshot_date: date,
) -> Path:

    return (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )


def load_movie_ids(
    snapshot_date: date,
) -> list[int]:

    manifest_path = (
        get_snapshot_path(snapshot_date)
        / "movie_ids.json"
    )

    if not manifest_path.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_path}"
        )

    with open(
        manifest_path,
        "r",
        encoding="utf-8",
    ) as file:
        movie_ids = json.load(file)

    if not isinstance(movie_ids, list):
        raise RuntimeError(
            f"Invalid manifest: {manifest_path}"
        )

    if not movie_ids:
        raise RuntimeError(
            f"Manifest is empty: {manifest_path}"
        )

    if len(movie_ids) != len(set(movie_ids)):
        raise RuntimeError(
            "Manifest contains duplicate movie IDs"
        )

    return movie_ids


def get_movie_details(
    movie_id: int,
):

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


def get_details_path(
    snapshot_date: date,
    movie_id: int,
) -> Path:

    details_path = (
        get_snapshot_path(snapshot_date)
        / "details"
    )

    details_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        details_path
        / f"movie_{movie_id}.json"
    )


def save_details(
    data: dict,
    file_path: Path,
):

    with open(
        file_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4,
        )


def run_ingestion(
    snapshot_date: date,
):

    movie_ids = load_movie_ids(
        snapshot_date
    )

    print(
        f"Movies in manifest: {len(movie_ids)}"
    )

    success = 0
    skipped = 0
    failed_movie_ids = []

    for index, movie_id in enumerate(
        movie_ids,
        start=1,
    ):

        file_path = get_details_path(
            snapshot_date,
            movie_id,
        )

        if file_path.exists():

            skipped += 1

            print(
                f"{index}/{len(movie_ids)} "
                f"skipped {movie_id}"
            )

            continue

        try:

            data = get_movie_details(
                movie_id
            )

            save_details(
                data,
                file_path,
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