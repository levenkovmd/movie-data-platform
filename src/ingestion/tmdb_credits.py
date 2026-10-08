import argparse
import json
import time
from datetime import date, datetime
from pathlib import Path

import requests

from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES,
    RAW_DATA_PATH,
)


def get_movie_credits(
    movie_id: int,
) -> dict:

    url = (
        f"{TMDB_BASE_URL}"
        f"/movie/{movie_id}/credits"
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
            f"Manifest not found: "
            f"{manifest_path}"
        )

    with open(
        manifest_path,
        "r",
        encoding="utf-8",
    ) as file:

        movie_ids = json.load(file)

    if not isinstance(
        movie_ids,
        list,
    ):

        raise RuntimeError(
            f"Invalid manifest: "
            f"{manifest_path}"
        )

    if len(movie_ids) != len(
        set(movie_ids)
    ):

        raise RuntimeError(
            "Manifest contains "
            "duplicate movie IDs"
        )

    return movie_ids


def get_credits_path(
    snapshot_date: date,
    movie_id: int,
) -> Path:

    credits_path = (
        get_snapshot_path(snapshot_date)
        / "credits"
    )

    credits_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        credits_path
        / f"movie_{movie_id}.json"
    )


def save_credits(
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

    print(
        f"[{datetime.now()}] "
        f"Saved {file_path}"
    )


def run_ingestion(
    snapshot_date: date,
):

    movie_ids = load_movie_ids(
        snapshot_date
    )

    print(
        f"Movies in manifest: "
        f"{len(movie_ids)}"
    )

    saved = 0
    skipped = 0
    failed_ids = []

    for index, movie_id in enumerate(
        movie_ids,
        start=1,
    ):

        print(
            f"[{index}/{len(movie_ids)}] "
            f"Movie {movie_id}"
        )

        file_path = get_credits_path(
            snapshot_date,
            movie_id,
        )

        if file_path.exists():

            print(
                f"Credits already exist "
                f"for movie {movie_id} "
                f"— skipped"
            )

            skipped += 1

            continue

        try:

            data = get_movie_credits(
                movie_id
            )

            save_credits(
                data,
                file_path,
            )

            saved += 1

        except Exception as e:

            print(
                f"ERROR movie "
                f"{movie_id}: {e}"
            )

            failed_ids.append(
                movie_id
            )

        time.sleep(1)

    print("\nFinished")
    print(f"Saved: {saved}")
    print(f"Skipped: {skipped}")
    print(
        f"Errors: "
        f"{len(failed_ids)}"
    )

    if failed_ids:

        raise RuntimeError(
            f"Failed movie IDs: "
            f"{failed_ids}"
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
        snapshot_date=args.snapshot_date,
    )