import argparse
import json
from datetime import date
from pathlib import Path

import requests

from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES,
    RAW_DATA_PATH,
)


def get_genres():

    url = f"{TMDB_BASE_URL}/genre/movie/list"

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


def get_genres_path(
    snapshot_date: date,
) -> Path:

    genres_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "genres"
    )

    genres_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        genres_path
        / "genres.json"
    )


def save_genres(
    data: dict,
    file_path: Path,
):

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

    print(
        f"Genres saved: {file_path}"
    )


def run_ingestion(
    snapshot_date: date,
):

    file_path = get_genres_path(
        snapshot_date
    )

    if file_path.exists():

        print(
            f"Genres already exist for "
            f"{snapshot_date} - skipped"
        )

        return

    genres = get_genres()

    save_genres(
        genres,
        file_path,
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