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


def get_popular_movies(page: int) -> dict:

    url = f"{TMDB_BASE_URL}/movie/popular"

    params = {
        "api_key": TMDB_API_KEY,
        "page": page,
    }

    response = requests.get(
        url,
        params=params,
        proxies=PROXIES,
        timeout=30,
    )

    response.raise_for_status()

    return response.json()


def get_page_path(
    page: int,
    snapshot_date: date,
) -> Path:

    raw_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
        / "movies"
    )

    raw_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        raw_path
        / f"movies_page_{page:03d}.json"
    )


def save_raw_page(
    data: dict,
    filename: Path,
):

    with open(
        filename,
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
        f"[{datetime.now()}] Saved {filename}"
    )


def run_ingestion(
    snapshot_date: date,
    start_page: int = 1,
    end_page: int = 300,
):

    failed_pages = []

    saved = 0
    skipped = 0

    for page in range(
        start_page,
        end_page + 1,
    ):

        filename = get_page_path(
            page,
            snapshot_date,
        )

        if filename.exists():

            print(
                f"Page {page} already exists - skipped"
            )

            skipped += 1

            continue

        print(
            f"Loading page {page}"
        )

        try:

            data = get_popular_movies(
                page
            )

            save_raw_page(
                data,
                filename,
            )

            saved += 1

            print(
                f"Movies loaded: "
                f"{len(data['results'])}"
            )

        except Exception as e:

            print(
                f"ERROR page {page}: {e}"
            )

            failed_pages.append(
                page
            )

        time.sleep(1)

    print("\nFinished")
    print(f"Saved: {saved}")
    print(f"Skipped: {skipped}")
    print(
        f"Errors: {len(failed_pages)}"
    )

    if failed_pages:

        raise RuntimeError(
            f"Failed pages: {failed_pages}"
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
        start_page=1,
        end_page=300,
    )