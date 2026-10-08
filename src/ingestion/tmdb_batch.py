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


TARGET_MOVIES_COUNT = 6000


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


def get_snapshot_path(
    snapshot_date: date,
) -> Path:

    snapshot_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    snapshot_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return snapshot_path


def get_page_path(
    snapshot_date: date,
    page: int,
) -> Path:

    movies_path = (
        get_snapshot_path(snapshot_date)
        / "movies"
    )

    movies_path.mkdir(
        parents=True,
        exist_ok=True,
    )

    return (
        movies_path
        / f"movies_page_{page:03d}.json"
    )


def get_manifest_path(
    snapshot_date: date,
) -> Path:

    return (
        get_snapshot_path(snapshot_date)
        / "movie_ids.json"
    )


def load_raw_page(
    file_path: Path,
) -> dict:

    with open(
        file_path,
        "r",
        encoding="utf-8",
    ) as file:

        return json.load(file)


def save_raw_page(
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
        f"[{datetime.now()}] Saved {file_path}"
    )


def save_manifest(
    movie_ids: list[int],
    file_path: Path,
):

    with open(
        file_path,
        "w",
        encoding="utf-8",
    ) as file:

        json.dump(
            movie_ids,
            file,
            ensure_ascii=False,
            indent=4,
        )

    print(
        f"Manifest saved: {file_path}"
    )


def run_ingestion(
    snapshot_date: date,
    target_count: int = TARGET_MOVIES_COUNT,
):

    manifest_path = get_manifest_path(
        snapshot_date
    )

    # Если manifest уже создан,
    # snapshot считаем завершенным.
    if manifest_path.exists():

        with open(
            manifest_path,
            "r",
            encoding="utf-8",
        ) as file:

            movie_ids = json.load(file)

        if (
            len(movie_ids) == target_count
            and len(set(movie_ids)) == target_count
        ):

            print(
                f"Snapshot {snapshot_date} "
                f"already contains "
                f"{target_count} unique movies — skipped"
            )

            return

        raise RuntimeError(
            f"Invalid manifest: {manifest_path}"
        )

    unique_movie_ids = []
    seen_movie_ids = set()

    saved_pages = 0
    skipped_pages = 0

    page = 1

    while len(unique_movie_ids) < target_count:

        file_path = get_page_path(
            snapshot_date,
            page,
        )

        if file_path.exists():

            print(
                f"Page {page} already exists — skipped"
            )

            data = load_raw_page(
                file_path
            )

            skipped_pages += 1

        else:

            print(
                f"Loading page {page}"
            )

            try:

                data = get_popular_movies(
                    page
                )

            except Exception as e:

                raise RuntimeError(
                    f"Failed page {page}: {e}"
                ) from e

            save_raw_page(
                data,
                file_path,
            )

            saved_pages += 1

            time.sleep(1)

        results = data.get(
            "results",
            [],
        )

        for movie in results:

            movie_id = movie["id"]

            if movie_id in seen_movie_ids:
                continue

            seen_movie_ids.add(
                movie_id
            )

            unique_movie_ids.append(
                movie_id
            )

            if (
                len(unique_movie_ids)
                == target_count
            ):
                break

        print(
            f"Unique movies: "
            f"{len(unique_movie_ids)}"
            f"/{target_count}"
        )

        total_pages = data.get(
            "total_pages"
        )

        if (
            len(unique_movie_ids) < target_count
            and total_pages is not None
            and page >= total_pages
        ):

            raise RuntimeError(
                f"TMDB pages exhausted: "
                f"collected "
                f"{len(unique_movie_ids)} "
                f"unique movies, "
                f"target is {target_count}"
            )

        page += 1

    save_manifest(
        unique_movie_ids,
        manifest_path,
    )

    print("\nFinished")
    print(
        f"Unique movies: "
        f"{len(unique_movie_ids)}"
    )
    print(
        f"Pages scanned: {page}"
    )
    print(
        f"Pages downloaded: "
        f"{saved_pages}"
    )
    print(
        f"Pages skipped: "
        f"{skipped_pages}"
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