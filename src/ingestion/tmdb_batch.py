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

REQUEST_TIMEOUT = 30
REQUEST_DELAY = 0.3

MAX_RETRIES = 4
BASE_RETRY_DELAY = 2


def log(message):
    print(
        message,
        flush=True,
    )


def get_retry_delay(
    attempt: int,
) -> int:

    return (
        BASE_RETRY_DELAY
        * 2 ** (attempt - 1)
    )


def get_popular_movies(
    page: int,
) -> dict:

    url = (
        f"{TMDB_BASE_URL}/movie/popular"
    )

    params = {
        "api_key": TMDB_API_KEY,
        "page": page,
    }

    for attempt in range(
        1,
        MAX_RETRIES + 1,
    ):

        try:

            response = requests.get(
                url,
                params=params,
                proxies=PROXIES,
                timeout=REQUEST_TIMEOUT,
            )

            if response.status_code == 429:

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                retry_after = (
                    response.headers.get(
                        "Retry-After"
                    )
                )

                try:
                    delay = int(
                        retry_after
                    )
                except (
                    TypeError,
                    ValueError,
                ):
                    delay = get_retry_delay(
                        attempt
                    )

                log(
                    f"Page {page}: "
                    f"rate limit 429"
                )

                log(
                    f"Retry "
                    f"{attempt}/{MAX_RETRIES} "
                    f"in {delay} seconds"
                )

                time.sleep(
                    delay
                )

                continue

            if (
                500
                <= response.status_code
                < 600
            ):

                if attempt == MAX_RETRIES:
                    response.raise_for_status()

                delay = get_retry_delay(
                    attempt
                )

                log(
                    f"Page {page}: "
                    f"server error "
                    f"{response.status_code}"
                )

                log(
                    f"Retry "
                    f"{attempt}/{MAX_RETRIES} "
                    f"in {delay} seconds"
                )

                time.sleep(
                    delay
                )

                continue

            response.raise_for_status()

            return response.json()

        except requests.Timeout as e:

            if attempt == MAX_RETRIES:
                raise

            delay = get_retry_delay(
                attempt
            )

            log(
                f"Page {page}: "
                f"timeout on attempt "
                f"{attempt}/{MAX_RETRIES}: "
                f"{e}"
            )

            log(
                f"Retry in "
                f"{delay} seconds"
            )

            time.sleep(
                delay
            )


        except requests.ConnectionError as e:

            if attempt == MAX_RETRIES:
                raise

            delay = get_retry_delay(
                attempt
            )

            log(
                f"Page {page}: "
                f"connection error on "
                f"attempt "
                f"{attempt}/{MAX_RETRIES}: "
                f"{e}"
            )

            log(
                f"Retry in "
                f"{delay} seconds"
            )

            time.sleep(
                delay
            )


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
        get_snapshot_path(
            snapshot_date
        )
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
        get_snapshot_path(
            snapshot_date
        )
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

        return json.load(
            file
        )


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

    log(
        f"[{datetime.now()}] "
        f"Saved {file_path}"
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

    log(
        f"Manifest saved: "
        f"{file_path}"
    )


def run_ingestion(
    snapshot_date: date,
    target_count: int = TARGET_MOVIES_COUNT,
):

    manifest_path = (
        get_manifest_path(
            snapshot_date
        )
    )


    if manifest_path.exists():

        with open(
            manifest_path,
            "r",
            encoding="utf-8",
        ) as file:

            movie_ids = json.load(
                file
            )

        if (
            len(movie_ids)
            == target_count
            and len(set(movie_ids))
            == target_count
        ):

            log(
                f"Snapshot "
                f"{snapshot_date} "
                f"already contains "
                f"{target_count} "
                f"unique movies "
                f"— skipped"
            )

            return

        raise RuntimeError(
            f"Invalid manifest: "
            f"{manifest_path}"
        )

    unique_movie_ids = []
    seen_movie_ids = set()

    saved_pages = 0
    skipped_pages = 0

    page = 1


    while (
        len(unique_movie_ids)
        < target_count
    ):

        file_path = (
            get_page_path(
                snapshot_date,
                page,
            )
        )


        if file_path.exists():

            log(
                f"Page {page} "
                f"already exists "
                f"— skipped"
            )

            data = load_raw_page(
                file_path
            )

            skipped_pages += 1


        else:

            log(
                f"Loading page "
                f"{page}"
            )

            try:

                data = (
                    get_popular_movies(
                        page
                    )
                )

            except Exception as e:

                raise RuntimeError(
                    f"Failed page "
                    f"{page}: {e}"
                ) from e

            save_raw_page(
                data,
                file_path,
            )

            saved_pages += 1

            time.sleep(
                REQUEST_DELAY
            )

        results = data.get(
            "results",
            [],
        )

        for movie in results:

            movie_id = (
                movie["id"]
            )

            if (
                movie_id
                in seen_movie_ids
            ):
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

        log(
            f"Unique movies: "
            f"{len(unique_movie_ids)}"
            f"/{target_count}"
        )

        total_pages = data.get(
            "total_pages"
        )

        if (
            len(unique_movie_ids)
            < target_count
            and total_pages is not None
            and page >= total_pages
        ):

            raise RuntimeError(
                f"TMDB pages exhausted: "
                f"collected "
                f"{len(unique_movie_ids)} "
                f"unique movies, "
                f"target is "
                f"{target_count}"
            )

        page += 1


    save_manifest(
        unique_movie_ids,
        manifest_path,
    )

    log(
        "\nFinished"
    )

    log(
        f"Unique movies: "
        f"{len(unique_movie_ids)}"
    )

    log(
        f"Pages scanned: "
        f"{page - 1}"
    )

    log(
        f"Pages downloaded: "
        f"{saved_pages}"
    )

    log(
        f"Pages skipped: "
        f"{skipped_pages}"
    )


if __name__ == "__main__":

    parser = (
        argparse.ArgumentParser()
    )

    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=date.today(),
    )

    args = (
        parser.parse_args()
    )

    run_ingestion(
        snapshot_date=(
            args.snapshot_date
        ),
    )