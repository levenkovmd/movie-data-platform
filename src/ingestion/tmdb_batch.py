import json
import time
from pathlib import Path
from datetime import datetime
import requests
from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES
)

RAW_PATH = Path("data/raw/tmdb/movies")

def get_popular_movies(page: int):

    url = f"{TMDB_BASE_URL}/movie/popular"

    params = {
        "api_key": TMDB_API_KEY,
        "page": page
    }

    response = requests.get(
        url,
        params=params,
        proxies=PROXIES,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


def save_raw_page(data: dict, page: int):

    RAW_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    filename = (
        RAW_PATH /
        f"movies_page_{page:03d}.json"
    )

    with open(
        filename,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            data,
            file,
            ensure_ascii=False,
            indent=4
        )

    print(
        f"[{datetime.now()}] Saved {filename}"
    )


def run_ingestion(
    start_page=1,
    end_page=300
):

    for page in range(
        start_page,
        end_page + 1
    ):

        print(
            f"Loading page {page}"
        )

        try:

            data = get_popular_movies(page)

            save_raw_page(
                data,
                page
            )

            print(
                f"Movies loaded: {len(data['results'])}"
            )


        except Exception as e:

            print(
                f"ERROR page {page}: {e}"
            )


        time.sleep(1)


if __name__ == "__main__":

    run_ingestion(
        start_page=1,
        end_page=300
    )