import json
import time
from pathlib import Path
import requests
from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES
)

MOVIES_PATH = Path("data/raw/tmdb/movies")
CREDITS_PATH = Path("data/raw/tmdb/credits")


def get_movie_ids():
    movie_ids = set()
    for file in MOVIES_PATH.glob("movies_page_*.json"):

        with open(
            file,
            encoding="utf-8"
        ) as f:

            data = json.load(f)

        for movie in data.get("results", []):
            movie_ids.add(movie["id"])

    return movie_ids


def get_movie_credits(movie_id):

    url = (
        f"{TMDB_BASE_URL}/movie/{movie_id}/credits"
    )

    params = {
        "api_key": TMDB_API_KEY
    }

    response = requests.get(
        url,
        params=params,
        proxies=PROXIES,
        timeout=30
    )

    response.raise_for_status()

    return response.json()


def save_credits(data):

    CREDITS_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    movie_id = data["id"]

    file_path = (
        CREDITS_PATH /
        f"movie_{movie_id}.json"
    )

    with open(
        file_path,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=4
        )


def run_ingestion():
    movie_ids = get_movie_ids()

    print(
        f"Found movies: {len(movie_ids)}"
    )

    saved = 0
    skipped = 0
    errors = 0


    for index, movie_id in enumerate(movie_ids, 1):

        file_path = (
            CREDITS_PATH /
            f"movie_{movie_id}.json"
        )
        
        if file_path.exists():
            skipped += 1
            continue

        try:
            data = get_movie_credits(movie_id)
            save_credits(data)
            saved += 1

            print(
                f"{index}/{len(movie_ids)} "
                f"saved {movie_id}"
            )


        except Exception as e:
            errors += 1
            print(
                f"ERROR {movie_id}: {e}"
            )
        time.sleep(0.3)

    print("\nFinished")
    print(f"Saved: {saved}")
    print(f"Skipped: {skipped}")
    print(f"Errors: {errors}")


if __name__ == "__main__":

    run_ingestion()