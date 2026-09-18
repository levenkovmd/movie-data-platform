import json
from pathlib import Path
import requests
from src.config.settings import (
    TMDB_API_KEY,
    TMDB_BASE_URL,
    PROXIES
)

GENRES_PATH = Path("data/raw/tmdb/genres")

def get_genres():
    url = f"{TMDB_BASE_URL}/genre/movie/list"
    
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


def save_genres(data):

    GENRES_PATH.mkdir(
        parents=True,
        exist_ok=True
    )

    file = GENRES_PATH / "genres.json"

    with open(
        file,
        "w",
        encoding="utf-8"
    ) as f:

        json.dump(
            data,
            f,
            ensure_ascii=False,
            indent=4
        )


if __name__ == "__main__":
    genres = get_genres()
    save_genres(genres)