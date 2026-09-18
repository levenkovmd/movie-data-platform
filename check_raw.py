import json
from pathlib import Path


MOVIES_PATH = Path("data/raw/tmdb/movies")


def check_raw():

    files = list(
        MOVIES_PATH.glob("movies_page_*.json")
    )

    total_movies = 0
    movie_ids = []

    for file in files:

        with open(
            file,
            encoding="utf-8"
        ) as f:
            data = json.load(f)

        movies = data.get("results", [])

        total_movies += len(movies)

        for movie in movies:
            movie_ids.append(movie["id"])


    print(f"Файлов: {len(files)}")
    print(f"Фильмов: {total_movies}")
    print(f"Уникальных фильмов: {len(set(movie_ids))}")
    print(
        f"Дубликатов: {len(movie_ids) - len(set(movie_ids))}"
    )


if __name__ == "__main__":
    check_raw()