import json
from pathlib import Path


RAW_PATH = Path("data/raw/tmdb")

MOVIES_PATH = RAW_PATH / "movies"
GENRES_PATH = RAW_PATH / "genres"
DETAILS_PATH = RAW_PATH / "details"
CREDITS_PATH = RAW_PATH / "credits"


def get_movie_ids():

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

    return files, total_movies, movie_ids


def get_ids_from_files(path):
    ids = set()
    for file in path.glob("movie_*.json"):
        try:
            movie_id = int(
                file.stem.replace("movie_", "")
            )
            ids.add(movie_id)
        except ValueError:
            pass

    return ids


def check_genres():

    file = GENRES_PATH / "genres.json"

    if not file.exists():
        return 0

    with open(
        file,
        encoding="utf-8"
    ) as f:
        data = json.load(f)

    return len(data.get("genres", []))


def check_raw():

    files, total_movies, movie_ids = get_movie_ids()

    unique_movie_ids = set(movie_ids)

    details_ids = get_ids_from_files(
        DETAILS_PATH
    )

    credits_ids = get_ids_from_files(
        CREDITS_PATH
    )

    genre_count = check_genres()


    missing_details = (
        unique_movie_ids - details_ids
    )

    missing_credits = (
        unique_movie_ids - credits_ids
    )

    print("\nMOVIES")

    print(f"Файлов: {len(files)}")
    print(f"Фильмов: {total_movies}")
    print(
        f"Уникальных фильмов: "
        f"{len(unique_movie_ids)}"
    )
    print(
        f"Дубликатов: "
        f"{len(movie_ids) - len(unique_movie_ids)}"
    )


    print("\nGENRES")
    print(
        f"Жанров: {genre_count}"
    )


    print("\nDETAILS")
    print(
        f"Файлов: {len(details_ids)}"
    )
    print(
        f"Без details: "
        f"{len(missing_details)}"
    )


    print("\nCREDITS")

    print(
        f"Файлов: {len(credits_ids)}"
    )
    print(
        f"Без credits: "
        f"{len(missing_credits)}"
    )


    if missing_details:
        print("\nID без details:")
        print(
            sorted(missing_details)
        )


    if missing_credits:
        print("\nID без credits:")
        print(
            sorted(missing_credits)
        )



if __name__ == "__main__":
    check_raw()