import argparse
import json
from datetime import date
from pathlib import Path

from src.config.settings import RAW_DATA_PATH


TARGET_MOVIES_COUNT = 6000


def get_movies_stats(
    movies_path: Path,
):

    files = list(
        movies_path.glob("movies_page_*.json")
    )

    total_movies = 0
    movie_ids = []

    for file in files:

        with open(
            file,
            encoding="utf-8",
        ) as f:
            data = json.load(f)

        movies = data.get(
            "results",
            [],
        )

        total_movies += len(movies)

        for movie in movies:
            movie_ids.append(
                movie["id"]
            )

    return (
        files,
        total_movies,
        movie_ids,
    )


def load_manifest(
    manifest_path: Path,
):

    if not manifest_path.exists():

        raise RuntimeError(
            f"Manifest not found: "
            f"{manifest_path}"
        )

    with open(
        manifest_path,
        encoding="utf-8",
    ) as f:
        movie_ids = json.load(f)

    if not isinstance(
        movie_ids,
        list,
    ):

        raise RuntimeError(
            f"Invalid manifest: "
            f"{manifest_path}"
        )

    return movie_ids


def get_ids_from_files(
    path: Path,
):

    ids = set()

    for file in path.glob(
        "movie_*.json"
    ):

        try:

            movie_id = int(
                file.stem.replace(
                    "movie_",
                    "",
                )
            )

            ids.add(
                movie_id
            )

        except ValueError:
            pass

    return ids


def check_genres(
    genres_path: Path,
):

    file_path = (
        genres_path
        / "genres.json"
    )

    if not file_path.exists():
        return 0

    with open(
        file_path,
        encoding="utf-8",
    ) as f:
        data = json.load(f)

    return len(
        data.get(
            "genres",
            [],
        )
    )


def check_raw(
    snapshot_date: date,
):

    raw_path = (
        Path(RAW_DATA_PATH)
        / "tmdb"
        / snapshot_date.isoformat()
    )

    movies_path = (
        raw_path
        / "movies"
    )

    genres_path = (
        raw_path
        / "genres"
    )

    details_path = (
        raw_path
        / "details"
    )

    credits_path = (
        raw_path
        / "credits"
    )

    manifest_path = (
        raw_path
        / "movie_ids.json"
    )

    if not movies_path.exists():

        raise RuntimeError(
            f"Movies directory not found: "
            f"{movies_path}"
        )

    (
        files,
        total_movies,
        page_movie_ids,
    ) = get_movies_stats(
        movies_path
    )

    unique_page_ids = set(
        page_movie_ids
    )

    manifest_movie_ids = load_manifest(
        manifest_path
    )

    manifest_ids = set(
        manifest_movie_ids
    )

    details_ids = get_ids_from_files(
        details_path
    )

    credits_ids = get_ids_from_files(
        credits_path
    )

    genre_count = check_genres(
        genres_path
    )

    missing_details = (
        manifest_ids
        - details_ids
    )

    missing_credits = (
        manifest_ids
        - credits_ids
    )

    extra_details = (
        details_ids
        - manifest_ids
    )

    extra_credits = (
        credits_ids
        - manifest_ids
    )

    manifest_not_in_pages = (
        manifest_ids
        - unique_page_ids
    )

    print(
        f"\nSnapshot date: "
        f"{snapshot_date}"
    )

    print("\nMOVIES")

    print(
        f"Файлов страниц: "
        f"{len(files)}"
    )

    print(
        f"Всего позиций: "
        f"{total_movies}"
    )

    print(
        f"Уникальных фильмов "
        f"на страницах: "
        f"{len(unique_page_ids)}"
    )

    print(
        f"Повторных позиций: "
        f"{len(page_movie_ids) - len(unique_page_ids)}"
    )

    print("\nMANIFEST")

    print(
        f"ID в manifest: "
        f"{len(manifest_movie_ids)}"
    )

    print(
        f"Уникальных ID: "
        f"{len(manifest_ids)}"
    )

    print(
        f"Дубликатов ID: "
        f"{len(manifest_movie_ids) - len(manifest_ids)}"
    )

    print(
        f"ID manifest, которых "
        f"нет в movies pages: "
        f"{len(manifest_not_in_pages)}"
    )

    print("\nGENRES")

    print(
        f"Жанров: "
        f"{genre_count}"
    )

    print("\nDETAILS")

    print(
        f"Файлов: "
        f"{len(details_ids)}"
    )

    print(
        f"Без details: "
        f"{len(missing_details)}"
    )

    print(
        f"Лишних details: "
        f"{len(extra_details)}"
    )

    print("\nCREDITS")

    print(
        f"Файлов: "
        f"{len(credits_ids)}"
    )

    print(
        f"Без credits: "
        f"{len(missing_credits)}"
    )

    print(
        f"Лишних credits: "
        f"{len(extra_credits)}"
    )

    if missing_details:

        print(
            "\nID без details:"
        )

        print(
            sorted(
                missing_details
            )
        )

    if missing_credits:

        print(
            "\nID без credits:"
        )

        print(
            sorted(
                missing_credits
            )
        )

    if extra_details:

        print(
            "\nЛишние details:"
        )

        print(
            sorted(
                extra_details
            )
        )

    if extra_credits:

        print(
            "\nЛишние credits:"
        )

        print(
            sorted(
                extra_credits
            )
        )

    failed = False

    if not files:

        print(
            "\nERROR: "
            "нет файлов movies"
        )

        failed = True

    if (
        len(manifest_movie_ids)
        != TARGET_MOVIES_COUNT
    ):

        print(
            "\nERROR: "
            f"в manifest должно быть "
            f"{TARGET_MOVIES_COUNT} ID"
        )

        failed = True

    if (
        len(manifest_movie_ids)
        != len(manifest_ids)
    ):

        print(
            "\nERROR: "
            "manifest содержит дубликаты"
        )

        failed = True

    if manifest_not_in_pages:

        print(
            "\nERROR: "
            "manifest содержит ID, "
            "которых нет в raw pages"
        )

        failed = True

    if genre_count == 0:

        print(
            "\nERROR: "
            "нет данных genres"
        )

        failed = True

    if missing_details:
        failed = True

    if missing_credits:
        failed = True

    if extra_details:
        failed = True

    if extra_credits:
        failed = True

    if failed:

        raise RuntimeError(
            f"RAW DATA QUALITY FAILED "
            f"for snapshot "
            f"{snapshot_date}"
        )

    print(
        "\nRAW DATA QUALITY PASSED"
    )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--snapshot-date",
        type=date.fromisoformat,
        default=date.today(),
    )

    args = parser.parse_args()

    check_raw(
        snapshot_date=args.snapshot_date
    )