-- ============================================================
-- DWH DATA QUALITY CHECKS
-- ============================================================


-- ============================================================
-- DIM_MOVIE
-- ============================================================

-- Проверяем, есть ли фильмы без идентификатора
SELECT
    COUNT(*) AS null_movie_id
FROM dim_movie
WHERE movie_id IS NULL;


-- Ищем дубликаты идентификаторов фильмов
-- Для каждого movie_id должна существовать только одна строка
SELECT
    movie_id,
    COUNT(*) AS row_count
FROM dim_movie
GROUP BY movie_id
HAVING COUNT(*) > 1;


-- Проверяем, есть ли фильмы без названия
SELECT
    COUNT(*) AS null_title
FROM dim_movie
WHERE title IS NULL;



-- ============================================================
-- DIM_ACTOR
-- ============================================================

-- Проверяем, есть ли актеры без идентификатора
SELECT
    COUNT(*) AS null_actor_id
FROM dim_actor
WHERE actor_id IS NULL;


-- Ищем дубликаты идентификаторов актеров
-- Для каждого actor_id должна существовать только одна строка
SELECT
    actor_id,
    COUNT(*) AS row_count
FROM dim_actor
GROUP BY actor_id
HAVING COUNT(*) > 1;


-- Проверяем, есть ли актеры без имени
SELECT
    COUNT(*) AS null_actor_name
FROM dim_actor
WHERE actor_name IS NULL;



-- ============================================================
-- DIM_GENRE
-- ============================================================

-- Проверяем, есть ли жанры без идентификатора
SELECT
    COUNT(*) AS null_genre_id
FROM dim_genre
WHERE genre_id IS NULL;


-- Ищем дубликаты идентификаторов жанров
SELECT
    genre_id,
    COUNT(*) AS row_count
FROM dim_genre
GROUP BY genre_id
HAVING COUNT(*) > 1;


-- Проверяем, есть ли жанры без названия
SELECT
    COUNT(*) AS null_genre_name
FROM dim_genre
WHERE genre_name IS NULL;



-- ============================================================
-- DIM_DATE
-- ============================================================

-- Проверяем, есть ли даты без идентификатора
SELECT
    COUNT(*) AS null_date_id
FROM dim_date
WHERE date_id IS NULL;


-- Проверяем корректность номера месяца
-- Допустимые значения: от 1 до 12
SELECT
    COUNT(*) AS invalid_month
FROM dim_date
WHERE month < 1
   OR month > 12;


-- Проверяем корректность номера квартала
-- Допустимые значения: от 1 до 4
SELECT
    COUNT(*) AS invalid_quarter
FROM dim_date
WHERE quarter < 1
   OR quarter > 4;


-- Проверяем корректность номера дня месяца
SELECT
    COUNT(*) AS invalid_day
FROM dim_date
WHERE day < 1
   OR day > 31;



-- ============================================================
-- BRIDGE_MOVIE_ACTOR
-- ============================================================

-- Ищем полные дубликаты связей фильм-актер
-- Учитываем фильм, актера, персонажа и порядок в cast
SELECT
    movie_id,
    actor_id,
    character,
    cast_order,
    COUNT(*) AS row_count
FROM bridge_movie_actor
GROUP BY
    movie_id,
    actor_id,
    character,
    cast_order
HAVING COUNT(*) > 1;


-- Проверяем, что каждый movie_id существует в dim_movie
SELECT
    COUNT(*) AS invalid_movie_id
FROM bridge_movie_actor b
LEFT JOIN dim_movie m
    ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL;


-- Проверяем, что каждый actor_id существует в dim_actor
SELECT
    COUNT(*) AS invalid_actor_id
FROM bridge_movie_actor b
LEFT JOIN dim_actor a
    ON b.actor_id = a.actor_id
WHERE a.actor_id IS NULL;



-- ============================================================
-- BRIDGE_MOVIE_GENRE
-- ============================================================

-- Ищем дубликаты связей фильм-жанр
-- Одна пара movie_id + genre_id должна встречаться один раз
SELECT
    movie_id,
    genre_id,
    COUNT(*) AS row_count
FROM bridge_movie_genre
GROUP BY
    movie_id,
    genre_id
HAVING COUNT(*) > 1;


-- Проверяем, что каждый movie_id существует в dim_movie
SELECT
    COUNT(*) AS invalid_movie_id
FROM bridge_movie_genre b
LEFT JOIN dim_movie m
    ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL;


-- Проверяем, что каждый genre_id существует в dim_genre
SELECT
    COUNT(*) AS invalid_genre_id
FROM bridge_movie_genre b
LEFT JOIN dim_genre g
    ON b.genre_id = g.genre_id
WHERE g.genre_id IS NULL;



-- ============================================================
-- FACT_MOVIE_SNAPSHOT
-- ============================================================

-- Проверяем, есть ли строки без snapshot-даты
SELECT
    COUNT(*) AS null_snapshot_date
FROM fact_movie_snapshot
WHERE snapshot_date IS NULL;


-- Проверяем, есть ли строки без идентификатора фильма
SELECT
    COUNT(*) AS null_movie_id
FROM fact_movie_snapshot
WHERE movie_id IS NULL;


-- Проверяем, есть ли строки без названия фильма
SELECT
    COUNT(*) AS null_title
FROM fact_movie_snapshot
WHERE title IS NULL;


-- Проверяем уникальность grain фактовой таблицы
-- Один фильм должен встречаться не более одного раза
-- внутри одного snapshot
SELECT
    snapshot_date,
    movie_id,
    COUNT(*) AS row_count
FROM fact_movie_snapshot
GROUP BY
    snapshot_date,
    movie_id
HAVING COUNT(*) > 1;


-- Проверяем, что каждый movie_id существует в dim_movie
SELECT
    COUNT(*) AS invalid_movie_id
FROM fact_movie_snapshot f
LEFT JOIN dim_movie m
    ON f.movie_id = m.movie_id
WHERE m.movie_id IS NULL;


-- Проверяем, что каждая snapshot_date существует в dim_date
SELECT
    COUNT(*) AS invalid_snapshot_date
FROM fact_movie_snapshot f
LEFT JOIN dim_date d
    ON f.snapshot_date = d.date_id
WHERE d.date_id IS NULL;


-- Проверяем ссылку на главного актера
-- main_actor_id может быть NULL
SELECT
    COUNT(*) AS invalid_actor_id
FROM fact_movie_snapshot f
LEFT JOIN dim_actor a
    ON f.main_actor_id = a.actor_id
WHERE f.main_actor_id IS NOT NULL
  AND a.actor_id IS NULL;


-- Проверяем ссылку на жанр
-- genre_id может быть NULL
SELECT
    COUNT(*) AS invalid_genre_id
FROM fact_movie_snapshot f
LEFT JOIN dim_genre g
    ON f.genre_id = g.genre_id
WHERE f.genre_id IS NOT NULL
  AND g.genre_id IS NULL;



-- ============================================================
-- FACT METRICS
-- ============================================================

-- Проверяем рейтинг фильма
-- TMDB vote_average должен находиться в диапазоне от 0 до 10
SELECT
    COUNT(*) AS invalid_vote_average
FROM fact_movie_snapshot
WHERE vote_average < 0
   OR vote_average > 10;


-- Проверяем количество голосов
-- vote_count не может быть отрицательным
SELECT
    COUNT(*) AS invalid_vote_count
FROM fact_movie_snapshot
WHERE vote_count < 0;


-- Проверяем отсутствие рейтинга
SELECT
    COUNT(*) AS null_vote_average
FROM fact_movie_snapshot
WHERE vote_average IS NULL;


-- Проверяем отсутствие количества голосов
SELECT
    COUNT(*) AS null_vote_count
FROM fact_movie_snapshot
WHERE vote_count IS NULL;



-- ============================================================
-- FACT COMPLETENESS
-- ============================================================

-- Общая полнота фактовой таблицы
-- Считаем строки, для которых не удалось определить:
-- 1. главного актера
-- 2. жанр
-- 3. одновременно актера и жанр
SELECT
    COUNT(*) FILTER (
        WHERE main_actor_id IS NULL
    ) AS movies_without_actor,

    COUNT(*) FILTER (
        WHERE genre_id IS NULL
    ) AS movies_without_genre,

    COUNT(*) FILTER (
        WHERE main_actor_id IS NULL
          AND genre_id IS NULL
    ) AS movies_without_actor_and_genre

FROM fact_movie_snapshot;


-- Те же показатели отдельно для каждого snapshot
-- Это позволяет увидеть, в какой конкретно загрузке
-- появились пропуски
SELECT
    snapshot_date,

    COUNT(*) FILTER (
        WHERE main_actor_id IS NULL
    ) AS movies_without_actor,

    COUNT(*) FILTER (
        WHERE genre_id IS NULL
    ) AS movies_without_genre,

    COUNT(*) FILTER (
        WHERE main_actor_id IS NULL
          AND genre_id IS NULL
    ) AS movies_without_actor_and_genre

FROM fact_movie_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date;



-- ============================================================
-- FACT ROW COUNTS
-- ============================================================

-- Проверяем общее количество строк в фактовой таблице
SELECT
    COUNT(*) AS fact_row_count
FROM fact_movie_snapshot;


-- Проверяем количество фильмов в каждом snapshot
SELECT
    snapshot_date,
    COUNT(*) AS movie_count
FROM fact_movie_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date;