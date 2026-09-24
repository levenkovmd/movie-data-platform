-- DIM MOVIE
-- Проверяем, есть ли фильмы без идентификатора
SELECT COUNT(*) AS null_movie_id
FROM dim_movie
WHERE movie_id IS NULL;

-- Ищем дубликаты идентификаторов фильмов
-- Для каждого movie_id должно быть не более одной строки
SELECT
    movie_id,
    COUNT(*) AS row_count
FROM dim_movie
GROUP BY movie_id
HAVING COUNT(*) > 1;

-- Проверяем, есть ли фильмы без названия
SELECT COUNT(*) AS null_title
FROM dim_movie
WHERE title IS NULL;


-- DIM ACTOR
-- Проверяем, есть ли актеры без идентификатора
SELECT COUNT(*) AS null_actor_id
FROM dim_actor
WHERE actor_id IS NULL;

-- Ищем дубликаты идентификаторов актеров
-- Для каждого actor_id должна быть одна строка
SELECT
    actor_id,
    COUNT(*) AS row_count
FROM dim_actor
GROUP BY actor_id
HAVING COUNT(*) > 1;


-- DIM GENRE
-- Проверяем, есть ли жанры без идентификатора
SELECT COUNT(*) AS null_genre_id
FROM dim_genre
WHERE genre_id IS NULL;


-- Ищем дубликаты идентификаторов жанров
SELECT
    genre_id,
    COUNT(*) AS row_count
FROM dim_genre
GROUP BY genre_id
HAVING COUNT(*) > 1;


-- DIM DATE
-- Проверяем, есть ли даты без идентификатора
SELECT COUNT(*) AS null_date_id
FROM dim_date
WHERE date_id IS NULL;


-- Проверяем корректность номера месяца
-- Допустимые значения: от 1 до 12
SELECT COUNT(*) AS invalid_month
FROM dim_date
WHERE month < 1
   OR month > 12;

-- Проверяем корректность номера квартала
-- Допустимые значения: от 1 до 4
SELECT COUNT(*) AS invalid_quarter
FROM dim_date
WHERE quarter < 1
   OR quarter > 4;


-- BRIDGE MOVIE ACTOR
-- Ищем полные дубликаты связей между фильмом и актером
-- Учитываем все поля связи: фильм, актер, персонаж и порядок актера в списке
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


-- Проверяем, что каждый фильм из bridge_movie_actor
-- существует в таблице dim_movie
SELECT COUNT(*) AS invalid_movie_id
FROM bridge_movie_actor b
LEFT JOIN dim_movie m
    ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL;


-- Проверяем, что каждый актер из bridge_movie_actor
-- существует в таблице dim_actor
SELECT COUNT(*) AS invalid_actor_id
FROM bridge_movie_actor b
LEFT JOIN dim_actor a
    ON b.actor_id = a.actor_id
WHERE a.actor_id IS NULL;


-- BRIDGE MOVIE GENRE
-- Ищем дубликаты связей между фильмом и жанром
-- Одна пара movie_id + genre_id должна встречаться один раз
SELECT
    movie_id,
    genre_id,
    COUNT(*) AS relation_count
FROM bridge_movie_genre
GROUP BY movie_id, genre_id
HAVING COUNT(*) > 1;

-- Проверяем, что каждый фильм из bridge_movie_genre
-- существует в таблице dim_movie
SELECT COUNT(*) AS invalid_movie_id
FROM bridge_movie_genre b
LEFT JOIN dim_movie m
    ON b.movie_id = m.movie_id
WHERE m.movie_id IS NULL;

-- Проверяем, что каждый жанр из bridge_movie_genre
-- существует в таблице dim_genre
SELECT COUNT(*) AS invalid_genre_id
FROM bridge_movie_genre b
LEFT JOIN dim_genre g
    ON b.genre_id = g.genre_id
WHERE g.genre_id IS NULL;



-- FACT MOVIE SNAPSHOT
-- Проверяем, есть ли строки факта без даты snapshot
SELECT COUNT(*) AS null_snapshot_date
FROM fact_movie_snapshot
WHERE snapshot_date IS NULL;

-- Проверяем, есть ли строки факта без идентификатора фильма
SELECT COUNT(*) AS null_movie_id
FROM fact_movie_snapshot
WHERE movie_id IS NULL;

-- Проверяем, есть ли строки факта без названия фильма
SELECT COUNT(*) AS null_title
FROM fact_movie_snapshot
WHERE title IS NULL;

-- Проверяем уникальность ключа факта
-- Одна комбинация snapshot_date + movie_id
-- должна встречаться только один раз
SELECT
    snapshot_date,
    movie_id,
    COUNT(*) AS row_count
FROM fact_movie_snapshot
GROUP BY snapshot_date, movie_id
HAVING COUNT(*) > 1;

-- Проверяем, что каждый фильм из факта
-- существует в dim_movie
SELECT COUNT(*) AS invalid_movie_id
FROM fact_movie_snapshot f
LEFT JOIN dim_movie m
    ON f.movie_id = m.movie_id
WHERE m.movie_id IS NULL;

-- Проверяем корректность ссылки на главного актера
-- NULL здесь допустим, поэтому проверяем только заполненные значения
SELECT COUNT(*) AS invalid_actor_id
FROM fact_movie_snapshot f
LEFT JOIN dim_actor a
    ON f.main_actor_id = a.actor_id
WHERE f.main_actor_id IS NOT NULL
  AND a.actor_id IS NULL;

-- Проверяем корректность ссылки на жанр
-- NULL здесь допустим, поэтому проверяем только заполненные значения
SELECT COUNT(*) AS invalid_genre_id
FROM fact_movie_snapshot f
LEFT JOIN dim_genre g
    ON f.genre_id = g.genre_id
WHERE f.genre_id IS NOT NULL
  AND g.genre_id IS NULL;


-- ПОЛНОТА ДАННЫХ В FACT
-- Считаем фильмы, для которых не удалось определить:
-- 1. главного актера
-- 2. жанр
-- 3. одновременно и актера, и жанр
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



-- Проверяем общее количество строк в фактовой таблице
SELECT COUNT(*) AS fact_row_count
FROM fact_movie_snapshot;



-- РАСПРЕДЕЛЕНИЕ ФАКТА ПО SNAPSHOT-ДАТАМ
-- Проверяем, сколько фильмов приходится на каждую дату snapshot
SELECT
    snapshot_date,
    COUNT(*) AS movie_count
FROM fact_movie_snapshot
GROUP BY snapshot_date
ORDER BY snapshot_date;