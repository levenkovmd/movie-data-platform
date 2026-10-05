-- MART_MOVIE_PERFORMANCE

-- NULL в ключевых полях
SELECT COUNT(*) AS invalid_movie_performance_rows
FROM mart_movie_performance
WHERE snapshot_date IS NULL
   OR movie_id IS NULL
   OR title IS NULL;

-- Дубликаты grain: один фильм на одну дату
SELECT
    snapshot_date,
    movie_id,
    COUNT(*) AS row_count
FROM mart_movie_performance
GROUP BY snapshot_date, movie_id
HAVING COUNT(*) > 1;

-- Количество строк должно совпадать с fact
SELECT
    (SELECT COUNT(*) FROM fact_movie_snapshot) AS fact_count,
    (SELECT COUNT(*) FROM mart_movie_performance) AS mart_count;


-- MART_GENRE_STATS

-- Дубликаты grain: один жанр на одну дату
SELECT
    snapshot_date,
    genre_id,
    COUNT(*) AS row_count
FROM mart_genre_stats
GROUP BY snapshot_date, genre_id
HAVING COUNT(*) > 1;

-- Некорректные счетчики
SELECT COUNT(*) AS invalid_genre_counts
FROM mart_genre_stats
WHERE movie_count < 0
   OR rated_movie_count < 0
   OR rated_movie_count > movie_count
   OR total_vote_count < 0;

-- Некорректные рейтинги
SELECT COUNT(*) AS invalid_genre_ratings
FROM mart_genre_stats
WHERE avg_vote_average < 0
   OR avg_vote_average > 10
   OR weighted_vote_average < 0
   OR weighted_vote_average > 10;


-- MART_ACTOR_STATS


-- Дубликаты grain: один актер на одну дату
SELECT
    snapshot_date,
    actor_id,
    COUNT(*) AS row_count
FROM mart_actor_stats
GROUP BY snapshot_date, actor_id
HAVING COUNT(*) > 1;

-- У актера в витрине должен быть хотя бы один фильм
SELECT COUNT(*) AS invalid_actor_movie_count
FROM mart_actor_stats
WHERE movie_count <= 0;

-- Некорректные рейтинги
SELECT COUNT(*) AS invalid_actor_ratings
FROM mart_actor_stats
WHERE avg_vote_average < 0
   OR avg_vote_average > 10
   OR weighted_vote_average < 0
   OR weighted_vote_average > 10;



-- MART_MOVIE_TRENDS


-- NULL в ключевых полях
SELECT COUNT(*) AS invalid_trend_rows
FROM mart_movie_trends
WHERE snapshot_date IS NULL
   OR movie_id IS NULL
   OR title IS NULL;

-- Дубликаты grain
SELECT
    snapshot_date,
    movie_id,
    COUNT(*) AS row_count
FROM mart_movie_trends
GROUP BY snapshot_date, movie_id
HAVING COUNT(*) > 1;

-- Количество строк должно совпадать с fact
SELECT
    (SELECT COUNT(*) FROM fact_movie_snapshot) AS fact_count,
    (SELECT COUNT(*) FROM mart_movie_trends) AS trend_count;