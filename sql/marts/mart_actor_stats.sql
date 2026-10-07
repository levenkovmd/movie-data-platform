DROP TABLE IF EXISTS mart_actor_stats;

CREATE TABLE mart_actor_stats (
    snapshot_date DATE NOT NULL,
    actor_id BIGINT NOT NULL,
    actor_name TEXT NOT NULL,

    movie_count BIGINT NOT NULL,

    avg_popularity DOUBLE PRECISION,
    max_popularity DOUBLE PRECISION,

    avg_vote_average DOUBLE PRECISION,
    weighted_vote_average DOUBLE PRECISION,
    total_vote_count BIGINT,

    PRIMARY KEY (snapshot_date, actor_id)
);

INSERT INTO mart_actor_stats (
    snapshot_date,
    actor_id,
    actor_name,
    movie_count,
    avg_popularity,
    max_popularity,
    avg_vote_average,
    weighted_vote_average,
    total_vote_count
)
WITH actor_movies AS (
    SELECT DISTINCT
        movie_id,
        actor_id
    FROM bridge_movie_actor
)
SELECT
    f.snapshot_date,
    a.actor_id,
    a.actor_name,

    COUNT(*) AS movie_count,

    AVG(f.popularity) AS avg_popularity,

    MAX(f.popularity) AS max_popularity,

    AVG(f.vote_average) FILTER (
        WHERE f.vote_count > 0
    ) AS avg_vote_average,

    SUM(f.vote_average * f.vote_count)
        / NULLIF(SUM(f.vote_count), 0)
        AS weighted_vote_average,

    SUM(f.vote_count)::BIGINT AS total_vote_count

FROM actor_movies AS am

JOIN fact_movie_snapshot AS f
    ON am.movie_id = f.movie_id

JOIN dim_actor AS a
    ON am.actor_id = a.actor_id

GROUP BY
    f.snapshot_date,
    a.actor_id,
    a.actor_name;