DROP TABLE IF EXISTS mart_genre_stats;

CREATE TABLE mart_genre_stats (
    snapshot_date DATE NOT NULL,
    genre_id BIGINT NOT NULL,
    genre_name TEXT NOT NULL,
    movie_count BIGINT NOT NULL,
    rated_movie_count BIGINT NOT NULL,
    avg_popularity DOUBLE PRECISION,
    max_popularity DOUBLE PRECISION,
    avg_vote_average DOUBLE PRECISION,
    weighted_vote_average DOUBLE PRECISION,
    total_vote_count BIGINT,
    avg_budget DOUBLE PRECISION,

    PRIMARY KEY (snapshot_date, genre_id)
);

INSERT INTO mart_genre_stats (
    snapshot_date,
    genre_id,
    genre_name,
    movie_count,
    rated_movie_count,
    avg_popularity,
    max_popularity,
    avg_vote_average,
    weighted_vote_average,
    total_vote_count,
    avg_budget
)
SELECT
    f.snapshot_date,
    g.genre_id,
    g.genre_name,

    COUNT(*) AS movie_count,

    COUNT(*) FILTER (
        WHERE f.vote_count > 0
    ) AS rated_movie_count,

    AVG(f.popularity) AS avg_popularity,

    MAX(f.popularity) AS max_popularity,

    AVG(f.vote_average) FILTER (
        WHERE f.vote_count > 0
    ) AS avg_vote_average,

    SUM(f.vote_average * f.vote_count)
        / NULLIF(SUM(f.vote_count), 0)
        AS weighted_vote_average,

    SUM(f.vote_count)::BIGINT AS total_vote_count,

    AVG(f.budget) FILTER (
        WHERE f.budget > 0
    ) AS avg_budget

FROM fact_movie_snapshot AS f

JOIN bridge_movie_genre AS b
    ON f.movie_id = b.movie_id

JOIN dim_genre AS g
    ON b.genre_id = g.genre_id

GROUP BY
    f.snapshot_date,
    g.genre_id,
    g.genre_name;