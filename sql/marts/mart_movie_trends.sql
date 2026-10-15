DROP TABLE IF EXISTS mart_movie_trends;

CREATE TABLE mart_movie_trends (
    snapshot_date DATE NOT NULL,
    movie_id BIGINT NOT NULL,
    title TEXT NOT NULL,

    popularity NUMERIC(12, 4),
    previous_popularity NUMERIC(12, 4),
    popularity_change NUMERIC(12, 4),

    vote_average NUMERIC(6, 3),
    previous_vote_average NUMERIC(6, 3),
    vote_average_change NUMERIC(6, 3),

    vote_count BIGINT,
    previous_vote_count BIGINT,
    vote_count_change BIGINT,

    PRIMARY KEY (snapshot_date, movie_id)
);

INSERT INTO mart_movie_trends (
    snapshot_date,
    movie_id,
    title,

    popularity,
    previous_popularity,
    popularity_change,

    vote_average,
    previous_vote_average,
    vote_average_change,

    vote_count,
    previous_vote_count,
    vote_count_change
)
WITH trends AS (
    SELECT
        snapshot_date,
        movie_id,
        title,
        popularity,
        vote_average,
        vote_count,

        LAG(popularity) OVER (
            PARTITION BY movie_id
            ORDER BY snapshot_date
        ) AS previous_popularity,

        LAG(vote_average) OVER (
            PARTITION BY movie_id
            ORDER BY snapshot_date
        ) AS previous_vote_average,

        LAG(vote_count) OVER (
            PARTITION BY movie_id
            ORDER BY snapshot_date
        ) AS previous_vote_count

    FROM fact_movie_snapshot
)

SELECT
    snapshot_date,
    movie_id,
    title,

    ROUND(popularity::NUMERIC, 4),
    ROUND(previous_popularity::NUMERIC, 4),
    ROUND((popularity - previous_popularity)::NUMERIC, 4),

    ROUND(vote_average::NUMERIC, 3),
    ROUND(previous_vote_average::NUMERIC, 3),
    ROUND((vote_average - previous_vote_average)::NUMERIC, 3),

    vote_count,
    previous_vote_count,
    vote_count - previous_vote_count

FROM trends;