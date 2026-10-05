DROP TABLE IF EXISTS mart_movie_trends;

CREATE TABLE mart_movie_trends (
    snapshot_date DATE NOT NULL,
    movie_id BIGINT NOT NULL,
    title TEXT NOT NULL,

    popularity DOUBLE PRECISION,
    previous_popularity DOUBLE PRECISION,
    popularity_change DOUBLE PRECISION,

    vote_average DOUBLE PRECISION,
    previous_vote_average DOUBLE PRECISION,
    vote_average_change DOUBLE PRECISION,

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

    popularity,
    previous_popularity,
    popularity - previous_popularity AS popularity_change,

    vote_average,
    previous_vote_average,
    vote_average - previous_vote_average AS vote_average_change,

    vote_count,
    previous_vote_count,
    vote_count - previous_vote_count AS vote_count_change

FROM trends;