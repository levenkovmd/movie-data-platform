DROP TABLE IF EXISTS mart_movie_performance;

CREATE TABLE mart_movie_performance (
    snapshot_date DATE NOT NULL,
    movie_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    release_date DATE,
    release_year INTEGER,
    original_language TEXT,
    popularity DOUBLE PRECISION,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    budget BIGINT,
    main_actor_id BIGINT,
    main_actor_name TEXT,
    genre_id BIGINT,
    genre_name TEXT,

    PRIMARY KEY (snapshot_date, movie_id)
);

INSERT INTO mart_movie_performance (
    snapshot_date,
    movie_id,
    title,
    release_date,
    release_year,
    original_language,
    popularity,
    vote_average,
    vote_count,
    budget,
    main_actor_id,
    main_actor_name,
    genre_id,
    genre_name
)
SELECT
    f.snapshot_date,
    f.movie_id,
    f.title,
    m.release_date,
    EXTRACT(YEAR FROM m.release_date)::INTEGER AS release_year,
    f.original_language,
    f.popularity,
    f.vote_average,
    f.vote_count,
    f.budget,
    f.main_actor_id,
    f.main_actor_name,
    f.genre_id,
    f.genre_name
FROM fact_movie_snapshot AS f
JOIN dim_movie AS m 
    ON f.movie_id = m.movie_id;