CREATE TABLE IF NOT EXISTS dim_date (
    date_id DATE PRIMARY KEY,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL
);


CREATE TABLE IF NOT EXISTS dim_movie (
    movie_id BIGINT PRIMARY KEY,
    title TEXT NOT NULL,
    original_title TEXT,
    original_language TEXT,
    release_date DATE,
    runtime INTEGER,
    status TEXT,
    imdb_id TEXT,
    overview TEXT,
    tagline TEXT,
    poster_path TEXT,
    backdrop_path TEXT,
    adult BOOLEAN,
    video BOOLEAN
);


CREATE TABLE IF NOT EXISTS dim_actor (
    actor_id BIGINT PRIMARY KEY,
    actor_name TEXT NOT NULL,
    gender INTEGER,
    known_for_department TEXT
);


CREATE TABLE IF NOT EXISTS dim_genre (
    genre_id BIGINT PRIMARY KEY,
    genre_name TEXT NOT NULL
);


CREATE TABLE IF NOT EXISTS bridge_movie_genre (
    movie_id BIGINT NOT NULL,
    genre_id BIGINT NOT NULL,

    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES dim_movie(movie_id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_actor (
    movie_id BIGINT NOT NULL,
    actor_id BIGINT NOT NULL,
    character TEXT,
    cast_order INTEGER,

    PRIMARY KEY (movie_id, actor_id, character, cast_order),
    FOREIGN KEY (movie_id) REFERENCES dim_movie(movie_id),
    FOREIGN KEY (actor_id) REFERENCES dim_actor(actor_id)
);

CREATE TABLE IF NOT EXISTS fact_movie_snapshot (
    snapshot_date DATE NOT NULL,
    movie_id BIGINT NOT NULL,
    title TEXT NOT NULL,
    popularity DOUBLE PRECISION,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    budget BIGINT,
    main_actor_id BIGINT,
    main_actor_name TEXT,
    genre_id BIGINT,
    genre_name TEXT,
    original_language TEXT,

    PRIMARY KEY (snapshot_date, movie_id),
    FOREIGN KEY (snapshot_date) REFERENCES dim_date(date_id),
    FOREIGN KEY (movie_id) REFERENCES dim_movie(movie_id),
    FOREIGN KEY (main_actor_id) REFERENCES dim_actor(actor_id),
    FOREIGN KEY (genre_id) REFERENCES dim_genre(genre_id)
);