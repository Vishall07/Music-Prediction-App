CREATE TABLE IF NOT EXISTS songs (
    artist VARCHAR(100),
    song VARCHAR(150),
    duration_ms INT,
    explicit BOOLEAN,
    year INT,
    popularity INT,
    danceability FLOAT,
    energy FLOAT,
    feature1 INT,
    loudness FLOAT,
    feature2 INT,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    genre VARCHAR(100)
);

INSERT INTO songs (artist, song, year, popularity)
VALUES
('Artist 1', 'Song A', 2020, 85),
('Artist 2', 'Song B', 2019, 78),
('Artist 1', 'Song A', 2020, 85),
('Artist 2', 'Song B', 2019, 78),
('Artist 1', 'Song A', 2020, 85),
('Artist 2', 'Song B', 2019, 78),
('Artist 1', 'Song A', 2020, 85),
('Artist 2', 'Song B', 2019, 78);
