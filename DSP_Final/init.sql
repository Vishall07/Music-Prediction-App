CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflowpass';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

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

INSERT INTO songs (
    artist, song, duration_ms, explicit, year, popularity, danceability, energy, feature1, loudness,
    feature2, speechiness, acousticness, instrumentalness, liveness, valence, tempo, genre
)
VALUES
('Britney Spears', 'Oops!...I Did It Again', 211160, FALSE, 2000, 77, 0.751, 0.834, 1, -5.444, 0, 0.0437, 0.3, 1.77e-05, 0.355, 0.894, 95.053, 'pop'),
('blink-182', 'All The Small Things', 167066, FALSE, 1999, 79, 0.434, 0.897, 50, -4.918, 1, 0.0488, 0.0103, 0, 0.612, 0.684, 148.726, 'rock, pop'),
('Faith Hill', 'Breathe', 250546, FALSE, 1999, 66, 0.529, 0.496, 50, -9.007, 1, 0.029, 0.173, 0, 0.251, 0.278, 136.859, 'pop, country')
;


CREATE TABLE predictions (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    request_source VARCHAR(100),
    input_songs JSONB,
    output_predictions JSONB
);
