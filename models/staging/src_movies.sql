with raw_movies as (SELECT * from MOVIELENS.RAW.raw_movies)

SELECT
    movieId as movie_id,
    title,
    genres
from raw_movies
