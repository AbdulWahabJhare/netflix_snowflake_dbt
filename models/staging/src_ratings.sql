{{config (materialized = 'table')}} --we are canging this view to table to check our incremental logic
WITH raw_ratings AS (
    SELECT * FROM MOVIELENS.RAW.raw_ratings
)
SELECT
    userId AS user_id,
    movieId AS movie_id,
    rating,
    TO_TIMESTAMP_NTZ(timestamp) AS rating_timestamp
FROM raw_ratings