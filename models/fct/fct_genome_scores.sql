with src_scores as (
    SELECT * from {{ref('src_genome_scores')}}
)

select 
    movie_id,
    tag_id,
    round(relevance,4) as relevance_score
from src_scores
where relevance > 0