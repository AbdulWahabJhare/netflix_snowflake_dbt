SELECT
    relevance_score
FROM {{ ref('fct_genome_scores') }}
WHERE relevance_score <= 0