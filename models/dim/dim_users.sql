with ratings as(
    SELECT DISTINCT user_id from {{ ref('src_ratings')}}
),

tags as (
    select DISTINCT user_id from {{ ref('src_tags')}}
)

select DISTINCT user_id
from (select * from ratings
UNION
select * from tags)