
  
    

create or replace transient table AIRBNB.DEV.fct_reviews
    
    
    
    as (

WITH  __dbt__cte__src_reviews as (
with raw_reviews as
(
    select * from  AIRBNB.raw.raw_reviews
)
SELECT
   listing_id,
   date as review_date,
   reviewer_name,
   comments as review_text,
   sentiment as review_sentiment
FROM
    raw_reviews
), src_reviews AS (
  SELECT   md5(cast(coalesce(cast(listing_id as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(review_date as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(reviewer_name as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(review_text as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT))
    AS review_id,
    * 
  FROM __dbt__cte__src_reviews
)
SELECT * FROM src_reviews
WHERE review_text is not null


    )
;


  