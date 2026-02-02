select * from {{ref('fct_reviews') }} a
join {{ref('dim_listings_cleansed') }} b on a.listing_id =b.listing_id
where a.review_date < b.created_at