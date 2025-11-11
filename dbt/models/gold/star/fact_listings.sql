{{ config(materialized='table', alias='fact_listings') }}

select
  a.listing_id,
  a.host_id,
  a.scraped_date::date as scraped_date,  
  a.price::numeric     as price,
  a.number_of_reviews  as number_of_reviews,
  a.review_scores_rating,
  a.has_availability,
  a.availability_30
from {{ ref('airbnb_cleaned') }} a
