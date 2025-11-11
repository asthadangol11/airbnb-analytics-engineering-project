{{ config
(
    materialized='table',
    schema='silver',  
    alias='airbnb_cleaned',
    unique_key=['listing_id','scraped_date']) 
}}

with cleaned as (
  select
    listing_id,
    scrape_id,
    scraped_date::date as scraped_date,
    host_id,
    coalesce(host_name, 'Unknown') as host_name,
    coalesce(host_since, '2000-01-01')::date as host_since,
    coalesce(host_is_superhost, 'f') as host_is_superhost,
    coalesce(host_neighbourhood, 'Not specified') as host_neighbourhood,
    listing_neighbourhood,
    property_type,
    room_type,
    accommodates,
    (regexp_replace(price::text, '[^0-9\.]', '', 'g'))::numeric as price,
    has_availability,
    coalesce(availability_30, 0) as availability_30,
    coalesce(number_of_reviews, 0) as number_of_reviews,
    coalesce(review_scores_rating, 0) as review_scores_rating,
    coalesce(review_scores_accuracy, 0) as review_scores_accuracy,
    coalesce(review_scores_cleanliness, 0) as review_scores_cleanliness,
    coalesce(review_scores_checkin, 0) as review_scores_checkin,
    coalesce(review_scores_communication, 0) as review_scores_communication,
    coalesce(review_scores_value, 0) as review_scores_value
  from {{ ref('airbnb_raw') }}
)

select * from cleaned