{{ 
    config(
    materialized='table',  
    alias='dim_property', 
    unique_key='listing_id') 
}}

select distinct
  listing_id,
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,
  price,
  scraped_date      -- keep for timestamp snapshots
from {{ ref('airbnb_cleaned') }}