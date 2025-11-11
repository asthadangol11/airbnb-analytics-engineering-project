{{ config(materialized='table', alias='dim_listing') }}


select
  p.listing_id,
  p.property_type,
  p.room_type,
  p.accommodates,
  p.listing_neighbourhood
from {{ ref('property_snapshot') }} p
where p.dbt_valid_to is null