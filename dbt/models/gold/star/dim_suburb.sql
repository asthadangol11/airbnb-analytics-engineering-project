{{ config(materialized='table', alias='dim_suburb') }}

select
  suburb_name,
  lga_name
from {{ ref('lga_suburb_cleaned') }}
