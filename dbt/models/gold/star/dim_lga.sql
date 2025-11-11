{{ config(materialized='table', alias='dim_lga') }}

select
  lga_code,
  lga_name
from {{ ref('lga_code_cleaned') }}