{{ config(materialized='table', alias='lga_suburb_cleaned', unique_key='suburb_name') }}

select suburb_name, coalesce(lga_name,'Unknown') as lga_name
from {{ ref('nsw_lga_suburb') }}
