{{ config(materialized='table', alias='lga_code_cleaned', unique_key='lga_code') }}

select lga_code, lga_name
from {{ ref('nsw_lga_code') }}
where lga_code is not null
