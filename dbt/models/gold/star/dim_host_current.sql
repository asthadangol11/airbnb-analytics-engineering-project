{{ config(materialized='table', alias='dim_host') }}

-- Latest (current) host attributes from the SCD2 snapshot
select
  h.host_id,
  h.host_name,
  h.host_since,
  h.host_is_superhost,
  h.host_neighbourhood
from {{ ref('host_snapshot') }} h
where h.dbt_valid_to is null
