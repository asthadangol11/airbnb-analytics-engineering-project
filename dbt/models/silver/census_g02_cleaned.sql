{{ 
    config(
        materialized='table',
        unique_key='lga_code_2016',
        alias='census_g02_cleaned'
    ) 
}}
select
  lga_code_2016,
  coalesce(median_age_persons,0)               as median_age_persons,
  coalesce(median_mortgage_repay_monthly,0)    as median_mortgage_repay_monthly,
  coalesce(median_tot_prsnl_inc_weekly,0)      as median_tot_prsnl_inc_weekly,
  coalesce(median_rent_weekly,0)               as median_rent_weekly,
  coalesce(median_tot_fam_inc_weekly,0)        as median_tot_fam_inc_weekly,
  coalesce(average_num_psns_per_bedroom,0)     as average_num_psns_per_bedroom,
  coalesce(median_tot_hhd_inc_weekly,0)        as median_tot_hhd_inc_weekly,
  coalesce(average_household_size,0)           as average_household_size
from {{ ref('census_g02') }}

