{{ config(materialized='view', alias='dm_host_neighbourhood') }}


with base_fact as (
  select
    host_id,
    listing_id,
    scraped_date::date                                      as scraped_date,
    date_trunc('month', scraped_date)::date                 as month_start,
    price::numeric                                          as price,
    has_availability,
    availability_30
  from {{ ref('fact_listings') }}
),


host_attr_scd2 as (
  select host_id, host_neighbourhood, dbt_valid_from, dbt_valid_to
  from {{ ref('host_snapshot') }}
),


suburb_to_lga as (
  select lower(suburb_name) as suburb_name_lc, lga_name
  from {{ ref('lga_suburb_cleaned') }}
),

enriched as (
  select
    f.month_start,
    f.host_id,
    f.listing_id,
    coalesce(l.lga_name, h.host_neighbourhood)              as host_neighbourhood_lga,
    case when f.has_availability = 't' then 1 else 0 end    as is_active,
    greatest(0, 30 - coalesce(f.availability_30, 0))        as stays_in_30_days,
    f.price
  from base_fact f
  left join host_attr_scd2 h
    on f.host_id = h.host_id
   and f.scraped_date >= h.dbt_valid_from
   and (f.scraped_date < h.dbt_valid_to or h.dbt_valid_to is null)
  left join suburb_to_lga l
    on lower(h.host_neighbourhood) = l.suburb_name_lc
),


latest_per_listing_month as (
  select *
  from (
    select
      e.*,
      row_number() over (
        partition by e.listing_id, e.month_start
        order by e.month_start desc, e.stays_in_30_days desc
      ) as rn
    from enriched e
  ) z
  where z.rn = 1
),

agg_host_neigh_month as (
  select
    host_neighbourhood_lga,
    month_start,

    count(distinct host_id)                                  as distinct_hosts_count,

    -- revenue per active listing (average)
    case when sum(case when is_active = 1 then 1 else 0 end) > 0
      then sum( (case when is_active = 1 then stays_in_30_days else 0 end) * price )::numeric
           / nullif(sum(case when is_active = 1 then 1 else 0 end), 0)
    end                                                     as estimated_revenue_per_active_listing,

    -- revenue per host (total revenue divided by distinct hosts)
    case when count(distinct host_id) > 0
      then sum( (case when is_active = 1 then stays_in_30_days else 0 end) * price )::numeric
           / nullif(count(distinct host_id), 0)
    end                                                     as estimated_revenue_per_host
  from latest_per_listing_month
  group by 1,2
)

select
  host_neighbourhood_lga,
  month_start,
  distinct_hosts_count,
  estimated_revenue_per_active_listing,
  estimated_revenue_per_host
from agg_host_neigh_month
order by host_neighbourhood_lga, month_start