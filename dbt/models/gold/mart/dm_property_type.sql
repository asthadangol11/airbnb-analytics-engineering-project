{{ config(materialized='view', alias='dm_property_type') }}

with base_fact as (
  select
    listing_id,
    host_id,
    scraped_date::date                                      as scraped_date,
    date_trunc('month', scraped_date)::date                 as month_start,
    price::numeric                                          as price,
    has_availability,
    availability_30,
    review_scores_rating
  from {{ ref('fact_listings') }}
),

listing_attr_scd2 as (
  select listing_id, property_type, room_type, accommodates, dbt_valid_from, dbt_valid_to
  from {{ ref('property_snapshot') }}
),
host_attr_scd2 as (
  select host_id, host_is_superhost, dbt_valid_from, dbt_valid_to
  from {{ ref('host_snapshot') }}
),

enriched as (
  select
    f.*,
    a.property_type,
    a.room_type,
    a.accommodates,
    case when f.has_availability = 't' then 1 else 0 end     as is_active,
    greatest(0, 30 - coalesce(f.availability_30, 0))         as stays_in_30_days,
    h.host_is_superhost
  from base_fact f
  left join listing_attr_scd2 a
    on f.listing_id = a.listing_id
   and f.scraped_date >= a.dbt_valid_from
   and (f.scraped_date < a.dbt_valid_to or a.dbt_valid_to is null)
  left join host_attr_scd2 h
    on f.host_id = h.host_id
   and f.scraped_date >= h.dbt_valid_from
   and (f.scraped_date < h.dbt_valid_to or h.dbt_valid_to is null)
),

latest_per_listing_month as (
  select *
  from (
    select
      e.*,
      row_number() over (
        partition by e.listing_id, e.month_start
        order by e.scraped_date desc
      ) as rn
    from enriched e
  ) z
  where z.rn = 1
),

agg_prop_month as (
  select
    property_type,
    room_type,
    accommodates,
    month_start,

    count(*)::int                                            as total_listings_count,
    sum(is_active)::int                                      as active_listings_count,
    100.0 * sum(is_active) / nullif(count(*), 0)             as active_listings_rate,

    min(price) filter (where is_active = 1)                  as min_active_price,
    max(price) filter (where is_active = 1)                  as max_active_price,
    percentile_cont(0.5) within group (order by price)
      filter (where is_active = 1)                           as median_active_price,
    avg(price)  filter (where is_active = 1)                 as avg_active_price,

    count(distinct host_id)                                  as distinct_hosts_count,
    100.0 * count(distinct case when is_active = 1 and host_is_superhost = 't' then host_id end)
          / nullif(count(distinct host_id), 0)               as superhost_rate,

    avg(review_scores_rating) filter (where is_active = 1)   as avg_active_rating,

    sum(case when is_active = 1 then stays_in_30_days else 0 end) as total_active_stays,
    case when sum(case when is_active = 1 then 1 else 0 end) > 0
      then sum( (case when is_active = 1 then stays_in_30_days else 0 end) * price )::numeric
           / nullif(sum(case when is_active = 1 then 1 else 0 end), 0)
    end                                                      as avg_estimated_rev_per_active_listing,

    -- raw counts for MoM deltas
    sum(case when is_active = 1 then 1 else 0 end)           as active_ct,
    sum(case when is_active = 0 then 1 else 0 end)           as inactive_ct
  from latest_per_listing_month
  group by 1,2,3,4
)

select
  property_type,
  room_type,
  accommodates,
  month_start,
  active_listings_rate,
  min_active_price,
  max_active_price,
  median_active_price,
  avg_active_price,
  distinct_hosts_count,
  superhost_rate,
  avg_active_rating,
  total_active_stays,
  avg_estimated_rev_per_active_listing,

  case
    when lag(active_ct) over (partition by property_type, room_type, accommodates order by month_start) in (null, 0) then null
    else 100.0 * (active_ct - lag(active_ct) over (partition by property_type, room_type, accommodates order by month_start))
               / nullif(lag(active_ct) over (partition by property_type, room_type, accommodates order by month_start), 0)
  end as pct_change_active_listings,

  case
    when lag(inactive_ct) over (partition by property_type, room_type, accommodates order by month_start) in (null, 0) then null
    else 100.0 * (inactive_ct - lag(inactive_ct) over (partition by property_type, room_type, accommodates order by month_start))
               / nullif(lag(inactive_ct) over (partition by property_type, room_type, accommodates order by month_start), 0)
  end as pct_change_inactive_listings

from agg_prop_month
order by property_type, room_type, accommodates, month_start
