{{ config(materialized='view', alias='dm_listing_neighbourhood') }}

with base_fact as (
  select
    listing_id,
    host_id,
    scraped_date::date                              as scraped_date,
    date_trunc('month', scraped_date)::date         as month_start,
    price::numeric                                  as price,
    has_availability,
    availability_30,
    review_scores_rating
  from {{ ref('fact_listings') }}
),
listing_attr_scd2 as (
  select listing_id, listing_neighbourhood, dbt_valid_from, dbt_valid_to
  from {{ ref('property_snapshot') }}
),
host_attr_scd2 as (
  select host_id, host_is_superhost, dbt_valid_from, dbt_valid_to
  from {{ ref('host_snapshot') }}
),
enriched as (
  select
    f.*,
    a.listing_neighbourhood,
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
agg_neighbourhood_month as (
  select
    coalesce(listing_neighbourhood, 'Unknown')                as listing_neighbourhood,
    month_start,

    count(*)::int                                              as total_listings_count,
    sum(is_active)::int                                        as active_listings_count,
    (100::numeric * sum(is_active)::numeric / nullif(count(*), 0))::numeric as active_listings_rate,

    min(price) filter (where is_active = 1)                    as min_active_price,
    max(price) filter (where is_active = 1)                    as max_active_price,
    (percentile_cont(0.5) within group (order by price)
      filter (where is_active = 1))::numeric                   as median_active_price,
    avg(price)  filter (where is_active = 1)                   as avg_active_price,

    count(distinct host_id)                                    as distinct_hosts_count,
    (100::numeric * count(distinct case when host_is_superhost = 't' then host_id end)
      / nullif(count(distinct host_id), 0))::numeric           as superhost_rate,

    avg(review_scores_rating) filter (where is_active = 1)     as avg_active_rating,

    sum(case when is_active = 1 then stays_in_30_days else 0 end) as total_active_stays,

    case when sum(case when is_active = 1 then 1 else 0 end) > 0
      then (
        sum( (case when is_active = 1 then stays_in_30_days else 0 end) * price )::numeric
        / nullif(sum(case when is_active = 1 then 1 else 0 end), 0)
      )
    end                                                        as avg_estimated_rev_per_active_listing,

    -- raw counts for MoM deltas
    sum(case when is_active = 1 then 1 else 0 end)             as active_ct,
    sum(case when is_active = 0 then 1 else 0 end)             as inactive_ct
  from latest_per_listing_month
  group by 1,2
),
with_prev as (
  select
    a.*,
    lag(active_ct)   over (partition by listing_neighbourhood order by month_start) as prev_active_ct,
    lag(inactive_ct) over (partition by listing_neighbourhood order by month_start) as prev_inactive_ct
  from agg_neighbourhood_month a
)

select
  listing_neighbourhood,
  month_start,
  to_char(month_start, 'YYYY-MM')                              as month_year,
  round(active_listings_rate::numeric, 2)                      as active_listings_rate,
  round(min_active_price::numeric, 2)                          as min_active_price,
  round(max_active_price::numeric, 2)                          as max_active_price,
  round(median_active_price::numeric, 2)                       as median_active_price,
  round(avg_active_price::numeric, 2)                          as avg_active_price,
  distinct_hosts_count,
  round(superhost_rate::numeric, 2)                            as superhost_rate,
  round(avg_active_rating::numeric, 2)                         as avg_active_rating,
  total_active_stays,
  round(avg_estimated_rev_per_active_listing::numeric, 2)      as avg_estimated_rev_per_active_listing,
  case
    when prev_active_ct is null or prev_active_ct = 0 then null
    else round( (100::numeric * (active_ct - prev_active_ct)::numeric / nullif(prev_active_ct, 0))::numeric, 2)
  end as pct_change_active_listings,
  case
    when prev_inactive_ct is null or prev_inactive_ct = 0 then null
    else round( (100::numeric * (inactive_ct - prev_inactive_ct)::numeric / nullif(prev_inactive_ct, 0))::numeric, 2)
  end as pct_change_inactive_listings
from with_prev
order by listing_neighbourhood, month_start
