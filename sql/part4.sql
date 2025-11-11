
###################### Part 4.a #######################################################
/* a.What are the demographic differences (e.g., age group distribution, household size)between the top 3 performing and lowest 3 
performing LGAs based on estimated revenue per active listing over the last 12 months? */
######################################################################################

WITH
max_date AS (
  SELECT MAX(scraped_date) AS anchor_dt
  FROM dbt_astha_silver.airbnb_cleaned
),

last_12m AS (
  SELECT a.*
  FROM dbt_astha_silver.airbnb_cleaned a
  CROSS JOIN max_date m
  WHERE a.scraped_date >= (m.anchor_dt - INTERVAL '12 months')
),

-- pick ONE snapshot per listing per month (latest within the month)
monthly_latest AS (
  SELECT
    listing_id,
    date_trunc('month', scraped_date)::date AS month_start,
    MAX(scraped_date) AS last_dt
  FROM last_12m
  GROUP BY 1,2
),

suburb_to_lga AS (
  SELECT DISTINCT
         lower(trim(s.suburb_name))                         AS suburb_key,
         c.lga_name,
         c.lga_code                                         AS lga_code_raw,
         regexp_replace(c.lga_code::text, '[^0-9]', '', 'g') AS lga_code_key
  FROM dbt_astha_silver.lga_suburb_cleaned s
  JOIN dbt_astha_silver.lga_code_cleaned   c
    ON lower(trim(s.lga_name)) = lower(trim(c.lga_name))
),

rows_with_lga AS (
  SELECT
    a.listing_id,
    ml.month_start,
    a.scraped_date,
    a.price::numeric  AS price,
    a.availability_30,
    l.lga_name,
    l.lga_code_key
  FROM last_12m a
  JOIN monthly_latest ml
    ON a.listing_id = ml.listing_id
   AND a.scraped_date = ml.last_dt
  LEFT JOIN suburb_to_lga l
    ON lower(trim(a.listing_neighbourhood)) = l.suburb_key
  WHERE l.lga_code_key IS NOT NULL
),

-- revenue proxy per chosen monthly snapshot
row_revenue AS (
  SELECT
    listing_id,
    lga_code_key,
    lga_name,
    month_start,
    GREATEST(0, LEAST(30, 30 - COALESCE(availability_30, 0)))::int               AS stays_30,
    (COALESCE(price,0) * GREATEST(0, LEAST(30, 30 - COALESCE(availability_30, 0))))::numeric AS revenue_30
  FROM rows_with_lga
),

-- 12-month totals per listing (only active listings: >=1 booked night)
listing_12m AS (
  SELECT
    listing_id,
    lga_code_key,
    lga_name,
    SUM(stays_30)            AS stays_12m,
    SUM(revenue_30)::numeric AS revenue_12m
  FROM row_revenue
  GROUP BY listing_id, lga_code_key, lga_name
  HAVING SUM(stays_30) > 0
),

-- LGA performance
lga_perf AS (
  SELECT
    lga_code_key,
    lga_name,
    COUNT(*)                   AS active_listings,
    AVG(revenue_12m)::numeric AS avg_rev_per_active_listing
  FROM listing_12m
  GROUP BY lga_code_key, lga_name
),

top3 AS (
  SELECT * FROM lga_perf
  ORDER BY avg_rev_per_active_listing DESC NULLS LAST
  LIMIT 3
),
bottom3 AS (
  SELECT * FROM lga_perf
  ORDER BY avg_rev_per_active_listing ASC NULLS LAST
  LIMIT 3
),
selected_lgas AS (
  SELECT 'top3' AS grp, * FROM top3
  UNION ALL
  SELECT 'bottom3' AS grp, * FROM bottom3
),

-- Census demographics (fractions 0–1)
census_demo AS (
  SELECT
    regexp_replace(g01.lga_code_2016::text, '[^0-9]', '', 'g') AS lga_code_key,
    g02.average_household_size,
    (g01.age_0_4_yr_p  ::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_0_4,
    (g01.age_5_14_yr_p ::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_5_14,
    (g01.age_15_19_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_15_19,
    (g01.age_20_24_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_20_24,
    (g01.age_25_34_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_25_34,
    (g01.age_35_44_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_35_44,
    (g01.age_45_54_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_45_54,
    (g01.age_55_64_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_55_64,
    (g01.age_65_74_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_65_74,
    (g01.age_75_84_yr_p::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_75_84,
    (g01.age_85ov_p    ::numeric / NULLIF(g01.tot_p_p, 0)) AS pct_age_85_plus
  FROM dbt_astha_silver.census_g01_cleaned g01
  JOIN dbt_astha_silver.census_g02_cleaned g02
    ON regexp_replace(g02.lga_code_2016::text, '[^0-9]', '', 'g')
       = regexp_replace(g01.lga_code_2016::text, '[^0-9]', '', 'g')
)

SELECT
  s.grp,
  s.lga_code_key    AS lga_code,
  s.lga_name,
  s.active_listings,
  s.avg_rev_per_active_listing,
  d.average_household_size,
  d.pct_age_0_4,  d.pct_age_5_14, d.pct_age_15_19, d.pct_age_20_24,
  d.pct_age_25_34, d.pct_age_35_44, d.pct_age_45_54, d.pct_age_55_64,
  d.pct_age_65_74, d.pct_age_75_84, d.pct_age_85_plus
FROM selected_lgas s
LEFT JOIN census_demo d
  ON d.lga_code_key = s.lga_code_key
ORDER BY
  CASE WHEN s.grp = 'top3' THEN 0 ELSE 1 END,
  s.avg_rev_per_active_listing DESC;

###########end ######

####### part 4.b ##########################################################
/* b — Is there a correlation between the median age of a neighbourhood (from Census data) and the revenue generated per active listing in that neighbourhood?     */
#####################################################################################


WITH
airbnb_base AS (
  SELECT
    listing_id,
    LOWER(TRIM(listing_neighbourhood)) AS suburb_key,
    price::numeric                      AS price,
    availability_30
  FROM dbt_astha_silver.airbnb_cleaned
  WHERE listing_neighbourhood IS NOT NULL AND listing_neighbourhood <> ''
),
row_metrics AS (
  SELECT
    listing_id,
    suburb_key,
    GREATEST(0, LEAST(30, 30 - COALESCE(availability_30, 0)))::int AS stays_30,
    (COALESCE(price,0) * GREATEST(0, LEAST(30, 30 - COALESCE(availability_30, 0))))::numeric AS revenue_30
  FROM airbnb_base
),
listing_agg AS (
  SELECT
    listing_id,
    suburb_key,
    SUM(stays_30)            AS stays_total,
    SUM(revenue_30)::numeric AS revenue_total
  FROM row_metrics
  GROUP BY listing_id, suburb_key
),
active_listings AS (
  SELECT *
  FROM listing_agg
  WHERE stays_total > 0
),
neighbourhood_perf AS (
  SELECT
    suburb_key,
    AVG(revenue_total)::numeric AS revenue_per_active_listing
  FROM active_listings
  GROUP BY suburb_key
),
suburb_to_lga AS (
  SELECT DISTINCT
    LOWER(TRIM(s.suburb_name)) AS suburb_key,
    REGEXP_REPLACE(c.lga_code::text, '[^0-9]', '', 'g') AS lga_code_key
  FROM dbt_astha_silver.lga_suburb_cleaned s
  JOIN dbt_astha_silver.lga_code_cleaned   c
    ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(c.lga_name))
),
census_age AS (
  SELECT
    REGEXP_REPLACE(g2.lga_code_2016::text, '[^0-9]', '', 'g') AS lga_code_key,
    g2.median_age_persons::numeric AS median_age
  FROM dbt_astha_silver.census_g02_cleaned g2
),
neighbourhood_age AS (
  SELECT
    stl.suburb_key,
    AVG(ca.median_age)::numeric AS median_age
  FROM suburb_to_lga stl
  JOIN census_age ca
    ON ca.lga_code_key = stl.lga_code_key
  GROUP BY stl.suburb_key
),
final_pairs AS (
  SELECT
    INITCAP(np.suburb_key) AS listing_neighbourhood,
    na.median_age,
    np.revenue_per_active_listing
  FROM neighbourhood_perf np
  JOIN neighbourhood_age  na
    ON na.suburb_key = np.suburb_key
  WHERE np.revenue_per_active_listing IS NOT NULL
    AND na.median_age IS NOT NULL
)

SELECT
  listing_neighbourhood,
  median_age,
  revenue_per_active_listing
FROM final_pairs
ORDER BY revenue_per_active_listing DESC;




######################end#########################################################



###################### part 4.c####################################################

/*  c. What will be the best type of listing (property type, room type and accommodates for) for the top 5 “listing_neighbourhood” (in terms of average estimated revenue per active listing)
to have the highest number of stays? *?                        */
######################################################################################

WITH
-- 0) Base rows with “stays_30” (nights booked next 30 days) and revenue proxy
base_rows AS (
  SELECT
    listing_id,
    /* keep original for display but normalize a key for grouping/joining */
    listing_neighbourhood,
    LOWER(TRIM(listing_neighbourhood)) AS suburb_key,
    property_type,
    room_type,
    accommodates,
    price::numeric AS price,
    GREATEST(0, LEAST(30, 30 - COALESCE(availability_30, 0)))::int AS stays_30
  FROM dbt_astha_silver.airbnb_cleaned
  -- AND scraped_date >= CURRENT_DATE - INTERVAL '12 months'   -- <- enable if needed
),

-- 1) Aggregate per listing (treat a listing as “active” if it had at least 1 night)
listing_perf AS (
  SELECT
    listing_id,
    suburb_key,
    property_type,
    room_type,
    accommodates,
    SUM(stays_30)                             AS total_stays,
    SUM(price * stays_30)::numeric            AS total_revenue
  FROM base_rows
  GROUP BY listing_id, suburb_key, property_type, room_type, accommodates
  HAVING SUM(stays_30) > 0
),

-- 2) Neighbourhood performance: avg revenue per active listing
neighbourhood_perf AS (
  SELECT
    suburb_key,
    COUNT(*)                         AS active_listings,
    AVG(total_revenue)::numeric      AS avg_rev_per_active_listing
  FROM listing_perf
  GROUP BY suburb_key
),

-- 3) Top 5 neighbourhoods by that metric
top5_neighbourhoods AS (
  SELECT suburb_key
  FROM neighbourhood_perf
  ORDER BY avg_rev_per_active_listing DESC NULLS LAST
  LIMIT 5
),

-- 4) For those top 5, total stays by listing-type combination
type_stays AS (
  SELECT
    lp.suburb_key,
    lp.property_type,
    lp.room_type,
    lp.accommodates,
    SUM(lp.total_stays) AS total_stays
  FROM listing_perf lp
  JOIN top5_neighbourhoods t ON t.suburb_key = lp.suburb_key
  GROUP BY lp.suburb_key, lp.property_type, lp.room_type, lp.accommodates
),

-- 5) Pick the best combo (max total_stays) per neighbourhood
ranked_types AS (
  SELECT
    suburb_key,
    property_type,
    room_type,
    accommodates,
    total_stays,
    ROW_NUMBER() OVER (
      PARTITION BY suburb_key
      ORDER BY total_stays DESC, property_type, room_type, accommodates
    ) AS rnk
  FROM type_stays
)

-- Final result
SELECT
  INITCAP(suburb_key)      AS listing_neighbourhood,
  property_type,
  room_type,
  accommodates,
  total_stays
FROM ranked_types
WHERE rnk = 1
ORDER BY total_stays DESC;


###################################################end###########################




####################################part4.d ##########################################

/* d For hosts with multiple listings are their properties concentrated within the same LGA, or are they distributed across different LGAs?
*/

########################################################################################
WITH
-- 1) One row per listing
latest_listing AS (
  SELECT DISTINCT ON (listing_id)
         listing_id,
         host_id,
         listing_neighbourhood,
         scraped_date
  FROM dbt_astha_silver.airbnb_cleaned
  ORDER BY listing_id, scraped_date DESC
),

-- 2) Suburb → LGA map with a digits-only LGA key for robust joins
suburb_to_lga AS (
  SELECT DISTINCT
         LOWER(TRIM(s.suburb_name))                            AS suburb_key,
         c.lga_name,
         c.lga_code                                            AS lga_code_raw,
         REGEXP_REPLACE(c.lga_code::text, '[^0-9]', '', 'g')   AS lga_code_key
  FROM dbt_astha_silver.lga_suburb_cleaned s
  JOIN dbt_astha_silver.lga_code_cleaned   c
    ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(c.lga_name))
),

-- 3) Attach LGA to each listing
listings_with_lga AS (
  SELECT
    ll.listing_id,
    ll.host_id,
    LOWER(TRIM(ll.listing_neighbourhood)) AS suburb_key,
    stl.lga_code_key
  FROM latest_listing ll
  LEFT JOIN suburb_to_lga stl
    ON LOWER(TRIM(ll.listing_neighbourhood)) = stl.suburb_key
  WHERE stl.lga_code_key IS NOT NULL
),

-- 4) Keep only hosts with >1 distinct listings
multi_listing_hosts AS (
  SELECT host_id
  FROM listings_with_lga
  GROUP BY host_id
  HAVING COUNT(DISTINCT listing_id) > 1
),

-- 5) For those hosts, count how many distinct LGAs their listings span
host_lga_span AS (
  SELECT
    l.host_id,
    COUNT(DISTINCT l.lga_code_key) AS lga_count
  FROM listings_with_lga l
  JOIN multi_listing_hosts m USING (host_id)
  GROUP BY l.host_id
),

-- 6) Classify each host
classified_hosts AS (
  SELECT
    CASE WHEN lga_count = 1
         THEN 'Concentrated in one LGA'
         ELSE 'Distributed across multiple LGAs'
    END AS distribution_type
  FROM host_lga_span
)

-- 7) Final 2-row summary
SELECT
  distribution_type,
  COUNT(*) AS num_hosts
FROM classified_hosts
GROUP BY distribution_type
ORDER BY distribution_type;




############################################end######################################



###########################part 4.e#################################################


/* e 
For hosts with a single Airbnb listing does the estimated revenue over the last 12 months cover the annualised median mortgage repayment in the corresponding LGA? Which LGA has the highest percentage of hosts that can cover it?
*/

#######################################################################################

/* e) Single-listing hosts: does 12-month estimated revenue cover the annualised
      median mortgage repayment in the listing’s LGA? Which LGA has the highest %? */

WITH
-- 0) Anchor to latest date
max_date AS (
  SELECT MAX(scraped_date) AS anchor_dt
  FROM dbt_astha_silver.airbnb_cleaned
),

-- 1) Keep last 12 months
last_12m AS (
  SELECT a.*
  FROM dbt_astha_silver.airbnb_cleaned a
  CROSS JOIN max_date m
  WHERE a.scraped_date >= (m.anchor_dt - INTERVAL '12 months')
    AND a.listing_neighbourhood IS NOT NULL
    AND TRIM(a.listing_neighbourhood) <> ''
),

-- 2) One snapshot per listing per month (latest in that month)
monthly_latest AS (
  SELECT
    listing_id,
    date_trunc('month', scraped_date)::date AS month_start,
    MAX(scraped_date) AS last_dt
  FROM last_12m
  GROUP BY 1,2
),

-- 3) Latest monthly rows + suburb key
latest_rows AS (
  SELECT
    b.listing_id,
    b.host_id,
    b.scraped_date,
    date_trunc('month', b.scraped_date)::date AS month_start,
    LOWER(TRIM(b.listing_neighbourhood)) AS suburb_key,
    COALESCE(b.price,0)::numeric AS price,
    b.availability_30
  FROM last_12m b
  JOIN monthly_latest ml
    ON b.listing_id  = ml.listing_id
   AND b.scraped_date = ml.last_dt
),

-- 4) Suburb → LGA map (digits-only key)
suburb_to_lga AS (
  SELECT DISTINCT
         LOWER(TRIM(s.suburb_name))                          AS suburb_key,
         c.lga_name,
         REGEXP_REPLACE(c.lga_code::text, '[^0-9]', '', 'g') AS lga_code_key
  FROM dbt_astha_silver.lga_suburb_cleaned s
  JOIN dbt_astha_silver.lga_code_cleaned   c
    ON LOWER(TRIM(s.lga_name)) = LOWER(TRIM(c.lga_name))
),

-- 5) Attach LGA to each (deduped) monthly row
rows_with_lga AS (
  SELECT
    r.listing_id,
    r.host_id,
    r.month_start,
    m.lga_name,
    m.lga_code_key,
    GREATEST(0, LEAST(30, 30 - COALESCE(r.availability_30, 0)))::int AS stays_30,
    (r.price * GREATEST(0, LEAST(30, 30 - COALESCE(r.availability_30, 0))))::numeric AS revenue_30
  FROM latest_rows r
  JOIN suburb_to_lga m
    ON r.suburb_key = m.suburb_key
),

-- 6) 12-month totals per listing (active listings only)
listing_revenue_12m AS (
  SELECT
    listing_id,
    host_id,
    -- sum revenue across months
    SUM(stays_30)            AS stays_12m,
    SUM(revenue_30)::numeric AS revenue_12m
  FROM rows_with_lga
  GROUP BY 1,2
  HAVING SUM(stays_30) > 0
),

-- 7) A single, stable LGA per listing = the most recent snapshot in the last 12m
listing_latest_lga AS (
  SELECT DISTINCT ON (l.listing_id)
         l.listing_id,
         l.lga_name,
         l.lga_code_key
  FROM rows_with_lga l
  ORDER BY l.listing_id, l.month_start DESC
),

-- 8) Identify hosts with EXACTLY one listing overall (across full table, not just 12m)
all_time_latest_per_listing AS (
  SELECT DISTINCT ON (listing_id)
         listing_id, host_id, scraped_date
  FROM dbt_astha_silver.airbnb_cleaned
  ORDER BY listing_id, scraped_date DESC
),
host_listing_counts AS (
  SELECT host_id, COUNT(DISTINCT listing_id) AS listing_count
  FROM all_time_latest_per_listing
  GROUP BY host_id
),
single_listing_hosts AS (
  SELECT host_id
  FROM host_listing_counts
  WHERE listing_count = 1
),

-- 9) Bring revenue + latest LGA for single-listing hosts
single_listing_revenue AS (
  SELECT
    r.host_id,
    r.listing_id,
    r.revenue_12m,
    ll.lga_name,
    ll.lga_code_key
  FROM listing_revenue_12m r
  JOIN single_listing_hosts s USING (host_id)
  JOIN listing_latest_lga  ll USING (listing_id)
),

-- 10) Compare to Census mortgage (monthly → annual)
host_vs_mortgage AS (
  SELECT
    slr.host_id,
    slr.listing_id,
    slr.lga_name,
    slr.lga_code_key,
    slr.revenue_12m,
    g02.median_mortgage_repay_monthly::numeric AS median_mortgage_monthly,
    (g02.median_mortgage_repay_monthly::numeric * 12) AS annual_mortgage,
    CASE WHEN slr.revenue_12m >= (g02.median_mortgage_repay_monthly::numeric * 12)
         THEN 1 ELSE 0 END AS covers_flag
  FROM single_listing_revenue slr
  JOIN dbt_astha_silver.census_g02_cleaned g02
    ON REGEXP_REPLACE(g02.lga_code_2016::text, '[^0-9]', '', 'g') = slr.lga_code_key
  WHERE slr.revenue_12m IS NOT NULL
    AND g02.median_mortgage_repay_monthly IS NOT NULL
)

-- 11) Aggregate by LGA and rank
SELECT
  lga_name,
  COUNT(*)                                         AS total_single_listing_hosts,
  SUM(covers_flag)                                 AS hosts_covering_mortgage,
  ROUND(100.0 * SUM(covers_flag) / NULLIF(COUNT(*), 0), 2) AS coverage_percentage
FROM host_vs_mortgage
GROUP BY lga_name
ORDER BY coverage_percentage DESC, lga_name;



################################################end#####################################



