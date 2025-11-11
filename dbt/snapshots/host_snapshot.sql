{% snapshot host_snapshot %}
{{
  config(
    target_schema='silver_snapshots',
    strategy='timestamp',
    unique_key='host_id',
    updated_at='updated_at',
    invalidate_hard_deletes=true,
    pre_hook=[
      "CREATE INDEX IF NOT EXISTS idx_dim_host_uk ON dbt_astha_silver.dim_host(host_id)",
      "CREATE INDEX IF NOT EXISTS idx_dim_host_scraped ON dbt_astha_silver.dim_host(scraped_date)"
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_host_snap_current ON silver_snapshots.host_snapshot(host_id) WHERE dbt_valid_to IS NULL",
      "CREATE INDEX IF NOT EXISTS idx_host_snap_updated ON silver_snapshots.host_snapshot(updated_at)",
      "ANALYZE silver_snapshots.host_snapshot"
    ]
  )
}}

WITH src AS (
  SELECT
    host_id,
    host_name,
    host_since,
    host_is_superhost,
    host_neighbourhood,
    scraped_date,
    scraped_date::timestamp AS updated_at
  FROM {{ ref('dim_host') }}
)

SELECT
  host_id,
  host_name,
  host_since,
  host_is_superhost,
  host_neighbourhood,
  scraped_date,
  updated_at
FROM src

{% endsnapshot %}
