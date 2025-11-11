{% snapshot property_snapshot %}
{{
  config(
    target_schema='silver_snapshots',
    strategy='timestamp',
    unique_key='listing_id',
    updated_at='updated_at',
    invalidate_hard_deletes=true,
    pre_hook=[
      "CREATE INDEX IF NOT EXISTS idx_dim_property_uk ON dbt_astha_silver.dim_property(listing_id)",
      "CREATE INDEX IF NOT EXISTS idx_dim_property_scraped ON dbt_astha_silver.dim_property(scraped_date)"
    ],
    post_hook=[
      "CREATE INDEX IF NOT EXISTS idx_prop_snap_current ON silver_snapshots.property_snapshot(listing_id) WHERE dbt_valid_to IS NULL",
      "CREATE INDEX IF NOT EXISTS idx_prop_snap_updated ON silver_snapshots.property_snapshot(updated_at)",
      "ANALYZE silver_snapshots.property_snapshot"
    ]
  )
}}

WITH src AS (
  SELECT
    listing_id,
    property_type,
    room_type,
    accommodates,
    listing_neighbourhood,
    scraped_date,
    scraped_date::timestamp AS updated_at
  FROM {{ ref('dim_property') }}
)

SELECT
  listing_id,
  property_type,
  room_type,
  accommodates,
  listing_neighbourhood,
  scraped_date,
  updated_at
FROM src

{% endsnapshot %}
