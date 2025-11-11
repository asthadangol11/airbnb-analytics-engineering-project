{{ 
    config(
        unique_key=['listing_id','scraped_date'],  
        alias='airbnb'
    ) 
}}

select * from {{ source('raw', 'airbnb_raw') }}