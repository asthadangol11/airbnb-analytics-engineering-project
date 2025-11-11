{{ 
    config(
        unique_key='lga_code_2016',
        alias='census_g02'
    ) 
}}

select * from {{ source('raw', 'census_g02') }}