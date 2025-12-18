{{
    config(
        materialized='incremental',
        unique_key='diagnosis_code'
    )
}}

SELECT DISTINCT
    -- Stable numeric SK
    diagnosis_code,
    code_range,
    description,
    level_1_description,
    level_2_description,
    level_3_description,
    url
FROM {{source('raw', 'diagnosis_codes')}}
