{{
    config(
        materialized='incremental',
        unique_key='procedure_code'
    )
}}

with deduped as (
    Select 
        *, 
        ROW_NUMBER() OVER (
            PARTITION BY code
            ORDER BY [index] DESC
        ) AS rn
    FROM {{source('raw', 'procedure_codes')}}
)

SELECT DISTINCT
    --[index] as procedure_code_id,
    code as procedure_code,
    title,
    category,
    [description],
    [type] as code_qualifier
FROM deduped
where rn=1
