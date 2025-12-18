{{
     config(
        materialized='view'
    ) 
}}

WITH with_nulls AS (
    SELECT 
        {{null_if_all_cols(source('raw', 'medical_claim') )}}
    FROM {{ source('raw', 'medical_claim') }}
),

cast_and_rename AS (
    SELECT
        TRIM(claim_id) AS claim_id,
        TRIM(patient_id) AS patient_id,  
        TRY_CONVERT(DATE, date_service) AS claim_service_start_date,
        TRY_CONVERT(DATE, date_service_end) AS claim_service_end_date,
        UPPER(TRIM(location_of_care)) AS location_of_care,
        UPPER(TRIM(pay_type)) AS pay_type  
    FROM with_nulls
)

SELECT *
FROM cast_and_rename
