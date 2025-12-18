{{
     config(
        materialized='view'
    ) 
}}

WITH with_nulls AS (
    SELECT 
        {{null_if_all_cols(source('raw', 'diagnosis') )}}
    FROM {{ source('raw', 'diagnosis') }}
), 

cast_and_rename AS (

    SELECT
        TRIM(claim_id) AS claim_id,
        TRIM(patient_id) AS patient_id,  
        TRY_CONVERT(DATE, date_service) AS diagnosis_service_start_date,
        TRY_CONVERT(DATE, date_service_end) AS diagnosis_service_end_date,
        UPPER(TRIM(diagnosis_code)) AS diagnosis_code,
        UPPER(TRIM(diagnosis_qual)) AS diagnosis_qualifier,
        admit_diagnosis_ind AS admit_diagnosis_ind
    FROM with_nulls
)

SELECT *
FROM cast_and_rename
