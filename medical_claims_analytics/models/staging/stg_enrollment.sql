{{
     config(
        materialized='view'
    ) 
}}

WITH with_nulls AS (
    SELECT 
        {{null_if_all_cols(source('raw', 'enrollment') )}}
    FROM {{ source('raw', 'enrollment') }}
),

cast_and_rename AS (
    SELECT
        TRIM(patient_id) AS patient_id,  
        UPPER(TRIM(patient_gender)) AS patient_sex,
        patient_year_of_birth AS patient_year_of_birth,
        patient_zip3 AS patient_zip3,
        UPPER(TRIM(patient_state)) AS patient_state,
        TRY_CONVERT(DATE, date_start) AS enrollment_start_date,
        TRY_CONVERT(DATE, date_end) AS enrollment_end_date,
        UPPER(TRIM(benefit_type)) AS benefit_type,
        UPPER(TRIM(pay_type)) AS pay_type  
    FROM with_nulls
)

SELECT *
FROM cast_and_rename
