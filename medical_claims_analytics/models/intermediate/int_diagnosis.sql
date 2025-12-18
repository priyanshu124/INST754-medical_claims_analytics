{{
     config(
        materialized='table'
    ) 
}}


WITH rows_num AS (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id, claim_id, diagnosis_code
        ORDER BY diagnosis_service_end_date DESC
        ) AS rn
    FROM {{ref('stg_diagnosis')}}
),

dedup AS (
    SELECT
        *
    FROM rows_num
    WHERE rn = 1
)

SELECT
    patient_id,
    claim_id,
    diagnosis_service_start_date,
    diagnosis_service_end_date,
    diagnosis_code,
    diagnosis_qualifier,
    admit_diagnosis_ind
FROM dedup
