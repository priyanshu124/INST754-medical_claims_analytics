{{
    config(
        materialized='incremental',
        unique_key='patient_id'
    )
}}


-- Pick the latest record per patient
WITH ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id
            ORDER BY enrollment_end_date DESC
        ) AS rn
    FROM {{ ref('int_enrollment') }}
)

SELECT
    patient_id,
    patient_sex,
    patient_year_of_birth,
    patient_zip3,
    patient_state
FROM ranked
WHERE rn = 1
