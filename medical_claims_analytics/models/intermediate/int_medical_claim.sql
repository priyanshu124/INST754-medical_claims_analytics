{{
     config(
        materialized='table'
    ) 
}}


WITH rows_num AS (
    SELECT
    *,
    MIN(claim_service_start_date) OVER (
            PARTITION BY patient_id, claim_id
        ) AS first_service_date,
        MAX(claim_service_end_date) OVER (
            PARTITION BY patient_id, claim_id
        ) AS last_service_date,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id, claim_id
        ORDER BY claim_service_start_date DESC
        ) AS rn
    FROM {{ref('stg_medical_claim')}}
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
    first_service_date as claim_service_start_date,
    last_service_date as claim_service_end_date,

    CASE 
        WHEN
            UPPER(l.category)='INPATIENT' THEN
                DATEDIFF(
                    DAY,
                    first_service_date,
                    last_service_date
                ) + 1
        ELSE
        0
    END 
    AS length_of_stay_days,

    UPPER(TRIM(l.category)) as location_of_care_category,
    d.location_of_care,
    pay_type
FROM dedup d
LEFT JOIN {{ ref('location_of_care_mapping') }} l
    ON d.location_of_care = l.location_of_care


