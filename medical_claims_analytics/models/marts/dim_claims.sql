{{
    config(
        materialized='incremental',
        unique_key='claim_id'
    )
}}

With pharmacy_claims as (

    SELECT distinct 
        claim_id,
        patient_id,
        MIN(claim_service_start_date) OVER (
            PARTITION BY patient_id, claim_id
        ) AS claim_service_start_date,
        MAX(claim_service_start_date) OVER (
            PARTITION BY patient_id, claim_id
        ) AS claim_service_end_date,
        0 AS length_of_stay_days,
        'PHARMACY' AS location_of_care,
    '   PHARMACY' AS location_of_care_category,
        pay_type,
        'PHARMACY' AS claim_type
    FROM {{ ref('int_pharmacy_claim') }}
)

SELECT
    claim_id,
    patient_id,
    claim_service_start_date,
    claim_service_end_date,
    length_of_stay_days,
    location_of_care,
    location_of_care_category,
    pay_type,
    'MEDICAL' AS claim_type
    
FROM {{ ref('int_medical_claim') }}

UNION ALL 
SELECT 
    *
FROM pharmacy_claims
    