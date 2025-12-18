{{
     config(
        materialized='table'
    ) 
}}

WITH adjusted_per_date AS (
    SELECT
        patient_id,
        claim_id,
        claim_service_start_date,
        drug_ndc,
        pay_type,
        max(fill_number) as fill_number,
        sum(days_supply) as days_supply,
        sum(dispensed_quantity) as dispensed_quantity,
        sum(submitted_gross_amount) as submitted_gross_amount,
        sum(allowed_total_amount) as allowed_total_amount,
        sum(patient_pay_amount) as patient_pay_amount
    FROM {{ref('stg_pharmacy_claim')}}
    GROUP BY
        patient_id,
        claim_id,
        claim_service_start_date,
        drug_ndc,
        pay_type
    HAVING sum(submitted_gross_amount) > 0 
),

rows_num AS (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id, claim_id, drug_ndc
        ORDER BY claim_service_start_date DESC
        ) AS rn
    FROM adjusted_per_date
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
    claim_service_start_date,
    drug_ndc,
    fill_number,
    days_supply,
    dispensed_quantity,
    submitted_gross_amount,
    allowed_total_amount,
    patient_pay_amount,
    pay_type,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id, claim_id
        ORDER BY claim_service_start_date 
        ) AS pharmacy_service_line
FROM dedup

