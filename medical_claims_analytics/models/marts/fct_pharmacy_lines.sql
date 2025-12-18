{{
    config(
        materialized='incremental',
        unique_key='pharmacy_lines_sk'
    )
}}

-- Join to dim_patients and dim_medications to get foreign keys
WITH base AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'dpt.patient_id',
            'dc.claim_id',
            'p.drug_ndc'
            ]) 
        }} 
        AS pharmacy_lines_sk,
        dpt.patient_id,
        dc.claim_id,
        p.pharmacy_service_line,
        p.drug_ndc,
        p.claim_service_start_date,
        p.fill_number,
        p.days_supply,
        p.dispensed_quantity,
        p.submitted_gross_amount,
        p.allowed_total_amount,
        p.patient_pay_amount
    FROM {{ ref('int_pharmacy_claim') }} p
    LEFT JOIN {{ ref('dim_claims') }} dc
        ON p.claim_id = dc.claim_id
    LEFT JOIN {{ ref('dim_patients') }} dpt
        ON p.patient_id = dpt.patient_id
)

select * from base
