{{
     config(
        materialized='view'
    ) 
}}

WITH with_nulls AS (
    SELECT 
        {{null_if_all_cols(source('raw', 'pharmacy_claim') )}}      
    FROM {{ source('raw', 'pharmacy_claim') }}
),

cast_and_rename AS (
    SELECT
        TRIM(claim_id) AS claim_id,
        TRIM(patient_id) AS patient_id,  
        TRY_CONVERT(DATE, date_service) AS claim_service_start_date,
        UPPER(TRIM(ndc)) AS drug_ndc,
        TRY_CAST(fill_number AS INT) AS fill_number,
        TRY_CAST(days_supply AS INT) AS days_supply,
        TRY_CAST(dispensed_quantity AS FLOAT) AS dispensed_quantity,
        TRY_CAST(submitted_gross_due AS FLOAT) AS submitted_gross_amount,
        TRY_CAST(paid_gross_due AS FLOAT) AS allowed_total_amount,  
        TRY_CAST(copay_coinsurance AS FLOAT) AS patient_pay_amount,
        UPPER(TRIM(pay_type)) AS pay_type  
    FROM with_nulls  
)   

SELECT *
FROM cast_and_rename
