{{
     config(
        materialized='view'
    ) 
}}

WITH with_nulls AS (
    SELECT 
        {{null_if_all_cols(source('raw', 'procedure') )}}
    FROM {{ source('raw', 'procedure') }}
),          

cast_and_rename AS (    

    SELECT
        TRIM(claim_id) AS claim_id,
        TRIM(patient_id) AS patient_id, 
        TRY_CAST(service_line_number AS INT) AS service_line_number,
        TRY_CONVERT(DATE, date_service) AS procedure_service_start_date,
        TRY_CONVERT(DATE, date_service_end) AS procedure_service_end_date,
        UPPER(TRIM(procedure_code)) AS procedure_code,
        UPPER(TRIM(procedure_qual)) AS procedure_qualifier,
        TRY_CAST(procedure_units AS FLOAT) AS procedure_units,
        TRY_CAST(line_charge AS FLOAT) AS charged_amount,
        TRY_CAST(line_allowed AS FLOAT) AS allowed_amount,  
        procedure_modifier1 AS modifier_code_1,
        procedure_modifier2 AS modifier_code_2,
        procedure_modifier3 AS modifier_code_3,
        procedure_modifier4 AS modifier_code_4,
        TRY_CAST(revenue_code AS INT) AS revenue_code  
    FROM with_nulls      
)

SELECT *
FROM cast_and_rename
