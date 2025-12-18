{{
     config(
        materialized='table'
    ) 
}}

--NULL service line numbers are excluded as there is no amount associated
WITH line_numbers AS (
    SELECT
        *
    FROM {{ref('stg_procedure')}}
    WHERE service_line_number IS NOT NULL
),

-- Aggregate procedure units and amounts per date to adjust any redunds 
adjusted_per_date AS (
    SELECT
        claim_id,
        patient_id,
        service_line_number,
        procedure_code,
        procedure_qualifier,
        procedure_service_start_date,
        procedure_service_end_date,
        revenue_code,
        SUM(procedure_units) AS procedure_units,
        SUM(charged_amount) AS charged_amount,
        SUM(allowed_amount) AS allowed_amount
    FROM line_numbers
    GROUP BY
        claim_id,
        patient_id,
        service_line_number,
        procedure_code,
        procedure_qualifier,
        procedure_service_start_date,
        procedure_service_end_date,
        revenue_code
),


-- 1 line per service line number with min and max service dates azs amount for duplicated for each date
line_period  AS (
    SELECT
        *,
        MIN(procedure_service_start_date) OVER (
            PARTITION BY patient_id, claim_id, service_line_number
        ) AS first_service_date,
        MAX(procedure_service_start_date) OVER (
            PARTITION BY patient_id, claim_id, service_line_number
        ) AS last_service_date,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, claim_id, service_line_number
            ORDER BY procedure_service_start_date DESC
        ) AS rn
    FROM adjusted_per_date
),

dedup AS (
    SELECT
        *
    FROM line_period
    WHERE rn = 1
)

SELECT
        claim_id,
        patient_id,
        service_line_number,
        first_service_date as procedure_service_start_date,
        last_service_date as procedure_service_end_date,
        DATEDIFF(
            DAY,
            first_service_date,
            last_service_date
        ) + 1
        AS procdure_service_days,
        procedure_code,
        procedure_qualifier,
        revenue_code,
        procedure_units,
        charged_amount,
        allowed_amount
FROM line_period


