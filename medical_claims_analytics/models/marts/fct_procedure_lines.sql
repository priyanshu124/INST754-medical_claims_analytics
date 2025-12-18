{{
    config(
        materialized='incremental',
        unique_key='procedure_lines_sk'
    )
}}

WITH base AS (  
    SELECT Distinct
        {{ dbt_utils.generate_surrogate_key([    
            'dc.claim_id',
            'dpt.patient_id',
            'p.service_line_number',
            'p.procedure_code',
            'p.procedure_units',
            'p.charged_amount',
            'p.allowed_amount'

            ]) 
        }} as procedure_lines_sk,
        -- Foreign keys to dimensions
        dpt.patient_id,
        dc.claim_id,
        --dpc.procedure_code_id,
        p.service_line_number,
        p.procedure_code,
        p.procedure_service_start_date,
        p.procedure_service_end_date,
        p.procedure_units,
        p.charged_amount,
        p.allowed_amount
    FROM {{ ref('int_procedure') }} p
    LEFT JOIN {{ ref('dim_claims') }} dc
        ON p.claim_id = dc.claim_id
    LEFT JOIN {{ ref('dim_patients') }} dpt
        ON p.patient_id = dpt.patient_id
    LEFT JOIN {{ ref('dim_procedure_codes') }} dpc
        ON p.procedure_code = dpc.procedure_code

)

Select * from base
