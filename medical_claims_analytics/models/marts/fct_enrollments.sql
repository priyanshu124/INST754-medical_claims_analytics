{{
    config(
        materialized='incremental',
        unique_key='enrollment_sk'
    )
}}



-- Join to dim_patients
WITH joined AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key([
            'p.patient_id', 
            'enrollment_start_date', 
            'benefit_type', 
            'pay_type'
            ]) 
        }} 
        AS enrollment_sk,
        dp.patient_id,
        p.enrollment_start_date,
        p.enrollment_end_date,
        p.benefit_type,
        p.pay_type
        
    FROM {{ ref('int_enrollment') }} p
    LEFT JOIN {{ ref('dim_patients') }} dp
        ON p.patient_id = dp.patient_id
)

SELECT *
FROM joined
