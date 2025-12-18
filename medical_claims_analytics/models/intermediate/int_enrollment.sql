{{
     config(
        materialized='table'
    ) 
}}


WITH rows_num AS (
    SELECT
    *,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id, benefit_type, pay_type, enrollment_end_date
        ORDER BY enrollment_start_date DESC
        ) AS rn
    FROM {{ref('stg_enrollment')}}
),


dedup AS (
    SELECT
        *
    FROM rows_num
    WHERE rn = 1
)

SELECT
    patient_id,
    patient_sex,
    patient_year_of_birth,
    patient_zip3,
    patient_state,
    enrollment_start_date,  
    enrollment_end_date,    
    DATEDIFF(
            DAY,
            enrollment_start_date,
            enrollment_end_date
        ) + 1 
    AS enrollment_days,
    benefit_type,   
    pay_type
FROM dedup

