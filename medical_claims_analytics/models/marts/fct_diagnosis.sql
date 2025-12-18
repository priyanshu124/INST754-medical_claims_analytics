{{
    config(
        materialized='incremental',
        unique_key='claim_diagnosis_sk'
    )
}}

WITH base AS (

    SELECT Distinct
        -- Generate a surrogate key for the bridge
        {{ dbt_utils.generate_surrogate_key([
            'dc.claim_id',
            'd.diagnosis_code'
        ]) }} AS claim_diagnosis_sk,

        -- Foreign keys
        dc.claim_id AS claim_id,
        dpt.patient_id,
        d.diagnosis_code AS diagnosis_code,


        -- Dates from diagnosis table
        d.diagnosis_service_start_date,
        d.diagnosis_service_end_date,
        isnull(d.admit_diagnosis_ind, 'N') AS admit_diagnosis_ind

    FROM {{ ref('int_diagnosis') }} d
    LEFT JOIN {{ ref('dim_diagnosis_codes') }} ddc
        ON d.diagnosis_code = ddc.diagnosis_code
    LEFT JOIN {{ ref('dim_claims') }} dc
        ON dc.claim_id = d.claim_id
    LEFT JOIN {{ ref('dim_patients') }} dpt
        ON dpt.patient_id = d.patient_id

)

SELECT *
FROM base
