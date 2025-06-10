{{ config(
    materialized='incremental',
    unique_key='patient_id'
) }}

WITH patient_claim_totals AS (
    SELECT
        patient_id,
        SUM(claim_amount) AS total_claim_amount
    FROM
        {{ source('healthcare_data', 'claims_data_external') }}
    GROUP BY
        patient_id
),

high_claims AS (
    SELECT
        patient_id,
        total_claim_amount
    FROM
        patient_claim_totals
    WHERE
        total_claim_amount > 10000  
)

SELECT
    hc.patient_id,
    hc.total_claim_amount,
    pd.first_name,
    pd.last_name,
    pd.age,
    pd.gender,
    pd.insurance_type
FROM
    high_claims hc
LEFT JOIN
    {{ source('healthcare_data', 'patient_data_external') }} pd
ON
    hc.patient_id = pd.patient_id
