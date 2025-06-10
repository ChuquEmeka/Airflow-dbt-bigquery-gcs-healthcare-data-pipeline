{{ config(
    materialized='incremental',
    unique_key='claim_id'
    
) }}

SELECT
    claim_id,
    status,
    COUNT(*) AS claim_count,
    SUM(claim_amount) AS total_claim_amount,
    AVG(claim_amount) AS avg_claim_amount
FROM
    {{ source('healthcare_data', 'claims_data_external') }}
GROUP BY
    claim_id, status
