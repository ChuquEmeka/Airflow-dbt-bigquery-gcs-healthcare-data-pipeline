{{ config(
    materialized='incremental',
    unique_key='diagnosis_code'
    
) }}

SELECT
    diagnosis_code,
    diagnosis_desc,
    COUNT(DISTINCT patient_id) AS patient_count
FROM
    {{ source('healthcare_data', 'ehr_data_external') }}
WHERE
    diagnosis_code IN ('E11.9', 'I10', 'J45', 'N18.9')
GROUP BY
    diagnosis_code, diagnosis_desc
ORDER BY
    patient_count DESC
