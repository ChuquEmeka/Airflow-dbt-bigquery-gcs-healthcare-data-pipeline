{{ config(
    materialized='incremental',
    unique_key='patient_id'
    
) }}

SELECT
    patient_id,
    visit_date,
    heart_rate,
    blood_pressure,
    temperature,
    CASE
        WHEN heart_rate > 150 THEN 'High Heart Rate'
        WHEN blood_pressure = '200/120' THEN 'High Blood Pressure'
        WHEN temperature > 99.5 THEN 'High Temperature'
        ELSE 'Normal'
    END AS anomaly_flag
FROM
    {{ source('healthcare_data', 'ehr_data_external') }}
WHERE
    heart_rate > 150 OR blood_pressure = '200/120' OR temperature > 99.5
