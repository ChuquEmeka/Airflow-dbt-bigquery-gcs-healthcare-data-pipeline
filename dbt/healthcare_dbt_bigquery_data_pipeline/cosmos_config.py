from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path

DBT_CONFIG = ProfileConfig(
    profile_name='healthcare_dbt_bigquery_data_pipeline',
    # target_name='prod',
    target_name='dev',
    profiles_yml_filepath=Path('/usr/local/airflow/dbt/healthcare_dbt_bigquery_data_pipeline/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/dbt/healthcare_dbt_bigquery_data_pipeline/'
)
