import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq
import io
import json
from google.cloud import storage



from google.cloud import storage

# Specifying the full path to the JSON key file
# keyfile_path = "C:/Users/User/Desktop/Projects/healthcare-data-project-442109-f33db94a36a9.json"
keyfile_path = "/usr/local/airflow/include/gcp/service_account.json"
storage_client = storage.Client.from_service_account_json(keyfile_path)


buckets = list(storage_client.list_buckets())
print("Successfully connected to GCS. Available buckets:")
for bucket in buckets:
    print(f" - {bucket.name}")
fake = Faker()

BUCKET_NAME = 'healthcare-data-bucket-emeka'
DEV_PATH = 'dev/'
PROD_PATH = 'prod/'


DEV_RECORDS = 5000
PROD_RECORDS = 20000


start_date = datetime(2024, 11, 11)
end_date = datetime.today()

def create_bucket():
    try:
        bucket = storage_client.bucket(BUCKET_NAME)
        if not bucket.exists():
            storage_client.create_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created successfully.")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")
    except Exception as e:
        print(f"Error creating bucket: {e}")


def empty_gcs_folder(path):
    print(f"Emptying folder '{path}' in GCS bucket '{BUCKET_NAME}'...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = bucket.list_blobs(prefix=path)
    for blob in blobs:
        blob.delete()
        print(f"Deleted '{blob.name}'")
    print(f"Completed emptying folder '{path}'.")


def upload_to_gcs(data, path, filename, file_format):
    print(f"Uploading {filename} in {file_format} format to GCS...")
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(path + filename)

    if file_format == 'csv':
        csv_data = data.to_csv(index=False)
        blob.upload_from_string(csv_data, content_type='text/csv')
    elif file_format == 'json':
        json_data = "\n".join(data)
        blob.upload_from_string(json_data, content_type='application/json')
    elif file_format == 'parquet':
        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        buffer.seek(0)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
    print(f"Uploaded {filename} to {path}")


def generate_patients(num_records):
    print("Generating patient demographic data in CSV format...")
    patients = []
    for _ in range(num_records):
        patient_id = fake.unique.uuid4()
        first_name = fake.first_name()
        last_name = fake.last_name()
        age = random.randint(0, 100)
        gender = random.choice(['Male', 'Female'])
        zip_code = fake.zipcode()
        insurance_type = random.choice(['Private', 'Medicare', 'Medicaid'])
        registration_date = fake.date_between(start_date=start_date, end_date=end_date)

        patients.append({
            'patient_id': patient_id,
            'first_name': first_name,
            'last_name': last_name,
            'age': age,
            'gender': gender,
            'zip_code': zip_code,
            'insurance_type': insurance_type,
            'registration_date': str(registration_date)
        })
    return pd.DataFrame(patients)


def generate_ehr(num_records, patient_ids):
    print("Generating electronic health records data in newline-delimited JSON format...")
    ehr_records = []
    for _ in range(num_records):
        patient_id = random.choice(patient_ids)
        visit_date = fake.date_between(start_date=start_date, end_date=end_date)
        diagnosis_code = random.choice(['E11.9', 'I10', 'J45', 'N18.9', 'Z00.0'])
        diagnosis_desc = {
            'E11.9': 'Type 2 diabetes mellitus',
            'I10': 'Essential hypertension',
            'J45': 'Asthma',
            'N18.9': 'Chronic kidney disease',
            'Z00.0': 'General medical exam'
        }[diagnosis_code]
        heart_rate = random.randint(60, 190)
        blood_pressure = f"{random.randint(110, 220)}/{random.randint(70, 90)}"
        temperature = round(random.uniform(97.0, 99.5), 1)

        ehr_record = {
            'patient_id': patient_id,
            'visit_date': str(visit_date),
            'diagnosis_code': diagnosis_code,
            'diagnosis_desc': diagnosis_desc,
            'heart_rate': heart_rate,
            'blood_pressure': blood_pressure,
            'temperature': temperature
        }

        ehr_records.append(json.dumps(ehr_record))

    return ehr_records

# Helper function for generating claims data in Parquet format with explicit schema
def generate_claims(num_records, patient_ids):
    print("Generating claims data in Parquet format with explicit schema...")
    claims = []
    for _ in range(num_records):
        claim_id = fake.unique.uuid4()
        patient_id = random.choice(patient_ids)
        provider_id = fake.unique.uuid4()
        service_date = fake.date_between(start_date=start_date, end_date=end_date)
        service_date = datetime.combine(service_date, datetime.min.time())
        diagnosis_code = random.choice(['E11.9', 'I10', 'J45', 'N18.9'])
        procedure_code = random.choice(['99213', '80053', '83036', '93000'])
        claim_amount = round(random.uniform(100, 15000), 3)
        status = random.choice(['Paid', 'Denied', 'Pending'])

        claims.append({
            'claim_id': str(claim_id),
            'patient_id': str(patient_id),
            'provider_id': str(provider_id),
            'service_date': service_date,
            'diagnosis_code': diagnosis_code,
            'procedure_code': procedure_code,
            'claim_amount': float(claim_amount),
            'status': status
        })

    schema = pa.schema([
        ('claim_id', pa.string()),
        ('patient_id', pa.string()),
        ('provider_id', pa.string()),
        ('service_date', pa.timestamp('ms')),
        ('diagnosis_code', pa.string()),
        ('procedure_code', pa.string()),
        ('claim_amount', pa.float64()),
        ('status', pa.string())
    ])

    table = pa.Table.from_pandas(pd.DataFrame(claims), schema=schema)
    return table

# Main script
create_bucket()

empty_gcs_folder(DEV_PATH)
dev_patients = generate_patients(DEV_RECORDS)
dev_ehr = generate_ehr(DEV_RECORDS, dev_patients['patient_id'].tolist())
dev_claims = generate_claims(DEV_RECORDS, dev_patients['patient_id'].tolist())

upload_to_gcs(dev_patients, DEV_PATH, 'patient_data.csv', 'csv')
upload_to_gcs(dev_ehr, DEV_PATH, 'ehr_data.json', 'json')
upload_to_gcs(dev_claims, DEV_PATH, 'claims_data.parquet', 'parquet')

empty_gcs_folder(PROD_PATH)
prod_patients = generate_patients(PROD_RECORDS)
prod_ehr = generate_ehr(PROD_RECORDS, prod_patients['patient_id'].tolist())
prod_claims = generate_claims(PROD_RECORDS, prod_patients['patient_id'].tolist())

upload_to_gcs(prod_patients, PROD_PATH, 'patient_data.csv', 'csv')
upload_to_gcs(prod_ehr, PROD_PATH, 'ehr_data.json', 'json')
upload_to_gcs(prod_claims, PROD_PATH, 'claims_data.parquet', 'parquet')

print("Data generation and upload to GCS completed successfully.")
