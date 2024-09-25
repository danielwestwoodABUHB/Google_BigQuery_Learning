import os
import pandas as pd
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"

client = bigquery.Client()

def create_time_table():
    time_range = pd.date_range(start='00:00', end='23:00', freq='H').time
    time_table = pd.DataFrame(time_range, columns=['Time'])
    time_table['Hour'] = time_table['Time'].apply(lambda x: x.hour)
    time_table['Minute'] = time_table['Time'].apply(lambda x: x.minute)
    time_table['Second'] = time_table['Time'].apply(lambda x: x.second)
    time_table['Hour_Band'] = time_table['Time'].apply(lambda x: f"{x.hour:02d}:00 - {x.hour + 1:02d}:00" if x.hour < 23 else "23:00 - 00:00")
    return time_table

time_table = create_time_table()

project_id = 'vernal-tempo-410309'
dataset_id = 'dim'
table_id = f'{project_id}.{dataset_id}.dim_time_table'

try:
    dataset_ref = client.dataset(dataset_id)
    client.get_dataset(dataset_ref)
    print(f"Dataset {dataset_id} already exists.")
except Exception as e:
    print(f"Dataset {dataset_id} not found. Creating dataset...")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'
    client.create_dataset(dataset)
    print(f"Created dataset {dataset_id}.")

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

try:
    job = client.load_table_from_dataframe(time_table, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {table_id}.")
except Exception as e:
    print(f"An error occurred: {e}")
