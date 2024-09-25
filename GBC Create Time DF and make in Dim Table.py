import os
import pandas as pd
from google.cloud import bigquery

# Set the path to your Google Cloud service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"

# Initialize BigQuery client
client = bigquery.Client()

# Function to create a time table with hour and half-hour bands
def create_time_table():
    # Create a DataFrame for every hour and half-hour in a day
    time_range = pd.date_range(start='00:00', end='23:30', freq='30T').time  # Generate time for each half hour of the day
    
    # Create a DataFrame from the time range
    time_table = pd.DataFrame(time_range, columns=['Time'])
    
    # Add columns for hour, minute, and second
    time_table['Hour'] = time_table['Time'].apply(lambda x: x.hour)
    time_table['Minute'] = time_table['Time'].apply(lambda x: x.minute)
    time_table['Second'] = time_table['Time'].apply(lambda x: x.second)
    
    # Add a column for Hour Band (e.g., "00:00 - 01:00")
    time_table['Half Hour_Band'] = time_table['Time'].apply(lambda x: f"{x.hour:02d}:00 - {x.hour:02d}:{30 if x.minute == 30 else 00}")
    time_table['Hour_Band'] = time_table['Time'].apply(lambda x: f"{x.hour:02d}:00 - {x.hour:02d}:{30 if x.minute == 30 else 00}")
    time_table['Hour_Band'] = time_table['Time'].apply(lambda x: f"{x.hour:02d}:00 - {x.hour + 1:02d}:00" if x.hour < 23 else "23:00 - 00:00")

    return time_table

# Create the time table
time_table = create_time_table()

# Define the project and dataset
project_id = 'vernal-tempo-410309'
dataset_id = 'dim'
table_id = f'{project_id}.{dataset_id}.dim_time_table'  # Update the table name

# Create the dataset if it doesn't exist
try:
    dataset_ref = client.dataset(dataset_id)
    client.get_dataset(dataset_ref)  # This will throw an error if the dataset does not exist
    print(f"Dataset {dataset_id} already exists.")
except Exception as e:
    print(f"Dataset {dataset_id} not found. Creating dataset...")
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = 'US'  # Set location, e.g., 'US'
    client.create_dataset(dataset)  # API request
    print(f"Created dataset {dataset_id}.")

# Upload the DataFrame to Google BigQuery
job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,  # Overwrite the table if it exists
)

# Load the DataFrame into BigQuery
try:
    job = client.load_table_from_dataframe(time_table, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.
    print(f"Loaded {job.output_rows} rows into {table_id}.")
except Exception as e:
    print(f"An error occurred: {e}")
