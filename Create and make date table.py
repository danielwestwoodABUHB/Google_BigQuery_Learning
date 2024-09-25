import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import holidays

# Set the path to your Google Cloud service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"

# Initialize BigQuery client
client = bigquery.Client()

# Function to create a date table with holidays
def create_date_table(start_date, end_date):
    # Generate a date range
    date_range = pd.date_range(start=start_date, end=end_date)
    
    # Create a DataFrame from the date range
    date_table = pd.DataFrame(date_range, columns=['Date'])
    
    # Add columns for year, month, day, weekday, and holiday information
    date_table['Year'] = date_table['Date'].dt.year
    date_table['Month'] = date_table['Date'].dt.month
    date_table['Day'] = date_table['Date'].dt.day
    date_table['Weekday'] = date_table['Date'].dt.day_name()
    
    # Initialize holiday information
    us_holidays = holidays.US(years=date_table['Year'].unique())
    uk_holidays = holidays.UK(years=date_table['Year'].unique())
    
    # Determine if each date is a holiday
    date_table['US_Holiday'] = date_table['Date'].apply(lambda x: x in us_holidays)
    date_table['UK_Holiday'] = date_table['Date'].apply(lambda x: x in uk_holidays)
    
    # Get the name of the holiday
    date_table['US_Holiday_Name'] = date_table['Date'].apply(lambda x: us_holidays.get(x))
    date_table['UK_Holiday_Name'] = date_table['Date'].apply(lambda x: uk_holidays.get(x))
    
    return date_table

# Create the date table for the range 2000 to 2050
start_date = '2000-01-01'  # Start date
end_date = '2050-12-31'    # End date
date_table = create_date_table(start_date, end_date)

# Define the project and dataset
project_id = 'vernal-tempo-410309'
dataset_id = 'dim'
table_id = f'{project_id}.{dataset_id}.dim_date_table'

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
    job = client.load_table_from_dataframe(date_table, table_id, job_config=job_config)
    job.result()  # Waits for the job to complete.
    print(f"Loaded {job.output_rows} rows into {table_id}.")
except Exception as e:
    print(f"An error occurred: {e}")

