import os
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import holidays

# reminder updated location for 
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"


client = bigquery.Client()

def create_date_table(start_date, end_date):
    
    date_range = pd.date_range(start=start_date, end=end_date)
    
    
    date_table = pd.DataFrame(date_range, columns=['Date'])
    

    date_table['Year'] = date_table['Date'].dt.year
    date_table['Month'] = date_table['Date'].dt.month
    date_table['Day'] = date_table['Date'].dt.day
    date_table['Weekday'] = date_table['Date'].dt.day_name()
    

    us_holidays = holidays.US(years=date_table['Year'].unique())
    uk_holidays = holidays.UK(years=date_table['Year'].unique())
    

    date_table['US_Holiday'] = date_table['Date'].apply(lambda x: x in us_holidays)
    date_table['UK_Holiday'] = date_table['Date'].apply(lambda x: x in uk_holidays)
    
  
    date_table['US_Holiday_Name'] = date_table['Date'].apply(lambda x: us_holidays.get(x))
    date_table['UK_Holiday_Name'] = date_table['Date'].apply(lambda x: uk_holidays.get(x))
    
    return date_table


start_date = '2000-01-01'  
end_date = '2050-12-31'    
date_table = create_date_table(start_date, end_date)

project_id = 'vernal-tempo-410309'
dataset_id = 'dim'
table_id = f'{project_id}.{dataset_id}.dim_date_table'


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

try:
    job = client.load_table_from_dataframe(date_table, table_id, job_config=job_config)
    job.result()  
    print(f"Loaded {job.output_rows} rows into {table_id}.")
except Exception as e:
    print(f"An error occurred: {e}")

