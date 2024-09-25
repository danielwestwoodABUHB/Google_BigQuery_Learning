import os
import pandas as pd
from google.cloud import bigquery
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"


client = bigquery.Client()

# Big QUery 
query = """
    WITH daily_counts AS (
        SELECT 
            date,
            IFNULL(positive - LAG(positive) OVER (ORDER BY date), positive) AS daily_positive
        FROM 
            `bigquery-public-data.covid19_tracking.national_testing_and_outcomes`
        WHERE 
            date > '2020-02-27'
        ORDER BY 
            date ASC
    )

    SELECT 
        date,
        daily_positive
    FROM 
        daily_counts
    WHERE 
        daily_positive IS NOT NULL;
"""

# Making the data frame 
try:
    query_job = client.query(query)  
    results = query_job.result()     

    
    df = results.to_dataframe()

    # Reminder, Prophet must have Date, DS and Y
    df.rename(columns={'date': 'ds', 'daily_positive': 'y'}, inplace=True)

    # C
    model = Prophet(holidays=pd.DataFrame({
        'holiday': 'us_holidays',
        'ds': pd.to_datetime([
            '2020-01-01', '2020-07-04', '2020-12-25','2021-01-01', '2021-07-04', '2021-12-25',  # US Holidays
           
        ]),
        'lower_window': 0,
        'upper_window': 1,
    }))

    
    model.add_country_holidays(country_name='US')

    
    model.fit(df)

   
    future = model.make_future_dataframe(periods=30)  

 
    forecast = model.predict(future)

 
    forecast_to_insert = forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']].copy()  

    forecast_to_insert.rename(columns={'ds': 'date', 'yhat': 'predicted_daily_positive', 
                                       'yhat_lower': 'predicted_lower_bound', 
                                       'yhat_upper': 'predicted_upper_bound'}, inplace=True)

    # create a new table in BQ - this is my sandbox link
    destination_table = 'vernal-tempo-410309.your_dataset.ddw_fct_COVID_Prophet_Forcast' 

  
    forecast_to_insert.to_gbq(destination_table, project_id='vernal-tempo-410309', if_exists='replace')

    print("Forecast successfully written to BigQuery.")

except Exception as e:
    print(f"An error occurred: {e}")

