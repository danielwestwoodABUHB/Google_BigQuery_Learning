import os
import pandas as pd
from google.cloud import bigquery
from prophet import Prophet
from prophet.serialize import model_to_json, model_from_json
from prophet.plot import plot_plotly, plot_components_plotly

# Smust update the location of this path when downloading Key from the settings in big query
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"

# Initialize BigQuery client
client = bigquery.Client()

# Define your query
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


try:
    query_job = client.query(query) 
    results = query_job.result()    

   
    df = results.to_dataframe()

   
    df.rename(columns={'date': 'ds', 'daily_positive': 'y'}, inplace=True)

   
    model = Prophet(holidays=pd.DataFrame({
        'holiday': 'us_holidays',
        'ds': pd.to_datetime([
            '2020-01-01', '2020-07-04', '2020-12-25',  
           
        ]),
        'lower_window': 0,
        'upper_window': 1,
    }))

   
    model.add_country_holidays(country_name='US')

   
    model.fit(df)

    
    future = model.make_future_dataframe(periods=30)  

    
    forecast = model.predict(future)


    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])

except Exception as e:
    print(f"An error occurred: {e}")




fig1 = plot_plotly(model, forecast)
fig1.show()


fig2 = plot_components_plotly(model, forecast)
fig2.show()
