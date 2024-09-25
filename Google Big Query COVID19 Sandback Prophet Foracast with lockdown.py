import os
import pandas as pd
from google.cloud import bigquery
from prophet import Prophet
from prophet.plot import plot_plotly, plot_components_plotly

# Set the path to your Google Cloud service account key
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

# Run the query and process the results
try:
    query_job = client.query(query)  # API request
    results = query_job.result()     # Waits for the job to complete.

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Ensure 'date' column is in datetime format
    df['date'] = pd.to_datetime(df['date'])

    # Rename columns for Prophet
    df.rename(columns={'date': 'ds', 'daily_positive': 'y'}, inplace=True)

    # Create lockdown periods as a binary regressor
    lockdowns = pd.DataFrame({
        'ds': pd.to_datetime([
            '2020-03-15', '2020-06-01', '2020-12-15',  # Add actual lockdown dates
            '2021-01-01', '2021-05-01'                 # More sample lockdown periods
        ]),
        'lockdown': [1, 1, 1, 1, 1]  # 1 indicates lockdown period
    })

    # Merge the lockdown regressor with the original data
    df = pd.merge(df, lockdowns, on='ds', how='left')
    df['lockdown'].fillna(0, inplace=True)  # Fill non-lockdown periods with 0

    # Create and fit the Prophet model with logistic growth and U.S. holidays
    model = Prophet(growth='logistic', holidays=pd.DataFrame({
        'holiday': 'us_holidays',
        'ds': pd.to_datetime([
            '2020-01-01', '2020-07-04', '2020-12-25',  # New Year's, Independence Day, Christmas
            # Add more holidays as needed
        ]),
        'lower_window': 0,
        'upper_window': 1,
    }))

    # Adding built-in US holidays
    model.add_country_holidays(country_name='US')

    # Add lockdown regressor to the model
    model.add_regressor('lockdown')

    # Set logistic floor (0 for non-negative)
    df['cap'] = df['y'].max() * 1.5  # Set upper bound dynamically
    df['floor'] = 0  # Set lower bound to 0 to ensure non-negative predictions

    # Fit the model
    model.fit(df)

    # Create a DataFrame for future dates
    future = model.make_future_dataframe(periods=30)  # Forecast for the next 30 days
    future = pd.merge(future, lockdowns, on='ds', how='left')
    future['lockdown'].fillna(0, inplace=True)
    future['cap'] = df['cap'].max()  # Apply same cap for future data
    future['floor'] = 0  # Set floor to 0 for future data as well

    # Make predictions
    forecast = model.predict(future)

    # Ensure the forecasted values are not below 0 (post-processing safeguard)
    forecast['yhat'] = forecast['yhat'].apply(lambda x: max(x, 0))
    forecast['yhat_lower'] = forecast['yhat_lower'].apply(lambda x: max(x, 0))
    forecast['yhat_upper'] = forecast['yhat_upper'].apply(lambda x: max(x, 0))

    # Print forecast results (optional, just for checking)
    print(forecast[['ds', 'yhat', 'yhat_lower', 'yhat_upper']])

except Exception as e:
    print(f"An error occurred: {e}")
    model = None  # Ensure model is set to None if an error occurs

# Plot the forecast with lockdown shading only if model exists
if model:
    fig = plot_plotly(model, forecast)

    # Add shaded regions for lockdowns
    lockdown_periods = [
        # Define lockdown periods (start, end) tuples based on real data
        ('2020-03-15', '2020-06-01'),
        ('2020-12-15', '2021-02-01')  # Example lockdown periods
    ]

    for start, end in lockdown_periods:
        fig.add_shape(type="rect",
                      x0=start, x1=end, y0=0, y1=1,
                      fillcolor="LightSalmon", opacity=0.5,
                      layer="below", line_width=0,
                      yref="paper")  # yref="paper" ensures it covers the entire y-axis

    # Show the updated plot
    fig.show()

    # Seasonal component plot
    fig2 = plot_components_plotly(model, forecast)
    fig2.show()
else:
    print("Model training failed. No plots to show.")
