import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from google.cloud import bigquery

# Set the path to your Google Cloud service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"


query = """
    SELECT
        Year,
        County_of_Residence,
        County_of_Residence_FIPS,
        AVG(Ave_Birth_Weight_gms) AS Avg_Birth_Weight,
        AVG(Ave_Pre_pregnancy_BMI) AS Avg_Pre_Pregnancy_BMI
    FROM
        `bigquery-public-data.sdoh_cdc_wonder_natality.county_natality`
    GROUP BY
        Year, County_of_Residence, County_of_Residence_FIPS
    ORDER BY
        Year, County_of_Residence
"""

# Run the query and process the results
try:
    query_job = client.query(query)  # API request
    results = query_job.result()     # Waits for the job to complete.

    # Convert results to a DataFrame
    df = results.to_dataframe()

    # Drop rows with missing values
    df.dropna(subset=['Avg_Pre_Pregnancy_BMI', 'Avg_Birth_Weight'], inplace=True)

    # Calculate the Pearson correlation coefficient
    correlation = df['Avg_Pre_Pregnancy_BMI'].corr(df['Avg_Birth_Weight'])
    print(f"Pearson correlation coefficient: {correlation}")

    # Linear Regression Analysis
    X = df['Avg_Pre_Pregnancy_BMI']  # Independent variable
    y = df['Avg_Birth_Weight']        # Dependent variable

    # Add a constant to the model (intercept)
    X = sm.add_constant(X)

    # Fit the linear regression model
    model = sm.OLS(y, X).fit()

    # Print the summary of the regression analysis
    print(model.summary())

    # Optional: Visualizing the relationship
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='Avg_Pre_Pregnancy_BMI', y='Avg_Birth_Weight')
    plt.title('Scatter plot of Average Pre-Pregnancy BMI vs Average Birth Weight')
    plt.xlabel('Average Pre-Pregnancy BMI')
    plt.ylabel('Average Birth Weight (grams)')
    
    # Plotting the regression line
    sns.regplot(data=df, x='Avg_Pre_Pregnancy_BMI', y='Avg_Birth_Weight', scatter=False, color='red')

    plt.axhline(y=df['Avg_Birth_Weight'].mean(), color='r', linestyle='--', label='Mean Birth Weight')
    plt.axvline(x=df['Avg_Pre_Pregnancy_BMI'].mean(), color='g', linestyle='--', label='Mean Pre-Pregnancy BMI')
    plt.legend()
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")