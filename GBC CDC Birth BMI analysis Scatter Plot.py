import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import statsmodels.api as sm
from google.cloud import bigquery

# Reminder to use correct download credential key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\da046634\AppData\Local\Temp\MicrosoftEdgeDownloads\0edc4907-b6aa-48a7-af8c-4b7ced419855\vernal-tempo-410309-daa86de57214.json"



client = bigquery.Client()


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

try:
    query_job = client.query(query)  
    results = query_job.result()     
    df = results.to_dataframe()

    
    df.dropna(subset=['Avg_Pre_Pregnancy_BMI', 'Avg_Birth_Weight'], inplace=True)

   
    correlation = df['Avg_Pre_Pregnancy_BMI'].corr(df['Avg_Birth_Weight'])
    print(f"Pearson correlation coefficient: {correlation}")
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='Avg_Pre_Pregnancy_BMI', y='Avg_Birth_Weight')
    plt.title('Scatter plot of Average Pre-Pregnancy BMI vs Average Birth Weight')
    plt.xlabel('Average Pre-Pregnancy BMI')
    plt.ylabel('Average Birth Weight (grams)')
    plt.axhline(y=df['Avg_Birth_Weight'].mean(), color='r', linestyle='--', label='Mean Birth Weight')
    plt.axvline(x=df['Avg_Pre_Pregnancy_BMI'].mean(), color='g', linestyle='--', label='Mean Pre-Pregnancy BMI')
    plt.legend()
    plt.show()

except Exception as e:
    print(f"An error occurred: {e}")
