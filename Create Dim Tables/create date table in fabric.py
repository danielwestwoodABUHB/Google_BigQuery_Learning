import time
import pandas as pd
import holidays
import pyodbc
from datetime import datetime

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

# Change this to ensure correct date range 
start_date = '1920-01-01'
end_date = '2050-12-31'
date_table = create_date_table(start_date, end_date)

connection_string = (
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=xaufnozi4obebkcwim6j5xepvy-5ex6ixwc55su3oz3rqtrfvwoki.datawarehouse.fabric.microsoft.com;'
    'PORT=1433;'
    'DATABASE=dim_Date;'
    'Authentication=ActiveDirectoryInteractive;'
    'TrustServerCertificate=yes;'
)

try:
    # This connection allows link to Fabric 
    conn = pyodbc.connect(connection_string)
    cursor = conn.cursor()

    # optins here are to drop and recreate the dable if there is laready one with this name
    cursor.execute('''
        IF OBJECT_ID('dim_date_table', 'U') IS NOT NULL
            DROP TABLE dim_date_table;
        CREATE TABLE dim_date_table (
            Date DATE,
            Year INT,
            Month INT,
            Day INT,
            Weekday VARCHAR(50),
            US_Holiday BIT,
            UK_Holiday BIT,
            US_Holiday_Name VARCHAR(100),
            UK_Holiday_Name VARCHAR(100)
        );
    ''')
    conn.commit()
    print("Table created successfully.")

   
    insert_query = '''
        INSERT INTO dim_date_table (Date, Year, Month, Day, Weekday, US_Holiday, UK_Holiday, US_Holiday_Name, UK_Holiday_Name)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
    '''
    
    # this makes it faster
    cursor.fast_executemany = True
    
    #
    data_to_insert = [
        (row['Date'], row['Year'], row['Month'], row['Day'], row['Weekday'],
         row['US_Holiday'], row['UK_Holiday'], row['US_Holiday_Name'], row['UK_Holiday_Name'])
        for index, row in date_table.iterrows()
    ]

    # Insert in chunks and log progress to make sure long waits are logged.
    chunk_size = 500
    start_time = time.time()
    
    for i in range(0, len(data_to_insert), chunk_size):
        cursor.executemany(insert_query, data_to_insert[i:i+chunk_size])
        conn.commit()
        print(f"Inserted rows {i} to {i + chunk_size} in {(time.time() - start_time):.2f} seconds")
        start_time = time.time()  

    print(f"Successfully inserted {len(date_table)} rows into the dim_date_table.")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
 
    if conn:
        conn.close()

