import os
import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# MongoDB connection settings
mongo_client = MongoClient('mongodb://localhost:27017/')
db = mongo_client['leptospirosis_data']
collection = db['srilanka_data']  # Use the correct collection name

# Define the base path for CSV files
csv_base_path = '/home/naethree/Users/naethree/airflow/dags/slcsvs/'

def upload_csv_to_mongodb(csv_path, year):
    # Ensure the file exists before attempting to read
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        df['Year'] = year  # Add the year to the DataFrame
        records = df.to_dict(orient='records')
        for record in records:
            collection.update_one({'Year': year}, {'$set': record}, upsert=True)
        print(f"Uploaded data from {csv_path} (Year: {year}) to MongoDB collection 'srilanka'.")
    else:
        print(f"File not found: {csv_path}")

# Check if today is January 31st
today = datetime.now()
current_year = today.year

# Adjust year to process the data for the previous year
if today.month == 1 and today.day == 31:
    # Process data for the previous year (2024 in this case)
    year_to_process = current_year - 1
    csv_filename = f'leptospirosis_data_{year_to_process}.csv'
    csv_path = os.path.join(csv_base_path, csv_filename)

    # Run sl.py to download and process new data for the current year
    os.system(f'python3 /home/naethree/Users/naethree/airflow/dags/sl.py')

    # Upload the data for the previous year to MongoDB
    upload_csv_to_mongodb(csv_path, year_to_process)
else:
    # Use the existing CSV for the last year
    year_to_process = current_year - 1
    csv_path = os.path.join(csv_base_path, f'leptospirosis_data_{year_to_process}.csv')
    upload_csv_to_mongodb(csv_path, year_to_process)
