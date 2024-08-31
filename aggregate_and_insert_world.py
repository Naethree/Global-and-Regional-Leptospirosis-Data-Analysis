import pandas as pd
from pymongo import MongoClient


def aggregate_and_insert(csv_path, mongo_uri, db_name, collection_name):
    # Load CSV data into a DataFrame
    df = pd.read_csv(csv_path)

    # Aggregate cases by year
    aggregated_df = df.groupby('Year')['Cases'].sum().reset_index()

    # Prepare data for MongoDB
    total_cases = aggregated_df['Cases'].sum()
    world_data = {
        'Country': 'Sri Lanka',
        'Year': aggregated_df['Year'].tolist(),
        'Cases': aggregated_df['Cases'].tolist(),
        'Total': total_cases
    }

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Insert data into MongoDB
    collection.insert_one(world_data)

    print(f"Data successfully aggregated and inserted into {db_name}.{collection_name}")


# Set the parameters
csv_path = '/home/naethree/Users/naethree/airflow/dags/world/World.csv'
mongo_uri = 'mongodb+srv://naethreepremnath:4MKDUufmYjAWTkGa@leptosglobalcluster.sjkc1.mongodb.net/?retryWrites=true&w=majority&appName=LeptosglobalCluster'
db_name = 'leptospirosis_data'  # Database name
collection_name = 'world_data'  # Collection name

# Call the function
aggregate_and_insert(csv_path, mongo_uri, db_name, collection_name)
