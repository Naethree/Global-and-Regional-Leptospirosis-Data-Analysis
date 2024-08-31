import pandas as pd
from pymongo import MongoClient

def upload_csv_to_mongodb(csv_path, mongo_uri, db_name, collection_name):
    # Load CSV data into a DataFrame
    df = pd.read_csv(csv_path)

    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Clear the collection before inserting new data
    collection.delete_many({})

    # Convert DataFrame to dictionary and insert into MongoDB
    data = df.to_dict(orient='records')
    collection.insert_many(data)

    print(f"Data successfully inserted into {db_name}.{collection_name}")

# Set the parameters
csv_path = '/home/naethree/Users/naethree/airflow/dags/world/World.csv'  # Path to the CSV file
mongo_uri = 'mongodb+srv://naethreepremnath:4MKDUufmYjAWTkGa@leptosglobalcluster.sjkc1.mongodb.net/?retryWrites=true&w=majority&appName=LeptosglobalCluster'
db_name = 'leptospirosis_data'  # Database name
collection_name = 'world_data'  # Collection name

# Call the function
upload_csv_to_mongodb(csv_path, mongo_uri, db_name, collection_name)

