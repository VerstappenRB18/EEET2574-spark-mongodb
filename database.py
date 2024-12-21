import pymongo
import pandas as pd
import os

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://giabao29022004:zdxjvFbVvho8oXWi@hello.zj1gk.mongodb.net/?retryWrites=true&w=majority&appName=Hello")
assignment_db = client["assignment"]

# Directory paths
electricity_dir = "./Electricity"
gas_dir = "./Gas"

# File mappings for collections
collections_mapping = {
    "Electricity": {
        "2018": ["coteq_electricity_2018.csv", "stedin_electricity_2018.csv", "westland-infra_electricity_2018.csv"],
        "2019": ["coteq_electricity_2019.csv", "stedin_electricity_2019.csv", "westland-infra_electricity_2019.csv"],
        "2020": ["coteq_electricity_2020.csv", "stedin_electricity_2020.csv", "westland-infra_electricity_2020.csv"]
    },
    "Gas": {
        "2018": ["coteq_gas_2018.csv", "stedin_gas_2018.csv", "westland-infra_gas_2018.csv"],
        "2019": ["coteq_gas_2019.csv", "stedin_gas_2019.csv", "westland-infra_gas_2019.csv"],
        "2020": ["coteq_gas_2020.csv", "stedin_gas_2020.csv", "westland-infra_gas_2020.csv"]
    }
}

# Load files into MongoDB
def load_files_to_mongodb(database, directory, year, file_list):
    collection_name = f"{database}_{year}_data"  # Collection name by year
    collection = assignment_db[collection_name]
    for file_name in file_list:
        file_path = os.path.join(directory, file_name)
        df = pd.read_csv(file_path)
        records = df.to_dict(orient="records")
        collection.insert_many(records)
        print(f"Inserted data from {file_name} into {collection_name} collection.")

# Load data into the database
for year, file_list in collections_mapping["Electricity"].items():
    load_files_to_mongodb("electricity", electricity_dir, year, file_list)
    
for year, file_list in collections_mapping["Gas"].items():
    load_files_to_mongodb("gas", gas_dir, year, file_list)

print("All data successfully loaded into MongoDB!")