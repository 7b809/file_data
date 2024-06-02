from pymongo import MongoClient
import json
from bson import json_util
import os

# Function to load documents from MongoDB
def load_documents_from_mongo(mongo_url, db_name, collection_name):
    client = MongoClient(mongo_url)
    db = client[db_name]
    collection = db[collection_name]
    documents = collection.find()
    json_documents = [json.loads(json_util.dumps(doc)) for doc in documents]
    print("Documents loaded from MongoDB")
    return json_documents

# Function to calculate electricity bill
def calculate_electricity_bill(power_consumption_watts):
    power_consumption_kilowatts = power_consumption_watts / 1000
    electricity_bill = power_consumption_kilowatts * 24 * 0.12
    return round(electricity_bill, 2)

# Function to calculate electricity units
def calculate_electricity_units(power_consumption_watts):
    power_consumption_kilowatts = power_consumption_watts / 1000
    return power_consumption_kilowatts
    
# Function to convert profits to number
def convert_profits_to_number(profits_str):
    profits_str = profits_str.replace("$", "").replace("/day", "").replace(",", "").strip()
    if profits_str.lower() == "unknown":
        return None
    is_negative = profits_str.startswith("-")
    profits_number = float(profits_str.replace("-", ""))
    if is_negative:
        profits_number = -profits_number
    return profits_number

# Function to clear all collections in a database
def clear_database(client, db_name):
    client.drop_database(db_name)
    print(f"Database {db_name} cleared.")

# Load MongoDB connection details from environment variables
load_mongo_url = os.getenv("load_mongo_url")
db_name = "mydatabase"
collection_name = "asinc_profits"

# Load data from the source MongoDB database
data = load_documents_from_mongo(load_mongo_url, db_name, collection_name)

# Format the data
formatted_data = []
grouped_data = {}
for item in data:
    name = item["name"]
    if name not in grouped_data:
        grouped_data[name] = []
    profits_per_day = convert_profits_to_number(item["rentability"])
    electricity_bill_per_day = calculate_electricity_bill(int(item["power_consumption"].replace("W", "")))
    electricity_units = calculate_electricity_units(int(item["power_consumption"].replace("W", "")))
    formatted_item = {
        "Timestamp": item["updated_timestamp"],
        "Name": item["name"],
        "Model Version": item["date"],
        "Hashrate": item["hash_rate"],
        "Power Consumption": item["power_consumption"],
        "Noise Level": item["noise_level"],
        "Algorithm": item["algorithm"],
        "Profits Per Day ($)": profits_per_day,
        "Electricity Units Per Day (kw)": round(electricity_units * 24, 4),
        "Electricity Bill Per Day ($)": electricity_bill_per_day,
        "Profits Without Expenses ($)":( profits_per_day + electricity_bill_per_day ) if profits_per_day is not None else None,
        "Profits Per Month ($)": profits_per_day * 30 if profits_per_day is not None else None,
        "Electricity Units Per Month (kw)": round(electricity_units * 24 * 30, 4),
        "Electricity Bill Per Month ($)": electricity_bill_per_day * 30,
        "Monthly Profits Without Expenses ($)": ( (profits_per_day * 30) + (30 * electricity_bill_per_day) ) if profits_per_day is not None else None
    }
    grouped_data[name].append(formatted_item)

for name, details in grouped_data.items():
    machine_entry = {
        "Machine Name": name,
        "Machine Data": details
    }
    formatted_data.append(machine_entry)

data = formatted_data

# Load target MongoDB connection details from environment variables
target_mongo_url = os.getenv("target_mongo_url")
target_db_name = "channel_related_json"

# Connect to the target MongoDB server
target_client = MongoClient(target_mongo_url)

# Clear the target database
clear_database(target_client, target_db_name)

# Get the target database
target_db = target_client[target_db_name]

# Insert data into the target database
total_collections = len(data)
for index, machine_entry in enumerate(data, start=1):
    machine_name = machine_entry["Machine Name"]
    machine_data = machine_entry["Machine Data"]
    collection = target_db[machine_name]
    collection.insert_many(machine_data)
    print(f"Inserted {len(machine_data)} documents into collection {machine_name} ({index}/{total_collections})")

print("All data inserted into MongoDB collections successfully!")
