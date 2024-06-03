import pymongo
import json
import os
import re
import shutil  # Importing shutil module for removing directories and their contents
from bson import ObjectId

# Replace these values with your target MongoDB connection details
target_mongo_url = os.getenv("target_mongo_url")
target_db_name = "channel_related_json"

# Connect to MongoDB
client = pymongo.MongoClient(target_mongo_url)
db = client[target_db_name]

# Function to convert ObjectId fields to string representation
def convert_object_ids(doc):
    for key in doc:
        if isinstance(doc[key], ObjectId):
            doc[key] = str(doc[key])
    return doc

# Function to sanitize collection name for file naming
def sanitize_collection_name(collection_name):
    return re.sub(r'[^\w\-_\.]', '_', collection_name)

# Ensure the json_files directory exists
if os.path.exists('json_files'):
    print("Directory 'json_files' exists. Deleting it completely.")
    # Remove the directory and all its contents
    shutil.rmtree('json_files')
    print("Directory 'json_files' and its contents have been deleted.")

# Create the json_files directory
os.makedirs('json_files')

# Fetch all collections and save each as a JSON file
for collection_name in db.list_collection_names():
    collection = db[collection_name]
    documents = collection.find()
    documents_list = [convert_object_ids(doc) for doc in documents]

    # Sanitize collection name for file naming
    sanitized_collection_name = sanitize_collection_name(collection_name)

    # Define the output file path
    file_path = os.path.join('json_files', f'{sanitized_collection_name}.json')
    
    # Save the documents to the JSON file
    with open(file_path, 'w') as json_file:
        json.dump(documents_list, json_file, indent=4)
    
    print(f"Data saved to {file_path} successfully.")

# Print all files in the current working directory
print("\nFiles in the current directory:")
for file_name in os.listdir('.'):
    print(file_name)

# Close the MongoDB connection
client.close()
