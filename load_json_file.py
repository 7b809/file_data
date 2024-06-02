import pymongo
import json
import os
import re
import shutil  # For removing directories and their contents
from bson import ObjectId
import zipfile  # For creating zip files

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
collection_names = db.list_collection_names()[:5]
total_collections = len(collection_names)
for index, collection_name in enumerate(collection_names, start=1):
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
    
    print(f"Data saved to {file_path} successfully. ({index} out of {total_collections} files)")

# Zip the json_files directory
zip_file_path = 'json_files.zip'
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('json_files'):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, 'json_files')
            zipf.write(file_path, arcname)

print(f"Directory 'json_files' has been zipped into '{zip_file_path}'.")

# Connect to the target MongoDB database for storing the zip file
zip_db = client['zip_files']
zip_collection = zip_db['json_files']

# Check if the collection is not empty and delete its contents if it is not empty
if zip_collection.count_documents({}) > 0:
    print("Collection 'json_files' is not empty. Deleting all documents.")
    zip_collection.delete_many({})
    print("All documents in 'json_files' collection have been deleted.")

# Read the zip file and insert it into MongoDB
with open(zip_file_path, 'rb') as file_data:
    zip_binary = file_data.read()
    zip_document = {
        "filename": "json_files.zip",
        "filedata": zip_binary
    }
    zip_collection.insert_one(zip_document)

print("Zip file has been saved to MongoDB successfully.")


# Close the MongoDB connection
client.close()
