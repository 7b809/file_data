import pymongo
from bson import ObjectId
from datetime import datetime
import os
import json
import shutil
import zipfile
from gridfs import GridFS

# Function to parse timestamp into date, day, and hour
def parse_timestamp(timestamp):
    try:
        timestamp_obj = datetime.strptime(timestamp, '%A, %b %d, %Y, %I %p')
        return timestamp_obj.strftime('%Y-%m-%d %H:%M:%S')
    except ValueError:
        return 'Invalid Timestamp'

# Function to sanitize file names by replacing special characters with underscores
def sanitize_file_name(name):
    return re.sub(r'\W+', '_', name)

# Replace these values with your target MongoDB connection details
target_mongo_url = os.getenv("target_mongo_url")
target_db_name = "channel_related_json"

# Connect to MongoDB
client = pymongo.MongoClient(target_mongo_url)
db = client[target_db_name]

# Ensure the json_files directory exists
if os.path.exists('json_files'):
    print("Directory 'json_files' exists. Deleting it completely.")
    # Remove the directory and all its contents
    shutil.rmtree('json_files')
    print("Directory 'json_files' and its contents have been deleted.")

# Create the json_files directory
os.makedirs('json_files')

# Get all collection names in the database
collection_names = db.list_collection_names()

total_files = len(collection_names)

# Iterate over each collection and save its data to a JSON file
for idx, collection_name in enumerate(collection_names):
    collection = db[collection_name]
    
    # Fetch all documents from the collection
    documents = collection.find()
    
    # Convert ObjectId fields to string representation
    def convert_object_ids(doc):
        for key in doc:
            if isinstance(doc[key], ObjectId):
                doc[key] = str(doc[key])
        return doc
    
    documents_list = [convert_object_ids(doc) for doc in documents]
    
    # Parse "Timestamp" field into a formatted string
    for doc in documents_list:
        if "Timestamp" in doc:
            doc["Timestamp"] = parse_timestamp(doc["Timestamp"])
    
    # Sanitize collection name to use it as a file name
    sanitized_collection_name = sanitize_file_name(collection_name)
    json_file_path = os.path.join('json_files', f"{sanitized_collection_name}.json")
    
    try:
        # Save documents list to JSON file
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(documents_list, json_file, ensure_ascii=False, indent=4)
        
        print(f"File {idx + 1} out of {total_files} saved: {json_file_path}")
    except Exception as e:
        print(f"Failed to save file {sanitized_collection_name}.json due to {e}")

# Create a zip file of the json_files directory
zip_file_path = 'json_files.zip'
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('json_files'):
        for file in files:
            zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), 'json_files'))

print(f"JSON files have been zipped into {zip_file_path}")

# Connect to the MongoDB database for storing the zip file
zip_db_name = "zip_files"
zip_collection_name = "json_files"
zip_db = client[zip_db_name]
fs = GridFS(zip_db, zip_collection_name)

# Check if the GridFS collection is empty and remove existing files if not
if fs.exists({}):
    print("Existing files found in the collection. Removing them.")
    fs_files = zip_db['fs.files']
    fs_chunks = zip_db['fs.chunks']
    fs_files.delete_many({})
    fs_chunks.delete_many({})
    print("All existing files in the collection have been removed.")

# Store the zip file in GridFS
with open(zip_file_path, 'rb') as f:
    grid_out = fs.put(f, filename='json_files.zip')

# Print the size of the zip file
file_info = fs.find_one({"_id": grid_out})
file_size = file_info.length
print(f"Zip file {zip_file_path} has been saved to MongoDB in database {zip_db_name}, collection {zip_collection_name}, with size: {file_size / 1048576:.2f} MB.")

# Close the MongoDB connection
client.close()
