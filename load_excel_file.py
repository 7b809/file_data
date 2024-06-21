import pandas as pd
import pymongo
from bson import ObjectId
from datetime import datetime
import os
import re
import shutil
import zipfile
from gridfs import GridFS

pd.set_option('future.no_silent_downcasting', True)

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
# Replace these values with your target MongoDB connection details
target_mongo_url = os.getenv("target_mongo_url")
target_db_name = "channel_related_json"

# Connect to MongoDB
client = pymongo.MongoClient(target_mongo_url)
db = client[target_db_name]

# Ensure the base directory exists
base_directory = 'excel_folders'
if os.path.exists(base_directory):
    print(f"Base directory '{base_directory}' exists. Deleting it completely.")
    shutil.rmtree(base_directory)
    print(f"Base directory '{base_directory}' and its contents have been deleted.")

# Create the base directory
os.makedirs(base_directory)

# Get all collection names in the database
collection_names = db.list_collection_names()

total_files = len(collection_names)
max_collections_per_folder = 60
num_folders = (total_files + max_collections_per_folder - 1) // max_collections_per_folder

# Function to save DataFrame to Excel with header formatting
def save_df_to_excel(df, excel_file_path):
    try:
        # Save DataFrame to Excel with auto-width columns and add blue color design
        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1', startrow=1, header=False)

            workbook = writer.book
            worksheet = writer.sheets['Sheet1']

            # Add a header format with border and centered text
            header_format = workbook.add_format({
                'bold': True,
                'text_wrap': True,
                'valign': 'top',
                'align': 'center',
                'font_size': 11,
                'fg_color': '#3EC6EC',  # Blue color
                'border': 1
            })

            # Write the column headers with the defined format
            for col_num, value in enumerate(df.columns.values):
                worksheet.write(0, col_num, value, header_format)
                # Set column width for headers
                column_width = max(len(value), 10) + 2  # Adjust 10 to a suitable default width
                worksheet.set_column(col_num, col_num, column_width)

            # Set column width and apply blue color to all data cells with border and centered text
            for i, col in enumerate(df.columns):
                column_width = max(df[col].astype(str).map(len).max(), len(col))
                worksheet.set_column(i, i, column_width)
                for j in range(len(df[col])):
                    cell_format = workbook.add_format({
                        'fg_color': '#DDEBF7',  # Light blue color
                        'border': 1,
                        'align': 'center',
                        'font_size': 9,
                        'valign': 'vcenter'
                    })
                    worksheet.write(j + 1, i, df[col][j], cell_format)

            # Add filter to every column
            worksheet.autofilter(0, 0, len(df), len(df.columns) - 1)

        print(f"Excel file saved: {excel_file_path}")
    except Exception as e:
        print(f"Failed to save file {excel_file_path} due to {e}")

# Function to zip a directory and return the zip file path
def zip_directory(directory_path):
    zip_file_path = f"{directory_path}.zip"
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(directory_path):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), directory_path))
    print(f"Directory '{directory_path}' has been zipped into {zip_file_path}")
    return zip_file_path

# Process collections and save to folders
for folder_index in range(num_folders):
    folder_name = f"folder_{folder_index + 1}"
    folder_path = os.path.join(base_directory, folder_name)
    os.makedirs(folder_path)

    start_index = folder_index * max_collections_per_folder
    end_index = min((folder_index + 1) * max_collections_per_folder, total_files)

    for idx in range(start_index, end_index):
        collection_name = collection_names[idx]
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

        # Create DataFrame and drop the '_id' column
        df = pd.DataFrame(documents_list)
        if '_id' in df.columns:
            df.drop('_id', axis=1, inplace=True)

        # Replace NaN and infinite values with a placeholder value (e.g., 'NA')
        df = df.fillna('NA').astype(object)
        df.replace([float('inf'), float('-inf')], 'NA', inplace=True)

        # Sanitize collection name to use it as a file name
        sanitized_collection_name = sanitize_file_name(collection_name)
        excel_file_path = os.path.join(folder_path, f"{sanitized_collection_name}.xlsx")

        # Save DataFrame to Excel
        save_df_to_excel(df, excel_file_path)

# Zip each folder and save to MongoDB
for folder_index in range(num_folders):
    folder_name = f"folder_{folder_index + 1}"
    folder_path = os.path.join(base_directory, folder_name)

    # Zip the folder
    zip_file_path = zip_directory(folder_path)

    # Connect to the target MongoDB database for storing the zip file
    zip_db = client['zip_files']
    zip_collection = zip_db['excel_files']
    # Check if the collection is not empty and delete its contents if it is not empty
    if zip_collection.count_documents({}) > 0:
        print("Collection 'excel_files' is not empty. Deleting all documents.")
        zip_collection.delete_many({})
        print("All documents in 'excel_files' collection have been deleted.")


    # Read the zip file and insert it into MongoDB
    with open(zip_file_path, 'rb') as file_data:
        zip_binary = file_data.read()
        zip_document = {
            "filename": f"{folder_name}.zip",
            "filedata": zip_binary
        }
        zip_collection.insert_one(zip_document)

    print(f"Zip file '{zip_file_path}' has been saved to MongoDB successfully.")

    # Delete the zip file from local storage
    try:
        os.remove(zip_file_path)
        print(f"Zip file '{zip_file_path}' has been deleted from local storage.")
    except OSError as e:
        print(f"Error: {zip_file_path} : {e.strerror}")

# Close the MongoDB connection
client.close()
