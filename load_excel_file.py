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
target_mongo_url = os.getenv("target_mongo_url")
target_db_name = "channel_related_json"

# Connect to MongoDB
client = pymongo.MongoClient(target_mongo_url)
db = client[target_db_name]

# Ensure the excel_files directory exists
if os.path.exists('excel_files'):
    print("Directory 'excel_files' exists. Deleting it completely.")
    # Remove the directory and all its contents
    shutil.rmtree('excel_files')
    print("Directory 'excel_files' and its contents have been deleted.")

# Create the excel_files directory
os.makedirs('excel_files')

# Get all collection names in the database
collection_names = db.list_collection_names()

total_files = len(collection_names)

# Iterate over each collection and save its data to an Excel file
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
    
    # Create DataFrame and drop the '_id' column
    df = pd.DataFrame(documents_list)
    if '_id' in df.columns:
        df.drop('_id', axis=1, inplace=True)
    
    # Replace NaN and infinite values with a placeholder value (e.g., 'NA')
    df = df.fillna('NA').astype(object)
    df.replace([float('inf'), float('-inf')], 'NA', inplace=True)

    # Sanitize collection name to use it as a file name
    sanitized_collection_name = sanitize_file_name(collection_name)
    excel_file_path = os.path.join('excel_files', f"{sanitized_collection_name}.xlsx")
    
    try:
        # Save DataFrame to Excel with auto-width columns and add blue color design
        with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='Sheet1', startrow=1, header=False)

            workbook  = writer.book
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
        
        print(f"File {idx + 1} out of {total_files} saved: {excel_file_path}")
    except Exception as e:
        print(f"Failed to save file {sanitized_collection_name}.xlsx due to {e}")

# Create a zip file of the excel_files directory
zip_file_path = 'excel_files.zip'
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('excel_files'):
        for file in files:
            zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), 'excel_files'))

print(f"Excel files have been zipped into {zip_file_path}")

# Connect to the MongoDB database for storing the zip file
zip_db_name = "zip_files"
zip_collection_name = "excel_files"
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
    grid_out = fs.put(f, filename='excel_files.zip')

# Print the size of the zip file
file_info = fs.find_one({"_id": grid_out})
file_size = file_info.length
print(f"Zip file {zip_file_path} has been saved to MongoDB in database {zip_db_name}, collection {zip_collection_name}, with size: {file_size / 1048576:.2f} MB.")

# Close the MongoDB connection
client.close()
