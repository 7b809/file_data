import pymongo
from bson import ObjectId
from datetime import datetime
import os
import re
import shutil
import zipfile
import pandas as pd
from gridfs import GridFS
import io

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

# Function to split DataFrame into chunks based on size limit
def split_documents(df, max_size=15000000):
    chunks = []
    current_size = 0
    current_chunk = pd.DataFrame()

    for index, row in df.iterrows():
        row_size = row.memory_usage(deep=True).sum()
        if current_size + row_size > max_size:
            chunks.append(current_chunk)
            current_chunk = pd.DataFrame()
            current_size = 0
        current_chunk = current_chunk.append(row, ignore_index=True)
        current_size += row_size

    if not current_chunk.empty:
        chunks.append(current_chunk)

    return chunks


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

    # Split DataFrame into chunks based on document size limit (approx. 15MB)
    df_chunks = split_documents(df)

    # Save each chunk as a separate Excel file
    for i, chunk in enumerate(df_chunks):
        # Sanitize collection name to use it as a file name
        sanitized_collection_name = sanitize_file_name(f"{collection_name}_{i}")
        excel_file_path = os.path.join('excel_files', f"{sanitized_collection_name}.xlsx")

        try:
            # Save DataFrame chunk to Excel with auto-width columns and add blue color design
            with pd.ExcelWriter(excel_file_path, engine='xlsxwriter') as writer:
                chunk.to_excel(writer, index=False, sheet_name='Sheet1', startrow=1, header=False)

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
                for col_num, value in enumerate(chunk.columns.values):
                    worksheet.write(0, col_num, value, header_format)
                    # Set column width for headers
                    column_width = max(len(value), 10) + 2  # Adjust 10 to a suitable default width
                    worksheet.set_column(col_num, col_num, column_width)

                # Set column width and apply blue color to all data cells with border and centered text
                for j, col in enumerate(chunk.columns):
                    column_width = max(chunk[col].astype(str).map(len).max(), len(col))
                    worksheet.set_column(j, j, column_width)
                    for k in range(len(chunk[col])):
                        cell_format = workbook.add_format({
                            'fg_color': '#DDEBF7',  # Light blue color
                            'border': 1,
                            'align': 'center',
                            'font_size': 9,
                            'valign': 'vcenter'
                        })
                        worksheet.write(k + 1, j, chunk[col].iloc[k], cell_format)

                # Add filter to every column
                worksheet.autofilter(0, 0, len(chunk), len(chunk.columns) - 1)

            print(f"File {i + 1} of {len(df_chunks)} for {collection_name} saved: {excel_file_path}")
        except Exception as e:
            print(f"Failed to save file {sanitized_collection_name}.xlsx due to {e}")

# Create a zip file of the excel_files directory
zip_file_path = 'excel_files.zip'
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk('excel_files'):
        for file in files:
            zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), 'excel_files'))

print(f"Excel files have been zipped into {zip_file_path}")

# Connect to the target MongoDB database for storing the zip file
zip_db = client['zip_files']
zip_collection = zip_db['excel_files']

# Check if the collection is not empty and delete its contents if it is not empty
if zip_collection.count_documents({}) > 0:
    print("Collection 'excel_files' is not empty. Deleting all documents.")
    zip_collection.delete_many({})
    print("All documents in 'excel_files' collection have been deleted.")

# Read the zip file and insert it into MongoDB using GridFS
fs = GridFS(zip_db)
with open(zip_file_path, 'rb') as file_data:
    file_id = fs.put(file_data, filename="excel_files.zip")

print("Zip file has been saved to MongoDB successfully.")

# Delete the zip file from local storage
try:
    os.remove(zip_file_path)
    print(f"Zip file '{zip_file_path}' has been deleted from local storage.")
except OSError as e:
    print(f"Error: {zip_file_path} : {e.strerror}")

# Close the MongoDB connection
client.close()
