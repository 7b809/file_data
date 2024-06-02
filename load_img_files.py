import os
import re
import time
from datetime import datetime
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import zipfile
import io
from gridfs import GridFS

# Get the MongoDB URL from the environment variable
target_mongo_url =os.getenv("target_mongo_url")
if not target_mongo_url:
    print("MongoDB URL not found in environment variables.")
    exit(1)

# Database name
target_db_name = "channel_related_json"

# Connect to the MongoDB client
client = pymongo.MongoClient(target_mongo_url)
db = client[target_db_name]
print(f"Connected to MongoDB database '{target_db_name}'.")

# Output folder for saving images
output_folder = r"temp_img_files"
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

# Get a list of collection names
collection_names = db.list_collection_names()
total_collections = len(collection_names)
print(f"Total collections found: {total_collections}")

for idx, collection_name in enumerate(collection_names, 1):
    try:
        start_time = time.time()
        print(f"\nProcessing collection {idx} out of {total_collections}: {collection_name}")

        collection = db[collection_name]
        documents = list(collection.find())

        timestamps = []
        profits_per_day = []
        total_profits = []

        for doc in documents:
            try:
                timestamps.append(datetime.strptime(doc["Timestamp"], "%A, %b %d, %Y, %I %p"))
                profits_per_day.append(doc["Profits Per Day ($)"])
                total_profits.append(doc["Profits Without Expenses ($)"])
            except Exception as e:
                print(f"Error processing document {doc}: {e}")

        ymin_profits_per_day = min(profits_per_day, default=0)
        ymax_profits_per_day = max(profits_per_day, default=0)
        ymin_total_profits = min(total_profits, default=0)
        ymax_total_profits = max(total_profits, default=0)

        data = {
            'timestamp': timestamps,
            'profit': profits_per_day,
            'full_profit': total_profits
        }
        df = pd.DataFrame(data)

        def categorize_hour(hour):
            if hour >= 0 and hour < 6:
                return '00:00 - 05:59'
            elif hour >= 6 and hour < 12:
                return '06:00 - 11:59'
            elif hour >= 12 and hour < 18:
                return '12:00 - 17:59'
            else:
                return '18:00 - 23:59'

        def get_color(hour):
            category = categorize_hour(hour)
            if category == '00:00 - 05:59':
                return 'blue'
            elif category == '06:00 - 11:59':
                return 'green'
            elif category == '12:00 - 17:59':
                return 'orange'
            else:
                return 'red'

        fig, axs = plt.subplots(2, 1, figsize=(24, 20))

        for idx, row in df.iterrows():
            hour = row['timestamp'].hour
            color = get_color(hour)
            axs[0].scatter(row['timestamp'], row['profit'], color=color)
            axs[1].scatter(row['timestamp'], row['full_profit'], color=color)

        axs[0].set_xlabel('Timestamp')
        axs[0].set_ylabel('Profits Per Day ($)')
        axs[0].set_ylim(ymin_profits_per_day - 5, ymax_profits_per_day + 10)
        axs[0].set_title(f'{collection_name} - Profits Per Day Electricity Included \n\n Low Value during period {ymin_profits_per_day} \n Highest Value during period {ymax_profits_per_day}')

        axs[1].set_xlabel('Timestamp')
        axs[1].set_ylabel('Profits Without Expenses ($)')
        axs[1].set_ylim(ymin_total_profits - 5, ymax_total_profits + 10)
        axs[1].set_title(f'{collection_name} - Total Profits Without Expenses \n\n Lowest Value during period {ymin_total_profits} \n Highest Value during period {ymax_total_profits}')

        for ax in axs:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
            plt.setp(ax.get_xticklabels(), rotation=45)

        handles = [
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=10, label='00:00 - 05:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='green', markersize=10, label='06:00 - 11:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='orange', markersize=10, label='12:00 - 17:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='18:00 - 23:59')
        ]

        axs[0].legend(handles=handles, title='Hour Category', bbox_to_anchor=(1, 1), loc='upper left')
        axs[1].legend(handles=handles, title='Hour Category', bbox_to_anchor=(1, 1), loc='upper left')

        plt.subplots_adjust(hspace=0.6)
        plt.tight_layout()

        sanitized_collection_name = re.sub(r'\W+', '_', collection_name)
        output_file = os.path.join(output_folder, f"{sanitized_collection_name}.png")
        plt.savefig(output_file)
        plt.close()

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Total execution time for {collection_name}: {execution_time:.2f} seconds\n\n")

    except Exception as e:
        print(f"Error processing collection {collection_name}: {e}")


# Database name and collection name for storing the zip file
zip_db_name = "zip_files"
zip_collection_name = "img_files"

# Get remaining space in MongoDB database
def get_remaining_space(client):
    try:
        db_stats = client.admin.command('dbStats')
        storage_size_mb = db_stats['storageSize'] / (1024 * 1024)  # Convert bytes to megabytes
        data_size_mb = db_stats['dataSize'] / (1024 * 1024)  # Convert bytes to megabytes
        remaining_space_mb = storage_size_mb - data_size_mb
        print(f"Remaining space in MongoDB database: {remaining_space_mb:.2f} MB")
    except Exception as e:
        print(f"Error getting remaining space in MongoDB database: {e}")

# Get size of zip file
def get_zip_file_size(zip_file_path):
    try:
        zip_file_size = os.path.getsize(zip_file_path) / (1024 * 1024)  # Convert bytes to megabytes
        print(f"Size of zip file: {zip_file_size:.2f} MB")
    except Exception as e:
        print(f"Error getting size of zip file: {e}")

# Connect to the MongoDB database for storing the zip file
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

# After processing all images, create a zip file and store it in MongoDB
try:
    zip_file_path = 'img_files.zip'
    with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(output_folder):
            for file in files:
                zipf.write(os.path.join(root, file), os.path.relpath(os.path.join(root, file), output_folder))

    # Save zip file to MongoDB GridFS
    with open(zip_file_path, 'rb') as f:
        grid_out = fs.put(f, filename='img_files.zip')

    # Print the size of the zip file
    file_info = fs.find_one({"_id": grid_out})
    file_size = file_info.length
    print(f"Zip file {zip_file_path} has been saved to MongoDB in database {zip_db_name}, collection {zip_collection_name}, with size: {file_size / 1048576:.2f} MB.")

    # Print remaining space in MongoDB database
    get_remaining_space(client)

except Exception as e:
    print(f"Error saving zip file to MongoDB: {e}")
finally:
    # Clean up temporary directory
    if os.path.exists(output_folder):
        for file in os.listdir(output_folder):
            os.remove(os.path.join(output_folder, file))
        os.rmdir(output_folder)

    # Close the MongoDB connection
    client.close()
