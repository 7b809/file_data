import os
import re
import time
from datetime import datetime
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import zipfile
from scipy.stats import zscore
from PIL import Image
import io

# MongoDB connection details
# Get the MongoDB URL from the environment variable
target_mongo_url =os.getenv("target_mongo_url")
if not target_mongo_url:
    print("MongoDB URL not found in environment variables.")
    exit(1)


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

def remove_outliers_zscore(data):
    z_scores = zscore(data)
    abs_z_scores = abs(z_scores)
    filtered_entries = (abs_z_scores < 5)  # Threshold for Z-score
    return filtered_entries

def compress_image(image_path, output_path, max_size_kb=50):
    quality = 95
    while quality > 0:
        img = Image.open(image_path)
        img_byte_arr = io.BytesIO()
        img.save(img_byte_arr, format='PNG', quality=quality)
        size_kb = len(img_byte_arr.getvalue()) / 1024
        if size_kb <= max_size_kb:
            with open(output_path, 'wb') as f:
                f.write(img_byte_arr.getvalue())
            return
        quality -= 5
    img.save(output_path, format='PNG', quality=quality)

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

        # Remove outliers
        profits_per_day_filtered = [p for p, valid in zip(profits_per_day, remove_outliers_zscore(profits_per_day)) if valid]
        total_profits_filtered = [p for p, valid in zip(total_profits, remove_outliers_zscore(total_profits)) if valid]
        timestamps_filtered = [t for t, valid in zip(timestamps, remove_outliers_zscore(profits_per_day)) if valid]

        ymin_profits_per_day = min(profits_per_day_filtered, default=0)
        ymax_profits_per_day = max(profits_per_day_filtered, default=0)
        ymin_total_profits = min(total_profits_filtered, default=0)
        ymax_total_profits = max(total_profits_filtered, default=0)

        data = {
            'timestamp': timestamps_filtered,
            'profit': profits_per_day_filtered,
            'full_profit': total_profits_filtered
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
            plt.setp(ax.get_xticklabels(), rotation=60)

        handles = [
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='blue', markersize=10, label='00:00 - 05:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='green', markersize=10, label='06:00 - 11:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='orange', markersize=10, label='12:00 - 17:59'),
            plt.Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=10, label='18:00 - 23:59')
        ]

        axs[0].legend(handles=handles, title='Hour Category', bbox_to_anchor=(1, 1), loc='upper left')
        axs[1].legend(handles=handles, title='Hour Category', bbox_to_anchor=(1, 1), loc='upper left')
        # Enabling both grid lines:
        axs[0].grid(which = "both")
        axs[0].minorticks_on()
        axs[0].tick_params(which = "minor", bottom = False, left = False)

        plt.subplots_adjust(hspace=0.6)
        plt.tight_layout()

        sanitized_collection_name = re.sub(r'\W+', '_', collection_name)
        output_file = os.path.join(output_folder, f"{sanitized_collection_name}.png")
        temp_output_file = os.path.join(output_folder, f"temp_{sanitized_collection_name}.png")
        
        # Save the plot to a temporary file first
        plt.savefig(temp_output_file)
        plt.close()

        # Compress the image
        compress_image(temp_output_file, output_file)

        # Remove the temporary file
        os.remove(temp_output_file)

        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Total execution time for {collection_name}: {execution_time:.2f} seconds\n\n")

    except Exception as e:
        print(f"Error processing collection {collection_name}: {e}")

# Zip the image files
zip_file_path = 'img_files.zip'
with zipfile.ZipFile(zip_file_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
    for root, dirs, files in os.walk(output_folder):
        for file in files:
            file_path = os.path.join(root, file)
            arcname = os.path.relpath(file_path, output_folder)
            zipf.write(file_path, arcname)

print(f"Directory '{output_folder}' has been zipped into '{zip_file_path}'.")


# Connect to the target MongoDB database for storing the zip file
zip_db = client['zip_files']
zip_collection = zip_db['img_files']

# Check if the collection is not empty and delete its contents if it is not empty
if zip_collection.count_documents({}) > 0:
    print("Collection 'img_files' is not empty. Deleting all documents.")
    zip_collection.delete_many({})
    print("All documents in 'img_files' collection have been deleted.")

# Read the zip file and insert it into MongoDB
with open(zip_file_path, 'rb') as file_data:
    zip_binary = file_data.read()
    zip_document = {
        "filename": "img_files.zip",
        "filedata": zip_binary
    }
    zip_collection.insert_one(zip_document)

print("Zip file has been saved to MongoDB successfully.")

# Delete the zip file from local storage
try:
    os.remove(zip_file_path)
    print(f"Zip file '{zip_file_path}' has been deleted from local storage.")
except OSError as e:
    print(f"Error: {zip_file_path} : {e.strerror}")

# Close the MongoDB connection
client.close()
