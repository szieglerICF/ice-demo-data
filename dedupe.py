import csv
import os

import pandas as pd

import dedupe

# Sample CSV File Path (Update with your actual file path)
INPUT_CSV = "src/fake_data/official/fake_immigrate_records_testing.csv"
OUTPUT_CSV = "deduplicated_output.csv"
SETTINGS_FILE = "dedupe_settings"
TRAINING_FILE = "training.json"

# Step 1: Load Data
def read_data(file_path):
    data = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row_id, row in enumerate(reader):
            data[row_id] = row  # Dedupe requires a dictionary format
    return data

data = read_data(INPUT_CSV)

# Step 2: Define Fields for Deduplication
fields = [
    {'field': 'name', 'type': 'String'},
    {'field': 'address', 'type': 'String'},
    {'field': 'email', 'type': 'String'}
]

# Step 3: Set Up the Deduper
deduper = dedupe.Dedupe(fields)

if os.path.exists(SETTINGS_FILE):
    print("Loading existing settings...")
    with open(SETTINGS_FILE, 'rb') as sf:
        deduper.read_settings(sf)
else:
    print("Training new model...")
    deduper.prepare_training(data)
    
    # Interactive Labeling
    print("Starting active learning. Please label some examples.")
    dedupe.console_label(deduper)
    
    deduper.train()

    # Save settings and training data
    with open(SETTINGS_FILE, 'wb') as sf:
        deduper.write_settings(sf)

    with open(TRAINING_FILE, 'w') as tf:
        deduper.write_training(tf)

# Step 4: Find and Cluster Duplicates
threshold = deduper.threshold(data, recall_weight=1)
clusters = deduper.match(data, threshold)

# Step 5: Process and Save Results
clustered_data = []
id_to_cluster = {}

for cluster_id, (record_ids, confidence) in enumerate(clusters):
    for record_id in record_ids:
        id_to_cluster[record_id] = cluster_id

with open(INPUT_CSV, 'r', encoding='utf-8') as f:
    reader = csv.DictReader(f)
    fieldnames = reader.fieldnames + ['cluster_id']
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as out_f:
        writer = csv.DictWriter(out_f, fieldnames=fieldnames)
        writer.writeheader()
        for row_id, row in enumerate(reader):
            row['cluster_id'] = id_to_cluster.get(row_id, -1)
            writer.writerow(row)

print(f"Deduplication completed. Results saved to {OUTPUT_CSV}")
