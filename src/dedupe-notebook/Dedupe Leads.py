# Databricks notebook source
!pip install dedupe




# COMMAND ----------

pip install unidecode 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM `hive_metastore`.`default`.`ice_data_2025_02_28_135500_2_csv`;
# MAGIC
# MAGIC

# COMMAND ----------

import pandas as pd
from unidecode import unidecode
import re 
import dedupe 
import os


# COMMAND ----------


# Step 1: Clean the fields for dedupe
df = spark.table("hive_metastore.default.ice_data_2025_02_28_135500_2_csv").toPandas()
df.head()

# Convert the DataFrame to a dictionary with row_id as the key
data_dict = df.set_index('row_id').to_dict(orient='index')


def preProcess(column):
    """
    Do a little bit of data cleaning with the help of Unidecode and Regex.
    Things like casing, extra spaces, quotes and new lines can be ignored.
    """
    if not column:
      return None
    column = unidecode(column)
    column = re.sub("  +", " ", column)
    column = re.sub("\n", " ", column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column


count = 0  
for k, v in data_dict.items():
    count +=1 
    preprocessed_columns = v.copy()
    print(f"{count}. {k}")
    for column in preprocessed_columns:
      print(f"\t{column}")
      preprocessed_columns[column] = preProcess(v[column])
    data_dict[k] = preprocessed_columns



import json
# Convert the dict into a DataFrame with the dict key as the row_id and value as a dict of column value pairs
df = pd.DataFrame.from_dict(data_dict, orient='index')


df.head(5)



# COMMAND ----------

# Step 2:  Define the fields dedupe will pay attention to
fields = [
    dedupe.variables.String("anumber", has_missing=True),
    dedupe.variables.String("fingerprint_hash", has_missing=True),
    dedupe.variables.String("first_name", has_missing=False),
    dedupe.variables.String("last_name", has_missing=False),
    dedupe.variables.String("country_of_origin", has_missing=False),
    dedupe.variables.String("last_known_address", has_missing=False),
    dedupe.variables.String("date_of_birth", has_missing=False),
    dedupe.variables.String("phone_number", has_missing=False),
    dedupe.variables.String("distinguishing_marks", has_missing=False),
]

# Create a new deduper object and pass our data model to it.
deduper = dedupe.Dedupe(fields)

# COMMAND ----------

output_file = "dedupe_output.csv"
settings_file = "dedupe_config_settings"
training_file = "dedupe_config_training.json"

data_d = data_dict


# If a settings file already exists, we'll just load that and skip training
if os.path.exists(settings_file):
  print("reading from", settings_file)
  with open(settings_file, "rb") as f:
      deduper = dedupe.StaticDedupe(f)
else:
  # # Training
  # If we have training data saved from a previous run of dedupe,
  # look for it and load it in.
  # __Note:__ if you want to train from scratch, delete the training_file
  if os.path.exists(training_file):
      print("reading labeled examples from ", training_file)
      with open(training_file, "rb") as f:
          deduper.prepare_training(data_d, f)
  else:
      deduper.prepare_training(data_d)

  # ## Active learning
  # Dedupe will find the next pair of records
  # it is least certain about and ask you to label them as duplicates
  # or not.
  # use 'y', 'n' and 'u' keys to flag duplicates
  # press 'f' when you are finished
  print("starting active labeling...")

  dedupe.console_label(deduper)

  # Using the examples we just labeled, train the deduper and learn
  # blocking predicates
  deduper.train()

  # When finished, save our training to disk
  with open(training_file, "w") as tf:
      deduper.write_training(tf)

  # Save our weights and predicates to disk.  If the settings file
  # exists, we will skip all the training and learning next time we run
  # this file.
  with open(settings_file, "wb") as sf:
      deduper.write_settings(sf)

# COMMAND ----------

# ## Clustering
print("clustering...")
clustered_dupes = deduper.partition(data_d, 0.5)

print("# duplicate sets", len(clustered_dupes))

# ## Writing Results

# Write our original data back out to a CSV with a new column called
# 'Cluster ID' which indicates which records refer to each other.

cluster_membership = {}
for cluster_id, (records, scores) in enumerate(clustered_dupes):
    for record_id, score in zip(records, scores):
        cluster_membership[record_id] = {
            "Cluster ID": cluster_id,
            "confidence_score": score,
        }

print(cluster_membership)
print(json.dumps(cluster_membership, indent=3))

df = df.join(pd.DataFrame.from_dict(cluster_membership, orient="index"))
df.head()




# COMMAND ----------

df = df[ ["Cluster ID", "confidence_score", "lead_id", "record_date", "anumber", "fingerprint_hash", "first_name", "last_name", "country_of_origin", "last_known_address", "date_of_birth", "phone_number", "distinguishing_marks"] ]

# show the cluster for lead_id in this list "0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"
sample = df[df["dedupe_id"].isin(["0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"])]

display(sample)



