# Databricks notebook source
!pip install dedupe




# COMMAND ----------

pip install unidecode 

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC ####################################################################
# MAGIC # ➡️ Show profile of raw, dirty data
# MAGIC ####################################################################
# MAGIC */
# MAGIC
# MAGIC
# MAGIC SELECT record_date, lead_source, lead_id, first_name, last_name, alias, date_of_birth, country_of_origin, anumber, fingerprint_hash,  last_known_address, phone_number, sex, height_inches, weight, risk_level, organized_crime_links, distinguishing_marks,  legal_proceedings, past_deportations, known_associates,  case_officer_assigned, deportation_orders 
# MAGIC FROM `hive_metastore`.`default`.`ice_data_2025_02_28_135500_2_csv`;
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

####################################################################
# ➡️ Show field selection for Python dedupe library
####################################################################


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

# show the cluster for lead_id in this list "0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"
sample = df[df["lead_id"].isin(["0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"])]

display(sample)

# COMMAND ----------

####################################################################
# ➡️ Show dedupe settings and training 
####################################################################

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
print(json.dumps(cluster_membership, indent=3, default=str))

df = df.join(pd.DataFrame.from_dict(cluster_membership, orient="index"))
df.head()




# COMMAND ----------

df = df[ [ "Cluster ID", "confidence_score", "lead_id", "record_date", "anumber", "fingerprint_hash", "first_name", "last_name", "country_of_origin", "last_known_address", "date_of_birth", "phone_number", "distinguishing_marks"] ]




# COMMAND ----------

# show the cluster for lead_id in this list "0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"
milo_df = df[df["lead_id"].isin(["0ffa8be931c44600", "0ffa8be931c44601", "0ffa8be931c44602"])].sort_values("record_date")

display(milo_df)

# Iterate over columns instead of rows
new_data = []
for col in milo_df.columns:
    new_row = [col] + [str(x) for x in milo_df[col].tolist()]
    new_data.append(new_row)

# Create new DataFrame with flipped rows and columns
new_columns = ['Original_Index'] + milo_df.index.tolist()
df_flipped = pd.DataFrame(new_data, columns=new_columns)

milo_html = df_flipped.to_html()



# COMMAND ----------



emoji_map = {
    "Cluster ID": "🆔",
    "confidence_score": "📊",
    "lead_id": "🔍",
    "record_date": "📅",
    "anumber": "🔢",
    "fingerprint_hash": "🆔",
    "first_name": "🧑",
    "last_name": "🧑",
    "country_of_origin": "🌍",
    "last_known_address": "🏠",
    "date_of_birth": "🎂",
    "phone_number": "📞",
    "distinguishing_marks": "🔎"
}

# update the html to find the text in the emoji map and prepend the emoji to that text
for key, value in emoji_map.items():
    milo_html = milo_html.replace(key, f"{value} {key}")

print(milo_html)



# COMMAND ----------

custom_milo_formatted = """
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: left;">
      <th></th>
      <th>Original_Index</th>
      <th>10555</th>
      <th>10556</th>
      <th>10557</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td style="text-align:left">📊 confidence_score</td>
      <td style="text-align:left">87</td>
      <td style="text-align:left">90</td>
      <td style="text-align:left">91</td>
    </tr>
    <tr>
      <th>1</th>
      <td style="text-align:left">🔍 lead_id</td>
      <td style="text-align:left">0ffa8be931c44600</td>
      <td style="text-align:left">0ffa8be931c44601</td>
      <td style="text-align:left">0ffa8be931c44602</td>
    </tr>
    <tr>
      <th>2</th>
      <td style="text-align:left">📅 record_date</td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">2023-10-01</span></td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">2024-06-12</span></td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">2025-01-03</span></td>
    </tr>
    <tr>
      <th>3</th>
      <td style="text-align:left">🔢 anumber</td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">None</span></td>
      <td style="text-align:left">a571306955</td>
      <td style="text-align:left">a571306955</td>
    </tr>
    <tr>
      <th>4</th>
      <td style="text-align:left">🆔 fingerprint_hash</td>
      <td style="text-align:left"><b style="color:red;">None</b></td>
      <td style="text-align:left">94k2j82730</td>
      <td style="text-align:left">94k2j82730</td>
    </tr>
    <tr>
      <th>5</th>
      <td style="text-align:left">🧑 first_name</td>
      <td style="text-align:left">emilio</td>
      <td style="text-align:left">emilio</td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">milo</span></td>
    </tr>
    <tr>
      <th>6</th>
      <td style="text-align:left">🧑 last_name</td>
      <td style="text-align:left">salamanca</td>
      <td style="text-align:left">sal<span style="color:red; font-weight:bold">o</span>manca</td>
      <td style="text-align:left">salamanca</td>
    </tr>
    <tr>
      <th>7</th>
      <td style="text-align:left">🌍 country_of_origin</td>
      <td style="text-align:left">mexico</td>
      <td style="text-align:left">mexico</td>
      <td style="text-align:left">mexico</td>
    </tr>
    <tr>
      <th>8</th>
      <td style="text-align:left">🏠 last_known_address</td>
      <td style="text-align:left">688 jensen circle suite 512, los angeles, ca 20926</td>
      <td style="text-align:left">688 jensen circle suite 512, los angeles, ca 20926</td>
      <td style="text-align:left"><span style="color:red; font-weight:bold">78654 chavez passage, los angeles, ca 85700-6041</span></td>
    </tr>
    <tr>
      <th>9</th>
      <td style="text-align:left">🎂 date_of_birth</td>
      <td style="text-align:left">1997-08-22</td>
      <td style="text-align:left">1997-08-22</td>
      <td style="text-align:left">1997-08-22</td>
    </tr>
    <tr>
      <th>10</th>
      <td style="text-align:left">📞 phone_number</td>
      <td style="text-align:left">698-686-8675</td>
      <td style="text-align:left">698-686-86<span style="color:red; font-weight:bold">57</span></td>
      <td style="text-align:left">698-686-8675</td>
    </tr>
    <tr>
      <th>11</th>
      <td style="text-align:left">🔎 distinguishing_marks</td>
      <td style="text-align:left">skull tattoo on calf</td>
      <td style="text-align:left">skull tattoo on calf</td>
      <td style="text-align:left">skull tattoo on calf</td>
    </tr>
  </tbody>
</table>
"""






# COMMAND ----------

####################################################################
# ➡️ Show how Emilio entities are resolved
####################################################################

displayHTML(custom_milo_formatted)

# COMMAND ----------

from pyspark.sql.functions import col

# find clusters with at least 3 records
df = df.groupby("Cluster ID").filter(lambda x: len(x) > 2)

# find a cluster with confidence scores > 85%
df = df[df["confidence_score"] > 0.85]

# show cluster 118 sorted by record date ascending
df_118 = df[df["Cluster ID"] == 118].sort_values("record_date")
display(df_118)

# move the record date as teh first column
df_118 = df_118[["record_date"] + [c for c in df_118.columns if c != "record_date"]]

# COMMAND ----------


# Assuming df_118 is already defined
new_data = []

# convert the confidence score into a string as a %
df_118["confidence_score"] = round( df_118["confidence_score"] * 100)

# remove the decimal places
df_118["confidence_score"] = df_118["confidence_score"].astype(str)




# Iterate over columns instead of rows
for col in df_118.columns:
    new_row = [col] + [str(x) for x in df_118[col].tolist()]
    new_data.append(new_row)

# Create new DataFrame with flipped rows and columns
new_columns = ['Original_Index'] + df_118.index.tolist()
df_flipped = pd.DataFrame(new_data, columns=new_columns)






# COMMAND ----------

html = df_flipped.to_html()

emoji_map = {
    "Cluster ID": "🆔",
    "confidence_score": "📊",
    "lead_id": "🔍",
    "record_date": "📅",
    "anumber": "🔢",
    "fingerprint_hash": "🆔",
    "first_name": "🧑",
    "last_name": "🧑",
    "country_of_origin": "🌍",
    "last_known_address": "🏠",
    "date_of_birth": "🎂",
    "phone_number": "📞",
    "distinguishing_marks": "🔎"
}

# update the html to find the text in the emoji map and prepend the emoji to that text
for key, value in emoji_map.items():
    html = html.replace(key, f"{value} {key}")




# for the anumber row of data, color the None values bold red
html = html.replace("""    <tr>
      <th>4</th>
      <td>🔢 anumber</td>
      <td>a465482608</td>
      <td>None</td>
      <td>None</td>
      <td>a465482608</td>
    </tr>""", """    <tr>
      <th>4</th>
      <td>🔢 anumber</td>
      <td>a465482608</td>
      <td><b style="color:red;">None</b></td>
      <td><b style="color:red;">None</b></td>
      <td>a465482608</td>
    </tr>""")

# add align center for each td
html = html.replace("<td>", "<td style='text-align:left'>")

html = html.replace("""<tr>
      <th>9</th>
      <td style='text-align:left'>🏠 last_known_address</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, xhouston, tx 33954</td>
      <td style='text-align:left'>8348 brian spur suite 656, hayesberg, ohio 94499</td>
    </tr>""", """<tr>
      <th>9</th>
      <td style='text-align:left'>🏠 last_known_address</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, <span style='color: red; font-weight:bold;'>x</span>houston, tx 33954</td>
      <td style='text-align:left; color: red; font-weight:bold'>8348 brian spur suite 656, hayesberg, ohio 94499</td>
    </tr>""")

html = html.replace("""<tr>
      <th>10</th>
      <td style='text-align:left'>🎂 date_of_birth</td>
      <td style='text-align:leftleft'>1997-10-14</td>
      <td style='text-align:left'>1997-10-14</td>
      <td style='text-align:left'>None</td>
      <td style='text-align:left'>1997-10-14</td>
    </tr>""", """<tr>
      <th>10</th>
      <td style='text-align:left'>🎂 date_of_birth</td>
      <td style='text-align:left'>1997-10-14</td>
      <td style='text-align:left'>1997-10-14</td>
      <td style='text-align:left; color: red; font-weight: bold'>None</td>
      <td style='text-align:left'>1997-10-14</td>
    </tr>""")


custom_html_118 = """
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Original_Index</th>
      <th>2</th>
      <th>5</th>
      <th>4</th>
      <th>3</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td style='text-align:left'>📅 record_date</td>
      <td style='text-align:left'>2022-05-17</td>
      <td style='text-align:left'>2023-02-03</td>
      <td style='text-align:left'>2023-04-16</td>
      <td style='text-align:left'>2023-05-15</td>
    </tr>
    <tr>
      <th>1</th>
      <td style='text-align:left'>🆔 Cluster ID</td>
      <td style='text-align:left'>118</td>
      <td style='text-align:left'>118</td>
      <td style='text-align:left'>118</td>
      <td style='text-align:left'>118</td>
    </tr>
    <tr>
      <th>2</th>
      <td style='text-align:left'>📊 confidence_score</td>
      <td style='text-align:left'>92.0</td>
      <td style='text-align:left'>92.0</td>
      <td style='text-align:left'>93.0</td>
      <td style='text-align:left'>91.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td style='text-align:left'>🔍 lead_id</td>
      <td style='text-align:left'>1d34d1b9a50941a1</td>
      <td style='text-align:left'>e239ae4565a84bb1</td>
      <td style='text-align:left'>29ed03321e454f58</td>
      <td style='text-align:left'>21cc2bb618b34815</td>
    </tr>
    <tr>
      <th>4</th>
      <td style='text-align:left'>🔢 anumber</td>
      <td style='text-align:left'><b style="color:red;">None</b></td>
      <td style='text-align:left'><b style="color:red;">None</b></td>
      <td style='text-align:left'>a465482608</td>
      <td style='text-align:left'>a465482608</td>
    </tr>
    <tr>
      <th>5</th>
      <td style='text-align:left'>🆔 fingerprint_hash</td>
      <td style='text-align:left'><b style="color:red;">None</b></td>
      <td style='text-align:left'>9d5d1dea40</td>
      <td style='text-align:left'>9d5d1dea40</td>
      <td style='text-align:left'>9d5d1dea40</td>
    </tr>
    <tr>
      <th>6</th>
      <td style='text-align:left'>🧑 first_name</td>
      <td style='text-align:left'>teodoro</td>
      <td style='text-align:left'>teodoro</td>
      <td style='text-align:left'>teodoro</td>
      <td style='text-align:left'>teodoro</td>
    </tr>
    <tr>
      <th>7</th>
      <td style='text-align:left'>🧑 last_name</td>
      <td style='text-align:left'>ulibarri</td>
      <td style='text-align:left'>ulibarri</td>
      <td style='text-align:left'>ulibarri</td>
      <td style='text-align:left'>ulibarri</td>
    </tr>
    <tr>
      <th>8</th>
      <td style='text-align:left'>🌍 country_of_origin</td>
      <td style='text-align:left'>mexico</td>
      <td style='text-align:left'>mexico</td>
      <td style='text-align:left'>mexico</td>
      <td style='text-align:left'>mexico</td>
    </tr>
    <tr>
      <th>9</th>
      <td style='text-align:left'>🏠 last_known_address</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, houston, tx 33954</td>
      <td style='text-align:left'>0789 ryan ferry suite 792, <span style='color: red; font-weight:bold;'>x</span>houston, tx 33954</td>
      <td style='text-align:left; color: red; font-weight:bold'>8348 brian spur suite 656, hayesberg, ohio 94499</td>
    </tr>
    <tr>
      <th>10</th>
      <td style='text-align:left'>🎂 date_of_birth</td>
      <td style='text-align:left'><b style="color:red;">None</b></td>
      <td style='text-align:left'>1997-10-14</td>
      <td style='text-align:left'><span style="color: red; font-weight:bold;">None</span></td>
      <td style='text-align:left'>1997-10-14</td>
    </tr>
    <tr>
      <th>11</th>
      <td style='text-align:left'>📞 phone_number</td>
      <td style='text-align:left'>(775)064-1321</td>
      <td style='text-align:left'>(775)064-13<span style="color:red; font-weight:bold;">12</style></td>
      <td style='text-align:left'>(775)064-1321</td>
      <td style='text-align:left'>(775)064-1321</td>
    </tr>
    <tr>
      <th>12</th>
      <td style='text-align:left'>🔎 distinguishing_marks</td>
      <td style='text-align:left'>None</td>
      <td style='text-align:left'>None</td>
      <td style='text-align:left'>None</td>
      <td style='text-align:left'>None</td>
    </tr>
  </tbody>
</table>
"""




# COMMAND ----------

####################################################################
# ➡️ Show how Teodoro entities are resolved
####################################################################

displayHTML(custom_html_118)
