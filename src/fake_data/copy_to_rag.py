import json

import pandas as pd

# load the records from the csv
csv = "/Users/sziegler/Documents/GitHub/ice-demo-data/src/fake_data/fake_immigrant_records_training.csv"
df = pd.read_csv(csv, encoding="windows-1252")

print(df.info())


path = "/Users/sziegler/Documents/GitHub/es-doc-search-loader-container/scripts/test_page_analytics/input_docs/ICE Demo/"
# loop through each record 
for index, row in df.iterrows():
    if index % 100 == 0:
        print(f"Processing record {index}")
    # write the record to a file
    with open(f"{path}ice_{row['Lead ID']}.txt", "w") as file:
        file.write(json.dumps(row.to_dict(), indent=3))

    with open(f"../rag_data/raw_text_files/ice_{row['Lead ID']}.txt", "w") as file:
        # convert record dict to a multiline string
        # with <field> : <value>
        lines = [f"{k} : {v}" for k,v in row.to_dict().items()]
        file.write("\n".join(lines))


        
        
