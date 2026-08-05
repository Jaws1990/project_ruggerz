# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************

import json
import os
import requests
from notebookutils import mssparkutils

class APIIngestor:
    def __init__(self, api_key):
        self.api_key = api_key

    def fetch_data(self, api_url, query_params=None):
        headers = {'x-apisports-key': f"{self.api_key}"}
        response = requests.get(api_url, headers=headers, params=query_params)
        print(f"Attempting extract of data from {api_url}.")
        if response.status_code == 200:
            print(json.dumps(response.json()))
            return response.json()
        else:
            response.raise_for_status()

    def save_json(self, data, output_path, filename):
        if not mssparkutils.fs.exists(output_path):
            mssparkutils.fs.mkdirs(output_path)
            print(f"📂 Created directory: {output_path}")
        else:
            print(f"📂 Directory already exists: {output_path}")

        # Create parent directories if they don't exist
        #print(f"Creating directory if not exists: {os.path.dirname(output_path)}.")
        #mssparkutils.fs.mkdirs(f"file://{os.path.dirname(output_path)}")

        print(f"Attempting saving of data to {output_path}.")
        #with open(output_path, "w", encoding="utf-8") as f:
            #json.dump(data, f, indent=2)

        mssparkutils.fs.put(
            f"{output_path}/{filename}",
            json.dumps(data, indent=2),
            overwrite=True
        )


    def download_json(self, api_url, output_path, filename, query_params=None):
        data = self.fetch_data(api_url, query_params=query_params)
        self.save_json(data, output_path, filename)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
