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
from notebookutils import mssparkutils, credentials
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from delta.tables import DeltaTable
from uuid import uuid4

class APIIngestor:
    def __init__(self):
        self.var_library = notebookutils.variableLibrary.getLibrary("PR_variables")
        self.api_key = credentials.getSecret(
            self.var_library.getVariable("key_vault_url"),
            "api-sports-key"
        )
        self.base_api_url = self.var_library.getVariable("api_base_url")

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

        print(f"Attempting saving of data to {output_path}.")

        mssparkutils.fs.put(
            f"{output_path}/{filename}",
            json.dumps(data, indent=2),
            overwrite=True
        )


    def download_json(self, api_endpoint, output_path, filename, query_params=None):
        data = self.fetch_data(f"{self.base_api_url}/{api_endpoint}", query_params=query_params)
        self.save_json(data, output_path, filename)

    def write_to_bronze_table(
        self,
        df: DataFrame,
        table_name: str,
        mode: str = "overwrite",
        merge_condition: str | None = None
    ) -> None:
        """
        Enriches a DataFrame with ingestion metadata and writes it to a Bronze
        Delta table using overwrite, append, or merge mode.

        Args:
            df (DataFrame):
                Spark DataFrame containing the data to be written.

            table_name (str):
                Name of the Bronze Lakehouse table, excluding the schema.

            mode (str, optional):
                Write mode. Supported values are:
                    - "overwrite": Replaces the existing table.
                    - "append": Adds new records to the existing table.
                    - "merge": Upserts records into the existing table.

                Defaults to "overwrite".

            merge_condition (str, optional):
                Delta merge condition used when mode is "merge".
                For example:
                    "target.team_id = source.team_id"

                Required when mode is "merge".

        Raises:
            ValueError:
                If table_name is not provided, mode is invalid, or merge_condition
                is missing when using merge mode.
        """

        if not table_name:
            raise ValueError("table_name must be provided")

        valid_modes = {"overwrite", "append", "merge"}

        if mode not in valid_modes:
            raise ValueError(
                f"Invalid mode '{mode}'. "
                f"Supported modes are: {', '.join(valid_modes)}"
            )

        if mode == "merge" and not merge_condition:
            raise ValueError(
                "merge_condition must be provided when mode='merge'"
            )

        enriched = (
            df.withColumn("source_file",regexp_replace(input_file_name(),r".*?/Files/","Files/"))
            .withColumn("ingested_at", current_timestamp())
            .withColumn("batch_id", lit(str(uuid4())))
        )
        display(enriched.limit(5))

        table_path = f"bronze.{table_name}"

        if mode == "merge":
            delta_table = DeltaTable.forName(self.spark, table_path)
            (
                delta_table.alias("target")
                .merge(
                    enriched.alias("source"),
                    merge_condition
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            (
                enriched.write
                .format("delta")
                .mode(mode)
                .option(
                    "overwriteSchema",
                    "true" if mode == "overwrite" else "false"
                )
                .saveAsTable(table_path)
            )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
