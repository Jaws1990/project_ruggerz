# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from delta.tables import DeltaTable


class DeltaTableManager:

    def upsert(
        self,
        source_df: DataFrame,
        target_table: str,
        key_cols: list[str],
    ) -> None:
        """
        Upserts source data into a Delta table using the specified key columns.

        Existing records are updated and new records are inserted.
        Adds a processed_at timestamp to each source record before merging.

        Parameters:
            source_df (DataFrame): Source PySpark DataFrame.
            target_table (str): Target Delta table name.
            key_cols (List[str]): Columns used to identify matching records.
        """

        target_delta_tbl = DeltaTable.forName(
            spark,
            target_table,
        )

        # Add processing metadata
        source_df = source_df.withColumn(
            "processed_at",
            F.current_timestamp(),
        )

        # Join on all key columns
        merge_condition = " AND ".join(
            [
                f"target.{key} = source.{key}"
                for key in key_cols
            ]
        )

        (
            target_delta_tbl.alias("target")
            .merge(
                source=source_df.alias("source"),
                condition=merge_condition,
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    def merge_scd2(
        self,
        source_df: DataFrame,
        target_table: str,
        key_cols: list[str],
    ) -> None:
        """
        Performs an SCD Type 2 merge into a Delta table.

        Args:
            source_df (DataFrame):
                Spark DataFrame containing the already transformed data to be written.

            table_name (str):
                Name of the Lakehouse table, including the schema.

            key_cols (list[str]):
                list of primary key column names
        """

        target_delta_tbl = DeltaTable.forName(
            spark,
            target_table,
        )

        # Join incoming data with existing current records on key columns
        comparison_df = (
            source_df.alias("source")
            .join(
                target_delta_tbl
                .toDF()
                .alias("target")
                .filter(F.col("is_current") == 1),
                key_cols,
                "left",
            )
        )

        # Determine what we need to do for each incoming record
        comparison_df = comparison_df.withColumn(
            "action",
            F.when(
                F.col(f"target.{key_cols[0]}").isNull(),
                "new",
            )
            .when(
                F.col("source.row_hash") != F.col("target.row_hash"),
                "update",
            )
            .otherwise("no change"),
        )

        # Changed records need their existing versions expired
        updates_df = comparison_df.filter(
            F.col("action") == "update"
        )

        if not updates_df.isEmpty():
            # Get only the source colums, set up metadata columns
            updates_df = (
                updates_df
                .select(
                    *[F.col(f"source.{col}") for col in source_df.columns],
                )
                .withColumn("processed_at", F.current_timestamp())
                .drop("action")
            )

            # Join on all key columns
            merge_condition = " AND ".join(
                [
                    f"target.{key} = source.{key}"
                    for key in key_cols
                ]
            )

            (
                target_delta_tbl.alias("target")
                .merge(
                    source=updates_df.alias("source"),
                    condition=(
                        f"{merge_condition} "
                        f"AND target.is_current = 1"
                    ),
                )
                .whenMatchedUpdate(
                    set={
                        "is_current": F.lit(False),
                        "valid_to": F.current_date(),
                    },
                )
                .execute()
            )

        # New and changed records need to be inserted
        inserts_df = comparison_df.filter(
            F.col("action").isin(["new", "update"])
        )

        if not inserts_df.isEmpty():
            # Get only the source colums, set up SCD metadata columns
            inserts_df = (
                inserts_df
                .select(
                    *[F.col(f"source.{col}") for col in source_df.columns],
                )
                .withColumn("is_current", F.lit(True))
                .withColumn(
                    "valid_from",
                    F.when(F.col("action") == "new", F.lit("1900-01-01").cast("date"))
                    .otherwise(F.current_date())
                )
                .withColumn(
                    "valid_to",
                    F.lit(None).cast("date"),
                )
                .withColumn("processed_at", F.current_timestamp())
                .drop("action")
            )

            (
                inserts_df
                .write
                .format("delta")
                .mode("append")
                .saveAsTable(target_table)
            )

        


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
