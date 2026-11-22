# Databricks notebook source
# MAGIC %md
# MAGIC # ingest nhtsa recalls (auto loader)
# MAGIC
# MAGIC Real public data: every US vehicle recall since 2010, from NHTSA's bulk flat file. `scripts/land_nhtsa_recalls.sh`
# MAGIC drops each snapshot into the landing volume (standing in for fivetran - serverless can't reach the internet).
# MAGIC
# MAGIC Auto loader reads the folder incrementally: it checkpoints which files it has already processed, so each run
# MAGIC only loads files that arrived since the last one. Columns land exactly as NHTSA sends them (all text) plus the
# MAGIC source file and load time. Typing, cleaning and de-duplication happen in dbt staging, where they're tested.
# MAGIC
# MAGIC The file is a full snapshot, not a delta: every new file repeats every earlier recall. Bronze keeps every copy
# MAGIC (it's the raw record of what arrived); staging keeps the latest version of each record.

# COMMAND ----------

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

LANDING = "/Volumes/workspace/ev_bronze/landing/nhtsa/recalls"
CHECKPOINT = "/Volumes/workspace/ev_bronze/landing/_checkpoints/nhtsa_recalls"
TARGET = "workspace.ev_bronze.nhtsa_recalls"

# the 29 fields from NHTSA's layout file (RCL.txt), in order. no header row in the file
FIELDS = ["record_id", "campno", "maketxt", "modeltxt", "yeartxt", "mfgcampno", "compname", "mfgname", "bgman", "endman",
          "rcltypecd", "potaff", "odate", "influenced_by", "mfgtxt", "rcdate", "datea", "rpno", "fmvss", "desc_defect",
          "conequence_defect", "corrective_action", "notes", "rcl_cmpt_id", "mfr_comp_name", "mfr_comp_desc",
          "mfr_comp_ptno", "do_not_drive", "park_outside"]
schema = StructType([StructField(f, StringType()) for f in FIELDS])

# COMMAND ----------

stream = (spark.readStream.format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("sep", "\t")
          .option("header", "false")
          .option("quote", "\u0000")            # free text has stray quotes - tab is the only delimiter, so turn quoting off
          .option("encoding", "ISO-8859-1")
          .option("mode", "PERMISSIVE")
          .schema(schema)
          .load(LANDING)
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))

query = (stream.writeStream
         .option("checkpointLocation", CHECKPOINT)
         .trigger(availableNow=True)            # process whatever is new, then stop - batch semantics, streaming bookkeeping
         .toTable(TARGET))
query.awaitTermination()

# COMMAND ----------

by_file = (spark.table(TARGET).groupBy("_source_file").agg(F.count("*").alias("rows"))
           .orderBy("_source_file").collect())
result = {"rows_in_bronze": spark.table(TARGET).count(),
          "files_loaded": {r._source_file.split("/")[-1]: r.rows for r in by_file},
          "rows_this_run": query.lastProgress["numInputRows"] if query.lastProgress else 0}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
