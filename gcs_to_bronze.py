"""Parse interleaved BankEvalDB files and append new rows to BigQuery Bronze.

File format: HDR|ENTITY, column header, pipe-delimited rows, FTR|RECORDCOUNT|N.
The entity/table/column mapping is read from Bronze metadata_entity_config.
"""
import hashlib
import json
import re
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

PROJECT_ID = "project-437b3d91-7f41-4318-ad5"
BUCKET = "hackathon-hcl"
BRONZE_DATASET = "bronze_dataset"
DAYS = ("day0", "day1", "day2")
CONFIG_TABLE = f"{PROJECT_ID}.{BRONZE_DATASET}.metadata_entity_config"


def normalize(name):
    """Make header matching case- and punctuation-insensitive."""
    return re.sub(r"[^a-z0-9]", "", name.strip().lower())


def load_entity_config(spark):
    """Read entity-to-table and column mappings created by the DAG's metadata step."""
    rows = spark.read.format("bigquery").load(CONFIG_TABLE).collect()
    config = {}

    for row in rows:
        entity = (row.entity_name or "").strip().upper()
        table = (row.target_table or "").strip()
        columns = list(row.business_columns or [])

        if not entity or not table or not columns:
            raise ValueError(f"Incomplete metadata row for entity {entity!r}")
        if entity in config:
            raise ValueError(f"Duplicate metadata for entity {entity}")

        config[entity] = {"table": table, "columns": columns}

    if not config:
        raise RuntimeError(f"No entity metadata found in {CONFIG_TABLE}")
    return config


def parse_file(path, content, batch, config, parsed):
    """Parse each entity block and check its declared footer record count."""
    entity = None
    headers = None
    pending = ""
    entity_count = 0
    source_record_number = 0
    footer_count = 0

    for line_number, raw_line in enumerate(content.lstrip("\ufeff").splitlines(), 1):
        line = raw_line.rstrip("\r")

        # Header/footer lines are control records between data rows.
        if not pending:
            if not line.strip():
                continue
            if line.upper().startswith("HDR|"):
                entity = line.split("|", 1)[1].strip().upper()
                if entity not in config:
                    raise ValueError(f"Unknown entity {entity!r} at {path}:{line_number}")
                headers = None
                entity_count = 0
                continue
            if line.upper().startswith("FTR|"):
                parts = line.split("|")
                if len(parts) >= 3 and parts[1].strip().upper() == "RECORDCOUNT":
                    try:
                        declared_count = int(parts[2].strip())
                    except ValueError as exc:
                        raise ValueError(f"Invalid record count at {path}:{line_number}") from exc
                    if declared_count != entity_count:
                        raise ValueError(
                            f"{path}: {entity} footer says {declared_count}, parsed {entity_count}"
                        )
                    footer_count += 1
                entity, headers = None, None
                continue
            if entity is None:
                continue
            if headers is None:
                headers = [value.strip() for value in line.split("|")]
                normalized = [normalize(value) for value in headers]
                if len(normalized) != len(set(normalized)):
                    raise ValueError(f"Duplicate column headers at {path}:{line_number}")
                missing = [column for column in config[entity]["columns"]
                           if normalize(column) not in normalized]
                if missing:
                    raise ValueError(f"{entity} is missing configured columns: {missing}")
                continue
            pending = line
        else:
            # Continue a record that spans multiple physical lines.
            pending += "\n" + line

        values = pending.split("|")
        if len(values) < len(headers):
            continue
        if len(values) > len(headers):
            raise ValueError(f"Too many pipe fields at {path}:{line_number} for {entity}")

        source_row = {normalize(name): value for name, value in zip(headers, values)}
        business_values = [source_row.get(normalize(column))
                           for column in config[entity]["columns"]]
        record_hash = hashlib.sha256(
            json.dumps(business_values, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()

        source_record_number += 1
        entity_count += 1
        parsed[entity].append(tuple(business_values) + (
            path,
            batch,
            source_record_number,
            datetime.now(timezone.utc).replace(tzinfo=None),
            record_hash,
        ))
        pending = ""

    if pending:
        raise ValueError(f"Incomplete final record in {path}, entity={entity}")
    return footer_count


def bronze_schema(business_columns):
    """Bronze business columns stay STRING; lineage columns have fixed types."""
    fields = [StructField(column, StringType(), True) for column in business_columns]
    fields += [
        StructField("_source_file", StringType(), True),
        StructField("_batch_id", StringType(), True),
        StructField("_source_record_number", LongType(), True),
        StructField("_ingested_at", TimestampType(), True),
        StructField("_record_hash", StringType(), True),
    ]
    return StructType(fields)


def main():
    spark = SparkSession.builder.appName("CustomerTransactionBronzeLoad").getOrCreate()
    try:
        config = load_entity_config(spark)
        print(f"Loaded metadata for {len(config)} entities from {CONFIG_TABLE}")
        parsed = {entity: [] for entity in config}
        files_seen = 0
        footers_checked = 0

        # Read each day's GCS folder. wholeTextFiles keeps each interleaved file together.
        for day in DAYS:
            input_path = f"gs://{BUCKET}/landing/{day}/*"
            files = spark.sparkContext.wholeTextFiles(input_path).collect()
            print(f"{day}: found {len(files)} file(s)")
            files_seen += len(files)

            for path, content in files:
                print(f"Parsing {path}")
                footers_checked += parse_file(path, content, day, config, parsed)

        if files_seen == 0:
            raise RuntimeError(f"No files found in gs://{BUCKET}/landing/day0, day1, or day2")

        # Load only records whose source identity is not already in the Bronze table.
        identity_columns = ["_batch_id", "_source_file", "_source_record_number", "_record_hash"]
        for entity, settings in config.items():
            rows = parsed[entity]
            if not rows:
                print(f"{entity}: no rows in these files")
                continue

            incoming = spark.createDataFrame(rows, bronze_schema(settings["columns"]))
            target = f"{PROJECT_ID}.{BRONZE_DATASET}.{settings['table']}"
            existing = (spark.read.format("bigquery").load(target)
                        .select(*identity_columns).dropDuplicates())
            fresh = incoming.join(existing, identity_columns, "left_anti")
            new_count = fresh.count()

            if new_count:
                (fresh.write.format("bigquery")
                 .option("writeMethod", "direct")
                 .mode("append")
                 .save(target))
            print(f"{entity}: added {new_count} new row(s) to {target}")

        print(f"Done. Files read: {files_seen}; footer counts checked: {footers_checked}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()

