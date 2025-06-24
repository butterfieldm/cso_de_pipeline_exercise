import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
from apache_beam import pvalue
import csv
import json
import re
import os
import logging
import argparse
from io import TextIOWrapper
from jsonschema import validate, ValidationError
from datetime import datetime, timezone

##############################
# Utility Functions & Mappers
##############################

def map_bq_type(bq_type):
    """Map BigQuery data types to JSON schema types."""
    return {
        "STRING": "string",
        "INTEGER": "number",
        "FLOAT": "number",
        "NUMERIC": "number",
        "BOOLEAN": "boolean",
        "DATE": "string",
        "TIMESTAMP": "string"
    }.get(bq_type.upper(), "string")


def load_json_schema(bucket, table_name):
    """
    Load BigQuery schema JSON from GCS and convert it to:
      (a) a JSON Schema for row validation, and 
      (b) a BigQuery schema string for writing.
    """
    schema_path = f'gs://{bucket}/schemas/{table_name}_schema.json'
    gcs = GcsIO()
    with gcs.open(schema_path, 'r') as f:
        schema_fields = json.load(f)

    json_schema = {
        "type": "object",
        "properties": {
            field["name"]: {"type": map_bq_type(field["type"])}
            for field in schema_fields
        },
        "required": [field["name"] for field in schema_fields if field.get("mode", "") == "REQUIRED"]
    }

    # Compose schema string in the form "field1:TYPE,field2:TYPE,..." 
    # (suitable for streaming inserts)
    bq_schema_str = ",".join(f"{field['name']}:{field['type']}" for field in schema_fields)

    return json_schema, bq_schema_str


def list_csv_files(bucket, prefix):
    """List CSV files in the GCS bucket with the given prefix. Returns full URIs."""
    gcs = GcsIO()
    return [f for f in gcs.list_prefix(f'gs://{bucket}/{prefix}') if f.endswith('.csv')]


##############################
# Beam DoFns
##############################

class ExtractFileMetadata(beam.DoFn):
    """Extract metadata (filename, table name, full GCS path) from a CSV file path."""
    def process(self, file_path):
        filename = os.path.basename(file_path)
        match = re.match(r"([a-zA-Z0-9_]+)\.csv", filename)
        if match:
            table_name = match.group(1).lower()
            yield {
                'filename': filename,
                'table_name': table_name,
                'gcs_path': file_path
            }


class ParseValidateRows(beam.DoFn):
    """
    Parse CSV rows from a file, validate each row against the corresponding JSON schema,
    and yield two tagged outputs: 'valid' and 'error'.
    Each valid row is enriched with:
        - the destination table name,
        - the BigQuery schema string (as 'schema_str'),
        - the source file path,
        - the CSV fields.
    """
    def __init__(self, schema_bucket):
        self.schema_bucket = schema_bucket
        self.schema_cache = {}

    def process(self, element):
        file_path = element['gcs_path']
        table_name = element['table_name']

        # Load and cache the schema (JSON schema and BQ schema string) for the table.
        if table_name not in self.schema_cache:
            try:
                json_schema, bq_schema_str = load_json_schema(self.schema_bucket, table_name)
                self.schema_cache[table_name] = (json_schema, bq_schema_str)
            except Exception as e:
                yield pvalue.TaggedOutput('error', {
                    'table_name': table_name,
                    'error': f"Schema load error: {str(e)}",
                    'row': None,
                    'file_path': file_path
                })
                return
        else:
            json_schema, bq_schema_str = self.schema_cache[table_name]

        try:
            with beam.io.filesystems.FileSystems.open(file_path) as f:
                reader = csv.DictReader(TextIOWrapper(f, encoding='utf-8', errors='replace'))

                expected_fields = set(json_schema["properties"].keys())
                actual_fields = set(reader.fieldnames)
                if expected_fields != actual_fields:
                    raise ValueError(
                        f"CSV header mismatch for {file_path}. Expected: {expected_fields}, Found: {actual_fields}"
                    )

                for i, row in enumerate(reader, start=1):
                    try:
                        # Validate the CSV row using jsonschema.
                        validate(instance=row, schema=json_schema)
                        # Construct the valid record.
                        valid_row = {
                            'table_name': table_name,
                            'schema_str': bq_schema_str,  # using schema string for streaming inserts
                            'source_file': file_path,
                            **row
                        }
                        yield pvalue.TaggedOutput('valid', valid_row)
                    except ValidationError as ve:
                        yield pvalue.TaggedOutput('error', {
                            'table_name': table_name,
                            'error': f"Validation error on row {i}: {ve.message}",
                            'row': row,
                            'file_path': file_path
                        })

        except Exception as e:
            yield pvalue.TaggedOutput('error', {
                'table_name': table_name,
                'error': f"File-level error: {str(e)}",
                'row': None,
                'file_path': file_path
            })


##############################
# Main Pipeline
##############################

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_bucket', required=True, help="GCS bucket containing CSV and schema files")
    parser.add_argument('--data_prefix', required=True, help="Prefix path for CSV files in the bucket")
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    project = options.get_all_options().get("project")
    if not project:
        raise ValueError("Missing required PipelineOption: --project")

    files = list_csv_files(args.data_bucket, args.data_prefix)
    logging.info(f"Found {len(files)} CSV files: {files}")

    # Configure pipeline options
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = False

    with beam.Pipeline(options=options) as p:
        file_list = p | 'Create file list' >> beam.Create(files)
        metadata = file_list | 'Extract metadata' >> beam.ParDo(ExtractFileMetadata())

        parsed = metadata | 'Parse and validate' >> beam.ParDo(ParseValidateRows(args.data_bucket)).with_outputs('valid', 'error')

        # Log outputs for debugging
        parsed.valid | 'Log valid rows' >> beam.Map(lambda x: logging.info(f"VALID OUTPUT: {x}"))
        parsed.error | 'Log error rows' >> beam.Map(lambda x: logging.info(f"ERROR OUTPUT: {x}"))

        valid = parsed.valid
        errors = parsed.error

        # Write valid rows to BigQuery using streaming inserts.
        # Streaming inserts allow the schema callable to receive each record element.
        valid | 'Write valid to BQ' >> WriteToBigQuery(
            table=lambda row: f"{project}:cso_exercise_bq_staging.{row['table_name']}",
            schema=lambda row: row['schema_str'],
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS
        )

        # Write error rows to BigQuery with an updated error schema.
        errors | 'Format errors' >> beam.Map(lambda r: {
            'table': r['table_name'],
            'error': r['error'],
            'file_path': r['file_path'],
            'raw_row': json.dumps(r['row']) if r['row'] else None,
            'ingestion_ts': datetime.now(timezone.utc).isoformat()
        }) | 'Write errors to BQ' >> WriteToBigQuery(
            table=f"{project}:cso_exercise_bq_error_hospital.error_log",
            schema={
                'fields': [
                    {"name": "table", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "error", "type": "STRING", "mode": "REQUIRED"},
                    {"name": "file_path", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "raw_row", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ingestion_ts", "type": "TIMESTAMP", "mode": "REQUIRED"}
                ]
            },
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS
        )


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
