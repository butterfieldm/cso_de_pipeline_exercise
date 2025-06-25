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

def map_bq_type(bq_type):
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

    bq_schema_str = ",".join(f"{field['name']}:{field['type']}" for field in schema_fields)
    return json_schema, bq_schema_str

class ExtractFileMetadata(beam.DoFn):
    def process(self, file_path):
        filename = os.path.basename(file_path)
        match = re.match(r"([a-zA-Z0-9_]+)\.csv", filename)
        if match:
            yield {
                'filename': filename,
                'table_name': match.group(1).lower(),
                'gcs_path': file_path
            }

class ParseValidateRows(beam.DoFn):
    def __init__(self, schema_bucket):
        self.schema_bucket = schema_bucket
        self.schema_cache = {}

    def process(self, element):
        file_path = element['gcs_path']
        table_name = element['table_name']

        try:
            if table_name not in self.schema_cache:
                json_schema, bq_schema_str = load_json_schema(self.schema_bucket, table_name)
                self.schema_cache[table_name] = (json_schema, bq_schema_str)
            else:
                json_schema, bq_schema_str = self.schema_cache[table_name]

            with beam.io.filesystems.FileSystems.open(file_path) as f:
                reader = csv.DictReader(TextIOWrapper(f, encoding='utf-8', errors='replace'))

                expected_fields = set(json_schema["properties"].keys())
                actual_fields = set(reader.fieldnames)
                if expected_fields != actual_fields:
                    raise ValueError(f"Header mismatch: {expected_fields} vs {actual_fields}")

                for i, row in enumerate(reader, start=1):
                    try:
                        validate(instance=row, schema=json_schema)
                        yield pvalue.TaggedOutput('valid', {
                            'table_name': table_name,
                            'schema_str': bq_schema_str,
                            'source_file': file_path,
                            **row
                        })
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

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_bucket', required=True, help="GCS bucket containing CSV and schema files")
    parser.add_argument('--data_prefix', required=False, help="Prefix path for CSV files in the bucket")
    parser.add_argument('--target_file', required=False, help="Specific CSV file to process (relative path)")
    args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)
    options.view_as(SetupOptions).save_main_session = True
    options.view_as(StandardOptions).streaming = False

    project = options.get_all_options().get("project")
    if not project:
        raise ValueError("Missing required PipelineOption: --project")

    # Determine which files to process
    if args.target_file:
        files = [f'gs://{args.data_bucket}/{args.target_file}']
    elif args.data_prefix:
        gcs = GcsIO()
        files = [f for f in gcs.list_prefix(f'gs://{args.data_bucket}/{args.data_prefix}') if f.endswith('.csv')]
    else:
        raise ValueError("Either --target_file or --data_prefix must be provided")

    logging.info(f"Files to process: {files}")

    with beam.Pipeline(options=options) as p:
        file_list = p | 'Create file list' >> beam.Create(files)
        metadata = file_list | 'Extract metadata' >> beam.ParDo(ExtractFileMetadata())
        parsed = metadata | 'Parse and validate' >> beam.ParDo(ParseValidateRows(args.data_bucket)).with_outputs('valid', 'error')

        parsed.valid | 'Write valid to BQ' >> WriteToBigQuery(
            table=lambda row: f"{project}:cso_exercise_bq_staging.{row['table_name']}",
            schema=lambda row: row['schema_str'],
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS
        )

        parsed.error | 'Format and write errors' >> beam.Map(lambda r: {
            'table': r['table_name'],
            'error': r['error'],
            'file_path': r['file_path'],
            'raw_row': json.dumps(r['row']) if r['row'] else None,
            'ingestion_ts': datetime.now(timezone.utc).isoformat()
        }) | WriteToBigQuery(
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
