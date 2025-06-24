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

# === Load BigQuery schema from GCS ===
def load_bq_schema_from_gcs(bucket, table_name):
    schema_path = f'gs://{bucket}/schemas/{table_name}_schema.json'
    gcs = GcsIO()
    with gcs.open(schema_path, 'r') as f:
        schema_json = json.load(f)
    return {'fields': schema_json}

# === Get all matching CSVs from a GCS prefix ===
def list_csv_files(bucket, prefix):
    gcs = GcsIO()
    return [f for f in gcs.list_prefix(f'gs://{bucket}/{prefix}') if f.endswith('.csv')]

# === Extract metadata from GCS path ===
class ExtractFileMetadata(beam.DoFn):
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

# === Parse and validate CSV rows ===
class ParseAndValidateCSV(beam.DoFn):
    def __init__(self, schema_bucket):
        self.schema_bucket = schema_bucket
        self.schemas_cache = {}

    def get_schema(self, table_name):
        if table_name not in self.schemas_cache:
            schema_path = f'gs://{self.schema_bucket}/schemas/{table_name}_schema.json'
            gcs = GcsIO()
            with gcs.open(schema_path, 'r') as f:
                schema_json = json.load(f)

            json_schema = {
                "type": "object",
                "properties": {field["name"]: {"type": self.map_type(field["type"])} for field in schema_json},
                "required": [field["name"] for field in schema_json if field["mode"] == "REQUIRED"]
            }

            self.schemas_cache[table_name] = json_schema

        return self.schemas_cache[table_name]

    def map_type(self, bq_type):
        return {
            "STRING": "string",
            "INTEGER": "number",
            "FLOAT": "number",
            "NUMERIC": "number",
            "BOOLEAN": "boolean",
            "DATE": "string",
            "TIMESTAMP": "string"
        }.get(bq_type, "string")

    def process(self, element):
        file_path = element['gcs_path']
        table_name = element['table_name']

        try:
            with beam.io.filesystems.FileSystems.open(file_path) as f:
                raw_bytes = f.read()
            text = raw_bytes.decode('utf-8', errors='replace').replace('\x00', '')
            lines = text.splitlines()

            reader = csv.DictReader(lines)

            for i, row in enumerate(reader, start=1):
                try:
                    if not row or any(v is None for v in row.values()):
                        yield pvalue.TaggedOutput('error', {
                            'table_name': table_name,
                            'error': f'Invalid row {i}: {row}',
                            'row': row
                        })
                        continue

                    valid_row = {
                        **row,
                        'source_file': file_path,
                        'table_name': table_name
                    }
                    yield pvalue.TaggedOutput('valid', valid_row)

                except Exception as row_e:
                    yield pvalue.TaggedOutput('error', {
                        'table_name': table_name,
                        'error': f'Error processing row {i}: {row_e}',
                        'row': row
                    })

        except Exception as e:
            yield pvalue.TaggedOutput('error', {
                'table_name': table_name,
                'error': f'Failed to process file: {e}',
                'row': None
            })

# === Attach schema ===
class AttachSchema(beam.DoFn):
    def __init__(self, schema_bucket):
        self.schema_bucket = schema_bucket

    def process(self, element):
        schema = load_bq_schema_from_gcs(self.schema_bucket, element['table_name'])
        yield {
            **element,
            'schema': schema
        }

# === Main run ===
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--data_bucket', required=True, help='GCS bucket name')
    parser.add_argument('--data_prefix', required=True, help='Prefix path to CSVs, e.g. data/')
    parser.add_argument('--project', required=True)

    pipeline_options = PipelineOptions(argv)  # Use full argv for Beam options
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = False

    known_args, _ = parser.parse_known_args(argv)  # parse args after

    gcs = GcsIO()
    input_files = list_csv_files(known_args.data_bucket, known_args.data_prefix)
    logging.info(f"Found input files: {input_files}")

    with beam.Pipeline(options=pipeline_options) as p:
        files = p | 'Create file list' >> beam.Create(input_files)
        metadata = files | 'Extract metadata' >> beam.ParDo(ExtractFileMetadata())

        parsed = metadata | 'Parse & Validate' >> beam.ParDo(ParseAndValidateCSV(known_args.data_bucket)).with_outputs('valid', 'error')
        valid = parsed.valid
        errors = parsed.error

        valid | 'Attach schema' >> beam.ParDo(AttachSchema(known_args.data_bucket)) \
              | 'Write valid to BQ' >> WriteToBigQuery(
                    table=lambda e: f"{known_args.project}:cso_exercise_bq_staging.{e['table_name']}",
                    schema=lambda e: e['schema'],
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

        errors | 'Format error records' >> beam.Map(lambda r: {
                    'table': r['table_name'],
                    'error': r['error'],
                    'raw_row': json.dumps(r['row'])
                }) | 'Write errors to BQ' >> WriteToBigQuery(
                    table=f"{known_args.project}:cso_exercise_bq_error_hospital.error_log",
                    schema={
                        'fields': [
                            {'name': 'table', 'type': 'STRING'},
                            {'name': 'error', 'type': 'STRING'},
                            {'name': 'raw_row', 'type': 'STRING'}
                        ]
                    },
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )


if __name__ == '__main__':
    run()
