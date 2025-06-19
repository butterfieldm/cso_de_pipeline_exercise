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

# === Load BigQuery schema dynamically ===
def load_bq_schema_from_file(table_name):
    schema_path = f'schemas/{table_name}_schema.json'
    with open(schema_path) as f:
        schema_json = json.load(f)
    return {
        'fields': schema_json
    }

# === DoFn to extract metadata from Pub/Sub ===
class ExtractFileInfo(beam.DoFn):
    def process(self, element):
        payload = json.loads(element.decode('utf-8'))
        gcs_path = payload['name']
        filename = os.path.basename(gcs_path)

        # Only match CSV files with the expected pattern
        match = re.match(r"([a-zA-Z]+)\.csv", filename, re.IGNORECASE)
        if not match:
            # Skip files that do not match the expected naming pattern
            return

        raw_table_name = match.group(1).lower()

        yield {
            "filename": filename,
            "table_name": raw_table_name,
            "gcs_path": f"gs://{payload['bucket']}/{gcs_path}"
        }

# === Parse and validate CSVs ===
class ParseAndValidateCSV(beam.DoFn):
    def __init__(self, schemas_dir='schemas/'):
        self.schemas_dir = schemas_dir
        self.schemas_cache = {}

    def get_schema(self, table_name):
        if table_name not in self.schemas_cache:
            schema_filename = f'{table_name}_schema.json'
            gcs_path = f'gs://cso-exercise-ingestion-raw/schemas/{schema_filename}'

            gcs = GcsIO()
            with gcs.open(gcs_path, 'r') as f:
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
                            error_info = {
                                'table_name': table_name,
                                'error': f'Invalid row {i}: {row}',
                                'row': row,
                            }
                            yield pvalue.TaggedOutput('error', error_info)
                            continue

                        valid_row = {
                            **row,
                            'source_file': file_path,
                            'table_name': table_name
                        }
                        yield pvalue.TaggedOutput('valid', valid_row)

                    except Exception as row_e:
                        error_info = {
                            'table_name': table_name,
                            'error': f'Error processing row {i}: {row_e}',
                            'row': row
                        }
                        yield pvalue.TaggedOutput('error', error_info)

            except Exception as e:
                error_info = {
                    'table_name': table_name,
                    'error': f'Failed to read/process file: {e}',
                    'row': None
                }
                yield pvalue.TaggedOutput('error', error_info)


# === Attach schema for valid rows ===
class AddSchema(beam.DoFn):
    def process(self, element):
        row = element['row']
        table_name = element['table_name']
        schema = load_bq_schema_from_file(table_name)
        yield {
            'row': row,
            'table_name': table_name,
            'schema': schema
        }

# === Main pipeline run ===
def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_file', required=True, help='GCS path to the input CSV')
    parser.add_argument('--table_name', required=True, help='Target table name for BQ')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = False

    with beam.Pipeline(options=pipeline_options) as p:
        metadata = p | 'Create file metadata' >> beam.Create([{
            "filename": os.path.basename(known_args.input_file),
            "table_name": known_args.table_name,
            "gcs_path": known_args.input_file
        }])

        parsed = metadata | 'Parse and validate CSV' >> beam.ParDo(ParseAndValidateCSV()).with_outputs('valid', 'error')

        valid = parsed.valid
        errors = parsed.error

        valid | 'Load schema' >> beam.ParDo(AddSchema()) \
              | 'Write to BigQuery' >> WriteToBigQuery(
                    table=lambda e: f"cso-deng-pipeline.cso_exercise_bq_staging.{e['table_name']}",
                    schema=lambda e: e['schema'],
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

        errors | 'Write errors to BQ' >> beam.Map(lambda r: {
                    "table": r["table_name"],
                    "error": r["error"],
                    "raw_row": json.dumps(r["row"])
                }) | WriteToBigQuery(
                    table="cso-deng-pipeline.cso_exercise_bq_error_hospital.error_log",
                    schema={
                        'fields': [
                            {"name": "table", "type": "STRING"},
                            {"name": "error", "type": "STRING"},
                            {"name": "raw_row", "type": "STRING"}
                        ]
                    },
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )


if __name__ == '__main__':
    run()
