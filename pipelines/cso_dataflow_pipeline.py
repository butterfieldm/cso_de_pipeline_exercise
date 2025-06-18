import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.gcsio import GcsIO
import csv
import json
import re
import os
from jsonschema import validate, ValidationError

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

        # Manual mapping (e.g. plural → singular) if needed
        table_name_mapping = {
            'customers': 'customer',
            'transactions': 'transaction',
        }
        table_name = table_name_mapping.get(raw_table_name, raw_table_name)

        yield {
            "filename": filename,
            "table_name": table_name,
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
        schema = self.get_schema(table_name)

        with beam.io.filesystems.FileSystems.open(file_path) as f:
            lines = [line.decode('utf-8') for line in f.readlines()]
            reader = csv.DictReader(lines)
            for row in reader:
  
                try:
                    # Type coercion for numbers
                    for key, val in row.items():
                        expected_type = schema["properties"].get(key, {}).get("type")
                        if expected_type == "number":
                            row[key] = float(val) if val else None
                    validate(instance=row, schema=schema)
                    yield beam.pvalue.TaggedOutput('valid', {
                        "table_name": table_name,
                        "row": row
                    })
                except ValidationError as e:
                    yield beam.pvalue.TaggedOutput('error', {
                        "table_name": table_name,
                        "error": str(e),
                        "row": row
                    })

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
def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        messages = p | 'Read from Pub/Sub' >> ReadFromPubSub(
            subscription='projects/cso-deng-pipeline/subscriptions/gcs-upload-sub')

        file_metadata = messages | 'Extract file info' >> beam.ParDo(ExtractFileInfo())

        parsed = file_metadata | 'Parse and validate' >> beam.ParDo(ParseAndValidateCSV()).with_outputs('valid', 'error')

        valid = parsed.valid
        errors = parsed.error

        # === Write valid data to BigQuery staging tables ===
        valid | 'Load schema for valid rows' >> beam.ParDo(AddSchema()) \
              | 'Write valid rows to BigQuery' >> WriteToBigQuery(
                    table=lambda e: f"cso-deng-pipeline.cso_exercise_bq_staging.{e['table_name']}",
                    schema=lambda e: e['schema'],
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

        # === Write invalid data to error hospital table ===
        errors | 'Write errors to BQ' >> beam.Map(lambda r: {
                    "table": r["table_name"],
                    "error": r["error"],
                    "raw_row": json.dumps(r["row"])
                }) | WriteToBigQuery(
                    table="cso-deng-pipeline.cso_exercise_bq_error_hospital.error_log",
                    schema={
                        'fields': [
                            {"name": "table", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "error", "type": "STRING", "mode": "REQUIRED"},
                            {"name": "raw_row", "type": "STRING", "mode": "REQUIRED"}
                        ]
                    },
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

if __name__ == '__main__':
    run()
