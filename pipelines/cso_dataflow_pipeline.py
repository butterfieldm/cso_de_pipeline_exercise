import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import csv
import json
import re
import os
from jsonschema import validate, ValidationError

# === Load schema dynamically based on filename ===
def load_schema(file_type):
    schema_path = f'schemas/{file_type}_schema.json'
    with open(schema_path) as f:
        return json.load(f)

class ExtractFileInfo(beam.DoFn):
    def process(self, element):
        payload = json.loads(element.decode('utf-8'))
        gcs_path = payload['name']
        filename = os.path.basename(gcs_path)
        match = re.match(r"(.*)\.csv", filename)
        if match:
            yield {
                "filename": filename,
                "table_name": match.group(1),
                "gcs_path": f"gs://{payload['bucket']}/{gcs_path}"
            }

class ParseAndValidateCSV(beam.DoFn):
    def __init__(self, schemas_dir='schemas/'):
        self.schemas_dir = schemas_dir
        self.schemas_cache = {}

    def get_schema(self, table_name):
        if table_name not in self.schemas_cache:
            with open(f'{self.schemas_dir}/{table_name}_schema.json') as f:
                self.schemas_cache[table_name] = json.load(f)
        return self.schemas_cache[table_name]

    def process(self, element):
        file_path = element['gcs_path']
        table_name = element['table_name']

        schema = self.get_schema(table_name)
        with beam.io.filesystems.FileSystems.open(file_path) as f:
            for line in f:
                line = line.decode('utf-8')
                reader = csv.DictReader([line])
                row = next(reader)
                try:
                    # Type coercion
                    for key, val in row.items():
                        if schema["properties"].get(key, {}).get("type") == "number":
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

def run():
    pipeline_options = PipelineOptions()
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        messages = p | 'Read from Pub/Sub' >> ReadFromPubSub(subscription='projects/cso-deng-pipeline/subscriptions/gcs-upload-sub')

        file_metadata = messages | 'Extract file info' >> beam.ParDo(ExtractFileInfo())

        parsed = file_metadata | 'Parse and validate' >> beam.ParDo(ParseAndValidateCSV()).with_outputs('valid', 'error')

        valid = parsed.valid
        errors = parsed.error

        valid | 'Write valid to BQ' >> beam.Map(lambda r: r['row']) \
              | WriteToBigQuery(
                    table=lambda elem: f"cso-deng-pipeline.cso_exercise_bq_staging.{elem['table_name']}",
                    schema='SCHEMA_AUTODETECT',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

        errors | 'Write errors to BQ' >> beam.Map(lambda r: {
                    "table": r["table_name"],
                    "error": r["error"],
                    "raw_row": json.dumps(r["row"])
                }) | WriteToBigQuery(
                    table="cso-deng-pipeline.cso_exercise_bq_error_hospital.error_log",
                    schema='SCHEMA_AUTODETECT',
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                )

if __name__ == '__main__':
    run()
