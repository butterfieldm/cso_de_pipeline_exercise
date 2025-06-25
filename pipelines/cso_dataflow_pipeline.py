import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
import json
import jsonschema
from google.cloud import storage
import yaml
import datetime

def load_mapping(schema_bucket, mapping_path):
    storage_client = storage.Client()
    bucket = storage_client.bucket(schema_bucket)
    blob = bucket.blob(mapping_path)
    content = blob.download_as_text()

    if mapping_path.endswith('.yaml') or mapping_path.endswith('.yml'):
        mapping = yaml.safe_load(content)
    else:
        mapping = json.loads(content)
    return mapping

class ValidateAndFormat(beam.DoFn):
    def __init__(self, schema_bucket, schema_path, file_path):
        self.schema_bucket = schema_bucket
        self.schema_path = schema_path
        self.schema = None
        self.storage_client = None
        self.file_path = file_path
        self.header = None

    def bq_to_jsonschema(self, bq_schema_fields):
        type_map = {
            "STRING": "string",
            "FLOAT": "number",
            "INTEGER": "integer",
            "BOOLEAN": "boolean",
            "TIMESTAMP": "string",
            "DATE": "string"
        }
        return {
            "type": "object",
            "properties": {
                f["name"]: {"type": type_map.get(f["type"], "string")}
                for f in bq_schema_fields
            },
            "required": [f["name"] for f in bq_schema_fields if f["mode"] == "REQUIRED"]
        }

    def setup(self):
        self.storage_client = storage.Client()
        schema_blob = self.storage_client.bucket(self.schema_bucket).blob(self.schema_path)
        bq_schema = json.loads(schema_blob.download_as_text())
        self.schema = self.bq_to_jsonschema(bq_schema)

    def process(self, element):
        if self.header is None:
            self.header = element.split(',')
            return

        values = element.split(',')
        row = dict(zip(self.header, values))

        try:
            jsonschema.validate(instance=row, schema=self.schema)
            yield row
        except jsonschema.ValidationError as e:
            yield beam.pvalue.TaggedOutput(
                'errors',
                {
                    'table': self.schema_path.split('/')[-1].replace('_schema.json', ''),
                    'error': str(e),
                    'file_path': self.file_path,
                    'raw_row': str(row),
                    'ingestion_ts': datetime.datetime.utcnow().isoformat()
                }
            )


def run(pipeline_args, data_bucket, schema_bucket, changed_files, project):

    if not any(arg.startswith('--project') for arg in pipeline_args):
        pipeline_args.append(f'--project={project}')    
    
    options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=options)

    mapping = load_mapping(schema_bucket, 'config/mappings.yaml')
    error_hospital_table = f'{project}.cso_exercise_bq_error_hospital.error_log'
    for file_path in changed_files:
        # Find mapping entry
        entry = next((m for m in mapping['mappings'] if m['csv_path'] == file_path), None)
        if not entry:
            raise ValueError(f"No mapping found for file {file_path}")

        dataset = entry['dataset']
        table = entry['table']
        bq_table = f"{project}.{dataset}.{table}"
    
        filename = file_path.split('/')[-1].replace('.csv', '')
        schema_path = f"schemas/{filename}_schema.json"

        validated_and_errors = (
            p
            | f"Read_{filename}" >> ReadFromText(f"gs://{data_bucket}/{file_path}")
            | f"Validate_{filename}" >> beam.ParDo(ValidateAndFormat(schema_bucket, schema_path, file_path)).with_outputs('errors', main='valid_rows')
        )

        valid_rows = validated_and_errors.valid_rows
        errors = validated_and_errors.errors

        valid_rows | f"WriteToBQ_{filename}" >> WriteToBigQuery(
            bq_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

        errors | f"WriteErrorsToBQ_{filename}" >> WriteToBigQuery(
            error_hospital_table,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_bucket", required=True)
    parser.add_argument("--schema_bucket", required=True)
    parser.add_argument("--changed_files", required=True,
                    help="List of changed CSV file paths in the bucket, e.g. data/customers.csv;data/orders.csv")
    parser.add_argument("--project", required=True, help="GCP project ID")

    known_args, pipeline_args = parser.parse_known_args()

    # Parse comma-separated list string into a list
    changed_files = [f.strip() for f in known_args.changed_files.split(";") if f.strip()]
    run(pipeline_args, known_args.data_bucket, known_args.schema_bucket, changed_files, known_args.project)
