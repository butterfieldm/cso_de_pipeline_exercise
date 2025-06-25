import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromText
import json
import jsonschema
from google.cloud import storage

class ValidateAndFormat(beam.DoFn):
    def __init__(self, schema_bucket, schema_path):
        self.schema_bucket = schema_bucket
        self.schema_path = schema_path
        self.schema = None
        self.storage_client = None

    def setup(self):
        self.storage_client = storage.Client()
        schema_blob = self.storage_client.bucket(self.schema_bucket).blob(self.schema_path)
        schema_data = schema_blob.download_as_text()
        self.schema = json.loads(schema_data)

    def process(self, element):
        # element is a CSV line (string)
        # parse CSV line, validate with JSON schema, output dict if valid

        import csv
        from io import StringIO

        reader = csv.DictReader(StringIO(element))
        for row in reader:
            try:
                jsonschema.validate(instance=row, schema=self.schema)
                yield row  # valid row dict
            except jsonschema.ValidationError:
                # skip invalid rows or log them somewhere else
                pass

def run(pipeline_args, data_bucket, schema_bucket, changed_files):
    options = PipelineOptions(pipeline_args)
    p = beam.Pipeline(options=options)

    for file_path in changed_files:
        # file_path example: 'data/customers.csv'
        filename = file_path.split('/')[-1].replace('.csv', '')
        schema_path = f"schemas/{filename}.json"
        bq_table = f"your-project.your_dataset.{filename}"

        rows = (
            p
            | f"Read_{filename}" >> ReadFromText(f"gs://{data_bucket}/{file_path}", skip_header_lines=1)
            | f"Validate_{filename}" >> beam.ParDo(ValidateAndFormat(schema_bucket, schema_path))
            # Add any transformation / cleaning here
            | f"WriteToBQ_{filename}" >> WriteToBigQuery(
                bq_table,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

    result = p.run()
    result.wait_until_finish()

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_bucket", required=True)
    parser.add_argument("--schema_bucket", required=True)
    parser.add_argument("--changed_files", required=True,
                    help="List of changed CSV file paths in the bucket, e.g. data/customers.csv,data/orders.csv")
    known_args, pipeline_args = parser.parse_known_args()

    # Parse comma-separated list string into a list
    changed_files = [f.strip() for f in known_args.changed_files.split(",") if f.strip()]
    run(pipeline_args, known_args.data_bucket, known_args.schema_bucket, changed_files)
