import functions_framework
import os
import time
import build
from google.oauth2 import service_account

@functions_framework.cloud_event
def trigger_dataflow(cloud_event):
    data = cloud_event.data

    # Only react to CSV files
    if not data.get("name", "").endswith(".csv"):
        print("Not a CSV file. Skipping.")
        return

    file_name = data['name']
    bucket_name = data['bucket']
    base_name = file_name.split('.')[0]
    schema_file_name = f"{base_name}_schema.json"

    if not file_name.startswith("data/"):
        print("Not in 'data/' folder, skipping.")
        return

    csv_gcs_path = f"gs://{bucket_name}/data/{file_name}"
    schema_gcs_path = f"gs://{bucket_name}/schemas/{schema_file_name}"

    job_name = f"cso-dataflow-job-{base_name.replace('_', '-')}-{int(time.time())}"

    # Get environment variables
    project = os.environ["GCP_PROJECT"]
    region = os.environ["REGION"]
    template_path = os.environ["TEMPLATE_GCS_PATH"]

    credentials = service_account.Credentials.from_service_account_file(
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]
    )

    service = build("dataflow", "v1b3", credentials=credentials)

    request_body = {
        "launchParameter": {
            "jobName": job_name,
            "containerSpecGcsPath": template_path,
            "parameters": {
                "input_file": csv_gcs_path,
                "schema_file": schema_gcs_path,
                "table_name": base_name
            }
        }
    }

    request = service.projects().locations().flexTemplates().launch(
        projectId=project,
        location=region,
        body=request_body
    )
    response = request.execute()

    print(f"Launched Dataflow job: {response}")

