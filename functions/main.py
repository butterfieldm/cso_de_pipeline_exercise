import functions_framework
import os
import time
from googleapiclient.discovery import build
from google.oauth2 import service_account

@functions_framework.cloud_event
def trigger_dataflow(cloud_event):
    data = cloud_event.data

    if not data.get("name", "").endswith(".csv"):
        print("Not a CSV file. Skipping.")
        return

    file_name = data['name']
    bucket_name = data['bucket']

    if not file_name.startswith("data/"):
        print("Not in 'data/' folder, skipping.")
        return

    job_name = f"cso-dataflow-job-{file_name.replace('/', '-').replace('_', '-')}-{int(time.time())}"

    # Environment variables
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
                "data_bucket": bucket_name,
                "data_prefix": "data/",   # Can make this dynamic if needed
                "project": project
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
