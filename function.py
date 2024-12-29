from googleapiclient.discovery import build


def trigger_df_job(cloud_event,environment):   
 
    service = build('dataflow', 'v1b3')
    project = "prj-poc-001"

    template_path = "gs://dataflow-templates-us-central1/latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName": "bq-load",  # Provide a unique name for the job
        "parameters": {
        "javascriptTextTransformGcsPath": "gs://bkt-dataflow-metadata-icc/udf.js",
        "JSONPath": "gs://bkt-dataflow-metadata-icc/bq.json",
        "javascriptTextTransformFunctionName": "transform",
        "outputTable": "prj-poc-001:cricket_dataset.icc-ranking",
        "inputFilePattern": "gs://bkt-ranking-data/icc_ranking_to_GCP.csv",
        "bigQueryLoadingTemporaryDirectory": "gs://bkt-dataflow-metadata-icc",
        }
    }

    request = service.projects().templates().launch(projectId=project,gcsPath=template_path, body=template_body)
    response = request.execute()
    print(response)