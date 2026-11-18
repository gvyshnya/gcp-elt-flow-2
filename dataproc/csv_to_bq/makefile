
REGION="europe-west1"
ZONE="europe-west1-b"
TEMPLATE_ID="csv_to_bq"

dev_dataproc_assets_bucket="gs://xyz-dataproc-assets/csv_to_bq/"
gcp_project_id="lorelee-keep-312708"

upload_assets:
	gsutil cp main.py ${dev_dataproc_assets_bucket} --region ${REGION} --project ${gcp_project_id}

upload_template:
	gcloud dataproc workflow-templates import ${TEMPLATE_ID} --region ${REGION} --source workflow-tmpl.yaml