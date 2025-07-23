from flask import Flask, request, make_response
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account

from helper import download_json_from_gcs, transform_contracts_to_flat_table, load_to_bigquery

# variables

project_id = 'masterschool-gcp'
dataset_id = 'analytics_stage'
table_id = 'operations'
bucket_id = 'transform_etl_students'
uri = 'gs://transform_etl_students/operational_data_clean_12224.json'
file_path = '/'.join(uri.split('/')[3:])


# authentification
### for local debugging
credentials = service_account.Credentials.from_service_account_file(
     'masterschool-gc_to_bq.json', scopes=[
         "https://www.googleapis.com/auth/drive",
         "https://www.googleapis.com/auth/cloud-platform"],
 )

# ## for cloud
#    scopes = [
#         "https://www.googleapis.com/auth/drive",
#         "https://www.googleapis.com/auth/spreadsheets",
#         "https://www.googleapis.com/auth/cloud-platform"
#     ]
#     credentials, _ = google.auth.default(scopes=scopes)

#initialization
bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
storage_client = storage.Client(credentials=credentials)

test = download_json_from_gcs(bucket_id, file_path, storage_client)
test_transformed =  transform_contracts_to_flat_table(test)
load_to_bigquery(bigquery, bigquery_client, test_transformed, project_id, dataset_id, table_id)