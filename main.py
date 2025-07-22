from flask import Flask, request, make_response
import os
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery

# variables

project_id = os.environ["project_id"]
print (project_id)
dataset_id = os.environ["dataset_id"]
table_id = os.environ["table_id"]


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


client = bigquery.Client(credentials=credentials, project=project_id)