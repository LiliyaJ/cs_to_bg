from flask import Flask, request, make_response
import os
import google.auth
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
import traceback
import json

from helper import download_json_from_gcs, transform_contracts_to_flat_table, load_to_bigquery

app = Flask(__name__)



try: 

    # authentification
    ### for local debugging
    # credentials = service_account.Credentials.from_service_account_file(
    #      'masterschool-gc_to_bq.json', scopes=[
    #          "https://www.googleapis.com/auth/drive",
    #          "https://www.googleapis.com/auth/cloud-platform"],
    #  )

    ## for cloud
    scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    credentials, _ = google.auth.default(scopes=scopes)

    # variables
    project_id = os.environ["project_id"]
    dataset_id = os.environ["dataset_id"]
    table_id = os.environ["table_id"]
    bucket_id = os.environ["bucket_id"]
    uri = os.environ["uri"]
    file_path = '/'.join(uri.split('/')[3:])

    #initialization
    bigquery_client = bigquery.Client(credentials=credentials, project=project_id)
    storage_client = storage.Client(credentials=credentials)

except Exception as e:
    print("Startup error:", e)
    traceback.print_exc()


@app.route("/", methods=["POST"])
def handle_request():
    try:
        json_file = download_json_from_gcs(bucket_id, file_path, storage_client)
        json_file_transformed =  transform_contracts_to_flat_table(json_file)
        load_to_bigquery(bigquery, bigquery_client, json_file_transformed, project_id, dataset_id, table_id)

        return make_response(json.dumps({"result": "Successfully uploaded the data"}), 200)
    
    except Exception as e:
        print("Error in request:", e)
        traceback.print_exc()
        return make_response(json.dumps({"error": str(e)}), 500)
    
# if __name__ == "__main__":
#     port = int(os.environ.get("PORT", 8080))
#     app.run(host="0.0.0.0", port=port)