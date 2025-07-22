import json
from datetime import datetime

def download_json_from_gcs(bucket_id, file_path, storage_client):
    """Download JSON file from Cloud Storage"""
    try:
        bucket = storage_client.bucket(bucket_id)
        blob = bucket.blob(file_path)
        
        json_content = blob.download_as_text()
        data = json.loads(json_content)
        
        print(f"Successfully downloaded {file_path}")
        return data
        
    except Exception as e:
        print(f"Error downloading file: {e}")
        return None
    

    

def transform_contracts_to_flat_table(raw_data):
    def to_iso(date_str):
        """Convert '5/7/24' to '2024-07-05', or return original if already ISO"""
        if not date_str:
            return None
        try:
            if '/' in date_str:
                m, d, y = date_str.split('/')
                y = f"20{y}" if len(y) == 2 else y
                return f"{y}-{m.zfill(2)}-{d.zfill(2)}"
            return date_str  # assume already ISO-like
        except:
            return None

    flat_data = []
    for c in raw_data.get("contracts", []):
        flat_data.append({
            "leasing_contract_id": c.get("leasing_contract_id"),
            "account_id": c.get("account_id"),

            # Bike info
            "bike_brand": c.get("bike_info", {}).get("brand"),
            "bike_type": c.get("bike_info", {}).get("type"),
            "bike_sale_price": c.get("bike_info", {}).get("sale_price"),
            "insurance_type": c.get("bike_info", {}).get("insurance_type"),

            # Contract details with parsed dates
            "request_date": to_iso(c.get("contract_details", {}).get("request_date")),
            "start_date": to_iso(c.get("contract_details", {}).get("start_date")),
            "end_date": to_iso(c.get("contract_details", {}).get("end_date")),
            "contract_state": c.get("contract_details", {}).get("state"),
            "contract_status": c.get("contract_details", {}).get("status"),

            # Financials
            "monthly_revenue_target": c.get("financial_summary", {}).get("monthly_revenue_target"),
            "total_revenue_generated": c.get("financial_summary", {}).get("total_revenue_generated"),
            "total_maintenance_costs": c.get("financial_summary", {}).get("total_maintenance_costs"),
            "net_profit": c.get("financial_summary", {}).get("net_profit"),
            "profit_margin_percent": c.get("financial_summary", {}).get("profit_margin_percent"),

            # Performance
            "performance_total_downtime_hours": c.get("performance_metrics", {}).get("total_downtime_hours"),
            "performance_maintenance_frequency_per_year": c.get("performance_metrics", {}).get("maintenance_frequency_per_year"),
            "performance_warranty_claims_count": c.get("performance_metrics", {}).get("warranty_claims_count"),
            "performance_parts_replacement_count": c.get("performance_metrics", {}).get("parts_replacement_count"),
            "performance_avg_customer_satisfaction": c.get("performance_metrics", {}).get("average_customer_satisfaction"),

            # Keep raw nested lists (optional for later processing)
            "maintenance_records": c.get("maintenance_records", []),
            "revenue_records": c.get("revenue_records", [])
        })

    return flat_data


def load_to_bigquery(bq, client, data, project_id, dataset_id, table_id):
    full_table_id = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bq.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    job = client.load_table_from_json(data, full_table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(data)} rows into {full_table_id}")

