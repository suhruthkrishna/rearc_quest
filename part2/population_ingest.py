import os
import json
import hashlib
import requests
import boto3
from dotenv import load_dotenv

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(root_dir, '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, override=True)
else:
    load_dotenv(override=True)
#Again no hard coded variable names everything can be tweaked directly from the commonly shared .env file
def ingest_population_data():
    api_url = os.getenv("POPULATION_API_URL")
    s3_bucket = os.getenv("AWS_BUCKET_NAME")
    s3_prefix = os.getenv("AWS_BUCKET_PREFIX", "")
    from_email = os.getenv("CONTACT_EMAIL")
    region = os.getenv("AWS_REGION")

    if not api_url:
        return {'status': 'ERROR', 'message': 'Missing POPULATION_API_URL'}

    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region
    )

    print(f"Fetching: {api_url}")
    try:
        resp = requests.get(api_url, headers={'User-Agent': f"DataBot ({from_email})"}, timeout=30)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"API Request failed: {e}")
        return {'status': 'ERROR', 'message': str(e)}

    # Serialize with sort_keys=True to ensure consistent hashing and also to check for duplication, again going with the lighweight hashing logic to avoid duplicates
    json_bytes = json.dumps(data, sort_keys=True).encode('utf-8')
    new_hash = hashlib.md5(json_bytes).hexdigest()
    
    filename = "population_data.json"
    s3_key = f"{s3_prefix}{filename}" if s3_prefix else filename

    try:
        existing = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        if existing['ETag'].strip('"') == new_hash:
            print(f" Data identical: {filename}")
            return {'status': 'SKIPPED', 'hash': new_hash}
    except Exception:
        pass #It has checked all the possible situations that is if file exists or if the file is not present, since both are cleared we upload
#Again the print statements are refined or added taking assistance from claude to beautify the code.
    print(f"Uploading to s3://{s3_bucket}/{s3_key}")#Routing step to understand where the file is landing in the S3 bucket.
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json_bytes,
            ContentType='application/json'
        )
        print("Upload complete")
        return {'status': 'UPDATED', 'hash': new_hash}
    except Exception as e:
        print(f"Upload error: {e}")
        return {'status': 'ERROR', 'message': str(e)}

if __name__ == "__main__":
    ingest_population_data()