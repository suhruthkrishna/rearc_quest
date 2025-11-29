import os
import time
import hashlib
from urllib.parse import urljoin
import requests
import boto3
from bs4 import BeautifulSoup
from dotenv import load_dotenv

#Since we wont be hardcoding anything here we will be taking all the requirements from .env
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(root_dir, '.env')
if os.path.exists(env_path):
    load_dotenv(dotenv_path=env_path, override=True)
else:
    load_dotenv(override=True)

def sync_bls_to_s3():
    bls_url = os.getenv("BLS_BASE_URL")
    s3_bucket = os.getenv("AWS_BUCKET_NAME")
    s3_prefix = os.getenv("AWS_BUCKET_PREFIX", "")
    from_email = os.getenv("CONTACT_EMAIL")
    region = os.getenv("AWS_REGION")
    
    #To avoid the 403 Error since government website applies ban on bots rerouted it to an actual mail address and then used the User-Agent logic
    headers = {
        'User-Agent': f"DataResearchBot/1.0 (Contact: {from_email})",
        'From': from_email,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
    s3_client = boto3.client(
        's3',
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=region
    )
 #Taking everything into measure here that is how we check for deduplication   
    stats = {'uploaded': 0, 'failed': 0, 'skipped': 0, 'deleted': 0}
    
    def get_existing_files():
        existing = {}
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    fname = obj['Key'].replace(s3_prefix, '').lstrip('/')
                    existing[fname] = obj['ETag'].strip('"')
        return existing

    def file_needs_upload(filename, content, existing_files):
        norm_name = filename.lstrip('/')
        if norm_name not in existing_files:
            return True
        return existing_files[norm_name] != hashlib.md5(content).hexdigest() #There are few ways of checking if the file exists the method I chose is the most lightweight one that is add hash values which would act as keys
#The print statements have been refined or added using Claude for better readability or better communication of code.
    print("Checking existing files in S3...")
    existing_files = get_existing_files()
    print(f"Found {len(existing_files)} existing files")
    
    print(f"Fetching BLS directory: {bls_url}")
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(bls_url, timeout=30)
        if response.status_code == 403:
            headers['User-Agent'] = 'Mozilla/5.0 (compatible; DataResearchBot/2.0)'
            response = session.get(bls_url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"Error fetching directory: {e}")
        return stats
    #Used BeautfulSoup as the webscraper since it is a very lightweight scraping task
    soup = BeautifulSoup(response.text, 'html.parser')
    files = []
    for link in soup.find_all('a'):
        href = link.get('href', '')
        if href and 'pr.' in href and href not in ['../', '/']:
            files.append({'name': href, 'url': urljoin(bls_url, href)})
            
    print(f"Found {len(files)} files in directory")
    time.sleep(1)
    
    for file_info in files:
        basename = os.path.basename(file_info['name'])
        s3_key = f"{s3_prefix}{basename}" if s3_prefix else basename
        
        try:
            time.sleep(0.5)
            resp = session.get(file_info['url'], timeout=60)
            resp.raise_for_status()
            content = resp.content
            
            if not file_needs_upload(basename, content, existing_files):
                stats['skipped'] += 1
                print(f"[SKIPPED FILE] {basename}")
                continue
            
            print(f"Uploading {basename}...")
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=content)
            stats['uploaded'] += 1
            print(f"[OK] Uploaded {basename}")
            
        except Exception as e:
            stats['failed'] += 1
            print(f"[FAILED FILE] {basename}: {e}")

    print("Checking for deletions...")
    current_files = {os.path.basename(f['name']) for f in files}
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                norm_name = key.replace(s3_prefix, '').lstrip('/')
                
                if norm_name not in current_files:
                    try:
                        print(f"[DELETED FILE] {norm_name}")
                        s3_client.delete_object(Bucket=s3_bucket, Key=key)
                        stats['deleted'] += 1
                    except Exception as e:
                        print(f"[FAILED TASK] Delete {norm_name}: {e}")

    print(f"Sync complete: {stats}")
    return stats

if __name__ == "__main__":
    sync_bls_to_s3()