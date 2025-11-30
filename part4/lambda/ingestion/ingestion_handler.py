
import os
import json
import time
import hashlib
import logging
from urllib.parse import urljoin
import requests
import boto3
from bs4 import BeautifulSoup

#Tracking logging info to keep track of the work
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize S3 client also to check for IAM permissions here
s3_client = boto3.client('s3')

def sync_bls_to_s3(s3_bucket: str, s3_prefix: str, bls_url: str, from_email: str):

    headers = {
        'User-Agent': f"DataResearchBot/1.0 (Contact: {from_email})",
        'From': from_email,
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }
    
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
        return existing_files[norm_name] != hashlib.md5(content).hexdigest()

    logger.info("Checking existing files in S3...")
    existing_files = get_existing_files()
    logger.info(f"Found {len(existing_files)} existing files")
    
    logger.info(f"Fetching BLS directory: {bls_url}")
    try:
        session = requests.Session()
        session.headers.update(headers)
        response = session.get(bls_url, timeout=30)
        if response.status_code == 403:
            headers['User-Agent'] = 'Mozilla/5.0 (compatible; DataResearchBot/2.0)'
            response = session.get(bls_url, headers=headers, timeout=30)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Error fetching directory: {e}")
        return stats
    
    soup = BeautifulSoup(response.text, 'html.parser')
    files = []
    for link in soup.find_all('a'):
        href = link.get('href', '')
        if href and 'pr.' in href and href not in ['../', '/']:
            files.append({'name': href, 'url': urljoin(bls_url, href)})
            
    logger.info(f"Found {len(files)} files in directory")
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
                logger.info(f"[SKIPPED] {basename}")
                continue
            
            logger.info(f"Uploading {basename}...")
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=content)
            stats['uploaded'] += 1
            logger.info(f"[OK] Uploaded {basename}")
            
        except Exception as e:
            stats['failed'] += 1
            logger.error(f"[FAILED] {basename}: {e}")

    logger.info("Checking for deletions...")
    current_files = {os.path.basename(f['name']) for f in files}
    paginator = s3_client.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                norm_name = key.replace(s3_prefix, '').lstrip('/')
                
                if norm_name not in current_files:
                    try:
                        logger.info(f"[DELETED] {norm_name}")
                        s3_client.delete_object(Bucket=s3_bucket, Key=key)
                        stats['deleted'] += 1
                    except Exception as e:
                        logger.error(f"[FAILED DELETE] {norm_name}: {e}")

    logger.info(f"BLS sync complete: {stats}")
    return stats


def ingest_population_data(s3_bucket: str, api_url: str, from_email: str):

    s3_key = "population_data.json"
    
    logger.info(f"Fetching population data from: {api_url}")
    try:
        resp = requests.get(
            api_url, 
            headers={'User-Agent': f"DataBot ({from_email})"}, 
            timeout=30
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        error_msg = f"API Request failed: {e}"
        logger.error(error_msg)
        return {'status': 'ERROR', 'message': error_msg}

    # Serialize with sort_keys=True for consistent hashing
    json_bytes = json.dumps(data, sort_keys=True).encode('utf-8')
    new_hash = hashlib.md5(json_bytes).hexdigest()
    logger.info(f"Calculated MD5 hash: {new_hash}")


    try:
        existing = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
        existing_etag = existing['ETag'].strip('"')
        if existing_etag == new_hash:
            logger.info(f"Data identical: {s3_key}")
            return {'status': 'SKIPPED', 'message': 'No changes detected', 'hash': new_hash}
        logger.info(f"Data changed - new hash differs from existing")
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            logger.info(f"File does not exist in S3 - will upload new file")
        else:
            error_msg = f"Error checking S3: {e}"
            logger.error(error_msg)
            return {'status': 'ERROR', 'message': error_msg}

    # Upload to S3
    logger.info(f"Uploading to s3://{s3_bucket}/{s3_key}")
    try:
        s3_client.put_object(
            Bucket=s3_bucket,
            Key=s3_key,
            Body=json_bytes,
            ContentType='application/json'
        )
        logger.info("Successfully uploaded population data to S3")
        return {
            'status': 'UPDATED',
            'message': 'Population data uploaded successfully',
            'hash': new_hash,
            's3_key': s3_key
        }
    except Exception as e:
        error_msg = f"Failed to upload to S3: {e}"
        logger.error(error_msg)
        return {'status': 'ERROR', 'message': error_msg}


def lambda_handler(event, context):

    logger.info("Starting data ingestion pipeline")
    
    # Get configuration from environment variables to keep the whole thing in track
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    s3_prefix = os.environ.get('S3_BUCKET_PREFIX', '')
    bls_url = os.environ.get('BLS_BASE_URL')
    population_api_url = os.environ.get('POPULATION_API_URL')
    from_email = os.environ.get('CONTACT_EMAIL', 'data@example.com')
    
    if not s3_bucket:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET_NAME not configured'})
        }
    
    results = {
        'timestamp': context.aws_request_id if context else 'local',
        'bls_sync': None,
        'population_ingest': None,
        'success': False
    }
    
    # Execute BLS sync
    if bls_url:
        try:
            logger.info("Starting BLS sync...")
            results['bls_sync'] = sync_bls_to_s3(s3_bucket, s3_prefix, bls_url, from_email)
            logger.info(f"BLS sync completed: {results['bls_sync']}")
        except Exception as e:
            logger.error(f"BLS sync failed: {e}", exc_info=True)
            results['bls_sync'] = {'status': 'ERROR', 'error': str(e)}
    else:
        logger.warning("BLS_BASE_URL not configured - skipping BLS sync")
        results['bls_sync'] = {'status': 'SKIPPED', 'message': 'BLS_BASE_URL not configured'}
    
    # Execute Population ingestion
    if population_api_url:
        try:
            logger.info("Starting population data ingestion...")
            results['population_ingest'] = ingest_population_data(s3_bucket, population_api_url, from_email)
            logger.info(f"Population ingestion completed: {results['population_ingest']}")
        except Exception as e:
            logger.error(f"Population ingestion failed: {e}", exc_info=True)
            results['population_ingest'] = {'status': 'ERROR', 'error': str(e)}
    else:
        logger.warning("POPULATION_API_URL not configured - skipping population ingestion")
        results['population_ingest'] = {'status': 'SKIPPED', 'message': 'POPULATION_API_URL not configured'}
    
    # Determine overall success
    results['success'] = (
        results['population_ingest'] and 
        results['population_ingest'].get('status') in ['UPDATED', 'SKIPPED']
    )
    
    logger.info(f"Ingestion pipeline complete: {json.dumps(results, default=str)}")
    
    return {
        'statusCode': 200,
        'body': json.dumps(results, default=str)
    }

