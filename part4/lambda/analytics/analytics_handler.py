
import os
import json
import logging
import boto3
import pandas as pd

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')


def load_bls_master(s3_bucket: str, s3_prefix: str) -> pd.DataFrame:
    """
    Load and clean all BLS files from S3
    
    Args:
        s3_bucket: S3 bucket name
        s3_prefix: S3 prefix (e.g., "bls-data/")
    
    Returns:
        pd.DataFrame: Cleaned BLS master DataFrame
    """
    full_prefix = s3_prefix if s3_prefix.endswith('/') else f"{s3_prefix}/"
    
    logger.info(f"Listing BLS objects under prefix: '{full_prefix}' in bucket '{s3_bucket}'")
    paginator = s3_client.get_paginator('list_objects_v2')
    
    dfs = []
    for page in paginator.paginate(Bucket=s3_bucket, Prefix=full_prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            # Only keeping the BLS tracking and not checking with the population_data
            if key.endswith('/') or 'population_data.json' in key.lower():
                continue
            # Only include time-series data files (pr.data.*) since there is only puerto rico records for now
            if 'pr.data' in key:
                logger.info(f"[BLS] Loading {key}")
                body = s3_client.get_object(Bucket=s3_bucket, Key=key)['Body']
                df = pd.read_csv(body, sep=r'\s+', dtype=str, on_bad_lines='skip')
                df['source_key'] = key
                dfs.append(df)
    
    if not dfs:
        raise RuntimeError(f"No BLS files found under prefix {full_prefix}")
    
    df_bls_master = pd.concat(dfs, ignore_index=True)
    
    # Phase 2: Cleaning
    # 1) Trim column name whitespace
    df_bls_master.columns = df_bls_master.columns.str.strip()
    
    # 2) Trim whitespace in key text columns
    for col in ['series_id', 'period']:
        if col in df_bls_master.columns:
            df_bls_master[col] = df_bls_master[col].astype(str).str.strip()
    
    # 3) Enforce numeric "value"
    if 'value' in df_bls_master.columns:
        df_bls_master['value'] = pd.to_numeric(df_bls_master['value'], errors='coerce')
    
    # 4) Standardize year as integer
    if 'year' in df_bls_master.columns:
        df_bls_master['year'] = pd.to_numeric(df_bls_master['year'], errors='coerce').astype('Int64')
    
    logger.info(f"[BLS] Loaded {len(df_bls_master):,} rows from {len(dfs)} files")
    return df_bls_master


def load_population_df(s3_bucket: str) -> pd.DataFrame:
#The code here was confusing to deal with so checked with AI model to streamline it
    pop_key = "population_data.json"  
    logger.info(f"[POP] Loading {pop_key} from bucket {s3_bucket}")
    
    obj = s3_client.get_object(Bucket=s3_bucket, Key=pop_key)
    raw = obj['Body'].read()
    payload = json.loads(raw)
    
    records = payload.get('data', [])
    if not records:
        raise RuntimeError("Population JSON has no 'data' records")
    
    df_pop = pd.DataFrame(records)
    
    # Phase 3: Standardization
    # Rename to match BLS conventions
    rename_map = {}
    if 'Year' in df_pop.columns:
        rename_map['Year'] = 'year'
    if 'Population' in df_pop.columns:
        rename_map['Population'] = 'population'
    df_pop = df_pop.rename(columns=rename_map)
    
    # Enforce integer year and numeric population
    if 'year' in df_pop.columns:
        df_pop['year'] = pd.to_numeric(df_pop['year'], errors='coerce').astype('Int64')
    if 'population' in df_pop.columns:
        df_pop['population'] = pd.to_numeric(df_pop['population'], errors='coerce')
    
    logger.info(f"[POP] Loaded {len(df_pop):,} population records")
    return df_pop


def task_a_population_stats(df_pop: pd.DataFrame) -> dict:
   
    logger.info("Executing Task A: Population Statistics (2013-2018)")
    
    df_filtered = df_pop[
        (df_pop['year'] >= 2013) & 
        (df_pop['year'] <= 2018)
    ].copy()
    
    if len(df_filtered) == 0:
        return {'status': 'ERROR', 'message': 'No data found for years 2013-2018'}
    
    stats = df_filtered['population'].agg(['mean', 'std'])
    
    result = {
        'status': 'SUCCESS',
        'mean': float(stats['mean']),
        'std': float(stats['std']),
        'years': sorted(df_filtered['year'].unique().tolist()),
        'row_count': len(df_filtered)
    }
    
    logger.info(f"Task A complete: Mean={result['mean']:,.2f}, Std={result['std']:,.2f}")
    return result


def task_b_best_year_report(df_bls: pd.DataFrame) -> dict:
    
    logger.info("Executing Task B: Best Year Report")
    
    required_cols = ['series_id', 'year', 'value']
    missing_cols = [col for col in required_cols if col not in df_bls.columns]
    if missing_cols:
        return {'status': 'ERROR', 'message': f'Missing required columns: {missing_cols}'}
    
    df_annual = df_bls.groupby(['series_id', 'year'], as_index=False)['value'].sum()
    
    df_annual_sorted = df_annual.sort_values('value', ascending=False)
    df_best_years = df_annual_sorted.drop_duplicates(subset=['series_id'], keep='first')
    
    # Convert to list of dicts for JSON serialization
    results_list = df_best_years[['series_id', 'year', 'value']].to_dict('records')
    
    result = {
        'status': 'SUCCESS',
        'total_series': len(df_best_years),
        'results': results_list[:10],  # Top 10 for summary
        'total_results': len(results_list)
    }
    
    logger.info(f"Task B complete: {result['total_series']} series analyzed")
    return result


def task_c_unified_report(df_bls: pd.DataFrame, df_pop: pd.DataFrame, 
                          series_id: str = 'PRS30006032', period: str = 'Q01') -> dict:
    logger.info(f"Executing Task C: Unified Report (Series {series_id}, Period {period})")
    
    # Merge BLS and Population data on year
    df_merged = df_bls.merge(
        df_pop[['year', 'population']],
        on='year',
        how='left'
    )
    
    df_filtered = df_merged[
        (df_merged['series_id'] == series_id) & 
        (df_merged['period'] == period)
    ].copy()
    
    if len(df_filtered) == 0:
        return {
            'status': 'ERROR', 
            'message': f'No data found for series_id={series_id}, period={period}'
        }
    
    output_cols = ['series_id', 'year', 'period', 'value', 'population']
    available_cols = [col for col in output_cols if col in df_filtered.columns]
    
    if len(available_cols) < len(output_cols):
        return {
            'status': 'ERROR',
            'message': f'Missing columns: {set(output_cols) - set(available_cols)}'
        }
    
    df_result = df_filtered[output_cols].copy()
    
    results_list = df_result.to_dict('records')
    
    result = {
        'status': 'SUCCESS',
        'series_id': series_id,
        'period': period,
        'row_count': len(df_result),
        'results': results_list
    }
    
    logger.info(f"Task C complete: {result['row_count']} rows in unified report")
    return result


def lambda_handler(event, context):
    logger.info("Starting analytics pipeline")
    
    s3_bucket = os.environ.get('S3_BUCKET_NAME')
    s3_prefix = os.environ.get('S3_BUCKET_PREFIX', '')
    
    if not s3_bucket:
        return {
            'statusCode': 500,
            'body': json.dumps({'error': 'S3_BUCKET_NAME not configured'})
        }
    
    try:
        if 'Records' in event and len(event['Records']) > 0:
            record = event['Records'][0]
            if 'body' in record:
                s3_notification = json.loads(record['body'])
                if 'Records' in s3_notification and len(s3_notification['Records']) > 0:
                    s3_record = s3_notification['Records'][0]
                    bucket = s3_record.get('s3', {}).get('bucket', {}).get('name')
                    key = s3_record.get('s3', {}).get('object', {}).get('key')
                    logger.info(f"Processing S3 event: s3://{bucket}/{key}")
    except Exception as e:
        logger.warning(f"Could not parse SQS event: {e}. Proceeding with analytics anyway.")
    
    results = {
        'timestamp': context.aws_request_id if context else 'local',
        'task_a': None,
        'task_b': None,
        'task_c': None,
        'success': False
    }
    
    try:
        # Load data
        logger.info("Loading BLS and Population data from S3...")
        df_bls = load_bls_master(s3_bucket, s3_prefix)
        df_pop = load_population_df(s3_bucket)
        
        # Execute Task A
        try:
            results['task_a'] = task_a_population_stats(df_pop)
        except Exception as e:
            logger.error(f"Task A failed: {e}", exc_info=True)
            results['task_a'] = {'status': 'ERROR', 'error': str(e)}
        
        # Execute Task B
        try:
            results['task_b'] = task_b_best_year_report(df_bls)
        except Exception as e:
            logger.error(f"Task B failed: {e}", exc_info=True)
            results['task_b'] = {'status': 'ERROR', 'error': str(e)}
        
        # Execute Task C
        try:
            results['task_c'] = task_c_unified_report(df_bls, df_pop)
        except Exception as e:
            logger.error(f"Task C failed: {e}", exc_info=True)
            results['task_c'] = {'status': 'ERROR', 'error': str(e)}
        
        # Determine overall success this automates the whole thing
        results['success'] = all(
            task.get('status') == 'SUCCESS' 
            for task in [results['task_a'], results['task_b'], results['task_c']]
            if task
        )
        
    except Exception as e:
        logger.error(f"Analytics pipeline failed: {e}", exc_info=True)
        results['error'] = str(e)
        results['success'] = False
    
    logger.info(f"Analytics pipeline complete: {json.dumps(results, default=str)}")
    
    return {
        'statusCode': 200 if results['success'] else 500,
        'body': json.dumps(results, default=str)
    }

