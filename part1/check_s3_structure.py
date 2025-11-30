import boto3
import os
from dotenv import load_dotenv

root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(root_dir, '.env')
load_dotenv(dotenv_path=env_path, override=True)

s3 = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)

bucket = os.getenv('AWS_BUCKET_NAME')
prefix = os.getenv('AWS_BUCKET_PREFIX', '')

print("=" * 70)
print("S3 Bucket Structure Check")
print("=" * 70)
print(f"Bucket: {bucket}")
print(f"Configured Prefix: [{prefix}]")
print()

# Check files in bls-data/
print("Files in 'bls-data/' prefix:")
response = s3.list_objects_v2(Bucket=bucket, Prefix='bls-data/', MaxKeys=10)
if 'Contents' in response:
    for obj in response['Contents']:
        print(f"  {obj['Key']}")
else:
    print("  (no files found)")

print()

# Check files in old location
print("Files in 'pub/time.series/pr/' prefix:")
response = s3.list_objects_v2(Bucket=bucket, Prefix='pub/time.series/pr/', MaxKeys=10)
if 'Contents' in response:
    for obj in response['Contents']:
        print(f"  {obj['Key']}")
else:
    print("  (no files found)")

print("=" * 70)

