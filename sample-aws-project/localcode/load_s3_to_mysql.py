import os
import pandas as pd
import pymysql
import boto3
from dotenv import load_dotenv

# Load credentials
load_dotenv()

# S3 and DB credentials
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')
S3_KEY = os.getenv('S3_KEY')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
MYSQL_TABLE = os.getenv('MYSQL_TABLE')

# Download CSV from S3
s3 = boto3.client('s3',
                  aws_access_key_id=AWS_ACCESS_KEY_ID,
                  aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                  region_name=AWS_REGION)

tmp_csv = 'downloaded.csv'
s3.download_file(S3_BUCKET, S3_KEY, tmp_csv)
print(f"✅ Downloaded {S3_KEY} from S3.")

# Read CSV
df = pd.read_csv(tmp_csv)
print(f"📄 CSV loaded with {len(df)} records.")

# Connect to MySQL
conn = pymysql.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    db=MYSQL_DB,
    port=MYSQL_PORT,
    charset='utf8mb4',
    cursorclass=pymysql.cursors.DictCursor
)

# Insert data
with conn.cursor() as cursor:
    for _, row in df.iterrows():
        sql = f"INSERT INTO {MYSQL_TABLE} (id, name, department) VALUES (%s, %s, %s)"
        cursor.execute(sql, (int(row['id']), row['name'], row['department']))
    conn.commit()

conn.close()
print(f"✅ Inserted {len(df)} rows into {MYSQL_TABLE}.")
