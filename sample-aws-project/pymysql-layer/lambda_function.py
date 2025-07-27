import boto3
import pymysql
import csv
import os
import tempfile

def lambda_handler(event, context):
    # Set up S3 client
    s3 = boto3.client('s3')

    # Get the S3 bucket and object key from the event
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = event['Records'][0]['s3']['object']['key']

    # Download the CSV file from S3 to a temporary location
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        s3.download_file(bucket, key, tmp_file.name)

    # Connect to the MySQL database using PyMySQL
    connection = pymysql.connect(
        host=os.environ['DB_HOST'],
        port=int(os.environ['DB_PORT']),
        user=os.environ['DB_USER'],
        password=os.environ['DB_PASSWORD'],
        db=os.environ['DB_NAME'],
        cursorclass=pymysql.cursors.Cursor
    )

    # Read the CSV and insert rows
    with open(tmp_file.name, 'r') as csv_file:
        csv_reader = csv.reader(csv_file)
        next(csv_reader)  # Skip header
        with connection.cursor() as cursor:
            for row in csv_reader:
                cursor.execute(
                    "INSERT INTO employees (id, name, department) VALUES (%s, %s, %s)",
                    (int(row[0]), row[1], row[2])
                )
        connection.commit()

    connection.close()
    os.unlink(tmp_file.name)

    return {
        'statusCode': 200,
        'body': f'CSV data from {key} inserted successfully into MySQL database.'
    }
