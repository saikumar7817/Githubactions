import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import StringIO, BytesIO

def lambda_handler(event, context):
    s3 = boto3.client('s3')

    try:
        # Extract bucket and object key from the event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        object_key = event['Records'][0]['s3']['object']['key']
        destination_bucket = 'pgi-parquet-output-bucket'

        print(f"Triggered by file: {object_key} in bucket: {source_bucket}")

        # Check if the file is a CSV
        if not object_key.endswith('.csv'):
            print("File is not a CSV. Skipping.")
            return {
                'statusCode': 400,
                'body': 'Uploaded file is not a CSV.'
            }

        # Download the CSV file from S3
        csv_obj = s3.get_object(Bucket=source_bucket, Key=object_key)
        csv_data = csv_obj['Body'].read().decode('utf-8')

        # Read CSV into pandas DataFrame
        df = pd.read_csv(StringIO(csv_data))
        print(f"CSV read successfully. Rows: {len(df)}")

        # Convert DataFrame to Parquet format
        table = pa.Table.from_pandas(df)
        parquet_buffer = BytesIO()
        pq.write_table(table, parquet_buffer)
        print("Conversion to Parquet successful.")

        # Define output file name and path
        parquet_key = f"converted/{object_key.replace('.csv', '.parquet')}"

        # Upload the Parquet file to the destination bucket
        s3.put_object(Bucket=destination_bucket, Key=parquet_key, Body=parquet_buffer.getvalue())
        print(f"Parquet file uploaded to: {destination_bucket}/{parquet_key}")

        return {
            'statusCode': 200,
            'body': f'Successfully converted {object_key} to Parquet and uploaded to {destination_bucket}/{parquet_key}'
        }

    except Exception as e:
        print(f"Error during processing: {str(e)}")
        return {
            'statusCode': 500,
            'body': f'Error processing file {object_key}: {str(e)}'
        }
