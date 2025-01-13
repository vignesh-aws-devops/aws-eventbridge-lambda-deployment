import boto3
import base64
import time
import pymysql
from datetime import datetime
import json

# AWS Configurations
S3_BUCKET_NAME = "test-deploy-lambda05"
DYNAMODB_TABLE_NAME = "S3DeploymentTimestamps"
RDS_DATABASE_NAME = "lambda_logs_db"
RDS_HOST = "lambda-logs-db.cvggc406yb7m.us-west-1.rds.amazonaws.com"
RDS_PORT = 3306  
RDS_USER = "admin"  
RDS_PASSWORD = "bluesubmarine661"  
SECRETS_MANAGER_SECRET_NAME = "arn:aws:secretsmanager:us-west-1:982723143439:secret:EC2-SECRET-KEY-YZnzpX"
EC2_PUBLIC_IP = "54.153.21.7"

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
secrets_client = boto3.client('secretsmanager')
ssm_client = boto3.client('ssm')
ec2_client = boto3.client('ec2')

def get_private_key_from_secrets_manager(secret_name):
    """
    Retrieve the private key from AWS Secrets Manager.
    """
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return response['SecretString']
    except Exception as e:
        return None

def get_sorted_s3_files(bucket_name):
    """
    Retrieve and sort files in S3 by last modified timestamp.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            return []
        files = response['Contents']
        files.sort(key=lambda x: x['LastModified'])
        return files
    except Exception as e:
        return []

def get_dynamodb_records(table_name):
    """
    Retrieve all records from DynamoDB and return a list of timestamps.
    """
    try:
        response = dynamodb_client.scan(TableName=table_name)
        items = response.get('Items', [])
        timestamps = [item['Timestamp']['S'] for item in items]
        return timestamps
    except Exception as e:
        return []

def update_dynamodb_record(table_name, new_timestamp):
    """
    Update the DynamoDB table with the new timestamp.
    """
    try:
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                'Timestamp': {'S': new_timestamp}
            }
        )
    except Exception as e:
        pass

def log_to_rds(date, timestamp, status):
    """
    Insert a new log record into the MySQL RDS database using pymysql.
    Here, Version will be auto-incremented by MySQL.
    """
    connection = None  # Initialize the connection variable
    try:
        # Connect to the MySQL RDS instance
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=RDS_DATABASE_NAME,
            port=RDS_PORT
        )

        with connection.cursor() as cursor:
            # The Version field is excluded from the query, MySQL will auto-increment it
            query = f"""
            INSERT INTO deployment_logs (Date, Time, Status)
            VALUES ('{date}', '{timestamp}', '{status}')
            """
            cursor.execute(query)
            connection.commit()
    except pymysql.MySQLError as e:
        if "Unknown database" in str(e):
            pass
    except Exception as e:
        pass
    finally:
        if connection:
            try:
                connection.close()
            except Exception as e:
                pass

def get_instance_id_by_ip(ip_address):
    """
    Retrieve the EC2 instance ID for a given public IP address.
    """
    try:
        response = ec2_client.describe_instances(
            Filters=[
                {
                    'Name': 'ip-address',
                    'Values': [ip_address]
                }
            ]
        )
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                return instance['InstanceId']
        return None
    except Exception as e:
        return None

def deploy_to_ec2(file_name):
    """
    Deploy the file to EC2 using SSM commands.
    """
    instance_id = get_instance_id_by_ip(EC2_PUBLIC_IP)
    if not instance_id:
        return False

    try:
        s3_file_path = f"s3://{S3_BUCKET_NAME}/{file_name}"
        ec2_target_path = "/var/www/html/index.html"
        command = f"aws s3 cp {s3_file_path} {ec2_target_path}"

        response = ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={
                "commands": [command]
            }
        )
        return True
    except Exception as e:
        return False

def lambda_handler(event, context):
    # Step 1: Retrieve sorted files from S3
    sorted_files = get_sorted_s3_files(S3_BUCKET_NAME)
    if not sorted_files:
        return

    # Step 2: Get all recorded timestamps from DynamoDB
    recorded_timestamps = get_dynamodb_records(DYNAMODB_TABLE_NAME)

    # Step 3: Deploy files that have timestamps greater than the recorded timestamps
    for file in sorted_files:
        file_timestamp = file['LastModified'].strftime('%Y-%m-%d %H:%M:%S')

        # Check if this file's timestamp is greater than any of the existing timestamps in DynamoDB
        if not recorded_timestamps or file_timestamp > max(recorded_timestamps):
            # Deploy the file to EC2
            if deploy_to_ec2(file['Key']):
                # Step 4: Log to RDS and update DynamoDB
                date = datetime.now().strftime('%Y-%m-%d')
                log_to_rds(date, file_timestamp, "Success")
                update_dynamodb_record(DYNAMODB_TABLE_NAME, file_timestamp)

    return