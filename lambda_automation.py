import boto3
import pymysql
import json
from datetime import datetime

# Constants
S3_BUCKET_NAME = "test-deploy-lambda05"
DYNAMODB_TABLE_NAME = "S3DeploymentTimestamps"
SECRETS_MANAGER_SECRET_NAME = "arn:aws:secretsmanager:us-west-1:982723143439:secret:EC2-SECRET-KEY-YZnzpX"
EC2_PUBLIC_IP = "54.153.21.7"

# Initialize AWS clients
s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
secrets_client = boto3.client('secretsmanager')
ssm_client = boto3.client('ssm')
ec2_client = boto3.client('ec2')

def get_credentials_from_secrets_manager(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def get_sorted_s3_files(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            return []
        files = response['Contents']
        files.sort(key=lambda x: x['LastModified'])
        return files
    except Exception as e:
        print(f"Error listing S3 objects: {e}")
        return []

def get_dynamodb_records(table_name):
    try:
        response = dynamodb_client.scan(TableName=table_name)
        return [item['Timestamp']['S'] for item in response.get('Items', [])]
    except Exception as e:
        print(f"Error fetching DynamoDB records: {e}")
        return []

def update_dynamodb_record(table_name, new_timestamp):
    try:
        dynamodb_client.put_item(
            TableName=table_name,
            Item={'Timestamp': {'S': new_timestamp}}
        )
    except Exception as e:
        print(f"Error updating DynamoDB record: {e}")

def log_to_rds(date, timestamp, status):
    credentials = get_credentials_from_secrets_manager(SECRETS_MANAGER_SECRET_NAME)
    if not credentials:
        return

    connection = None
    try:
        connection = pymysql.connect(
            host=credentials['host'],
            user=credentials['username'],
            password=credentials['password'],
            database=credentials['database'],
            port=credentials['port']
        )

        with connection.cursor() as cursor:
            query = """
            INSERT INTO deployment_logs (Date, Time, Status)
            VALUES (%s, %s, %s)
            """
            cursor.execute(query, (date, timestamp, status))
            connection.commit()
    except pymysql.MySQLError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Error logging to RDS: {e}")
    finally:
        if connection:
            connection.close()

def get_instance_id_by_ip(ip_address):
    try:
        response = ec2_client.describe_instances(
            Filters=[{'Name': 'ip-address', 'Values': [ip_address]}]
        )
        for reservation in response['Reservations']:
            for instance in reservation['Instances']:
                return instance['InstanceId']
        return None
    except Exception as e:
        print(f"Error retrieving instance ID: {e}")
        return None

def deploy_to_ec2(file_name):
    instance_id = get_instance_id_by_ip(EC2_PUBLIC_IP)
    if not instance_id:
        print("Instance ID not found.")
        return False

    try:
        s3_file_path = f"s3://{S3_BUCKET_NAME}/{file_name}"
        ec2_target_path = "/var/www/html/index.html"
        command = f"aws s3 cp {s3_file_path} {ec2_target_path}"

        ssm_client.send_command(
            InstanceIds=[instance_id],
            DocumentName="AWS-RunShellScript",
            Parameters={"commands": [command]}
        )
        return True
    except Exception as e:
        print(f"Error deploying to EC2: {e}")
        return False

def lambda_handler(event, context):
    """Main Lambda function handler."""
    sorted
