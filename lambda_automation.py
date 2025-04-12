import boto3
import pymysql
from datetime import datetime

S3_BUCKET_NAME = "test-deploy-lambda05"
DYNAMODB_TABLE_NAME = "S3DeploymentTimestamps"
SECRETS_MANAGER_SECRET_NAME = "arn:aws:secretsmanager:us-west-1:982723143439:secret:EC2-SECRET-KEY-YZnzpX"
EC2_PUBLIC_IP = "54.153.21.7"

s3_client = boto3.client('s3')
dynamodb_client = boto3.client('dynamodb')
secrets_client = boto3.client('secretsmanager')
ssm_client = boto3.client('ssm')
ec2_client = boto3.client('ec2')

def get_credentials_from_secrets_manager(secret_name):
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        return json.loads(response['SecretString'])
    except Exception:
        return None

def get_sorted_s3_files(bucket_name):
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if 'Contents' not in response:
            return []
        files = response['Contents']
        files.sort(key=lambda x: x['LastModified'])
        return files
    except Exception:
        return []

def get_dynamodb_records(table_name):
    try:
        response = dynamodb_client.scan(TableName=table_name)
        items = response.get('Items', [])
        timestamps = [item['Timestamp']['S'] for item in items]
        return timestamps
    except Exception:
        return []

def update_dynamodb_record(table_name, new_timestamp):
    try:
        dynamodb_client.put_item(
            TableName=table_name,
            Item={
                'Timestamp': {'S': new_timestamp}
            }
        )
    except Exception:
        pass

def log_to_rds(date, timestamp, status):
    connection = None
    credentials = get_credentials_from_secrets_manager(SECRETS_MANAGER_SECRET_NAME)
    if not credentials:
        return

    try:
        connection = pymysql.connect(
            host=credentials['host'],
            user=credentials['username'],
            password=credentials['password'],
            database=credentials['database'],
            port=credentials['port']
        )

        with connection.cursor() as cursor:
            query = f"""
            INSERT INTO deployment_logs (Date, Time, Status)
            VALUES ('{date}', '{timestamp}', '{status}')
            """
            cursor.execute(query)
            connection.commit()
    except pymysql.MySQLError as e:
        if "Unknown database" in str(e):
            pass
    except Exception:
        pass
    finally:
        if connection:
            try:
                connection.close()
            except Exception:
                pass

def get_instance_id_by_ip(ip_address):
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
    except Exception:
        return None

def deploy_to_ec2(file_name):
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
    except Exception:
        return False

def lambda_handler(event, context):
    sorted_files = get_sorted_s3_files(S3_BUCKET_NAME)
    if not sorted_files:
        return

    recorded_timestamps = get_dynamodb_records(DYNAMODB_TABLE_NAME)

    for file in sorted_files:
        file_timestamp = file['LastModified'].strftime('%Y-%m-%d %H:%M:%S')

        if not recorded_timestamps or file_timestamp > max(recorded_timestamps):
            if deploy_to_ec2(file['Key']):
                date = datetime.now().strftime('%Y-%m-%d')
                log_to_rds(date, file_timestamp, "Success")
                update_dynamodb_record(DYNAMODB_TABLE_NAME, file_timestamp)

    return
