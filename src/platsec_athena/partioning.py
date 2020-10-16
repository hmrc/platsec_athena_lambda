import boto3
import datetime
import time
import os
# EDIT THE FOLLOWING#
# ----------------------#

# This should be the name of your Athena database
ATHENA_DATABASE = os.environ['ATHENA_DB']

# This should be the name of your Athena database table
ATHENA_TABLE = os.environ['ATHENA_TB']

# This is the Amazon S3 bucket name you want partitioned and logs queried from:
LOG_BUCKET = os.environ['ATHENA_SRC']

# AWS Account number for the Amazon S3 path to your CloudTrail logs
AWS_ACCOUNT_ID = os.environ['AWS_ACCOUNT']

# This is the Amazon S3 bucket name for the Athena Query results:
OUTPUT_LOG_BUCKET = os.environ['ATHENA_RESULTS']

# Define regions to partition
REGION1 = "eu-west-2"
REGION2 = "eu-west-1"

RETRY_COUNT = 50

# Getting the current date and splitting into variables to use in queries below
CURRENT_DATE = datetime.datetime.today()
DATEFORMATTED = (CURRENT_DATE.isoformat())
ATHENA_YEAR = str(DATEFORMATTED[:4])
ATHENA_MONTH = str(DATEFORMATTED[5:7])
ATHENA_DAY = str(DATEFORMATTED[8:10])

# location for the Athena query results
OUTPUT_LOCATION = "s3://"+OUTPUT_LOG_BUCKET+"/DailyAthenaLogs/CloudTrail/"+str(CURRENT_DATE.isoformat())

# Athena Query definitions for PCI DSS requirements
YEAR_MONTH_DAY = f'year=\'{ATHENA_YEAR}\' AND month=\'{ATHENA_MONTH}\' AND day=\'{ATHENA_DAY}\';'
ATHENA_DB_TABLE = f'{ATHENA_DATABASE}.{ATHENA_TABLE}'
PARTITION_STATEMENT_1 = f'partition (region="{REGION1}", month="{ATHENA_MONTH}", day="{ATHENA_DAY}", year="{ATHENA_YEAR}")'
LOCATION_1 = f' location "s3://{LOG_BUCKET}/AWSLogs/{AWS_ACCOUNT_ID}/CloudTrail/{REGION1}/{ATHENA_YEAR}/{ATHENA_MONTH}/{ATHENA_DAY}/"'
PARTITION_STATEMENT_2 = f'partition (region="{REGION2}", month="{ATHENA_MONTH}", day="{ATHENA_DAY}", year="{ATHENA_YEAR}")'
LOCATION_2 = f' location "s3://{LOG_BUCKET}/AWSLogs/{AWS_ACCOUNT_ID}/CloudTrail/{REGION2}/{ATHENA_YEAR}/{ATHENA_MONTH}/{ATHENA_DAY}/"'
SELECT_STATEMENT = "SELECT * FROM "+ATHENA_DB_TABLE+ " WHERE "
LIKE_BUCKET = f' \'%{LOG_BUCKET}%\''


# Query to partition selected regions
QUERY_1 = f'ALTER TABLE {ATHENA_DB_TABLE} ADD IF NOT EXISTS {PARTITION_STATEMENT_1} {LOCATION_1} {PARTITION_STATEMENT_2} {LOCATION_2}'

# Access to audit trails or CHD 10.2.1/10.2.3
QUERY_2 = f'{SELECT_STATEMENT} requestparameters LIKE {LIKE_BUCKET} AND sourceipaddress <> \'cloudtrail.amazonaws.com\' AND sourceipaddress <> \'athena.amazonaws.com\' AND eventName = \'GetObject\' AND {YEAR_MONTH_DAY}'

# Root Actions PCI DSS 10.2.2
QUERY_3 = f'{SELECT_STATEMENT} userIdentity.sessionContext.sessionIssuer.userName LIKE \'%root%\' AND {YEAR_MONTH_DAY}'

# Failed Logons PCI DSS 10.2.4
QUERY_4 = f'{SELECT_STATEMENT} eventname = \'ConsoleLogin\' AND responseelements LIKE \'%Failure%\' AND {YEAR_MONTH_DAY}'

# Privilege changes PCI DSS 10.2.5.b, 10.2.5.c
QUERY_5 = f'{SELECT_STATEMENT} eventname LIKE \'%AddUserToGroup%\' AND requestparameters LIKE \'%Admin%\' AND {YEAR_MONTH_DAY}'

# Initialization, stopping, or pausing of the audit logs PCI DSS 10.2.6
QUERY_6 = f'{SELECT_STATEMENT} eventname = \'StopLogging\' OR eventname = \'StartLogging\' AND {YEAR_MONTH_DAY}'

# Suspicious activity PCI DSS 10.6
QUERY_7 = f'{SELECT_STATEMENT} eventname LIKE \'%DeleteSecurityGroup%\' OR eventname LIKE \'%CreateSecurityGroup%\' OR eventname LIKE \'%UpdateSecurityGroup%\' OR eventname LIKE \'%AuthorizeSecurityGroup%\' AND {YEAR_MONTH_DAY}'

QUERY_8 = f'{SELECT_STATEMENT} eventname LIKE \'%Subnet%\' and eventname NOT LIKE \'Describe%\' AND {YEAR_MONTH_DAY}'


# Defining function to generate query status for each query
def query_stat_fun(query, response):
    client = boto3.client('athena')
    query_execution_id = response['QueryExecutionId']
    print(query_execution_id +' : '+query)
    for i in range(1, 1 + RETRY_COUNT):
        query_status = client.get_query_execution(QueryExecutionId=query_execution_id)
        query_fail_status = query_status['QueryExecution']['Status']
        query_execution_status = query_fail_status['State']

        if query_execution_status == 'SUCCEEDED':
            print("STATUS:" + query_execution_status)
            break

        if query_execution_status == 'FAILED':
            print(query_fail_status)

        else:
            print("STATUS:" + query_execution_status)
            time.sleep(i)
    else:
        client.stop_query_execution(QueryExecutionId=query_execution_id)
        raise Exception('Maximum Retries Exceeded')


def lambda_handler(query, context):
    client = boto3.client('athena')
    queries = [QUERY_1, QUERY_2, QUERY_3, QUERY_4, QUERY_5, QUERY_6, QUERY_7, QUERY_8]
    for query in queries:
        response = client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={
                'Database': ATHENA_DATABASE},
            ResultConfiguration={
                'OutputLocation': OUTPUT_LOCATION})
        query_stat_fun(query, response)

