import pytest
import datetime
import time
from platsec_athena.config import LambdaEnvironment
from platsec_athena.querying import Statement

@pytest.mark.config
def test_s3_bucket_location_must_be_correctly_formatted() :
   config = get_config()
   expected_output = "s3://"+config.output+"/DailyAthenaLogs/CloudTrail/"+config.eventstamp

   statement = get_statement(config)

   assert statement.output == expected_output

@pytest.mark.config
def test_query_dates_and_statements_must_be_correctly_formatted():
   config = get_config()
   expected_date_clause =f'year=\'{config.athena_year}\' AND month=\'{config.athena_month}\' AND day=\'{config.athena_day}\';'


   statements = config.get_querydates()
   actual_date_clause = statements['date_clause']
   assert actual_date_clause == expected_date_clause

@pytest.mark.config
def test_config_statements_must_be_correctly_formatted():
    config = get_config()
    expected_statement  =f'{config.db}.{config.table}'

    statements = config.get_querydates()
    actual_source = statements['source_clause']

    assert actual_source == expected_statement

def get_config(db="test_db",table="test_table",bucket="test_bucket",output="test_output",account="test_account"):
    test_date = str(datetime.datetime.today().isoformat())
    test_config = LambdaEnvironment(db,table,bucket,output,account,test_date)
    return test_config

def get_statement(config):
    test_statement = Statement(config)

    return test_statement
