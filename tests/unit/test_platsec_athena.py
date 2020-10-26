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

@pytest.mark.config
def test_partition_statements_must_be_returned_for_each_region_specified():
    config = get_config()
    expected_statement_count = 2

    statements = config.get_partitions()
    assert expected_statement_count == len(statements)

@pytest.mark.config
def test_partition_statements_must_be_correctly_formatted():
    config = get_config()
    statement_count = 2
    expected_statements = []
    for i in range(len(config.regions)):
        statement_to_test = [f'partition (region="{config.regions[i]}", month="{config.athena_month}", day="{config.athena_day}", year="{config.athena_year}")']
        expected_statements.append(statement_to_test)

    statements = config.get_partitions()

    for expected_statement in expected_statements:
        assert statements_check(expected_statement,expected_statements) == True

@pytest.mark.config
def test_partition_statements_will_error_with_no_regions_specified():
    config = get_config(regions=[])
    with pytest.raises(IndexError) as excinfo:
        statements = config.get_partitions()
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "No regions specified"

@pytest.mark.config
def test_location_statements_count_must_equal_region_count():
    config = get_config()

    statements = config.get_locations()

    assert len(config.regions) == len(statements)

@pytest.mark.config
def test_location_statements_will_error_with_no_regions_specified():
    config = get_config(regions=[])
    with pytest.raises(IndexError) as excinfo:
        statements = config.get_locations()
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "No regions specified"

def test_location_statements_must_be_correctly_formatted():
    config = get_config()
    statement_count = 2
    expected_statements = []
    for i in range(len(config.regions)):
        statement_to_test = [f' location "s3://{config.bucket}/AWSLogs/{config.account}/CloudTrail/{i}/{config.athena_year}/{config.athena_month}/{config.athena_day}/"']
        expected_statements.append(statement_to_test)

    statements = config.get_partitions()

    for expected_statement in expected_statements:
        assert statements_check(expected_statement,expected_statements) == True

def get_config(db="test_db",table="test_table",bucket="test_bucket",output="test_output",account="test_account",regions=["eu-west-1","eu-west-2"]):
    test_date = str(datetime.datetime.today().isoformat())
    test_config = LambdaEnvironment(db,table,bucket,output,account,test_date,regions)
    return test_config

def get_statement(config):
    test_statement = Statement(config)

    return test_statement

def statements_check(item, items):
    if item in items:
        return True
    else:
        return False
