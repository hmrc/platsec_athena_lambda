import pytest
import datetime
import time
from platsec_athena.config import LambdaEnvironment
from platsec_athena.querying import AuditQuery, QueryType, QueryFactory
from platsec_athena.config import StatementType
from platsec_athena.partioning import Partition

@pytest.mark.config
def test_query_dates_and_statements_must_be_correctly_formatted():
    '''
    Test that query statements are
    configured correctly
    '''
    config = get_config()
    expected_date_clause =f'year=\'{config.athena_year}\' AND month=\'{config.athena_month}\' AND day=\'{config.athena_day}\';'


    statements = config.get_querydates()
    actual_date_clause = statements['date_clause']
    assert actual_date_clause == expected_date_clause

@pytest.mark.config
def test_config_statements_must_be_correctly_formatted():
    '''
       Test that config statements
       for Athena Database and Tables
       are formatted correctly
    '''
    config = get_config()
    expected_statement  =f'{config.db}.{config.table}'

    statements = config.get_querydates()
    actual_source = statements['source_clause']

    assert actual_source == expected_statement

@pytest.mark.config
def test_partition_statements_must_be_returned_for_each_region_specified():
    '''
       Tests that the number of partition statements
       are correctly set up
    '''
    config = get_config()
    expected_statement_count = 2

    statements = config.get_partitions()
    assert expected_statement_count == len(statements)

@pytest.mark.config
def test_partition_statements_must_be_correctly_formatted():
    '''
      Tests the partitions statements are
      correctly formatted
    '''
    sut = get_config()
    expected_statements = []
    for i in range(len(sut.regions)):
        statement_to_test = \
                f'partition (region="{sut.regions[i]}", month="{sut.athena_month}", day="{sut.athena_day}", year="{sut.athena_year}")'
        expected_statements.append(statement_to_test)

    statements = sut.get_partitions()

    for expected_statement in expected_statements:
        assert statements_check(expected_statement,statements) == True

@pytest.mark.config
def test_partition_statements_will_error_with_no_regions_specified():
    '''
        Tests that an error is returned
        when no regions are specified
    '''
    config = get_config(regions=[])
    with pytest.raises(IndexError) as excinfo:
        statements = config.get_partitions()
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "No regions specified"

@pytest.mark.config
def test_location_statements_count_must_equal_region_count():
    '''
    Tests that region count and the number of
    Location statements are the same
    '''
    config = get_config()

    statements = config.get_locations()

    assert len(config.regions) == len(statements)

@pytest.mark.config
def test_location_statements_will_error_with_no_regions_specified():
    '''
    Test that an error is returned 
    when the region number and the
    returned location count dont match
    '''
    config = get_config(regions=[])
    with pytest.raises(IndexError) as excinfo:
        statements = config.get_locations()
    exception_msg = excinfo.value.args[0]
    assert exception_msg == "No regions specified"

@pytest.mark.config
def test_location_statements_must_be_correctly_formatted():
    '''
    Tests that the locations statements
    are formatted correctly
    '''
    config = get_config()
    expected_statements = []
    for i in range(len(config.regions)):
        statement_to_test = f' location "s3://{config.bucket}/AWSLogs/{config.account}/CloudTrail/{config.regions[i]}/{config.athena_year}/{config.athena_month}/{config.athena_day}/"'
        expected_statements.append(statement_to_test)

    statements = config.get_locations()

    for expected_statement in expected_statements:
        assert statements_check(expected_statement,statements) == True

@pytest.mark.config
def test_select_statement_must_be_correctly_formatted():
    '''
    Tests that a selection statement
    is returned for all specified regions
    '''
    sut = get_config()
    expected_statement = f'SELECT * FROM {sut.table} WHERE '

    statement = sut.get_statements(StatementType.SELECT)

    assert expected_statement == statement

@pytest.mark.config
def test_location_statement_must_be_correctly_formatted():
    '''
    Tests the location statements
    are formatted correctly
    '''
    sut = get_config()
    expected_statement = f' \'%{sut.bucket}%\''

    statement = sut.get_statements(StatementType.BUCKET)

    assert expected_statement == statement

@pytest.mark.partition
def test_partioning_query_must_be_formatted_correctly():
    '''
    Tests that the partitioning query is correctly
    formatted
    '''
    config = get_config()
    partition = get_partition(config)
    config_partitions = config.get_partitions()
    config_locations = config.get_locations()
    expected_partition_query = f'ALTER TABLE {config.table} ADD IF NOT EXISTS {config_partitions[0]} {config_locations[0]} {config_partitions[1]} {config_locations[1]}'

    partition_query = partition.get_query()

    assert partition_query == expected_partition_query

@pytest.mark.query
@pytest.mark.parametrize('qt',[QueryType.AUDIT])
def test_query_factory_returns_specified_query_type(qt):
    '''
    Tests that the query factory
    returns the correct query
    '''
    sut = get_query_factory()
    expected_query_type = qt.value
    
    query = sut.get_query(qt.value)

    assert query.query_type == expected_query_type


#@pytest.mark.pciquery
#def test_pci_audit_trail_query_must_be_formatted_correctly():
#    config = get_config()
#    query_dates = config.get_querydates()
#    expected_audit_query = f'{config.get_statements(StatementType.SELECT)} requestparameters LIKE {config.get_statements(StatementType.BUCKET)} AND sourceipaddress <> \'cloudtrail.amazonaws.com\' AND sourceipaddress <> \'athena.amazonaws.com\' AND eventName = \'GetObject\' AND {query_dates["date_clause"]}'
#    athena_query = get_query(config)

#    assert audit_query == expected_audit_query

def get_config(db="test_db",table="test_table",bucket="test_bucket",output="test_output",account="test_account",regions=["eu-west-1","eu-west-2"]):
    '''
    Returns a LambdaEnvironment config
    object to allow for testing within tests
    '''
    test_date = str(datetime.datetime.today().isoformat())
    test_config = LambdaEnvironment(db,table,bucket,output,account,test_date,regions)
    return test_config

def get_partition(config):
    test_partition = Partition(config)
    return test_partition

def get_query_factory():
    '''
    Returns a query factory object 
    to be used in tests
    '''
    query_factory = QueryFactory()
    return query_factory

def statements_check(item, items):
    '''
    Helper function for checking items
    in a collection
    '''
    if item in items:
        return True
    else:
        return False
