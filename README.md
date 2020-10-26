
# platsec_athena_lambda

Repository for Athena Lambda code.  The repository contains tests and source code to set up
the Lambda that is used for AWS Athena.

The Lambda is written in python and will execute on a daily basis and is responsible for setting
up the partitions for the Athena table.

### Tests

The tests are written using the PyTest framework.

To run all the tests - pytest

### License

This code is open source software licensed under the [Apache 2.0 License]("http://www.apache.org/licenses/LICENSE-2.0.html").
