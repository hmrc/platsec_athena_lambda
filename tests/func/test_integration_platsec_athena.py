from platsec_athena.querying import QueryType, QueryFactory
from platsec_athena.config import LambdaEnvironment

def get_audit_query():
    config = LambdaEnvironment(account='werw',bucket='kjk',db="kl",output='ere',eventstamp='s',regions='d',table='f')
    qt = QueryType.AUDIT
    qf = QueryFactory()
    a = qf.get_query_type(qt)
    print(qf)
