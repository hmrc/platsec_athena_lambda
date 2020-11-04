from abc import ABC, abstractmethod
from enum import Enum
from platsec_athena.config import StatementType

class QueryType(Enum):
    AUDIT = 1

class AuditQuery:
    def __init__(self,query_type):
        self._query_type = query_type

    def get_query_text(self):
        '''
        Returns the text of query
        '''
        pass

    @property
    def query_type(self):
        return self._query_type

class QueryFactory:
    def get_query(self,query_type):
        queries = {
            1: get_audit_query(query_type)
            }

        query_instance = queries.get(query_type, "Invalid Query Type")
        return query_instance

def get_audit_query(query_type):
    '''
    Returns an Audit Query
    '''
    query = AuditQuery(query_type)

    return query
