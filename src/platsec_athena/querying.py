from abc import ABC, abstractmethod
from enum import Enum
from platsec_athena.config import StatementType

class QueryType(Enum):
    AUDIT = 1


class Query(ABC):
    def __init__(self,config,query_type):
      self._query_type = query_type.value

    @property
    @abstractmethod
    def get_query_type(self):
        return self._query_type

    @abstractmethod
    def get_query_text(self,config):
        pass

class AuditQuery(Query):
    def get_query_text(self,config):
        date_clause = query_dates["date_clause"]
        name = "k"
        age = 12
        query_text = f"Hello, {name}. You are {age}."

        return query_text

class QueryFactory:
    def get_query_type(self,query_type):
        queries = {
            1: AuditQuery
            }

        return queries.get(query_type, "Invalid Query Type")

