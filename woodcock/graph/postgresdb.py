"""The `EmbeddedGraph` class is implemented with PostgreSQL as storage
solution."""

import psycopg

from typing import Dict, Sequence, Mapping

from woodcock.graph.graph import GraphIndex, GraphQueryEngine
from woodcock.graph.sqldb import AbstractSQLDB, AbstractSQLDBIndex, \
    AbstractSQLDBQueryEngine, DatabaseDialect


class PostgreSQLConfig:
  """A configuration for a PostgreSQL instance."""

  def __init__(self, *, hostname: str = None, port: int = None,
               username: str = None, password: str = None,
               database: str = '__woodcock__') -> None:
    self.hostname = hostname
    self.port = port
    self.username = username
    self.password = password
    self.database = database

  @property
  def args(self) -> Dict:
    return {
        'host': self.hostname,
        'port': self.port,
        'user': self.username,
        'password': self.password,
        'dbname': self.database,
    }


class PostgreSQLDatabaseDialect(DatabaseDialect):
  """Database dialect for PostgreSQL."""

  def primary_key_row(self, name: str):
    return f'{name} SERIAL PRIMARY KEY'

  def insert_ignore_command(self, statement_name: str,
                            column_names: Sequence[str],
                            value_types: Sequence[type]) -> str:
    columns = ', '.join(column_names)
    values = ', '.join('%s' for _ in value_types)
    return f'''
  INSERT INTO {statement_name} ({columns}) VALUES ({values})
  ON CONFLICT DO NOTHING;
'''

  def var_sub(self, query: str, variables: Mapping[str, type]) -> str:
    variables = {k: '%s' for k, _ in variables.items()}
    return query % variables


class _PostgreSQLGraphEngine(AbstractSQLDBQueryEngine):
  """The PostgreSQL-specific implementation of the query engine."""

  def __init__(self, config: PostgreSQLConfig = None) -> None:
    self._config = config
    super().__init__(PostgreSQLDatabaseDialect())

  @property
  def _connection(self):
    if not self._con:
      self._con = psycopg.connect(**self._config.args)
    return self._con


class _PostgreSQLIndex(AbstractSQLDBIndex):
  """The PostgreSQL-specific implementation of the graph index."""

  def __init__(self, config: PostgreSQLConfig = None) -> None:
    self._config = config
    super().__init__(PostgreSQLDatabaseDialect())

  @property
  def _connection(self):
    if not self._con:
      self._con = psycopg.connect(**self._config.args)
    return self._con


class PostgreSQLGraph(AbstractSQLDB):
  """Implments the `EmbeddedGraph` class with PostgreSQL as storage solution."""

  def __init__(self, config: PostgreSQLConfig = None) -> None:
    if not config:
      raise ValueError('the config for PostgreSQL must be specified')
    self._config = config
    super().__init__(PostgreSQLDatabaseDialect())

  @property
  def _connection(self):
    if not self._con:
      self._con = psycopg.connect(**self._config.args)
    return self._con

  def index(self) -> GraphIndex[str, int, str, int]:
    return _PostgreSQLIndex(self._config)

  def query_engine(self) -> GraphQueryEngine[int, int]:
    return _PostgreSQLGraphEngine(self._config)
