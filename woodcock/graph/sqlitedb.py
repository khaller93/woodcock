"""The `EmbeddedGraph` class is implemented with SQLite3 as storage solution."""

import sqlite3
import tempfile

from os import makedirs
from os.path import join, exists
from woodcock.graph.graph import GraphIndex, GraphQueryEngine

from woodcock.graph.sqldb import AbstractSQLDB, AbstractSQLDBIndex, \
    AbstractSQLDBQueryEngine, SQLRead, SQLWrite


class _SQLite3graphEngine(AbstractSQLDBQueryEngine):

  def __init__(self, db_file_path: str) -> None:
    self._db_file_path = db_file_path
    super().__init__(SQLRead())

  @property
  def _connection(self):
    if not self._con:
      self._con = sqlite3.connect(self._db_file_path)
    return self._con


class _SQLite3Index(AbstractSQLDBIndex):
  """The SQLite3-specific implementation of the graph index."""

  def __init__(self, db_file_path: str) -> None:
    self._db_file_path = db_file_path
    super().__init__(SQLRead())

  @property
  def _connection(self):
    if not self._con:
      self._con = sqlite3.connect(self._db_file_path)
    return self._con


class SQLite3Graph(AbstractSQLDB):
  """Implments the `EmbeddedGraph` class with SQLite3 as storage solution.

  Args:
      db_dir_path: path to the directory at which to store the database files or
      `None`, if a temporary directory should be created. The later will be
      not accessible across multiple runs of this application.
  """

  def __init__(self, *, db_dir_path: str = None) -> None:
    self._db_dir_path = tempfile.mkdtemp() if db_dir_path is None \
        else db_dir_path
    if not exists(self._db_dir_path):
      makedirs(self._db_dir_path)
    self._db_file_path = join(self._db_dir_path, 'sqlite.db')
    super().__init__(SQLWrite(), SQLRead())

  @property
  def _connection(self):
    if not self._con:
      self._con = sqlite3.connect(self._db_file_path)
    return self._con

  def index(self) -> GraphIndex[str, int, str, int]:
    return _SQLite3Index(self._db_file_path)

  def query_engine(self) -> GraphQueryEngine[int, int]:
    return _SQLite3graphEngine(self._db_file_path)
