"""This module implements the interfaces for a knowledge graph for any SQL
database."""

from functools import lru_cache
from typing import Iterable, Tuple, Generator, Union

from woodcock.graph.graph import EmbeddedGraph, GraphIndex, GraphQueryEngine


class SQLWrite:
  """Specification of the SQL write commands."""

  @property
  def node_table_creation(self) -> str:
    return '''
  CREATE TABLE IF NOT EXISTS node (
    node_id INTEGER PRIMARY KEY AUTOINCREMENT,
    label VARCHAR NOT NULL UNIQUE
  );
'''

  @property
  def edge_table_creation(self) -> str:
    return '''
  CREATE TABLE IF NOT EXISTS property (
    prop_id INTEGER PRIMARY KEY AUTOINCREMENT,
    label VARCHAR NOT NULL UNIQUE
  );
'''

  @property
  def statement_table_creation(self) -> str:
    return '''
  CREATE TABLE IF NOT EXISTS statement (
    no INTEGER PRIMARY KEY AUTOINCREMENT,
    subj INTEGER NOT NULL,
    pred INTEGER NOT NULL,
    obj INTEGER NOT NULL,
    FOREIGN KEY (subj) REFERENCES node (node_id),
    FOREIGN KEY (pred) REFERENCES property (prop_id),
    FOREIGN KEY (obj) REFERENCES node (node_id),
    UNIQUE(subj, pred, obj)
  );
'''

  @property
  def insert_node(self) -> str:
    return '''
  INSERT OR IGNORE INTO node (label) VALUES (?);
'''

  @property
  def insert_property(self) -> str:
    return '''
  INSERT OR IGNORE INTO property (label) VALUES (?);
'''

  @property
  def insert_statement(self) -> str:
    return '''
  INSERT OR IGNORE INTO statement (subj, pred, obj) VALUES (?, ?, ?);
'''


class SQLRead:
  """Specification of the SQL read commands."""

  @property
  def get_node_id(self) -> str:
    return '''SELECT node_id FROM node WHERE label = ?;'''

  @property
  def get_node_ids(self) -> str:
    return '''SELECT node_id FROM node;'''

  @property
  def get_node_count(self) -> str:
    return '''SELECT count(*) FROM node;'''

  @property
  def get_node_label(self) -> str:
    return '''SELECT label FROM node WHERE node_id = ?;'''

  @property
  def get_property_id(self) -> str:
    return '''SELECT prop_id FROM property WHERE label = ?;'''

  @property
  def get_property_ids(self) -> str:
    return '''SELECT prop_id FROM property;'''

  @property
  def get_property_count(self) -> str:
    return '''SELECT count(*) FROM property;'''

  @property
  def get_property_label(self) -> str:
    return '''SELECT label FROM property WHERE prop_id = ?;'''

  @property
  def get_out_edges(self) -> str:
    return '''SELECT pred, obj FROM statement WHERE subj = ?;'''

  @property
  def get_prop_out_dist(self) -> str:
    return '''
SELECT pred, COUNT(*) FROM statement WHERE subj = ? GROUP BY pred;
'''

  @property
  def get_in_edges(self) -> str:
    return '''SELECT subj, pred FROM statement WHERE obj = ?;'''

  @property
  def get_prop_in_dist(self) -> str:
    return '''
SELECT pred, COUNT(*) FROM statement WHERE obj = ? GROUP BY pred;
'''

  @property
  def get_edges_search(self) -> str:
    return '''
  SELECT subj, pred, obj FROM statement
  WHERE (? IS NULL OR subj = ?) AND (? IS NULL OR pred =?)
         AND (? IS NULL OR obj = ?);
'''

  @property
  def get_all_edges(self) -> str:
    return '''SELECT subj, pred, obj FROM statement;'''


class AbstractSQLDBQueryEngine(GraphQueryEngine[int, int]):
  """An abstract implementation of `GraphQueryEngine` using any SQL database.

  Args:
      reader (SQLRead): the SQL reader specification used to query the database.
  """

  def __init__(self, reader: SQLRead) -> None:
    if reader is None:
      raise ValueError('the SQL reader specification must be specified')
    self._reader = reader
    self._con = None

  @property
  def _connection(self):
    raise NotImplementedError('must be implemented by sublcass')

  def node_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_node_ids)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def node_count(self) -> int:
    return self._connection.execute(self._reader.get_node_count).fetchone()[0]

  def property_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_property_ids)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def property_type_count(self) -> int:
    return self._connection.execute(self._reader.get_property_count) \
        .fetchone()[0]

  @lru_cache(maxsize=10_000)
  def _does_node_id_exist(self, node_id: int) -> bool:
    cursor = self._connection.cursor()
    try:
      return bool(cursor.execute('SELECT 1 FROM node WHERE node_id = ?;',
                                 [node_id]).fetchone())
    finally:
      cursor.close()

  def e_in(self, subj_node: int) -> Generator[Tuple[int, int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_in_edges, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1], subj_node
    finally:
      cursor.close()

  def prop_in_dist(self, subj_node: int) \
          -> Generator[Tuple[int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_prop_in_dist, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1]
    finally:
      cursor.close()

  def e_out(self, subj_node: int) \
          -> Generator[Tuple[int, int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_out_edges, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield subj_node, r[0], r[1]
    finally:
      cursor.close()

  def prop_out_dist(self, subj_node: int) \
          -> Generator[Tuple[int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(self._reader.get_prop_out_dist, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1]
    finally:
      cursor.close()

  def edges(self, *,
            subj_node: Union[int, None] = None,
            prop_type: Union[int, None] = None,
            obj_node: Union[int, None] = None) \
          -> Generator[Tuple[int, int, int], None, None]:
    cursor = self._connection.cursor()
    try:
      if subj_node is None and prop_type is None and obj_node is None:
        resp = cursor.execute(self._reader.get_all_edges)
      else:
        resp = cursor.execute(self._reader.get_edges_search,
                              [subj_node, subj_node,
                               prop_type, prop_type,
                               obj_node, obj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1], r[2]
    finally:
      cursor.close()

  def shutdown(self) -> None:
    if self._con is not None:
      self._con.close()


class AbstractSQLDBIndex(GraphIndex[str, int, str, int]):
  """An abstract implementation of `GraphIndex` using any SQL database.

  Args:
      reader (SQLRead): the SQL reader specification used to query the database.
  """

  def __init__(self, reader: SQLRead) -> None:
    if reader is None:
      raise ValueError('the SQL reader config must be specified')
    self._reader = reader
    self._con = None

  @property
  def _connection(self):
    raise NotImplementedError('must be implemented by sublcass')

  def node_ids_for(self, node_labels: Iterable[str]) \
          -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      for label in node_labels:
        r = cursor.execute(self._reader.get_node_id, [label]).fetchone()
        if r is None:
          raise ValueError(f'the node label "{r}" isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def node_labels_for(self, node_ids: Iterable[int]) \
          -> Generator[str, None, None]:
    cursor = self._connection.cursor()
    try:
      for node_id in node_ids:
        r = cursor.execute(self._reader.get_node_label, [node_id]).fetchone()
        if r is None:
          raise ValueError(f'the node ID "{r}" isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def property_ids_for(self, property_labels: Iterable[str]) \
          -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      for label in property_labels:
        r = cursor.execute(self._reader.get_property_id, [label]).fetchone()
        if r is None:
          raise ValueError(f'the property label "{r}" isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def property_labels_for(self, property_ids: Iterable[int]) \
          -> Generator[str, None, None]:
    cursor = self._connection.cursor()
    try:
      for prop_id in property_ids:
        r = cursor.execute(self._reader.get_property_label,
                           [prop_id]).fetchone()
        if r is None:
          raise ValueError(f'the property ID "{r}" isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def shutdown(self):
    if self._con is not None:
      self._con.close()


class AbstractSQLDB(EmbeddedGraph[str, int, str, int]):
  """An abstract implementation of `EmbeddedGraph` using any SQL database.

  Args:
      reader (SQLRead): the SQL reader specification used to query the database.
      writer (SQLRead): the SQL writer specification used to import data into
      the database.
  """

  def __init__(self, writer: SQLWrite, reader: SQLRead) -> None:
    super().__init__()
    if writer is None:
      raise ValueError('the SQL writer specification must be specified')
    if reader is None:
      raise ValueError('the SQL reader specification must be specified')
    self._writer = writer
    self._reader = reader
    self._con = None
    self._create_schema_if_not_exist()

  @property
  def _connection(self):
    raise NotImplementedError('must be implemented by sublcass')

  def _pre_table_schema_creation(self, cursor):
    """This hook is called before a database schema is created. It is also
    called, if the database schema has already been created.

    Args:
        cursor: The cursor connection to the DB.
    """
    pass

  def _post_table_schema_creation(self, cursor):
    """This hook is called after a database schema was created. It is also
    called, if the database schema has already been created and nothing was
    done.

    Args:
        cursor: The cursor connection to the DB.
    """
    pass

  def _create_schema_if_not_exist(self):
    """creates the database schema, if it doesn't already exist."""
    cursor = self._connection.cursor()
    try:
      self._pre_table_schema_creation(cursor)
      cursor.execute(self._writer.node_table_creation)
      cursor.execute(self._writer.edge_table_creation)
      cursor.execute(self._writer.statement_table_creation)
      self._connection.commit()
      self._post_table_schema_creation(cursor)
    finally:
      cursor.close()

  def import_data(self, data: Iterable[Tuple[str, str, str]]) -> None:
    cursor = self._connection.cursor()
    try:

      @lru_cache(maxsize=100_000)
      def insert_and_get_id(label: str, is_prop: bool = False) -> int:
        cursor.execute(self._writer.insert_property if is_prop
                       else self._writer.insert_node, [label])
        return cursor.execute(self._reader.get_property_id if is_prop
                              else self._reader.get_node_id,
                              [label]).fetchone()[0]

      for stmt in data:
        subj = insert_and_get_id(stmt[0])
        pred = insert_and_get_id(stmt[1], is_prop=True)
        obj = insert_and_get_id(stmt[2])
        cursor.execute(self._writer.insert_statement, [subj, pred, obj])
      self._connection.commit()
    finally:
      cursor.close()

  def shutdown(self) -> None:
    if self._con:
      self._con.close()
      self._con = None
