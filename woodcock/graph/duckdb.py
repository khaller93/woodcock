"""The `EmbeddedGraph` class is implemented with DuckDB as storage solution."""

import tempfile
from functools import lru_cache
from os import makedirs
from os.path import join, exists
from typing import Tuple, Iterable, Union, Generator

import duckdb

from woodcock.graph.graph import EmbeddedGraph, GraphQueryEngine, GraphIndex

_CREATE_RESOURCE_ID_SEQUENCE = '''
  CREATE SEQUENCE IF NOT EXISTS resource_id_seq START 1;
'''
_CREATE_NODE_TABLE = '''
  CREATE TABLE IF NOT EXISTS node (
    node_id INTEGER PRIMARY KEY,
    label VARCHAR NOT NULL UNIQUE
  );
'''
_CREATE_EDGE_TABLE = '''
  CREATE TABLE IF NOT EXISTS property (
    prop_id INTEGER PRIMARY KEY,
    label VARCHAR NOT NULL UNIQUE
  );
'''
_CREATE_STATEMENT_TABLE = '''
  CREATE TABLE IF NOT EXISTS statement (
    no INTEGER PRIMARY KEY,
    subj INTEGER NOT NULL,
    pred INTEGER NOT NULL,
    obj INTEGER NOT NULL,
    FOREIGN KEY (subj) REFERENCES node (node_id),
    FOREIGN KEY (pred) REFERENCES property (prop_id),
    FOREIGN KEY (obj) REFERENCES node (node_id),
    UNIQUE(subj, pred, obj)
  );
'''
_CREATE_STATEMENT_ID_SEQUENCE = '''
  CREATE SEQUENCE IF NOT EXISTS statement_id_seq START 1;
'''
_CREATE_META_TABLE = '''
  CREATE TABLE IF NOT EXISTS meta (
    key VARCHAR PRIMARY KEY,
    value VARCHAR NOT NULL
  );
'''
_INSERT_NODE = '''
  INSERT INTO node (node_id, label) VALUES (nextval('resource_id_seq'), ?);
'''
_INSERT_EDGE = '''
  INSERT INTO property (prop_id, label) VALUES (nextval('resource_id_seq'), ?);
'''
_INSERT_STATEMENT = '''
  INSERT OR IGNORE INTO statement (no, subj, pred, obj)
  VALUES (nextval('statement_id_seq'), ?, ?, ?);
'''

_GET_NODE_ID_FOR = '''SELECT node_id FROM node WHERE label = ?;'''
_GET_PROPERTY_ID_FOR = '''SELECT prop_id FROM property WHERE label = ?;'''
_GET_NODE_LABEL_FOR = '''SELECT label FROM node WHERE node_id = ?;'''
_GET_PROPERTY_LABEL_FOR = '''SELECT label FROM property WHERE prop_id = ?;'''
_GET_PROPERTY_ID_FOR = '''SELECT prop_id FROM property WHERE label = ?;'''
_FETCH_NODE_IDS = '''SELECT node_id FROM node;'''
_FETCH_NODE_COUNT = '''SELECT count(*) FROM node;'''
_FETCH_PROPERTY_TYPE_IDS = '''SELECT prop_id FROM property;'''
_FETCH_PROPERTY_TYPE_COUNT = '''SELECT count(*) FROM property;'''
_FETCH_OUT_HOPS = '''SELECT pred, obj FROM statement WHERE subj = ?;'''
_FETCH_OUT_DIST = '''
SELECT pred, COUNT(*) FROM statement WHERE subj = ? GROUP BY pred;
'''
_FETCH_IN_HOPS = '''SELECT subj, pred FROM statement WHERE obj = ?;'''
_FETCH_IN_DIST = '''
SELECT pred, COUNT(*) FROM statement WHERE obj = ? GROUP BY pred;
'''
_FETCH_EDGES = '''
  SELECT subj, pred, obj FROM statement
  WHERE (? IS NULL OR subj = ?) AND (? IS NULL OR pred =?)
         AND (? IS NULL OR obj = ?);
'''
_FETCH_ALL_EDGES = '''SELECT subj, pred, obj FROM statement;'''


class _DuckDBGraphQueryEngine(GraphQueryEngine[int, int]):
  """The DuckDB-specific implementation of the graph query engine."""

  def __init__(self, db_file_path: str):
    self._db_file_path = db_file_path
    self._con: Union[duckdb.DuckDBPyConnection, None] = None

  @property
  def _connection(self) -> duckdb.DuckDBPyConnection:
    if not self._con:
      self._con = duckdb.connect(self._db_file_path, True)
    return self._con

  def node_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      resp = cursor.sql(_FETCH_NODE_IDS)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def node_count(self) -> int:
    return self._connection.sql(_FETCH_NODE_COUNT).fetchone()[0]

  def property_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      resp = cursor.sql(_FETCH_PROPERTY_TYPE_IDS)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def property_type_count(self) -> int:
    return self._connection.sql(_FETCH_PROPERTY_TYPE_COUNT).fetchone()[0]

  @lru_cache(maxsize=10_000)
  def _does_node_id_exist(self, node_id: int) -> bool:
    return bool(self._connection.sql('SELECT 1 FROM node WHERE node_id = ?;',
                                     params=[node_id]).fetchone())

  def e_in(self, subj_node: int) -> Generator[Tuple[int, int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      resp = cursor.execute(_FETCH_IN_HOPS, [subj_node])
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
      resp = cursor.execute(_FETCH_IN_DIST, [subj_node])
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
      resp = cursor.execute(_FETCH_OUT_HOPS, [subj_node])
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
      resp = cursor.execute(_FETCH_OUT_DIST, [subj_node])
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
        resp = cursor.execute(_FETCH_ALL_EDGES)
      else:
        resp = cursor.execute(_FETCH_EDGES, [subj_node, subj_node,
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


class _DuckDBGraphIndex(GraphIndex[str, int, str, int]):
  """The DuckDB-specific implementation of the graph index."""

  def __init__(self, db_file_path: str) -> None:
    self._db_file_path = db_file_path
    self._con: Union[duckdb.DuckDBPyConnection, None] = None

  @property
  def _connection(self) -> duckdb.DuckDBPyConnection:
    if not self._con:
      self._con = duckdb.connect(self._db_file_path, True)
    return self._con

  def node_ids_for(self, node_labels: Iterable[str]) \
          -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      for label in node_labels:
        r = cursor.execute(_GET_NODE_ID_FOR, [label]).fetchone()
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
        r = cursor.execute(_GET_NODE_LABEL_FOR, [node_id]).fetchone()
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
        r = cursor.execute(_GET_PROPERTY_ID_FOR, [label]).fetchone()
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
        r = cursor.execute(_GET_PROPERTY_LABEL_FOR, [prop_id]).fetchone()
        if r is None:
          raise ValueError(f'the property ID "{r}" isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def shutdown(self):
    if self._con is not None:
      self._con.close()


class DuckDBGraph(EmbeddedGraph[str, int, str, int]):
  """Implments the `EmbeddedGraph` class with DuckDB as storage solution.

  Args:
      db_dir_path: path to the directory at which to store the database files or
      `None`, if a temporary directory should be created. The later will be
      not accessible across multiple runs of this application.
  """

  def __init__(self, *, db_dir_path: str = None):
    self._db_dir_path = tempfile.mkdtemp() if db_dir_path is None \
        else db_dir_path
    if not exists(self._db_dir_path):
      makedirs(self._db_dir_path)
    self._db_path = join(self._db_dir_path, 'duck.db')
    self._create_schema_if_not_exist()

  def _create_schema_if_not_exist(self):
    """creates the database schema, if it doesn't exist."""
    con = duckdb.connect(self._db_path)
    try:
      con.sql(_CREATE_NODE_TABLE)
      con.sql(_CREATE_EDGE_TABLE)
      con.sql(_CREATE_RESOURCE_ID_SEQUENCE)
      con.sql(_CREATE_STATEMENT_TABLE)
      con.sql(_CREATE_STATEMENT_ID_SEQUENCE)
      con.sql(_CREATE_META_TABLE)
      con.commit()
    finally:
      con.close()

  @staticmethod
  def _insert_and_get_id(con: duckdb.DuckDBPyConnection, label: str,
                         is_edge: bool = False) -> int:
    r_id = con.execute(_GET_PROPERTY_ID_FOR if is_edge else _GET_NODE_ID_FOR,
                       [label]).fetchone()
    if r_id is None:
      con.execute(_INSERT_EDGE if is_edge else _INSERT_NODE, [label])
      return con.execute('SELECT currval(\'resource_id_seq\');') \
          .fetchone()[0]
    return r_id[0]

  def import_data(self, data: Iterable[Tuple[str, str, str]]) -> None:
    con = duckdb.connect(self._db_path)
    try:
      for stmt in data:
        subj = self._insert_and_get_id(con, stmt[0])
        pred = self._insert_and_get_id(con, stmt[1], is_edge=True)
        obj = self._insert_and_get_id(con, stmt[2])
        con.execute(_INSERT_STATEMENT, [subj, pred, obj])
      con.commit()
    finally:
      con.close()

  def index(self) -> GraphIndex[str, int, str, int]:
    return _DuckDBGraphIndex(self._db_path)

  def query_engine(self) -> GraphQueryEngine[int, int]:
    return _DuckDBGraphQueryEngine(self._db_path)
