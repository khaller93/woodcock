import tempfile
from os import makedirs
from os.path import join, exists
from typing import Tuple, Iterable, Union

import duckdb

from woodcock.graph.graph import EmbeddedGraph, GraphQueryEngine, GraphIndex

_CREATE_RESOURCE_TABLE = '''
        CREATE TABLE IF NOT EXISTS resource (
            id INTEGER PRIMARY KEY,
            label VARCHAR NOT NULL UNIQUE
        );
    '''
_CREATE_RESOURCE_ID_SEQUENCE = '''
        CREATE SEQUENCE IF NOT EXISTS resource_id_seq START 1;
    '''
_CREATE_STATEMENT_TABLE = '''
        CREATE TABLE IF NOT EXISTS statement (
            no INTEGER PRIMARY KEY,
            subj INTEGER NOT NULL,
            pred INTEGER NOT NULL,
            obj INTEGER NOT NULL,
            FOREIGN KEY (subj) REFERENCES resource (id),
            FOREIGN KEY (pred) REFERENCES resource (id),
            FOREIGN KEY (obj) REFERENCES resource (id),
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
_INSERT_RESOURCE = '''
        INSERT INTO resource (id, label) VALUES (nextval('resource_id_seq'), ?);
    '''
_INSERT_STATEMENT = '''
        INSERT OR IGNORE INTO statement (no, subj, pred, obj)
        VALUES (nextval('statement_id_seq'), ?, ?, ?);
    '''

_GET_ID_FOR = '''SELECT id FROM resource WHERE label = ?;'''
_FETCH_NODE_IDS = '''
        SELECT DISTINCT id FROM (
            SELECT subj as id FROM statement UNION
            SELECT obj as id FROM statement
        );
    '''
_FETCH_PROPERTY_TYPE_IDS = '''SELECT DISTINCT pred as id FROM statement;'''
_FETCH_OUT_HOPS = '''SELECT pred, obj FROM statement WHERE subj = ?;'''
_FETCH_IN_HOPS = '''SELECT subj, pred FROM statement WHERE obj = ?;'''
_FETCH_EDGES = '''
        SELECT subj, pred, obj FROM statement
        WHERE (? IS NULL OR subj = ?) AND (? IS NULL OR pred =?)
            AND (? IS NULL OR obj = ?);
    '''


class _DuckDBGraphQueryEngine(GraphQueryEngine[int, int]):
  """The DuckDB-specific implementation of the graph query engine."""

  def __init__(self, db_file_path: str):
    self._db_file_path = db_file_path
    self._con: Union[duckdb.DuckDBPyConnection, None] = None

  def __enter__(self) -> '_DuckDBGraphQueryEngine':
    self.open()
    return self

  def open(self) -> None:
    self._con = duckdb.connect(self._db_file_path, True)

  def node_ids(self) -> Iterable[int]:
    cursor = self._con.cursor()
    try:
      resp = cursor.sql(_FETCH_NODE_IDS)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def property_type_ids(self) -> Iterable[int]:
    cursor = self._con.cursor()
    try:
      resp = cursor.sql(_FETCH_PROPERTY_TYPE_IDS)
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def e_in(self, subj_node: int) -> Iterable[Tuple[int, int, int]]:
    cursor = self._con.cursor()
    try:
      resp = cursor.execute(_FETCH_OUT_HOPS, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield subj_node, r[0], r[1]
    finally:
      cursor.close()

  def e_out(self, subj_node: int) -> Iterable[Tuple[int, int, int]]:
    cursor = self._con.cursor()
    try:
      resp = cursor.execute(_FETCH_IN_HOPS, [subj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1], subj_node
    finally:
      cursor.close()

  def edges(self, *, subj_node: Union[int, None] = None,
            property_type: Union[int, None] = None, obj_node:
            Union[int, None] = None) -> Iterable[Tuple[int, int, int]]:
    cursor = self._con.cursor()
    try:
      resp = cursor.execute(_FETCH_EDGES,
                            [subj_node, subj_node, property_type, property_type,
                             obj_node, obj_node])
      while True:
        r = resp.fetchone()
        if r is None:
          break
        yield r[0], r[1], r[2]
    finally:
      cursor.close()

  def node_count(self) -> int:
    pass

  def property_type_count(self) -> int:
    pass

  def edge_count(self) -> int:
    pass

  def close(self) -> None:
    if self._con is not None:
      self._con.close()

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()


class _DuckDBGraphIndex(GraphIndex[str, int, str, int]):
  """The DuckDB-specific implementation of the graph index."""

  def __init__(self, db_file_path: str) -> None:
    self._db_file_path = db_file_path
    self._con: Union[duckdb.DuckDBPyConnection, None] = None

  def __enter__(self) -> '_DuckDBGraphIndex':
    self.open()
    return self

  def open(self):
    self._con = duckdb.connect(self._db_file_path, True)

  def node_ids_for(self, node_labels: Iterable[str]) -> Iterable[int]:
    cursor = self._con.cursor()
    try:
      for label in node_labels:
        r = cursor.execute(_GET_ID_FOR, [label]).fetchone()
        if r is None:
          raise ValueError(f'the label {r} isn\'t in the db')
        yield r[0]
    finally:
      cursor.close()

  def close(self):
    if self._con is not None:
      self._con.close()

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.close()


class DuckDBGraph(EmbeddedGraph[str, int, str, int]):

  def __init__(self, *, db_dir_path: str = None):
    self._db_dir_path = tempfile.mkdtemp() if db_dir_path is None \
        else db_dir_path
    if not exists(self._db_dir_path):
      makedirs(self._db_dir_path)
    self._db_path = join(self._db_dir_path, 'duck.db')

  @staticmethod
  def _create_schema_if_not_exists(con: duckdb.DuckDBPyConnection):
    con.sql(_CREATE_RESOURCE_TABLE)
    con.sql(_CREATE_RESOURCE_ID_SEQUENCE)
    con.sql(_CREATE_STATEMENT_TABLE)
    con.sql(_CREATE_STATEMENT_ID_SEQUENCE)
    con.sql(_CREATE_META_TABLE)

  @staticmethod
  def _insert_and_get_id(con: duckdb.DuckDBPyConnection, label: str) -> int:
    r_id = con.execute(_GET_ID_FOR, [label]).fetchone()
    if r_id is None:
      con.execute(_INSERT_RESOURCE, [label])
      return con.execute('SELECT currval(\'resource_id_seq\');') \
          .fetchone()[0]
    return r_id[0]

  def import_data(self, data: Iterable[Tuple[str, str, str]]) -> None:
    con = duckdb.connect(self._db_path)
    try:
      self._create_schema_if_not_exists(con)
      for stmt in data:
        subj = self._insert_and_get_id(con, stmt[0])
        pred = self._insert_and_get_id(con, stmt[1])
        obj = self._insert_and_get_id(con, stmt[2])
        con.execute(_INSERT_STATEMENT, [subj, pred, obj])
      con.commit()
    finally:
      con.close()

  def index(self) -> GraphIndex[str, int, str, int]:
    return _DuckDBGraphIndex(self._db_path)

  def query_engine(self) -> GraphQueryEngine[int, int]:
    return _DuckDBGraphQueryEngine(self._db_path)
