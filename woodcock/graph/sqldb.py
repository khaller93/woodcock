"""This module implements the interfaces for a knowledge graph for any SQL
database."""

from functools import lru_cache
from typing import Iterable, Tuple, Generator, Union, Sequence, Mapping

from woodcock.graph.graph import EmbeddedGraph, GraphIndex, GraphQueryEngine


class DatabaseDialect:
  """Dialect of the database."""

  def primary_key_row(self, name: str) -> str:
    """Generated a primary key row for the particular database.

    Args:
        name (str): name of the primary key column.

    Returns:
        str: a primary key row for the particular database.
    """
    raise NotImplementedError()

  def insert_ignore_command(self, statement_name: str,
                            column_names: Sequence[str],
                            value_types: Sequence[type]) -> str:
    """Generates an INSERT SQL command, which ignores the insert if the
    unique constraint is violated.

    Args:
        statement_name (str): name of the table into which values shall be
        inserted.
        column_names (Sequence[str]): the names of the columns in the row.
        value_types (Sequence[type]): the types of the column values.

    Returns:
        str: gets the INSERT operation query, which quietly ignores a failed
        insert.
    """
    raise NotImplementedError()

  def var_sub(self, query: str, variables: Mapping[str, type]) -> str:
    """Substitute the variables in the given query string, if they are specified
    in given variables map.

    Args:
        query (str): the query string that shall be substituted.
        variables (Mapping[str, type]): a map of variable name and the data type
        of the variable.

    Returns:
        str: the query string substituted with the given variables.
    """
    raise NotImplementedError()


class SQLWriteCommands:
  """SQL write commands considering the given dialect."""

  def __init__(self, dialect: DatabaseDialect) -> None:
    if not dialect:
      raise ValueError('you must specify the database dialect')
    self._query: Mapping[str, str] = self._setup(dialect)

  @staticmethod
  def _setup(dialect: DatabaseDialect) -> Mapping[str, str]:
    query: Mapping[str, str] = {}
    query['node_table_creation'] = f'''
  CREATE TABLE IF NOT EXISTS node (
    {dialect.primary_key_row('node_id')},
    label VARCHAR NOT NULL UNIQUE
  );
'''
    query['edge_table_creation'] = f'''
  CREATE TABLE IF NOT EXISTS property (
    {dialect.primary_key_row('prop_id')},
    label VARCHAR NOT NULL UNIQUE
  );
'''
    query['statement_table_creation'] = f'''
  CREATE TABLE IF NOT EXISTS statement (
    {dialect.primary_key_row('no')},
    subj INTEGER NOT NULL,
    pred INTEGER NOT NULL,
    obj INTEGER NOT NULL,
    FOREIGN KEY (subj) REFERENCES node (node_id),
    FOREIGN KEY (pred) REFERENCES property (prop_id),
    FOREIGN KEY (obj) REFERENCES node (node_id),
    UNIQUE(subj, pred, obj)
  );
'''
    query['insert_node'] = dialect.insert_ignore_command('node',
                                                         ['label'], [str])
    query['insert_property'] = dialect.insert_ignore_command('property',
                                                             ['label'], [str])
    query['insert_statement'] = dialect.insert_ignore_command('statement',
                                                              ['subj', 'pred',
                                                               'obj'],
                                                              [int, int, int])
    return query

  def __getattr__(self, name):
    val = self._query.get(name)
    if val is None:
      raise AttributeError(f'{name} couldn\'t be found')
    return val


class SQLReadCommands:
  """SQL reader commands considering the given dialect."""

  def __init__(self, dialect: DatabaseDialect) -> None:
    if not dialect:
      raise ValueError('you must specify the database dialect')
    self._query: Mapping[str, str] = self._setup(dialect)

  @staticmethod
  def _setup(dialect: DatabaseDialect) -> Mapping[str, str]:
    query: Mapping[str, str] = {}
    query['get_node_id'] = dialect.var_sub('''
  SELECT node_id FROM node WHERE label = %(label)s;
''', {'label': str})
    query['get_node_ids'] = 'SELECT node_id FROM node;'
    query['get_node_count'] = 'SELECT count(*) FROM node;'
    query['get_node_label'] = dialect.var_sub('''
  SELECT label FROM node WHERE node_id = %(id)s;
''', {'id': int})
    query['node_id_exist'] = dialect.var_sub('''
  SELECT 1 FROM node WHERE node_id = %(id)s;
''', {'id': int})
    query['get_property_id'] = dialect.var_sub('''
  SELECT prop_id FROM property WHERE label = %(label)s;
''', {'label': str})
    query['get_property_ids'] = 'SELECT prop_id FROM property;'
    query['get_property_count'] = 'SELECT count(*) FROM property;'
    query['get_property_label'] = dialect.var_sub('''
  SELECT label FROM property WHERE prop_id = %(id)s;
''', {'id': int})
    query['get_out_edges'] = dialect.var_sub('''
  SELECT pred, obj FROM statement WHERE subj = %(id)s;
''', {'id': int})
    query['get_prop_out_dist'] = dialect.var_sub('''
  SELECT pred, COUNT(*) FROM statement WHERE subj = %(id)s
  GROUP BY pred;
''', {'id': int})
    query['get_in_edges'] = dialect.var_sub('''
  SELECT subj, pred FROM statement WHERE obj = %(id)s;
''', {'id': int})
    query['get_prop_in_dist'] = dialect.var_sub('''
  SELECT pred, COUNT(*) FROM statement WHERE obj = %(id)s
  GROUP BY pred;
''', {'id': int})
    query['get_edges_search'] = dialect.var_sub('''
  SELECT subj, pred, obj FROM statement
    WHERE (cast(%(id)s as int) IS NULL OR subj = %(id)s)
      AND (cast(%(id)s as int) IS NULL OR pred = %(id)s)
      AND (cast(%(id)s as int) IS NULL OR obj = %(id)s);
''', {'id': int})
    query['get_all_edges'] = 'SELECT subj, pred, obj FROM statement;'
    query['get_edges_search_count'] = dialect.var_sub('''
  SELECT count(*) FROM statement
    WHERE (cast(%(id)s as int) IS NULL OR subj = %(id)s)
      AND (cast(%(id)s as int) IS NULL OR pred = %(id)s)
      AND (cast(%(id)s as int) IS NULL OR obj = %(id)s);
''', {'id': int})
    query['get_all_edges_count'] = 'SELECT count(*) FROM statement;'
    return query

  def __getattr__(self, name):
    val = self._query.get(name)
    if val is None:
      raise AttributeError(f'{name} couldn\'t be found')
    return val


class SQLCommands:
  """SQL commands considering the given dialect."""

  def __init__(self, dialect: DatabaseDialect) -> None:
    if not dialect:
      raise ValueError('you must specify the database dialect')
    self.dialect = dialect
    self.read = SQLReadCommands(self.dialect)
    self.write = SQLWriteCommands(self.dialect)


class AbstractSQLDBQueryEngine(GraphQueryEngine[int, int]):
  """An abstract implementation of `GraphQueryEngine` using any SQL database.

  Args:
      reader (SQLRead): the SQL reader specification used to query the database.
  """

  def __init__(self, dialect: DatabaseDialect) -> None:
    if dialect is None:
      raise ValueError('the SQL dialect specification must be given')
    self._q = SQLCommands(dialect)
    self._con = None

  @property
  def _connection(self):
    raise NotImplementedError('must be implemented by sublcass')

  def node_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      cursor.execute(self._q.read.get_node_ids)
      while True:
        r = cursor.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def node_count(self) -> int:
    cursor = self._connection.cursor()
    try:
      return cursor.execute(self._q.read.get_node_count).fetchone()[0]
    finally:
      cursor.close()

  def property_ids(self) -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      cursor.execute(self._q.read.get_property_ids)
      while True:
        r = cursor.fetchone()
        if r is None:
          break
        yield r[0]
    finally:
      cursor.close()

  def property_type_count(self) -> int:
    cursor = self._connection.cursor()
    try:
      cursor.execute(self._q.read.get_property_count)
      return cursor.fetchone()[0]
    finally:
      cursor.close()

  def _does_node_id_exist(self, node_id: int) -> bool:
    cursor = self._connection.cursor()
    try:
      cursor.execute(self._q.read.node_id_exist, (node_id,))
      return bool(cursor.fetchone())
    finally:
      cursor.close()

  def e_in(self, subj_node: int) -> Generator[Tuple[int, int, int], None, None]:
    if not self._does_node_id_exist(subj_node):
      raise ValueError(f'node with ID "{subj_node}" doesn\'t exist in db')
    cursor = self._connection.cursor()
    try:
      cursor.execute(self._q.read.get_in_edges, [subj_node])
      while True:
        r = cursor.fetchone()
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
      cursor.execute(self._q.read.get_prop_in_dist, [subj_node])
      while True:
        r = cursor.fetchone()
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
      cursor.execute(self._q.read.get_out_edges, [subj_node])
      while True:
        r = cursor.fetchone()
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
      cursor.execute(self._q.read.get_prop_out_dist, [subj_node])
      while True:
        r = cursor.fetchone()
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
        cursor.execute(self._q.read.get_all_edges)
      else:
        cursor.execute(self._q.read.get_edges_search,
                       (subj_node, subj_node,
                        prop_type, prop_type,
                        obj_node, obj_node))
      while True:
        r = cursor.fetchone()
        if r is None:
          break
        yield r[0], r[1], r[2]
    finally:
      cursor.close()

  def edges_count(self, *, subj_node: Union[int, None] = None,
                  prop_type: Union[int, None] = None,
                  obj_node: Union[int, None] = None) -> int:
    cursor = self._connection.cursor()
    try:
      if subj_node is None and prop_type is None and obj_node is None:
        cursor.execute(self._q.read.get_all_edges_count)
      else:
        cursor.execute(self._q.read.get_edges_search_count,
                       (subj_node, subj_node,
                        prop_type, prop_type,
                        obj_node, obj_node))
      return cursor.fetchone()[0]
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

  def __init__(self, dialect: DatabaseDialect) -> None:
    if dialect is None:
      raise ValueError('the SQL reader config must be specified')
    self._q = SQLCommands(dialect)
    self._con = None

  @property
  def _connection(self):
    raise NotImplementedError('must be implemented by sublcass')

  def node_ids_for(self, node_labels: Iterable[str]) \
          -> Generator[int, None, None]:
    cursor = self._connection.cursor()
    try:
      for label in node_labels:
        cursor.execute(self._q.read.get_node_id, [label])
        r = cursor.fetchone()
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
        cursor.execute(self._q.read.get_node_label, [node_id])
        r = cursor.fetchone()
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
        cursor.execute(self._q.read.get_property_id, [label])
        r = cursor.fetchone()
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
        cursor.execute(self._q.read.get_property_label,
                       [prop_id])
        r = cursor.fetchone()
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
      dialect (DatabaseDialect): specification of the database dialect. It must
      not be None.
  """

  def __init__(self, dialect: DatabaseDialect) -> None:
    super().__init__()
    if not dialect:
      raise ValueError('the SQL dialect specification must be given')
    self._q = SQLCommands(dialect)
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
      cursor.execute(self._q.write.node_table_creation)
      cursor.execute(self._q.write.edge_table_creation)
      cursor.execute(self._q.write.statement_table_creation)
      self._connection.commit()
      self._post_table_schema_creation(cursor)
    finally:
      cursor.close()

  def import_data(self, data: Iterable[Tuple[str, str, str]]) -> None:
    cursor = self._connection.cursor()
    try:

      @lru_cache(maxsize=100_000)
      def insert_and_get_id(label: str, is_prop: bool = False) -> int:
        cursor.execute(self._q.write.insert_property if is_prop
                       else self._q.write.insert_node, (label,))
        cursor.execute(self._q.read.get_property_id if is_prop
                       else self._q.read.get_node_id, (label,))
        return cursor.fetchone()[0]

      for stmt in data:
        subj = insert_and_get_id(stmt[0])
        pred = insert_and_get_id(stmt[1], is_prop=True)
        obj = insert_and_get_id(stmt[2])
        cursor.execute(self._q.write.insert_statement, (subj, pred, obj))
      self._connection.commit()
    finally:
      cursor.close()

  def shutdown(self) -> None:
    if self._con:
      self._con.close()
      self._con = None
