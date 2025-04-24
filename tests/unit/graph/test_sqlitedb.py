"""Unit testing the DuckDB graph solution."""

import unittest
from typing import Hashable

from tests.unit.graph.graph import GraphIndexTesting, GraphQueryTesting
from woodcock.graph.sqlitedb import SQLite3Graph

from woodcock.graph.graph import Graph


class TestSQLite3GraphIndex(unittest.TestCase, GraphIndexTesting):

  def create_new_kg(self) -> Graph:
    return SQLite3Graph()

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1


class TestSQLite3GraphQuery(unittest.TestCase, GraphQueryTesting):

  def create_new_kg(self) -> Graph:
    return SQLite3Graph()

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1
