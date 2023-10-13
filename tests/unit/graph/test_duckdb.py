"""Unit testing the DuckDB graph solution."""

import unittest
from typing import Hashable

from tests.unit.graph.graph import GraphIndexTesting
from woodcock.graph.duckdb import DuckDBGraph

from woodcock.graph.graph import Graph


class TestDuckDBGraphIndex(unittest.TestCase, GraphIndexTesting):

  def create_new_kg(self) -> Graph:
    return DuckDBGraph()

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1

