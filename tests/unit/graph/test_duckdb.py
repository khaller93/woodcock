"""Unit testing the DuckDB graph solution."""

import unittest

from tests.unit.graph.graph import GraphIndexTesting
from woodcock.graph.duckdb import DuckDBGraph

from woodcock.graph.graph import Graph


class TestDuckDBGraphIndex(unittest.TestCase, GraphIndexTesting):

  def create_new_kg(self) -> Graph:
    return DuckDBGraph()
