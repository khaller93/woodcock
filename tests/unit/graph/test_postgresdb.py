"""Unit testing the DuckDB graph solution."""

import unittest
from typing import Hashable

import psycopg
import pytest

from tests.unit.graph.graph import GraphIndexTesting, GraphQueryTesting
from woodcock.graph.postgresdb import PostgreSQLGraph, PostgreSQLConfig
from woodcock.graph.graph import Graph


def get_config() -> PostgreSQLConfig:
  config = PostgreSQLConfig(database='postgres',
                            username='postgres',
                            password='T?N2DNaiuzQFdX<k+nbg47YJ',
                            hostname='localhost',
                            port=5432)
  return config


def clean_database(config: PostgreSQLConfig):
  """deletes all the tables in the database.

  Args:
      config (PostgreSQLConfig): PostgreSQL config to get a connection.
  """
  con = psycopg.connect(**config.args)
  cursor = con.cursor()
  try:
    cursor.execute('''
DO $$ DECLARE
    r RECORD;
BEGIN
    FOR r IN (SELECT tablename FROM pg_tables
    WHERE schemaname = current_schema()) LOOP
        EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || ' CASCADE';
    END LOOP;
END $$;
''')
    con.commit()
  finally:
    cursor.close()


@pytest.mark.skip(reason='not fixed yet')
class TestPostgreSQLGraphIndex(unittest.TestCase, GraphIndexTesting):

  def create_new_kg(self) -> Graph:
    conf = get_config()
    clean_database(conf)
    return PostgreSQLGraph(conf)

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1


@pytest.mark.skip(reason='not fixed yet')
class TestPostgreSQLGraphQuery(unittest.TestCase, GraphQueryTesting):

  def create_new_kg(self) -> Graph:
    conf = get_config()
    clean_database(conf)
    return PostgreSQLGraph(conf)

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1
