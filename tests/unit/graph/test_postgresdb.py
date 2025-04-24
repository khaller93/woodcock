"""Unit testing the DuckDB graph solution."""

import os
import unittest

from typing import Hashable

import psycopg

from tests.unit.graph.graph import GraphIndexTesting, GraphQueryTesting
from woodcock.graph.postgresdb import PostgreSQLGraph, PostgreSQLConfig
from woodcock.graph.graph import Graph


def get_config() -> PostgreSQLConfig:
  """Gathers the PostgreSQL configuration for testing from the environment.

  Returns:
      PostgreSQLConfig: gets the PostgreSQL configuration for testing.
  """
  host = os.environ['POSTGRES_HOST'] if 'POSTGRES_HOST' in os.environ \
      else 'localhost'
  port = int(os.environ['POSTGRES_PORT']) if 'POSTGRES_PORT' in os.environ \
      else 5432
  db_name = os.environ['POSTGRES_DB'] if 'POSTGRES_DB' in os.environ \
      else 'postgres'
  username = os.environ['POSTGRES_USER'] if 'POSTGRES_USER' in os.environ \
      else 'postgres'
  password = os.environ['POSTGRES_PASSWORD'] if 'POSTGRES_PASSWORD' in \
      os.environ else 'postgres'
  config = PostgreSQLConfig(database=db_name,
                            username=username,
                            password=password,
                            hostname=host,
                            port=port)
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


class TestPostgreSQLGraphIndex(unittest.TestCase, GraphIndexTesting):

  def create_new_kg(self) -> Graph:
    conf = get_config()
    clean_database(conf)
    return PostgreSQLGraph(conf)

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1


class TestPostgreSQLGraphQuery(unittest.TestCase, GraphQueryTesting):

  def create_new_kg(self) -> Graph:
    conf = get_config()
    clean_database(conf)
    return PostgreSQLGraph(conf)

  def get_unknown_node_id(self) -> Hashable:
    return -1

  def get_unknown_property_id(self) -> Hashable:
    return -1
