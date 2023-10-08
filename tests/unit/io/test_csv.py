"""Unit testing opening CSV files."""

from woodcock.io.csv import open_csv_source
from woodcock.io.utils import Compression

import pytest
import unittest

from os.path import dirname, join


def _get_resource_dir_path() -> str:
  return join(dirname(__file__), 'resources')


class TestCSVReading(unittest.TestCase):
  """Testing opening CSV files."""

  def test_must_throw_value_error_when_file_path_is_none(self):
    with pytest.raises(ValueError, match='a valid file path must be specified'):
      next(open_csv_source(f=None))

  def test_must_throw_value_error_when_file_path_is_empty(self):
    with pytest.raises(ValueError, match='a valid file path must be specified'):
      next(open_csv_source(f=''))

  def test_must_throw_file_not_found_error_when_file_path_doesnt_exist(self):
    f = join(_get_resource_dir_path(), 'not_existing.csv')
    with pytest.raises(FileNotFoundError):
      next(open_csv_source(f))

  def test_must_return_empty_list_when_empty_csv(self):
    f = join(_get_resource_dir_path(), 'empty.csv')
    edges = [e for e in open_csv_source(f)]

    assert edges == []

  def test_must_return_empty_list_when_opening_empty_csv(self):
    f = join(_get_resource_dir_path(), 'empty.csv.xz')
    edges = [e for e in open_csv_source(f, compression=Compression.XZ)]

    assert edges == []

  def test_must_throw_file_not_found_error_when_gz_file_path_doesnt_exist(self):
    f = join(_get_resource_dir_path(), 'not_existing.csv.gz')
    with pytest.raises(FileNotFoundError):
      next(open_csv_source(f, compression=Compression.GZIP))

  def test_must_return_edges_with_no_header_when_open_w_skip_header(self):
    f = join(_get_resource_dir_path(), 'meowth_with_header.csv')
    edges = [e for e in open_csv_source(f, skip_header=True)]

    assert edges is not None
    assert ('subj', 'pred', 'obj') not in edges
    assert len(edges) == 25
    assert ('pokemon/meowth', 'isAbleToApply', 'move/slash') in edges
    assert ('pokemon/meowth', 'describedInPokédex',
            'pokedex/national/entry/52') in edges

  def test_must_return_edges_when_open_tsv_w_no_header(self):
    f = join(_get_resource_dir_path(), 'raichu.tsv')
    edges = [e for e in open_csv_source(f, delimiter='\t')]

    assert edges is not None
    assert len(edges) == 5
    assert [
        ('pokemon/raichu', 'foundIn', 'Habitat:Forest'),
        ('pokemon/raichu', 'hasColour', 'dbpedia.org/resource/Yellow'),
        ('pokemon/raichu', 'hasShape', 'Shape:Upright'),
        ('pokemon/raichu', 'hasType', 'PokéType:Electric'),
        ('pokemon/raichu', 'hasType', 'PokéType:Psychic')
    ] == edges

  def test_must_return_edges_w_no_header_when_gz_tsv_w_skip_header(self):
    f = join(_get_resource_dir_path(), 'hitmonchan.tsv.gz')
    edges = [e for e in open_csv_source(f, delimiter='\t', skip_header=True,
                                        compression=Compression.GZIP)]

    assert edges is not None
    assert len(edges) == 4
    assert [
        ('pokemon/hitmonchan', 'foundIn', 'Habitat:Urban'),
        ('pokemon/hitmonchan', 'hasColour', 'dbpedia.org/resource/Brown'),
        ('pokemon/hitmonchan', 'hasShape', 'Shape:Humanoid'),
        ('pokemon/hitmonchan', 'hasType', 'PokéType:Fighting')
    ] == edges

  def test_must_return_edges_w_no_header_when_xz_tsv_w_skip_header(self):
    f = join(_get_resource_dir_path(), 'hitmonchan.tsv.xz')
    edges = [e for e in open_csv_source(f, delimiter='\t', skip_header=True,
                                        compression=Compression.XZ)]

    assert edges is not None
    assert len(edges) == 4
    assert [
        ('pokemon/hitmonchan', 'foundIn', 'Habitat:Urban'),
        ('pokemon/hitmonchan', 'hasColour', 'dbpedia.org/resource/Brown'),
        ('pokemon/hitmonchan', 'hasShape', 'Shape:Humanoid'),
        ('pokemon/hitmonchan', 'hasType', 'PokéType:Fighting')
    ] == edges

  def test_must_return_edges_w_no_header_when_bz2_tsv_w_skip_header(self):
    f = join(_get_resource_dir_path(), 'hitmonchan.tsv.bz2')
    edges = [e for e in open_csv_source(f, delimiter='\t', skip_header=True,
                                        compression=Compression.BZIP2)]

    assert edges is not None
    assert len(edges) == 4
    assert [
        ('pokemon/hitmonchan', 'foundIn', 'Habitat:Urban'),
        ('pokemon/hitmonchan', 'hasColour', 'dbpedia.org/resource/Brown'),
        ('pokemon/hitmonchan', 'hasShape', 'Shape:Humanoid'),
        ('pokemon/hitmonchan', 'hasType', 'PokéType:Fighting')
    ] == edges
