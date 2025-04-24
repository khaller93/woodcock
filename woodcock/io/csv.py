"""This module includes methods to read edges from CSV files."""

import csv
from typing import Iterable

from woodcock.graph.typing import Edge
from woodcock.io.typing import FilePath
from woodcock.io.utils import Compression, copen


def open_csv_source(fp: FilePath,
                    *,
                    encoding: str = None,
                    compression: Compression = None,
                    skip_header: bool = False,
                    delimiter: str = ',') -> Iterable[Edge]:
  """Reads the edges from the specified CSV source.

  Args:
      fp (FilePath): file path to the source containing the edges. It must not
      be None or an empty string.
      encoding (str, optional):  Name of the encoding used to decode or encode
      the content of the file. Defaults to None, which means the method
      `locale.getencoding()` is called to get the current locale encoding. This
      is platform-dependent.
      compression (Compression, optional): Compression type of the file. It can
      also be None, if no compression is used for the file. Defaults to None.
      skip_header (bool, optional): True, if the first row shall be skipped, or
      False otherwise. Defaults to False.
      delimiter (str, optional): Delimiter used to separate columns in the CSV
      file. Defaults to the comma character ','.

  Raises:
      IOError: An error occurred accessing the given file.
      ValueError: The content in the given CSV file is wrongly formatted.

  Returns:
      Iterable[Edge]: A iterable sequence of edges that are contained in the
      given file.
  """
  if not fp:
    raise ValueError('a valid file path must be specified')
  with copen(fp, mode='text', compression=compression, encoding=encoding) as f:
    reader = csv.reader(f, delimiter=delimiter)
    it = iter(reader)
    try:
      if skip_header:
        next(it)
      while True:
        row = next(it)
        if len(row) != 3:
          raise ValueError(
              f'edges must have exactly three columns, but has {len(row)}')
        yield row[0], row[1], row[2]
    except StopIteration:
      pass
