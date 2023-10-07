import csv
from typing import Iterable

from woodcock.graph.typing import Edge
from woodcock.io.typing import FilePath
from woodcock.io.utils import Compression, CompressionReader


def edges_from_file(f: FilePath,
                    *,
                    compression: Compression = None,
                    skip_header: bool = False,
                    delimiter: str = ',') -> Iterable[Edge]:
  """Reads the edges from the specified CSV file.

  Args:
      f: a file path to the source containing the edges.
      compression: the compression type of the file. It can also be `None`, if
      no compression is used for the file. It is `None` by default.
      skip_header: `True`, if the first row shall be skipped, or `False`
      otherwise. It is `False` by default.
      delimiter: a delimiter used to separate columns in the CSV file. By
      default, the comma character `,` is assumed as separator.

  Returns:
      A iterable sequence of edges that are contained in the given file.

  Raises:
      IOError: An error occurred accessing the given file.
      ValueError: The content in the given CSV file is wrongly formatted.
  """
  with CompressionReader(f, compression) as reader:
    reader = csv.reader(reader.source(), delimiter=delimiter)
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
