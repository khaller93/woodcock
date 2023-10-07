import bz2
import gzip
import lzma
from enum import Enum
from typing import Iterable

from woodcock.io.typing import FilePath


class Compression(Enum):
  """Type of the compression"""
  GZIP = 1
  BZIP2 = 2
  XY = 3


_compression_map = {
    Compression.GZIP: lambda x: gzip.open(x, mode='rt'),
    Compression.BZIP2: lambda x: bz2.open(x, mode='rt'),
    Compression.XY: lambda x: lzma.open(x, mode='rt')
}


class CompressionReader:
  """A utility class for wrapping a non-compressed or compressed file."""

  def __init__(self, f: FilePath, compression: Compression) -> None:
    """Creates a new compression reader, which is a reader for a
    non-compressed or compressed file.

    Args:
        f: the file that shall be wrapped by this reader.
        compression: the compression type of the file. It can also be
        `None`, which means that the file isn't compressed.
    """
    self._f = f
    self._compression = compression

  def __enter__(self) -> 'CompressionReader':
    if self._compression is None:
      self._source = open(self._f, 'rb')
    elif self._compression in _compression_map:
      self._source = _compression_map[self._compression](self._f)
    else:
      raise ValueError(
        f'the compression type "{self._compression.name}" isn\'t supported')
    return self

  def source(self) -> Iterable[str]:
    """Gets a TextIOWrapper for the specified file, which is an iterable
    sequence of lines.

    Returns:
        An iterable sequence of lines.
    """
    return self._source

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._source is not None:
      self._source.close()
