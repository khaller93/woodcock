"""A module for utility classes to import data."""

import bz2
import gzip
import lzma
from enum import Enum
from io import IOBase
from os.path import exists

from woodcock.io.typing import FilePath


class Compression(Enum):
  """Type of the compression"""
  GZIP = 1
  BZIP2 = 2
  XZ = 3


_compression_map = {
    Compression.GZIP: lambda f, m, enc: gzip.open(f, mode=m, encoding=enc),
    Compression.BZIP2: lambda f, m, enc: bz2.open(f, mode=m, encoding=enc),
    Compression.XZ: lambda f, m, enc: lzma.open(f, mode=m, encoding=enc)
}


class _CompressedFile:
  """A utility class for wrapping a non-compressed or compressed file."""

  def __init__(self, fp: FilePath, mode: str, compression: Compression,
               encoding: str) -> None:
    """Wraps a (compressed) file, and the caller can expect it to be
    uncompressed when reading.

    Args:
        fp (FilePath): Path to the file that shall be wrapped.
        mode (str): Describes the mode in which the file shall be opened.
        compression (Compression): Compression type of the file.
        encoding (str):  Name of the encoding used to decode or encode the file.
        This should only be used in text mode.

    Raises:
        ValueError: Received an argument that has an inappropriate value.
    """
    if not (fp and exists(fp)):
      raise FileNotFoundError('filepath must be specified')
    if not (mode == 'text' or mode == 'bytes'):
      raise ValueError(f'mode "{mode}" isn\'t ssupported')
    if mode != 'text' and encoding is not None:
      raise ValueError('the encoding must only be set in "text" mode')
    if compression and compression not in _compression_map:
      raise ValueError(
          f'the compression type "{compression}" isn\'t supported')

    self._fp = fp
    self._m = mode
    self._compr = compression
    self._enc = encoding

  def __enter__(self) -> IOBase:
    mode = 'rt' if self._m == 'text' else 'rb'
    if self._compr:
      self._f = _compression_map[self._compr](self._fp, mode, self._enc)
      return self._f
    else:
      self._f = open(self._fp, mode, encoding=self._enc)
      return self._f

  def __exit__(self, exc_type, exc_val, exc_tb):
    if self._f:
      self._f.close()


def copen(fp: FilePath, mode: str = 'text', *, compression: Compression,
          encoding: str = None) -> '_CompressedFile':
  """Opens the given file in a specified mode (`text` or `bytes`).

  Args:
      fp (FilePath): Path to the file that shall be opened.
      mode (str, optional): Describes the mode in which the file shall be
      opened. It can either be 'text' or 'bytes' mode. Defaults to 'text'.
      compression (Compression, optional): Compression type of the file. It
      can also be None, which means that the file isn't compressed. Defaults
      to None.
      encoding (str, optional): Name of the encoding used to decode or encode
      the file. This should only be used in 'text' mode. In text mode, if
      encoding is not specified the encoding used is platform-dependent, i.e.
      `locale.getencoding()` is called to get the current locale encoding. In
      'bytes' mode, the encoding must be None. Defaults to None.

  Raises:
      ValueError: Received an argument that has an inappropriate value.
  """
  return _CompressedFile(fp=fp, mode=mode, compression=compression,
                         encoding=encoding)
