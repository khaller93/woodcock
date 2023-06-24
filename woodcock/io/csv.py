import bz2
import csv
import gzip
import lzma
from enum import Enum
from os import PathLike
from typing import Iterable, Union

from woodcock.graph.typing import Edge

FilePath = Union[str, PathLike]


class Compression(Enum):
    GZIP = 1
    BZIP2 = 2
    XY = 3


_compression_map = {
    Compression.GZIP: lambda x: gzip.open(x, mode='rt'),
    Compression.BZIP2: lambda x: bz2.open(x, mode='rt'),
    Compression.XY: lambda x: lzma.open(x, mode='rt')
}


class _CompressionReader:
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

    def __enter__(self) -> '_CompressionReader':
        if self._compression is None:
            self._source = open(self._f, 'r')
        elif self._compression in _compression_map:
            self._source = _compression_map[self._compression](self._f)
        else:
            raise ValueError('the compression type "%s" isn\'t supported'
                             % self._compression.name)
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
    with _CompressionReader(f, compression) as reader:
        reader = csv.reader(reader.source(), delimiter=delimiter)
        it = iter(reader)
        try:
            if skip_header:
                next(it)
            while True:
                row = next(it)
                if len(row) != 3:
                    raise ValueError(
                        'edges must have exactly three columns, but has %d'
                        % len(row)
                    )
                yield row[0], row[1], row[2]
        except StopIteration:
            pass
