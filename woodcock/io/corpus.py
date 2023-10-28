"""A module to read and write corpus files."""
from typing import Generic

from lz4.frame import open as lz4_open

from woodcock.corpus.typing import Word, Sentence
from woodcock.io.typing import FilePath


class Reader(Generic[Word]):
  """A reader of sentences in a corpus.

    Args:
      Generic (_type_): the data type of words in the sentences.
  """

  def read(self) -> Sentence:
    """Reads the next sentence from the corpus.

    Returns:
        Sentence: the next sentence from the corpus, or `None`, if the end of
        the corpus has been reached.
    """
    raise NotImplementedError()

  def close(self) -> None:
    """Closes the reader."""
    pass


class Writer(Generic[Word]):
  """A writer of sentences to a corpus.

  Args:
      Generic (_type_): the data type of words in the sentences.
  """

  def write(self, sentence: Sentence) -> None:
    """Writes the given sentence to the corpus.

    Args:
        sentence (Sentence): the list of words to write to the corpus. It must
        not be `None`.
    """
    raise NotImplementedError()

  def close(self) -> None:
    """Flush and close the writer."""
    pass


_word_byte_size = 5


class BinaryReader(Reader[int]):
  """A reader of sentences from a LZ4-compressed binary file, which works only
  on sentences with integer words."""

  def __init__(self, fp: FilePath) -> None:
    """Initializes a new binary reader from the specified file.

    Args:
        fp (FilePath): path to file that stores the corpus in binary format.
    """
    super().__init__()
    self._f = lz4_open(fp, mode='rb')

  def __enter__(self) -> 'BinaryReader':
    return self

  def read(self) -> Sentence:
    sentence = []
    while True:
      b_arr = self._f.read(size=_word_byte_size)
      if not b_arr:
        return None
      word = int.from_bytes(b_arr, byteorder='big', signed=False)
      if word != 0:
        sentence.append(word - 1)
      else:
        break
    return tuple(sentence)

  def __exit__(self, typ, value, tb) -> None:
    if self._f:
      self._f.close()

  def close(self) -> None:
    self._f.close()


class BinaryWriter(Writer[int]):
  """A writer of sentences to a LZ4-compressed binary file, which works only on
  sentences with integer words."""

  max_word_value = (1 << (_word_byte_size * 8 + 1)) - 1

  def __init__(self, fp: FilePath) -> None:
    """Initializes a new binary writer to the specified file.

    Args:
        fp (FilePath): path to file that stores the corpus in binary format.
    """
    super().__init__()
    self._f = lz4_open(fp, mode='wb')

  def __enter__(self) -> 'BinaryWriter':
    return self

  def write(self, sentence: Sentence) -> None:
    b = bytearray()
    for word in sentence:
      if word < 0:
        raise ValueError(f'The value must be positive, but was {word}')
      if word >= self.max_word_value:
        raise ValueError(f'The value {word} is above 5 Byte limit')
      b += (int(word + 1).to_bytes(length=_word_byte_size, byteorder='big',
                                   signed=False))
    b += int(0).to_bytes(length=_word_byte_size, byteorder='big', signed=False)
    self._f.write(b)

  def __exit__(self, typ, value, tb) -> None:
    if self._f:
      self._f.close()

  def close(self) -> None:
    self._f.close()
