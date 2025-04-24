"""Unit testing reading and writting corpus files."""
import unittest
import tempfile

from woodcock.io.corpus import BinaryWriter, BinaryReader


class TestCorpusIO(unittest.TestCase):
  """Testing reading and writting corpus files."""

  def test_binary_writer_reader_must_lead_to_same_data(self):
    data = [(24, 1, 28, 2, 32), (48, 2, 24, 1, 24), (5, 1, 7, 1, 9)]
    with tempfile.NamedTemporaryFile() as test_f:
      # write compressed binary data
      with BinaryWriter(test_f.name) as writer:
        for sent in data:
          writer.write(sent)
      # read compressed binary data
      stored_data = []
      with BinaryReader(test_f.name) as reader:

        while True:
          sent = reader.read()
          if sent is None:
            break
          stored_data.append(sent)

      assert data == stored_data
