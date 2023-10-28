"""Type definitions for corpus generation."""

from typing import Tuple, TypeVar


Word = TypeVar('Word')

Sentence = Tuple[Word, ...]
