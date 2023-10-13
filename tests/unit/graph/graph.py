"""Abstract unit testing template for knwoledge graph solutions."""

import csv

from os.path import dirname, join
from typing import Iterable, Tuple, Hashable

from pytest import raises

from woodcock.graph.graph import EmbeddedGraph, Graph


def read_csv_file_from_resources(name: str) -> Iterable[Tuple[str, str, str]]:
  """reads test data from a CSV file in the resources folder, which is stored
  under the given name.

  Args:
      name (str): name of the CSV file from which the test data shall be read.

  Returns:
      Iterable[Tuple[str, str, str]]: a sequence of triples.
  """
  data_file = join(dirname(__file__), 'resources', name)
  with open(data_file, 'r', encoding='utf-8') as f:
    reader = csv.reader(f, delimiter=',')
    return [tuple(line) for line in reader]


# Meowth dataset:
# +----------------+----------------------+-----------------------------------+
# |      subj      |         pred         |                obj                |
# +----------------+----------------------+-----------------------------------+
# | pokemon/meowth | describedInPokédex   | pokedex/national/entry/52         |
# | pokemon/meowth | describedInPokédex   | pokedex/kanto/entry/52            |
# | pokemon/meowth | describedInPokédex   | pokedex/original-johto/entry/136  |
# | pokemon/meowth | describedInPokédex   | pokedex/updated-johto/entry/138   |
# | pokemon/meowth | describedInPokédex   | pokedex/conquest-gallery/entry/58 |
# | pokemon/meowth | foundIn              | Habitat:Urban                     |
# | pokemon/meowth | hasColour            | dbpedia.org/resource/Yellow       |
# | pokemon/meowth | hasShape             | Shape:Quadruped                   |
# | pokemon/meowth | hasType              | PokéType:Dark                     |
# | pokemon/meowth | hasType              | PokéType:Normal                   |
# | pokemon/meowth | hasType              | PokéType:Steel                    |
# | pokemon/meowth | inEggGroup           | EggGroup:Field                    |
# | pokemon/meowth | mayHaveAbility       | ability/pickup                    |
# | pokemon/meowth | mayHaveAbility       | ability/technician                |
# | pokemon/meowth | mayHaveAbility       | ability/unnerve                   |
# | pokemon/meowth | mayHaveHiddenAbility | ability/unnerve                   |
# | pokemon/meowth | hasHeight            | pokemon/meowth/height/quantity    |
# | pokemon/meowth | hasWeight            | pokemon/meowth/weight/quantity    |
# | pokemon/meowth | isAbleToApply        | move/pay-day                      |
# | pokemon/meowth | isAbleToApply        | move/scratch                      |
# | pokemon/meowth | isAbleToApply        | move/bite                         |
# | pokemon/meowth | isAbleToApply        | move/growl                        |
# | pokemon/meowth | isAbleToApply        | move/screech                      |
# | pokemon/meowth | isAbleToApply        | move/fury-swipes                  |
# | pokemon/meowth | isAbleToApply        | move/slash                        |
# +----------------+----------------------+-----------------------------------+

all_node_labels = [
    'pokemon/meowth',
    'pokedex/national/entry/52',
    'pokedex/kanto/entry/52',
    'pokedex/original-johto/entry/136',
    'pokedex/updated-johto/entry/138',
    'pokedex/conquest-gallery/entry/58',
    'Habitat:Urban',
    'dbpedia.org/resource/Yellow',
    'Shape:Quadruped',
    'PokéType:Dark',
    'PokéType:Normal',
    'PokéType:Steel',
    'EggGroup:Field',
    'ability/pickup',
    'ability/technician',
    'ability/unnerve',
    'pokemon/meowth/height/quantity',
    'move/pay-day',
    'move/scratch',
    'move/bite',
    'move/growl',
    'move/screech',
    'move/fury-swipes',
    'move/slash'
]

all_property_labels = [
    'describedInPokédex',
    'foundIn',
    'hasColour',
    'hasShape',
    'hasType',
    'inEggGroup',
    'mayHaveAbility',
    'mayHaveHiddenAbility',
    'hasHeight',
    'isAbleToApply'
]


class GraphTesting:
  """Abstract unit testing class for graph index and query."""

  def create_new_kg(self) -> Graph:
    """creates a new KG to test.

    Raises:
        NotImplementedError: if it hasn't been implemented by the test subclass.

    Returns:
        Graph: a new KG to test.
    """
    raise NotImplementedError()

  def create_data_db(self) -> Graph:
    """creates a new KG with data to test.

    Returns:
        EmbeddedGraph: a new KG with data to test.
    """
    db = self.create_new_kg()
    if isinstance(db, EmbeddedGraph):
      db.import_data(read_csv_file_from_resources('meowth.csv'))
    return db


class GraphIndexTesting(GraphTesting):
  """Unit testing for graph index."""

  def get_unknown_node_id(self) -> Hashable:
    """Gets a node ID representation that is unknown to the test data db.

    Returns:
        Hashable: a node ID representation that is unknown to the test data db.
    """
    pass

  def get_unknown_property_id(self) -> Hashable:
    """Gets a property ID representation that is unknown to the test data db.

    Returns:
        Hashable: a property ID representation that is unknown to the test
        data db.
    """
    pass

  def test_index_must_return_not_none_index(self):
    assert self.create_new_kg().index() is not None

  def test_node_id_for_must_raise_value_error_if_node_label_is_none(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_id_for(None)

  def test_node_id_for_must_raise_value_error_if_node_label_is_empty(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_id_for('')

  def test_node_id_for_must_raise_value_error_if_node_label_is_unknown(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_id_for('pokemon/raichu')

  def test_node_label_for_must_raise_value_error_if_node_id_is_none(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_label_for(None)

  def test_node_label_for_must_raise_value_error_if_node_id_is_unknown(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_label_for(self.get_unknown_node_id())

  def test_node_id_for_and_label_for_must_return_label_again(self):
    index = self.create_data_db().index()
    meowth_id = index.node_id_for('pokemon/meowth')
    assert meowth_id is not None
    meowth_label = index.node_label_for(meowth_id)
    assert meowth_label == 'pokemon/meowth'

  def test_node_ids_and_label_for_must_return_labels_again(self):
    index = self.create_data_db().index()
    ids = index.node_ids_for(all_node_labels)
    assert ids is not None
    ids_list = [id for id in ids]
    assert len(ids_list) == len(all_node_labels)
    labels = index.node_labels_for(ids_list)
    assert [l for l in labels] == all_node_labels

  def test_prop_id_for_must_raise_value_error_if_prob_label_is_none(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.property_id_for(None)

  def test_prop_id_for_must_raise_value_error_if_prob_label_is_empty(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.property_id_for('')

  def test_prop_id_for_must_raise_value_error_if_prob_label_is_unknown(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.property_id_for('hasUnknown')

  def test_prop_label_for_must_raise_value_error_if_prop_id_is_none(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_label_for(None)

  def test_prop_label_for_must_raise_value_error_if_prop_id_is_unknown(self):
    index = self.create_data_db().index()
    with raises(ValueError):
      index.node_label_for(self.get_unknown_property_id())

  def test_prop_id_for_and_label_for_must_return_label_again(self):
    index = self.create_data_db().index()
    move_id = index.property_id_for('isAbleToApply')
    assert move_id is not None
    move_label = index.property_label_for(move_id)
    assert move_label == 'isAbleToApply'

  def test_prop_ids_and_label_for_must_return_labels_again(self):
    index = self.create_data_db().index()
    ids = index.property_ids_for(all_property_labels)
    assert ids is not None
    ids_list = [id for id in ids]
    assert len(ids_list) == len(all_property_labels)
    labels = index.property_labels_for(ids_list)
    assert [l for l in labels] == all_property_labels


class GraphQueryTesting(GraphTesting):

  def test_nothing(self):
    pass
