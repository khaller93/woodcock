"""Abstract unit testing template for knwoledge graph solutions."""

from typing import Hashable

from pytest import raises

from woodcock.graph.graph import EmbeddedGraph, Graph

test_data = [
    ('pokemon/eevee', 'foundIn', 'Habitat:Urban'),
    ('pokemon/eevee', 'inEggGroup', 'EggGroup:Field'),
    ('pokemon/eevee', 'hasShape', 'Shape:Quadruped'),
    ('pokemon/eevee', 'hasType', 'PokéType:Normal'),
    ('pokemon/meowth', 'foundIn', 'Habitat:Urban'),
    ('pokemon/meowth', 'hasShape', 'Shape:Quadruped'),
    ('pokemon/meowth', 'hasType', 'PokéType:Dark'),
    ('pokemon/meowth', 'hasType', 'PokéType:Normal'),
    ('pokemon/meowth', 'hasType', 'PokéType:Steel'),
    ('pokemon/meowth', 'inEggGroup', 'EggGroup:Field'),
    ('pokemon/meowth', 'mayHaveAbility', 'ability/pickup'),
    ('pokemon/meowth', 'mayHaveAbility', 'ability/technician'),
    ('pokemon/meowth', 'mayHaveAbility', 'ability/unnerve'),
    ('pokemon/meowth', 'mayHaveHiddenAbility', 'ability/unnerve'),
    ('pokemon/meowth', 'isAbleToApply', 'move/pay-day'),
    ('pokemon/meowth', 'isAbleToApply', 'move/scratch'),
    ('pokemon/meowth', 'isAbleToApply', 'move/bite'),
    ('pokemon/meowth', 'isAbleToApply', 'move/growl'),
    ('pokemon/meowth', 'isAbleToApply', 'move/screech'),
    ('pokemon/meowth', 'isAbleToApply', 'move/fury-swipes'),
    ('pokemon/meowth', 'isAbleToApply', 'move/slash'),
    ('pokemon/jigglypuff', 'foundIn', 'Habitat:Grassland'),
    ('pokemon/jigglypuff', 'inEggGroup', 'EggGroup:Fairy'),
    ('pokemon/jigglypuff', 'hasShape', 'Shape:Humanoid'),
    ('pokemon/jigglypuff', 'hasType', 'PokéType:Fairy'),
    ('pokemon/jigglypuff', 'hasType', 'PokéType:Normal'),
]

all_node_labels = list({s for s, _, _ in test_data}
                       .union({o for _, _, o in test_data}))


def all_edges(i, *, subj=None, pred=None, obj=None):
  return [(i.node_id_for(s), i.property_id_for(p), i.node_id_for(o))
          for s, p, o in test_data if (subj is None or subj == s) and
          (pred is None or pred == p) and (obj is None or obj == o)]


all_property_labels = list({p for _, p, _ in test_data})


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
      db.import_data(test_data)
    return db

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


class GraphIndexTesting(GraphTesting):
  """Unit testing for graph index."""

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
  """Unit testing for query engine."""

  def test_query_engine_must_return_not_none_query_engine(self):
    engine = self.create_data_db().query_engine()
    assert engine is not None

  def test_node_ids_must_return_all_distinct_node_ids(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    node_ids = engine.node_ids()
    assert node_ids is not None
    node_id_list = [nid for nid in node_ids]
    assert len(node_id_list) == len(all_node_labels)
    node_labels = index.node_labels_for(node_id_list)
    assert sorted([l for l in node_labels]) == sorted(all_node_labels)

  def test_node_ids_must_return_empty_sequence_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    node_ids = engine.node_ids()
    assert node_ids is not None
    assert [nid for nid in node_ids] == []

  def test_node_count_must_return_0_when_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    assert engine.node_count() == 0

  def test_node_count_must_return_proper_count_when_complete_graph(self):
    engine = self.create_data_db().query_engine()
    assert engine.node_count() == len(all_node_labels)

  def test_property_ids_must_return_all_distinct_property_ids(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    prop_ids = engine.property_ids()
    assert prop_ids is not None
    prop_id_list = [pid for pid in prop_ids]
    assert len(prop_id_list) == len(all_property_labels)
    prop_labels = index.property_labels_for(prop_id_list)
    assert sorted([l for l in prop_labels]) == sorted(all_property_labels)

  def test_property_ids_must_return_empty_sequence_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    prop_ids = engine.property_ids()
    assert prop_ids is not None
    assert [pid for pid in prop_ids] == []

  def test_property_type_count_must_return_0_when_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    assert engine.property_type_count() == 0

  def test_property_type_count_must_return_cor_count_when_complete_graph(self):
    engine = self.create_data_db().query_engine()
    assert engine.property_type_count() == len(all_property_labels)

  def test_e_in_must_raise_value_error_when_id_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    with raises(ValueError):
      next(iter(engine.e_in(self.get_unknown_node_id())))

  def test_e_in_must_raise_value_error_when_id_unknown_for_graph(self):
    engine = self.create_data_db().query_engine()
    with raises(ValueError):
      next(iter(engine.e_in(self.get_unknown_node_id())))

  def test_e_in_must_return_ingoing_edges_when_id_known_for_graph(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.e_in(index.node_id_for('PokéType:Normal'))
    assert edges is not None
    assert sorted([e for e in edges]) == sorted(
        all_edges(index, obj='PokéType:Normal'))

  def test_prop_in_dist_must_raise_value_error_when_id_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    with raises(ValueError):
      next(iter(engine.prop_in_dist(self.get_unknown_node_id())))

  def test_prop_in_dist_must_raise_value_error_when_id_unknown_for_graph(self):
    engine = self.create_data_db().query_engine()
    with raises(ValueError):
      next(iter(engine.prop_in_dist(self.get_unknown_node_id())))

  def test_prop_in_dist_must_return_prop_dist_when_id_known_for_graph(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    dist = engine.prop_in_dist(index.node_id_for('PokéType:Normal'))
    assert dist is not None
    dist_list = [e for e in dist]
    assert len(dist_list) == 1
    assert dist_list[0] == (index.property_id_for('hasType'), 3)

  def test_e_out_must_raise_value_error_when_id_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    with raises(ValueError):
      next(iter(engine.e_out(self.get_unknown_node_id())))

  def test_e_out_must_raise_value_error_when_id_unknown_for_graph(self):
    engine = self.create_data_db().query_engine()
    with raises(ValueError):
      next(iter(engine.e_out(self.get_unknown_node_id())))

  def test_e_out_must_return_outgoing_edges_when_id_known_for_graph(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.e_out(index.node_id_for('pokemon/jigglypuff'))
    assert edges is not None
    assert sorted([e for e in edges]) == sorted(
        all_edges(index, subj='pokemon/jigglypuff'))

  def test_prop_out_dist_must_raise_value_error_when_id_for_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    with raises(ValueError):
      next(iter(engine.prop_out_dist(self.get_unknown_node_id())))

  def test_prop_out_dist_must_raise_value_error_when_id_unknown_for_graph(self):
    engine = self.create_data_db().query_engine()
    with raises(ValueError):
      next(iter(engine.prop_out_dist(self.get_unknown_node_id())))

  def test_prop_out_dist_must_return_prop_dist_when_id_known_for_graph(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    dist = engine.prop_out_dist(index.node_id_for('pokemon/meowth'))
    assert dist is not None
    dist_list = [e for e in dist]
    assert len(dist_list) == 7
    assert (index.property_id_for('isAbleToApply'), 7) in dist_list
    assert (index.property_id_for('hasType'), 3) in dist_list
    assert (index.property_id_for('mayHaveAbility'), 3) in dist_list
    assert (index.property_id_for('mayHaveHiddenAbility'), 1) in dist_list
    assert (index.property_id_for('inEggGroup'), 1) in dist_list
    assert (index.property_id_for('hasShape'), 1) in dist_list
    assert (index.property_id_for('foundIn'), 1) in dist_list

  def test_edges_must_return_empty_list_when_empty_graph(self):
    engine = self.create_new_kg().query_engine()
    edges = engine.edges()
    assert edges is not None
    edge_list = [e for e in edges]
    assert edge_list == []

  def test_edges_must_return_empty_list_when_empty_graph_with_unknow_subj(self):
    engine = self.create_new_kg().query_engine()
    edges = engine.edges(subj_node=self.get_unknown_node_id())
    assert edges is not None
    edge_list = [e for e in edges]
    assert edge_list == []

  def test_edges_must_return_empty_list_when_empty_graph_with_unknow_pred(self):
    engine = self.create_new_kg().query_engine()
    edges = engine.edges(prop_type=self.get_unknown_property_id())
    assert edges is not None
    edge_list = [e for e in edges]
    assert edge_list == []

  def test_edges_must_return_all_edges_when_graph_with_no_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges()
    assert edges is not None
    edge_list = [e for e in edges]
    assert sorted(edge_list) == sorted(all_edges(index))

  def test_edges_count_must_return_total_count_when_graph_with_no_filter(self):
    engine = self.create_data_db().query_engine()
    cnt = engine.edges_count()
    assert cnt == len(test_data)

  def test_edges_must_return_edges_for_eevee_when_graph_with_subj_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges(subj_node=index.node_id_for('pokemon/eevee'))
    assert edges is not None
    edge_list = [e for e in edges]
    assert sorted(edge_list) == sorted(all_edges(index, subj='pokemon/eevee'))

  def test_edges_count_must_return_jiggy_out_count_when_subj_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    cnt = engine.edges_count(subj_node=index.node_id_for('pokemon/jigglypuff'))
    assert cnt == len(all_edges(index, subj='pokemon/jigglypuff'))

  def test_edges_must_return_edges_normal_type_when_graph_with_obj_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges(obj_node=index.node_id_for('PokéType:Normal'))
    assert edges is not None
    edge_list = [e for e in edges]
    assert sorted(edge_list) == sorted(all_edges(index, obj='PokéType:Normal'))

  def test_edges_count_must_return_normal_type_in_count_when_obj_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    cnt = engine.edges_count(obj_node=index.node_id_for('PokéType:Normal'))
    assert cnt == len(all_edges(index, obj='PokéType:Normal'))

  def test_edges_must_return_edges_ability_when_graph_with_pred_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges(prop_type=index.property_id_for('isAbleToApply'))
    assert edges is not None
    edge_list = [e for e in edges]
    assert sorted(edge_list) == sorted(all_edges(index, pred='isAbleToApply'))

  def test_edges_count_must_return_ability_edges_count_when_pred_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    cnt = engine.edges_count(prop_type=index.property_id_for('isAbleToApply'))
    assert cnt == len(all_edges(index, pred='isAbleToApply'))

  def test_edges_must_return_edges_eevee_shape_when_multi_filter(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges(subj_node=index.node_id_for('pokemon/eevee'),
                         prop_type=index.property_id_for('hasShape'))
    assert edges is not None
    edge_list = [e for e in edges]
    assert sorted(edge_list) == sorted(all_edges(index, pred='hasShape',
                                                 subj='pokemon/eevee'))

  def test_edges_must_return_empty_list_when_subj_and_obj_not_linked(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    edges = engine.edges(subj_node=index.node_id_for('pokemon/eevee'),
                         obj_node=index.node_id_for('pokemon/jigglypuff'))
    assert not list(edges)

  def test_edges_count_must_return_zero_when_subj_and_obj_not_linked(self):
    engine = self.create_data_db().query_engine()
    index = self.create_data_db().index()
    cnt = engine.edges_count(subj_node=index.node_id_for('pokemon/eevee'),
                             obj_node=index.node_id_for('pokemon/jigglypuff'))
    assert cnt == 0

