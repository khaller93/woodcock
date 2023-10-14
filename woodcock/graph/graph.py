"""This module defines abstract classes for a knowledge graph, which could
theoretically exist in any storage solution.

A `Graph` is a simple knowledge graph, which has a `GraphIndex` and a
`GraphQueryEngine`. The `GraphIndex` maps the ID of entites (e.g. an URI/IRI)
from the external source to internal IDs (e.g. integer numbers). A
`GraphQueryEngine` can be used to perform simple queries on the knowledge graph.
The query interface is limited as the generation of corpora doesn't necessitate
anything more extensive.
"""

from typing import Tuple, Iterable, Generic, Generator, Union
from woodcock.graph.typing import NodeID, PropertyID, Edge, NodeLabel, \
    PropertyLabel


class GraphQueryEngine(Generic[NodeID, PropertyID]):
  """A serializable query engine over the complete graph."""

  def node_ids(self) -> Generator[NodeID, None, None]:
    """Gets all nodes in the graph without duplicates.

    Raises:
        IOError: An error occurred accessing the query engine.

    Yields:
        Generator[NodeID, None, None]: A sequence of all nodes in the
        graph, whereas no duplicates are returned.
    """
    raise NotImplementedError()

  def node_count(self) -> int:
    """Gets the number of distinct nodes in the graph.

    Raises:
        IOError: An error occurred accessing the query engine.

    Returns:
        int: The number of distinct nodes in the graph.
    """
    raise NotImplementedError()

  def property_ids(self) -> Generator[PropertyID, None, None]:
    """Gets all property type IDs in the graph without duplicates.

    Raises:
        IOError: An error occurred accessing the query engine.

    Yields:
        Generator[PropertyID, None, None]: A sequence of all property types in
        the graph, whereas no duplicates are returned.
    """
    raise NotImplementedError()

  def property_type_count(self) -> int:
    """Gets the number of distinct property types in the graph.

    Raises:
        IOError: An error occurred accessing the query engine.

    Returns:
        int: The number of distinct property types in the graph.
    """
    raise NotImplementedError()

  def e_in(self, subj_node: NodeID) -> Generator[Edge, None, None]:
    """Gets all ingoing edges for the given subject node.

    Args:
        subj_node (NodeID): ID of the node for which the ingoing edges shall be
        returned.

    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: If the subject node doesn't exist in the graph.

    Yields:
        Generator[Edge, None, None]: An iterable sequence of all ingoing edges.
        The subject node will be on the object position of these edges. For the
        node with the ID `ex:bob`, this would for example be:
        `[('ex:alice', 'foaf:knows', 'ex:bob')]`
    """
    raise NotImplementedError()

  def prop_in_dist(self, subj_node: NodeID) \
          -> Generator[Tuple[PropertyID, int], None, None]:
    """Gets the property distribution for the ingoing edges to the given
    subject node.

    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: If the subject node doesn't exist in the graph.

    Args:
        subj_node (NodeID): ID of the node for which the distribution shall be
        returned.

    Yields:
        Generator[Tuple[PropertyID, int], None, None]: The property
        distribution for the ingoing edges to the given subject node.
    """
    raise NotImplementedError()

  def e_out(self, subj_node: NodeID) -> Generator[Edge, None, None]:
    """Gets all outgoing edges for the given subject node.

    Args:
        subj_node (NodeID):  ID of the node for which the outgoing edges shall
        be returned.

    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: If the subject node doesn't exist in the graph.

    Yields:
        Generator[Edge, None, None]: An iterable sequence of all outgoing edges.
        The subject node will be on the subject position of these edges. For the
        node with the ID `ex:bob`, this would for example be:
        `[('ex:bob', 'foaf:knows', 'ex:ash')]`
    """
    raise NotImplementedError()

  def prop_out_dist(self, subj_node: NodeID) \
          -> Generator[Tuple[PropertyID, int], None, None]:
    """Gets the property distribution for the outgoing edges to the given
    subject node.

    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: If the subject node doesn't exist in the graph.

    Args:
        subj_node (NodeID): ID of the node for which the distribution shall be
        returned.

    Yields:
        Generator[Tuple[PropertyID, int], None, None]: The property
        distribution for the outgoing edges to the given subject node.
    """
    raise NotImplementedError()

  def edges(self, *, subj_node: Union[NodeID, None] = None,
            prop_type: Union[PropertyID, None] = None,
            obj_node: Union[NodeID, None] = None) \
          -> Generator[Edge, None, None]:
    """Gets all the edges that match the given filter.

    Args:
        subj_node (Union[NodeID, None], optional): ID of the node on the subject
        position or `None`, if no filter shall be applied on the subject.
        Defaults to None.
        prop_type (Union[PropertyID, None], optional): ID of the property
        type or `None`, if no filter shall be applied on the property type.
        Defaults to None.
        obj_node (Union[NodeID, None], optional): ID of the node on the object
        position or `None`, if no filter shall be applied on the object.
        Defaults to None.

    Raises:
        IOError: An error occurred accessing the query engine.

    Returns:
        Generator[Edge, None, None]: An iterable sequence of all matching edges.
        This method must not return `None` as a result, but an empty sequence,
        if no matching edges could be found.
    """
    raise NotImplementedError()

  def edge_count(self) -> int:
    """Gets the total number of edges in the graph.

    Returns:
        The number of total edges in the graph.
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def shutdown(self) -> None:
    """Shut the graph index down, and free up all resources."""
    pass


class GraphIndex(Generic[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A serializable index for a graph."""

  def node_id_for(self, node_label: NodeLabel) -> NodeID:
    """Gets the ID for the node with the given label.

    Args:
        node_label (NodeLabel): The label of the node for which the ID shall be
        returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: The given node label is unknown.

    Returns:
        NodeID: The ID for the node with the given label.
    """
    if node_label is None:
      raise ValueError('the node label must not be None')
    return next(iter(self.node_ids_for([node_label])))

  def node_ids_for(self, node_labels: Iterable[NodeLabel]) \
          -> Generator[NodeID, None, None]:
    """Gets sequence of node IDs for given node labels.

    Args:
        node_labels (Iterable[NodeLabel]): The sequence of node labels for which
        the IDs shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: One of the given node labels is unknown.

    Returns:
        Generator[NodeID]: A sequence of node IDs to the corresponding
        sequence of original node labels.
    """
    raise NotImplementedError()

  def node_label_for(self, node_id: NodeLabel) -> NodeLabel:
    """Gets the node label for the node with the given ID.

    Args:
        node_id (NodeLabel): The ID of the node for which the node label shall
        be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: The given node label is unknown.

    Returns:
        NodeLabel: The label for the node with the given ID.
    """
    if node_id is None:
      raise ValueError('the node ID must not be None')
    return next(iter(self.node_labels_for([node_id])))

  def node_labels_for(self, node_ids: Iterable[NodeID]) \
          -> Generator[NodeLabel, None, None]:
    """Gets sequence of node labels for the given node IDs.

    Args:
        node_ids (Iterable[NodeID]): The sequence of node IDs for which the node
        labels shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: One of the given node IDs is unknown.

    Yields:
        Generator[NodeLabel]: A sequence of node labels to the corresponding
        sequence of node IDs.
    """
    raise NotImplementedError()

  def property_id_for(self, property_label: PropertyLabel) -> PropertyID:
    """Gets the ID for the property with the given label.

    Args:
        property_label (PropertyLabel): The label of the property for which the
        ID shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: The given property label is unknown.

    Returns:
        PropertyID: The ID for the property with the given label.
    """
    if property_label is None:
      raise ValueError('the property label must not be None')
    return next(iter(self.property_ids_for([property_label])))

  def property_ids_for(self, property_labels: Iterable[PropertyLabel]) \
          -> Generator[PropertyID, None, None]:
    """Gets sequence of property IDs for given property labels.

    Args:
        property_labels (Iterable[PropertyLabel]): The sequence of property
        labels for which the IDs shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: One of the given property labels is unknown.

    Returns:
        Generator[PropertyID]: A sequence of property IDs to the corresponding
        sequence of original property labels.
    """
    raise NotImplementedError()

  def property_label_for(self, property_id: PropertyID) -> PropertyLabel:
    """Gets the property label for the property with the given ID.

    Args:
        property_id (PropertyID): The ID of the property for which the property
        label shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: The given property ID is unknown.

    Returns:
        PropertyLabel: The label for the property with the given ID.
    """
    if property_id is None:
      raise ValueError('the property ID must not be None')
    return next(iter(self.property_labels_for([property_id])))

  def property_labels_for(self, property_ids: Iterable[PropertyID]) \
          -> Generator[PropertyLabel, None, None]:
    """Gets sequence of property labels for the given property IDs.

    Args:
        property_ids (Iterable[PropertyID]): The sequence of property IDs for
        which the property labels shall be returned.

    Raises:
        IOError: An error occurred accessing the query index.
        ValueError: One of the given property IDs is unknown.

    Yields:
        Generator[PropertyLabel, None, None]: A sequence of property labels to
        the corresponding sequence of property IDs.
    """
    raise NotImplementedError()

  def shutdown(self) -> None:
    """Shut the graph index down, and free up all resources."""
    pass


class Graph(Generic[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A simple knowledge graph.

  A simple knowledge graph consists of nodes and directed edges between those
  nodes. Each node has a unique hashable ID such as an integer, or a string.
  An edge itself has a unique hashable ID as well. An edge puts two nodes into
  a specific relationship. However, it isn't possible to directly annotate edges
  with properties in contrast to labelled property graphs. Edges are always
  triples (i.e. `<subj node id> <edge id> <obj node id>`).
  """

  def index(self) -> GraphIndex[NodeLabel, NodeID, PropertyLabel, PropertyID]:
    """Gets a serializable index for this graph, which allows to get the ID for
    nodes as well as edge types given the original label.

    Raises:
        IOError: An error occurred creating the query engine.

    Returns:
        GraphIndex[NodeLabel, NodeID, PropertyLabel, PropertyID]: A serializable
        index for this graph.
    """
    raise NotImplementedError()

  def query_engine(self) -> GraphQueryEngine[NodeID, PropertyID]:
    """Gets a query engine for this graph.

    Raises:
        IOError: An error occurred creating the query engine.

    Returns:
        A serializable query engine for this graph.
    """
    raise NotImplementedError()


_ExtEdge = Tuple[NodeLabel, PropertyLabel, NodeLabel]


class EmbeddedGraph(Graph[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A simple knowledge graph that is embedded into the executing Python
  process."""

  def import_data(self, data: Iterable[_ExtEdge]) -> None:
    pass
