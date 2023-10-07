"""This module defines abstract classes for a knowledge graph, which could
theoretically exist in any storage solution.

A `Graph` is a simple knowledge graph, which has a `GraphIndex` and a 
`GraphQueryEngine`. The `GraphIndex` maps the ID of entites (e.g. an URI/IRI)
from the external source to internal IDs (e.g. integer numbers). A
`GraphQueryEngine` can be used to perform simple queries on the knowledge graph.
The query interface is limited as the generation of corpora doesn't necessitate
anything more extensive.
"""

from typing import Tuple, Iterable, Generic, Union
from woodcock.graph.typing import NodeID, PropertyID, Edge, NodeLabel, \
    PropertyLabel

class GraphQueryEngine(Generic[NodeID, PropertyID]):
  """A serializable query engine over the complete graph."""

  def open(self) -> None:
    """Opens connection to the query engine, or does nothing."""
    pass

  def node_ids(self) -> Iterable[NodeID]:
    """Gets all nodes in the graph without duplicates.

    Returns:
        An iterable sequence of all nodes in the graph, whereas no
        duplicates are returned.
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def edge_type_ids(self) -> Iterable[PropertyID]:
    """Gets all edge type IDs in the graph without duplicates.

    Returns:
        An iterable sequence of all edge types in the graph, whereas no
        duplicates are returned.
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def e_in(self, subj_node: NodeID) -> Iterable[Edge]:
    """Gets all ingoing edges for the given subject node.

    Args:
        subj_node: ID of the node for which the ingoing edges shall be
        returned.
    Returns:
        An iterable sequence of all ingoing edges. The subject node will be
        on the object position of these edges. For the node with the ID
        `ex:bob`, this would for example be:
        `[('ex:alice', 'foaf:knows', 'ex:bob')]`
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def e_out(self, subj_node: NodeID) -> Iterable[Edge]:
    """Gets all outgoing edges for the given subject node.

    Args:
        subj_node: ID of the node for which the outgoing edges shall be
        returned.
    Returns:
        An iterable sequence of all outgoing edges. The subject node will be
        on the subject position of these edges. For the node with the ID
        `ex:bob`, this would for example be:
        `[('ex:bob', 'foaf:knows', 'ex:ash')]`
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def edges(self, *, subj_node: Union[NodeID, None] = None,
            edge_type: Union[PropertyID, None] = None,
            obj_node: Union[NodeID, None] = None) -> Iterable[Edge]:
    """Gets all the edges that match the given filter.

    Args:
        subj_node: ID of the node on the subject position or `None`, if no
        filter shall be applied on the subject.
        edge_type: ID of the edge type or `None`, if no filter shall be
        applied on the edge type.
        obj_node: ID of the node on the object position or `None`, if no
        filter shall be applied on the object.
    Returns:
        An iterable sequence of all matching edges. This method must not
        return `None` as a result, but an empty sequence, if no matching
        edges could be found.
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def node_count(self) -> int:
    """Gets the number of distinct nodes in the graph.

    Returns:
        The number of distinct nodes in the graph.
    Raises:
        IOError: An error occurred accessing the query engine.
    """
    raise NotImplementedError()

  def edge_type_count(self) -> int:
    """Gets the number of distinct edge types in the graph.

    Returns:
        The number of distinct edge types in the graph.
    Raises:
        IOError: An error occurred accessing the query engine.
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

  def close(self) -> None:
    """Closes connection to the query engine, frees up resources, or does
    nothing."""
    pass


class GraphIndex(Generic[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A serializable index for a graph."""

  def open(self) -> None:
    pass

  def node_id_for(self, node_label: NodeLabel) -> NodeID:
    """Gets the internal node ID for the node with the given original label.

    Returns:
        The internal node ID for the node with the given original label.
    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: The given node label is unknown.
    """
    return next(iter(self.node_ids_for([node_label])))

  def node_ids_for(self, node_labels: Iterable[NodeLabel]) -> Iterable[NodeID]:
    """Gets iterable sequence of internal node IDs for given original node
    labels.

    Returns:
        An iterable sequence of internal node IDs for the iterable sequence
        of original node labels.
    Raises:
        IOError: An error occurred accessing the query engine.
        ValueError: One of the given node labels is unknown.
    """
    raise NotImplementedError()

  def close(self) -> None:
    pass


class Graph(Generic[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A simple knowledge graph.

  A simple knowledge graph consists of nodes and directed edges between those
  nodes. Each node has a unique hashable ID such as an integer, or a string.
  An edge itself has a unique hashable ID as well, which must have the
  same datatype as node IDs. An edge puts two nodes into a specific
  relationship. However, it isn't possible to directly annotate edges with
  properties in contrast to labelled property graphs. Edges are always
  triples (i.e. <subj node id> <edge id> <obj node id>).
  """

  def index(self) -> GraphIndex[NodeLabel, NodeID, PropertyLabel, PropertyID]:
    """Gets a serializable index for this graph, which allows to get the
    internal ID for nodes as well as edge types given the original label.

    Returns:
        A serializable index for this graph.
    Raises:
        IOError: An error occurred creating the query engine.
    """
    raise NotImplementedError()

  def query_engine(self) -> GraphQueryEngine[NodeID, PropertyID]:
    """Gets a query engine for this graph.

    Returns:
        A serializable query engine for this graph.
    Raises:
        IOError: An error occurred creating the query engine.
    """
    raise NotImplementedError()


_ExtEdge = Tuple[NodeLabel, PropertyLabel, NodeLabel]


class EmbeddedGraph(Generic[NodeLabel, NodeID, PropertyLabel, PropertyID]):
  """A simple knowledge graph that is embedded into the executing Python
  process."""

  def import_data(self, data: Iterable[_ExtEdge]) -> None:
    pass
