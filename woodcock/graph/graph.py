from abc import ABC, abstractmethod
from typing import TypeVar, Hashable, Tuple, Iterable

ID = Hashable
Edge = Tuple[ID, ID, ID]

_ID_T = TypeVar('_ID_T', bound=ID)
_Edge_T = Tuple[_ID_T, _ID_T, _ID_T]


class GraphQueryEngine(ABC):
    """A serializable query engine over the complete graph."""

    @abstractmethod
    def node_ids(self) -> Iterable[ID]:
        """Gets all nodes in the graph without duplicates.

        Returns:
            An iterable sequence of all nodes in the graph, whereas no
            duplicates are returned.
        Raises:
            IOError: An error occurred accessing the query engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def edge_ids(self) -> Iterable[ID]:
        """Gets all edge IDs in the graph without duplicates.

        Returns:

        Raises:
            IOError: An error occurred accessing the query engine.
        """
        raise NotImplementedError()

    @abstractmethod
    def out(self, subj_node: _ID_T) -> Iterable[_Edge_T]:
        """Gets all outgoing edges for the given subject node.

        Args:
            subj_node:
        Returns:

        Raises:
            IOError: An error occurred accessing the query engine.
        """
        raise NotImplementedError()


class EdgeImporter(ABC):
    """"""


class Graph(ABC):
    """A simple knowledge graph.

    A simple knowledge graph consists of nodes and directed edges between those
    nodes. Each node has a unique hashable ID such as an integer, or a string.
    An edge itself has a unique hashable ID as well, which must have the
    same datatype as node IDs. An edge puts two nodes into a specific
    relationship. However, it isn't possible to directly annotate edges with
    properties in contrast to labelled property graphs. Edges are always
    triples (i.e. <subj node id> <edge id> <obj node id>).
    """

    @abstractmethod
    def query_engine(self) -> GraphQueryEngine:
        """Gets a query engine for this graph.

        Returns:

        Raises:
            IOError: An error occurred creating the query engine.
        """
        raise NotImplementedError()


class EmbeddedGraph(Graph):
    """A simple knowledge graph that is embedded into this application."""

    def import_edges(self, edges: Iterable[Edge]) -> None:
        """"""
        with self._importer as importer:
            for edge in edges:
                pass

    @property
    @abstractmethod
    def _importer(self) -> EdgeImporter:
        """

        """
        raise NotImplementedError()
