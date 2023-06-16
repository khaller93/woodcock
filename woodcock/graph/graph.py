from typing import TypeVar, Hashable, Tuple, Iterable, Protocol

ID = Hashable

_R = TypeVar('_R', bound=ID)
_E = TypeVar('_E', bound=ID)
_T = Tuple[_R, _E, _R]


class GraphQueryEngine(Protocol[_R, _E]):
    """A serializable query engine over the complete graph."""

    def node_ids(self) -> Iterable[_R]:
        """Gets all nodes in the graph without duplicates.

        Returns:
            An iterable sequence of all nodes in the graph, whereas no
            duplicates are returned.
        Raises:
            IOError: An error occurred accessing the query engine.
        """
        raise NotImplementedError()

    def edge_type_ids(self) -> Iterable[_E]:
        """Gets all edge type IDs in the graph without duplicates.

        Returns:
            An iterable sequence of all edge types in the graph, whereas no
            duplicates are returned.
        Raises:
            IOError: An error occurred accessing the query engine.
        """
        raise NotImplementedError()

    def e_in(self, subj_node: _R) -> Iterable[_T]:
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

    def e_out(self, subj_node: _R) -> Iterable[_T]:
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

    def edges(self, subj_node: _R | None,
              edge_type: _E | None,
              obj_node: _R | None) -> Iterable[_T]:
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


class EdgeImporter:
    """"""


class Graph:
    """A simple knowledge graph.

    A simple knowledge graph consists of nodes and directed edges between those
    nodes. Each node has a unique hashable ID such as an integer, or a string.
    An edge itself has a unique hashable ID as well, which must have the
    same datatype as node IDs. An edge puts two nodes into a specific
    relationship. However, it isn't possible to directly annotate edges with
    properties in contrast to labelled property graphs. Edges are always
    triples (i.e. <subj node id> <edge id> <obj node id>).
    """

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
    def _importer(self) -> EdgeImporter:
        """

        """
        raise NotImplementedError()
