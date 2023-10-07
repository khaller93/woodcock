from typing import Hashable, Tuple, TypeVar

ID = Hashable

NodeLabel = TypeVar('NodeLabel', bound=ID)
PropertyLabel = TypeVar('PropertyLabel', bound=ID)
EdgeLabel = Tuple[NodeLabel, PropertyLabel, NodeLabel]

NodeID = TypeVar('NodeID', bound=ID)
PropertyID = TypeVar('PropertyID', bound=ID)
Edge = Tuple[NodeID, PropertyID, NodeID]
