import copy
import math
from typing import Any, Dict, List, Set

import numpy as np
from metagraph.plugins.core.types import Graph
from metagraph.plugins.core.wrappers import GraphWrapper

from katana.property_graph import PropertyGraph


class KatanaGraph(GraphWrapper, abstract=Graph):
    def __init__(
        self,
        pg_graph,
        is_weighted=True,
        edge_weight_prop_name="value",
        is_directed=True,
        node_weight_index=None,
        node_dtype=None,
        edge_dtype="int",
        has_neg_weight=False
    ):
        super().__init__()
        self._assert_instance(pg_graph, PropertyGraph)
        self.value = pg_graph
        self.is_weighted = is_weighted
        self.edge_weight_prop_name = edge_weight_prop_name
        self.is_directed = is_directed
        self.node_weight_index = node_weight_index
        self.node_dtype = node_dtype
        self.edge_dtype = edge_dtype
        self.has_neg_weight = has_neg_weight

    def copy(self):
        return KatanaGraph(copy.deepcopy(self.value), self.is_weighted, self.is_directed)

    class TypeMixin:
        @classmethod
        def _compute_abstract_properties(cls, obj, props: Set[str], known_props: Dict[str, Any]) -> Dict[str, Any]:
            ret = known_props.copy()
            # fast props
            for prop in {
                "is_directed",
                "node_type",
                "node_dtype",
                "edge_type",
                "edge_dtype",
                "edge_has_negative_weights",
            } - ret.keys():
                if prop == "is_directed":
                    ret[prop] = obj.is_directed
                if prop == "node_type":
                    ret[prop] = "set" if obj.node_weight_index is None else "map"
                if prop == "node_dtype":
                    ret[prop] = None if obj.node_weight_index is None else obj.node_dtype
                if prop == "edge_type":
                    ret[prop] = "map" if obj.is_weighted else "set"
                if prop == "edge_dtype":
                    ret[prop] = obj.edge_dtype if obj.is_weighted else None
                if prop == "edge_has_negative_weights":
                    ret[prop] = obj.has_neg_weight #TODO(pengfei): cover the neg-weight case, and add neg-weight test cases.
            return ret

        @classmethod
        def assert_equal(cls, obj1, obj2, aprops1, aprops2, cprops1, cprops2, *, rel_tal=1e-9, abs_tol=0.0):
            assert aprops1 == aprops2, f"proterty mismatch: {aprops1} != {aprops2}"
            pg1 = obj1.value
            pg2 = obj2.value
            assert pg1 == pg2, f"the two graphs does not match"
