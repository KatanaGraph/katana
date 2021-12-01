from collections import OrderedDict

import metagraph as mg
import networkx as nx
import numpy as np
import pyarrow
from metagraph import translator
from metagraph.plugins.networkx.types import NetworkXGraph
from scipy.sparse import csr_matrix

from katana.local.import_data import from_csr

from .types import KatanaGraph


@translator
def networkx_to_katanagraph(x: NetworkXGraph, **props) -> KatanaGraph:
    aprops = NetworkXGraph.Type.compute_abstract_properties(x, {"node_dtype", "node_type", "edge_type", "is_directed"})
    is_weighted = aprops["edge_type"] == "map"
    # get the edge list directly from the NetworkX Graph
    elist_raw = list(x.value.edges(data=True))
    # sort the eddge list and node list
    if aprops["is_directed"]:
        elist = sorted(elist_raw, key=lambda each: (each[0], each[1]))
    else:
        inv_elist = [(each[1], each[0], each[2]) for each in elist_raw]
        elist = sorted(elist_raw + inv_elist, key=lambda each: (each[0], each[1]))
    nlist = sorted(list(x.value.nodes(data=True)), key=lambda each: each[0])
    # build the CSR format from the edge list (weight, (src, dst))
    row = np.array([each_edge[0] for each_edge in elist])
    col = np.array([each_edge[1] for each_edge in elist])
    data = np.array([each_edge[2]["weight"] for each_edge in elist])
    csr = csr_matrix((data, (row, col)), shape=(len(nlist), len(nlist)))
    # call the katana api to build a Graph (unweighted) from the CSR format
    # noting that the first 0 in csr.indptr is excluded
    pg = from_csr(csr.indptr[1:], csr.indices)
    # add the edge weight as a new property
    t = pyarrow.table(dict(value_from_translator=data))
    pg.add_edge_property(t)
    # use the metagraph's Graph warpper to wrap the katana.local.Graph
    return KatanaGraph(
        pg_graph=pg,
        is_weighted=is_weighted,
        edge_weight_prop_name="value_from_translator",
        is_directed=aprops["is_directed"],
        node_weight_index=0,
    )


@translator
def katanagraph_to_networkx(x: KatanaGraph, **props) -> NetworkXGraph:
    pg = x.value
    dest_list = [dest for src in pg for dest in [pg.get_edge_dest(e) for e in pg.edge_ids(src)]]
    for src in pg:
        if pg.edge_ids(src) == range(0, 0):
            if src not in dest_list:
                raise ValueError("NetworkX does not support graph with isolated nodes")
    edge_dict_count = {(src, dest): 0 for src in pg for dest in [pg.get_edge_dest(e) for e in pg.edge_ids(src)]}
    for src in pg:
        for dest in [pg.get_edge_dest(e) for e in pg.edge_ids(src)]:
            edge_dict_count[(src, dest)] += 1
            if edge_dict_count[(src, dest)] > 1:
                raise ValueError("NetworkX does not support graph with duplicated edges")
    elist = []
    edge_weights = pg.get_edge_property(x.edge_weight_prop_name).to_pandas()
    if isinstance(edge_weights[0], np.int64):
        elist = [(nid, pg.get_edge_dest(j), int(edge_weights[j])) for nid in pg for j in pg.edge_ids(nid)]
    elif isinstance(edge_weights[0], pyarrow.lib.Int64Scalar):
        elist = [(nid, pg.get_edge_dest(j), edge_weights[j].as_py()) for nid in pg for j in pg.edge_ids(nid)]
        # TODO(pengfei): add more type conversion support: like np.float64 -> float??
    elist = list(OrderedDict.fromkeys(elist))
    if x.is_directed:
        graph = nx.DiGraph()
    else:
        graph = nx.Graph()
    graph.add_weighted_edges_from(elist)
    return mg.wrappers.Graph.NetworkXGraph(graph)
