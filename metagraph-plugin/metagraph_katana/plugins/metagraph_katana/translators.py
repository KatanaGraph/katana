import metagraph as mg
import networkx as nx
import numpy as np
import pyarrow as pa
from metagraph import translator
from metagraph.plugins.networkx.types import NetworkXGraph
from scipy.sparse import csr_matrix

from katana.property_graph import PropertyGraph

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
    # call the katana api to build a PropertyGraph (unweighted) from the CSR format
    # noting that the first 0 in csr.indptr is excluded
    pg = PropertyGraph.from_csr(csr.indptr[1:], csr.indices)
    # add the edge weight as a new property
    t = pa.table(dict(value_from_translator=data))
    pg.add_edge_property(t)
    # use the metagraph's Graph warpper to wrap the PropertyGraph
    return KatanaGraph(
        pg_graph=pg,
        is_weighted=is_weighted,
        edge_weight_prop_name="value_from_translator",
        is_directed=aprops["is_directed"],
        node_weight_index=0,
    )


@translator
def katanagraph_to_networkx(x: KatanaGraph, **props) -> NetworkXGraph:
    elist = []
    edge_weights = x.value.get_edge_property(x.edge_weight_prop_name).to_pandas()
    elist = [(nid, x.value.get_edge_dest(j), edge_weights[j]) for nid in x.value for j in x.value.edges(nid)]
    if x.is_directed:
        graph = nx.DiGraph()
    else:
        graph = nx.Graph()
    graph.add_weighted_edges_from(elist)
    return mg.wrappers.Graph.NetworkXGraph(graph)
