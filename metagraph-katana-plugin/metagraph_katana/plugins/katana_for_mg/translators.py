import os
# import sys

# from executing.executing import NodeFinder
import katana.local
from katana.example_utils import get_input
from katana.galois import set_active_threads
# from pathlib import Path
from icecream import ic
import pandas as pd
import pyarrow as pa
import csv
from scipy.sparse import csr_matrix


import metagraph as mg
from metagraph import translator
from metagraph.plugins import has_networkx

from .. import has_katana


if has_katana and has_networkx:
    import networkx as nx
    from katana.property_graph import PropertyGraph
    import numpy as np
    from .types import KatanaGraph
    from metagraph.plugins.python.types import dtype_casting
    from metagraph.plugins.networkx.types import NetworkXGraph
    from typing import Tuple
    
    @translator
    def nx_to_kg(x:NetworkXGraph, **props) -> KatanaGraph:
        aprops = NetworkXGraph.Type.compute_abstract_properties(x, {'node_dtype', 'node_type', 'edge_type', 'is_directed'})
        # ic(aprops)
        is_weighted = aprops['edge_type'] == 'map'
        elist_raw = list(x.value.edges(data=True))
        if aprops['is_directed']:
            elist = sorted(elist_raw, key=lambda each: (each[0], each[1]))
        else:
            inv_elist = [(each[1], each[0], each[2]) for each in elist_raw]
            elist = sorted(elist_raw+inv_elist, key=lambda each: (each[0], each[1]))
        nlist=sorted(list(x.value.nodes(data=True)), key=lambda each: each[0])
        row = np.array([each[0] for each in elist])
        col = np.array([each[1] for each in elist])
        data = np.array([each[2]['weight'] for each in elist])
        csr=csr_matrix((data, (row,col)), shape=(len(nlist), len(nlist)) )
        pg = PropertyGraph.from_csr(csr.indptr[1:], csr.indices)
        t = pa.table(dict(value_from_translator=data))
        pg.add_edge_property(t)
        return KatanaGraph(pg_graph=pg, is_weighted=is_weighted, edge_weight_prop_name='value_from_translator', is_directed=aprops['is_directed'], node_weight_index=0)
    
    # @translator # method 1
    # def kg_to_nx(x: KatanaGraph, **props) -> NetworkXGraph:
    #     elist = []
    #     edge_weights = x.value.get_edge_property(x.edge_weight_prop_name).to_pandas()
    #     elist = [ [nid, x.value.get_edge_dest(j), edge_weights[j]]
    #             for nid in x.value
    #             for j in x.value.edges(nid) ]
    #     df = pd.DataFrame(elist, columns=['Source', 'Destination', 'Weight'])
    #     # print (len(df))
    #     em = mg.wrappers.EdgeMap.PandasEdgeMap(df, 'Source', 'Destination', 'Weight', is_directed=x.is_directed)
    #     graph = mg.algos.util.graph.build(em)
    #     return graph

    @translator # method 2
    def kg_to_nx(x: KatanaGraph, **props) -> NetworkXGraph:
        elist = []
        edge_weights = x.value.get_edge_property(x.edge_weight_prop_name).to_pandas()
        elist = [ (nid, x.value.get_edge_dest(j), edge_weights[j])
                for nid in x.value
                for j in x.value.edges(nid) ]
        if x.is_directed:
            graph = nx.DiGraph()
        else:
            graph = nx.Graph()
        graph.add_weighted_edges_from(elist)
        return mg.wrappers.Graph.NetworkXGraph(graph)

