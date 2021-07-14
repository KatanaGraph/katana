# content of tests/test_top.py
import pytest
import sys
import os
import pandas as pd
import numpy as np
# from pathlib import Path
# from executing.executing import NodeFinder
# from pathlib import Path
from icecream import ic
import numpy as np
import pandas as pd
# import pyarrow as pa
import csv
from scipy.sparse import csr_matrix

import metagraph as mg




def get_info_pg(property_graph1):
    ic(property_graph1.num_nodes())
    ic(property_graph1.num_edges())
    num_nschema= len(property_graph1.node_schema())
    ic(dir(property_graph1.node_schema()))
    ic(num_nschema)
    for i in range(num_nschema):
        name = property_graph1.node_schema()[i].name
        ic(name)
        ic(property_graph1.node_schema()[i].type)
        ic(property_graph1.get_node_property(name).to_pandas()[0])
    num_eschema = len(property_graph1.edge_schema())
    ic(dir(property_graph1.edge_schema()))
    ic(num_eschema)
    for i in range(num_eschema):
        name = property_graph1.edge_schema()[i].name
        ic(name)
        ic(property_graph1.edge_schema()[i].type)
        ic(property_graph1.get_edge_property(name).to_pandas()[0])
    ic(property_graph1.edges(0))
    ic( [property_graph1.get_edge_dest(i) for i in property_graph1.edges(0)][0:5] )
    ic(property_graph1.edges(8))
    ic( [property_graph1.get_edge_dest(i) for i in property_graph1.edges(8)][0:5] )



def test_get_info(kg_rmat15_cleaned_di):
    # get_info_pg(kg_rmat15_cleaned_di.value)
    assert True

# ic| property_graph1.num_nodes(): 32768
# ic| property_graph1.num_edges(): 363194
# ic| num_nschema: 0
# ic| num_eschema: 1
# ic| name: 'value'
# ic| property_graph1.edge_schema()[i].type: DataType(int64)
# ic| property_graph1.get_edge_property(name).to_pandas()[0]: 339302416426
# ic| property_graph1.edges(0): range(0, 20767)
# ic| [property_graph1.get_edge_dest(i) for i in property_graph1.edges(0)][0:5]: [1, 2, 3, 4, 5]
# ic| property_graph1.edges(8): range(36475, 41133)
# ic| [property_graph1.get_edge_dest(i) for i in property_graph1.edges(8)][0:5]: [0, 9, 10, 11, 12]



def test_num_nodes(kg_rmat15_cleaned_di):
    # ic(kg_rmat15_cleaned_di.value.num_nodes())
    cnt = 0
    for nid in kg_rmat15_cleaned_di.value:
        cnt += 1
    assert kg_rmat15_cleaned_di.value.num_nodes()==32768
    assert kg_rmat15_cleaned_di.value.num_nodes()==cnt



def test_num_edges(kg_rmat15_cleaned_di):
    # ic(kg_rmat15_cleaned_di.value.num_edges())
    cnt = 0
    for nid in kg_rmat15_cleaned_di.value:
        cnt += len(kg_rmat15_cleaned_di.value.edges(nid))
    assert kg_rmat15_cleaned_di.value.num_edges()==363194
    assert kg_rmat15_cleaned_di.value.num_edges()==cnt

def test_node_schema(kg_rmat15_cleaned_di):
    assert 'names' in dir(kg_rmat15_cleaned_di.value.node_schema())
    assert 'types' in dir(kg_rmat15_cleaned_di.value.node_schema())
    assert len(kg_rmat15_cleaned_di.value.node_schema())==0

def test_edge_schema(kg_rmat15_cleaned_di):
    assert 'names' in dir(kg_rmat15_cleaned_di.value.edge_schema())
    assert 'types' in dir(kg_rmat15_cleaned_di.value.edge_schema())
    assert len(kg_rmat15_cleaned_di.value.edge_schema())==1


def test_edge_property(kg_rmat15_cleaned_di):
    assert kg_rmat15_cleaned_di.value.edge_schema()[0].name == 'value'
    assert kg_rmat15_cleaned_di.value.get_edge_property(0) == kg_rmat15_cleaned_di.value.get_edge_property('value')
    assert kg_rmat15_cleaned_di.value.get_edge_property('value').to_pandas()[0] == 339302416426


def test_topology(kg_rmat15_cleaned_di):
    assert kg_rmat15_cleaned_di.value.edges(0) == range(0, 20767)
    assert [kg_rmat15_cleaned_di.value.get_edge_dest(i) for i in kg_rmat15_cleaned_di.value.edges(0)][0:5] == [1, 2, 3, 4, 5]
    assert kg_rmat15_cleaned_di.value.edges(8) == range(36475, 41133)
    assert [kg_rmat15_cleaned_di.value.get_edge_dest(i) for i in kg_rmat15_cleaned_di.value.edges(8)][0:5] == [0, 9, 10, 11, 12]

# # this one is slow, can be commented
# def test_symmetric(kg_rmat15_cleaned_di):
#     for src in kg_rmat15_cleaned_di.value:
#         for dest in [kg_rmat15_cleaned_di.value.get_edge_dest(e) for e in kg_rmat15_cleaned_di.value.edges(src)]:
#             assert src in [kg_rmat15_cleaned_di.value.get_edge_dest(e) for e in kg_rmat15_cleaned_di.value.edges(dest)]

def get_info_nx(nx_graph1):
    ic (len(list(nx_graph1.value.nodes(data=True))))
    ic (len(list(nx_graph1.value.edges(data=True))))
    ic (list(nx_graph1.value.nodes(data=True)))
    ic (list(nx_graph1.value.edges(data=True)))

def test_get_info_mx(nx_weighted_undirected_8_12, nx_weighted_directed_8_12):
    # get_info_nx(nx_weighted_undirected_8_12)
    # get_info_nx(nx_weighted_directed_8_12)
    assert True

# ic| len(list(nx_graph1.value.nodes(data=True))): 8
# ic| len(list(nx_graph1.value.edges(data=True))): 12
# ic| list(nx_graph1.value.nodes(data=True)): [(0, {}), (1, {}), (3, {}), (4, {}), (2, {}), (5, {}), (6, {}), (7, {})]
# ic| list(nx_graph1.value.edges(data=True)): [(0, 1, {'weight': 4}),
#                                              (0, 3, {'weight': 2}),
#                                              (0, 4, {'weight': 7}),
#                                              (1, 3, {'weight': 3}),
#                                              (1, 4, {'weight': 5}),
#                                              (3, 4, {'weight': 1}),
#                                              (4, 2, {'weight': 5}),
#                                              (4, 7, {'weight': 4}),
#                                              (2, 5, {'weight': 2}),
#                                              (2, 6, {'weight': 8}),
#                                              (5, 6, {'weight': 4}),
#                                              (5, 7, {'weight': 6})]
# ic| len(list(nx_graph1.value.nodes(data=True))): 8
# ic| len(list(nx_graph1.value.edges(data=True))): 12
# ic| list(nx_graph1.value.nodes(data=True)): [(0, {}), (1, {}), (3, {}), (4, {}), (2, {}), (5, {}), (6, {}), (7, {})]
# ic| list(nx_graph1.value.edges(data=True)): [(0, 1, {'weight': 4}),
#                                              (0, 3, {'weight': 2}),
#                                              (0, 4, {'weight': 7}),
#                                              (1, 3, {'weight': 3}),
#                                              (1, 4, {'weight': 5}),
#                                              (3, 4, {'weight': 1}),
#                                              (4, 7, {'weight': 4}),
#                                              (2, 4, {'weight': 5}),
#                                              (2, 5, {'weight': 2}),
#                                              (2, 6, {'weight': 8}),
#                                              (5, 6, {'weight': 4}),
#                                              (5, 7, {'weight': 6})]

def test_num_nodes(nx_weighted_undirected_8_12, nx_weighted_directed_8_12):
    assert len(list(nx_weighted_undirected_8_12.value.nodes(data=True))) == 8
    assert len(list(nx_weighted_directed_8_12.value.nodes(data=True))) == 8

def test_num_nodes(nx_weighted_undirected_8_12, nx_weighted_directed_8_12):
    assert len(list(nx_weighted_undirected_8_12.value.edges(data=True))) == 12
    assert len(list(nx_weighted_directed_8_12.value.edges(data=True))) == 12


def test_topology(nx_weighted_undirected_8_12, nx_weighted_directed_8_12):
    assert list(nx_weighted_undirected_8_12.value.nodes(data=True)) == list(nx_weighted_directed_8_12.value.nodes(data=True))
    assert list(nx_weighted_undirected_8_12.value.nodes(data=True)) == [(0, {}), (1, {}), (3, {}), (4, {}), (2, {}), (5, {}), (6, {}), (7, {})]
    assert list(nx_weighted_undirected_8_12.value.edges(data=True)) == [(0, 1, {'weight': 4}),
                                                                        (0, 3, {'weight': 2}),
                                                                        (0, 4, {'weight': 7}),
                                                                        (1, 3, {'weight': 3}),
                                                                        (1, 4, {'weight': 5}),
                                                                        (3, 4, {'weight': 1}),
                                                                        (4, 2, {'weight': 5}),
                                                                        (4, 7, {'weight': 4}),
                                                                        (2, 5, {'weight': 2}),
                                                                        (2, 6, {'weight': 8}),
                                                                        (5, 6, {'weight': 4}),
                                                                        (5, 7, {'weight': 6})]
    assert list(nx_weighted_directed_8_12.value.edges(data=True)) == [(0, 1, {'weight': 4}),
                                                                      (0, 3, {'weight': 2}),
                                                                      (0, 4, {'weight': 7}),
                                                                      (1, 3, {'weight': 3}),
                                                                      (1, 4, {'weight': 5}),
                                                                      (3, 4, {'weight': 1}),
                                                                      (4, 7, {'weight': 4}),
                                                                      (2, 4, {'weight': 5}),
                                                                      (2, 5, {'weight': 2}),
                                                                      (2, 6, {'weight': 8}),
                                                                      (5, 6, {'weight': 4}),
                                                                      (5, 7, {'weight': 6})]