import pytest
import metagraph as mg

import metagraph_katana as mg_katana_src



def test_src_import():
    assert dir(mg_katana_src) == ['__builtins__',
                         '__cached__',
                         '__doc__',
                         '__file__',
                         '__loader__',
                         '__name__',
                         '__package__',
                         '__path__',
                         '__spec__',
                         'plugins',
                         'tests']

def test_import_types():
    assert dir(mg.types) == ['BipartiteGraph', 'DataFrame', 'EdgeMap', 'EdgeSet', 'Graph', 'Matrix', 'NodeMap', 'NodeSet', 'Vector']
    assert dir(mg.types.Graph) == ['KatanaGraphType', 'NetworkXGraphType', 'ScipyGraphType']
    assert dir(mg.wrappers.Graph) == ['KatanaGraph', 'NetworkXGraph', 'ScipyGraph']
    assert 'KatanaGraphType' in dir(mg.types.Graph)
    assert 'KatanaGraph' in dir(mg.wrappers.Graph)

def test_import_algorithms():
    assert dir(mg.algos) == ['bipartite', 'centrality', 'clustering', 'embedding', 'flow', 'subgraph', 'traversal', 'util']

