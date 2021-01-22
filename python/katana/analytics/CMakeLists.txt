# The expected public names (without _) are provided using pure python modules.

add_cython_module(plan plan.pyx
    LIBRARIES Katana::galois)

add_cython_module(_wrappers _wrappers.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_pagerank _pagerank.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_betweenness_centrality _betweenness_centrality.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_triangle_count _triangle_count.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_independent_set _independent_set.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_connected_components _connected_components.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_k_core _k_core.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_k_truss _k_truss.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_sssp _sssp.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_bfs _bfs.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)
