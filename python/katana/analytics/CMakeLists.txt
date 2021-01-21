# The expected public names (without _) are provided using pure python modules.

add_cython_module(plan
    SOURCES plan.pyx
    DEPENDS
    LIBRARIES Katana::galois)

add_cython_module(_wrappers
    SOURCES _wrappers.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_pagerank
    SOURCES _pagerank.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_betweenness_centrality
    SOURCES _betweenness_centrality.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_triangle_count
    SOURCES _triangle_count.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)

add_cython_module(_independent_set
    SOURCES _independent_set.pyx
    DEPENDS plan
    LIBRARIES Katana::galois)
