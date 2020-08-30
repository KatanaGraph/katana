def test_import_applications_flat_graph():
    import galois.lonestar.gr.analytics.sssp
    import galois.lonestar.gr.analytics.pagerank
    import galois.lonestar.gr.analytics.bfs
    import galois.lonestar.gr.analytics.jaccard
    import galois.lonestar.gr.analytics.connected_components


def test_import_applications_property_graph():
    import galois.lonestar.analytics.bfs
    import galois.lonestar.analytics.jaccard
    import galois.lonestar.analytics.pagerank
    import galois.lonestar.analytics.connected_components
    import galois.lonestar.analytics.kcore


def test_import_loops():
    import galois.loops


def test_import_property_graph():
    import galois.property_graph


def test_import_graph():
    import galois.graphs


def test_import_datastructures():
    import galois.datastructures


def test_import_atomic():
    import galois.atomic


def test_import_numba():
    import galois.numba_support.pyarrow
    import galois.numba_support.galois
