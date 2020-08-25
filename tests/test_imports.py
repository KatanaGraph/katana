def test_import_applications_flat_graph():
    import galois.sssp
    import galois.pagerank
    import galois.bfs
    import galois.jaccard
    import galois.connected_components


def test_import_applications_property_graph():
    import galois.bfs_property_graph
    import galois.jaccard_property_graph
    import galois.pagerank_property_graph
    import galois.connected_components_property_graph
    import galois.kcore_property_graph


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
