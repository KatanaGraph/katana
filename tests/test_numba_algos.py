from galois.lonestar.analytics.bfs import bfs_sync_pg, verify_bfs
from galois.lonestar.analytics.jaccard import jaccard


def test_bfs(property_graph):
    graph = property_graph
    startNode = 0
    propertyName = "NewProp"

    bfs_sync_pg(graph, startNode, propertyName)

    numNodeProperties = len(graph.node_schema())
    newPropertyId = numNodeProperties - 1
    verify_bfs(graph, startNode, newPropertyId)

    # TODO: This should assert that the results are correct.


def test_jaccard(property_graph):
    graph = property_graph
    startNode = 0
    propertyName = "NewProp"

    jaccard(graph, startNode, propertyName)

    # TODO: This should assert that the results are correct.


# TODO: Add more tests.
