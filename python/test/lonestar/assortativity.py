from enum import Enum
from math import sqrt
from test.lonestar.calculate_degree import calculate_degree

import numpy as np

from katana import do_all, do_all_operator
from katana.local import Graph
from katana.local.atomic import ReduceSum


class DegreeType(Enum):
    IN = 1
    OUT = 2


@do_all_operator()
def sum_degree_operator(
    graph: Graph,
    source_degree,
    sum_source: ReduceSum[np.uint64],
    destination_degree,
    sum_destination: ReduceSum[np.uint64],
    nid,
):
    for edge in graph.edge_ids(nid):
        sum_source.update(source_degree[nid])
        dst = graph.get_edge_dest(edge)
        sum_destination.update(destination_degree[dst])


def average_degree(graph: Graph, num_edges: int, source_degree, destination_degree):
    """
    Calculate the average in or out degree for the source and destination nodes
    Returns the result as a tuple in the form (average degree for source, average degree for destination)
    """
    sum_source_degrees = ReduceSum[np.uint64](0)
    sum_destination_degrees = ReduceSum[np.uint64](0)
    do_all(
        range(graph.num_nodes()),
        sum_degree_operator(graph, source_degree, sum_source_degrees, destination_degree, sum_destination_degrees),
        steal=True,
    )
    return (sum_source_degrees.reduce() / num_edges, sum_destination_degrees.reduce() / num_edges)


@do_all_operator()
def degree_assortativity_coefficient_operator(
    graph: Graph,
    source_degree,
    source_average,
    destination_degree,
    destination_average,
    product_of_dev: ReduceSum[float],
    square_of_source_dev: ReduceSum[float],
    square_of_destination_dev: ReduceSum[float],
    nid,
):
    # deviation of source node from average
    source_dev = source_degree[nid] - source_average
    for edge in graph.edge_ids(nid):
        dst = graph.get_edge_dest(edge)
        destination_dev = destination_degree[dst] - destination_average
        product_of_dev.update(source_dev * destination_dev)
        square_of_source_dev.update(source_dev * source_dev)
        square_of_destination_dev.update(destination_dev * destination_dev)


def degree_assortativity_coefficient(
    graph: Graph,
    source_degree_type: DegreeType = DegreeType.OUT,
    destination_degree_type: DegreeType = DegreeType.IN,
    weight=None,
):
    """
    Calculates and returns the degree assortativity of a given graph.
    Paramaters:
       * graph: the Graph to be analyzed
       * source_degree_type: description of degree type to consider for the source node on an edge
            expected values are DegreeType.IN or DegreeType.OUT
       * destination_degree_type: description the degree type to consider for the destination node on an edge
            expected values are DegreeType.IN or DegreeType.OUT
       * weight (optional): edge property to use if using weighted degrees
    """
    # get the tables associated with the degree types of the source and destination nodes
    calculate_degree(graph, "temp_DegreeType.IN", "temp_DegreeType.OUT", weight)
    source_degree = graph.get_node_property("temp_" + str(source_degree_type))
    destination_degree = graph.get_node_property("temp_" + str(destination_degree_type))

    try:
        # Calculate the average in and out degrees of graph
        # (with respect to number of edges, not number of nodes)
        num_edges = graph.num_edges()
        source_average, destination_average = average_degree(graph, num_edges, source_degree, destination_degree)

        # Calculate the numerator (product of deviation from mean)
        # and the factors of the denominator (square deviation from mean)
        product_of_dev = ReduceSum[float](0)
        square_of_source_dev = ReduceSum[float](0)
        square_of_destination_dev = ReduceSum[float](0)
        do_all(
            range(graph.num_nodes()),
            degree_assortativity_coefficient_operator(
                graph,
                source_degree,
                source_average,
                destination_degree,
                destination_average,
                product_of_dev,
                square_of_source_dev,
                square_of_destination_dev,
            ),
            steal=True,
            loop_name="degree assortativity coefficient calculation",
        )
        return product_of_dev.reduce() / sqrt(square_of_source_dev.reduce() * square_of_destination_dev.reduce())
    finally:
        graph.remove_node_property("temp_DegreeType.IN")
        graph.remove_node_property("temp_DegreeType.OUT")
