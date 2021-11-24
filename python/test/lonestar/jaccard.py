import numpy as np
import pyarrow

from katana import do_all, do_all_operator, set_active_threads
from katana.local import Graph
from katana.timer import StatTimer


@do_all_operator()
def jaccard_operator(g, n1_neighbors, n1_size, output, n2):
    intersection_size = 0
    n2_size = len(g.edge_ids(n2))
    for e_iter in g.edge_ids(n2):
        ne = g.get_edge_dest(e_iter)
        if n1_neighbors[ne]:
            intersection_size += 1
    union_size = n1_size + n2_size - intersection_size
    if union_size > 0:
        similarity = float(intersection_size) / union_size
    else:
        similarity = 1
    output[n2] = similarity


def jaccard(g, key_node, property_name):
    key_neighbors = np.zeros(len(g), dtype=bool)
    output = np.empty(len(g), dtype=float)

    for e in g.edge_ids(key_node):
        n = g.get_edge_dest(e)
        key_neighbors[n] = True

    do_all(
        g, jaccard_operator(g, key_neighbors, len(g.edge_ids(key_node)), output), steal=True, loop_name="jaccard",
    )

    g.add_node_property(pyarrow.table({property_name: output}))


def main():
    import argparse

    import katana.local

    katana.local.initialize()

    parser = argparse.ArgumentParser()
    parser.add_argument("--baseNode", type=int, default=0)
    parser.add_argument("--reportNode", type=int, default=1)
    parser.add_argument("--propertyName", type=str, default="NewProperty")
    parser.add_argument("--threads", "-t", type=int, default=1)
    parser.add_argument("input", type=str)
    args = parser.parse_args()

    print("Using threads:", set_active_threads(args.threads))

    g = Graph(args.input)

    timer = StatTimer("Jaccard (Property Graph) Numba")
    timer.start()
    jaccard(g, args.baseNode, args.propertyName)
    timer.stop()
    del timer

    print("Node {}: {}".format(args.reportNode, g.get_node_property(args.propertyName)[args.reportNode]))


if __name__ == "__main__":
    main()
