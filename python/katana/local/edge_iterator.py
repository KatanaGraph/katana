class EdgeIterator:
    """
    An iterator over the edges of a graph
    """

    def __init__(self, graph):
        self._graph = graph
        self._curr_node = 0
        self._curr_edge = 0

    def __next__(self):
        if self._curr_edge >= self._graph.num_edges():
            raise StopIteration
        if len(self._graph.edges(self._curr_node)) == 0:
            self._curr_node += 1
            return self.__next__()
        if self._curr_edge > self._graph.edges(self._curr_node)[-1]:
            self._curr_node += 1
        next_edge = (self._curr_node, self._graph.get_edge_dest(self._curr_edge))
        self._curr_edge += 1
        return next_edge

    def __iter__(self):
        return self
