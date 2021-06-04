from enum import Enum


class Architecture(Enum):
    """
    The architectures potentially supported by Katana algorithms.

    CPU
        Parallel NUMA-aware CPU. Not distributed.
    GPU

    DistributedCPU
        Distributed CPU.
    
    DistributedGPU
        Distributed GPU.
    """
    CPU = _Architecture.kCPU
    GPU = _Architecture.kGPU
    DistributedCPU = _Architecture.kDistributedCPU
    DistributedGPU = _Architecture.kDistributedGPU


cdef class Plan:
    """
    Each abstract algorithm has an execution `Plan` subclass.

    Execution plans contain any tuning parameters the abstract algorithm requires. In general, this will include
    selecting the concrete algorithm to use and providing its tuning parameters (e.g., tile size). Plans do not affect
    the result (beyond potentially returning a different valid result in the case of non-deterministic algorithms or
    floating-point operations).

    All plans provide static methods to create a plan each concrete algorithm and constructors for any automatic
    heuristics. The automatic constructors may either take no input and make decisions at run time, use fixed default
    parameters, or take a graph to be analyzed to determine appropriate parameters. A plan created automatically for one
    graph can still be used with other graphs, though the algorithm will be tuned for the original graph and may not be
    efficient. This may be useful, for instance, to use a sampled subgraph to compute a plan for use on a the original
    larger graph.
    """
    cdef _Plan* underlying(self) except NULL:
        raise NotImplementedError()

    @property
    def architecture(self) -> Architecture:
        """
        The architecture on which the algorithm will run.
        """
        return Architecture(self.underlying().architecture())


cdef class Statistics:
    """
    Each abstract algorithm has an `Statistics` class that computes and stores appropriate statistics on the outputs of
    of that algorithm. Computing the statistics is generally linear in the size of the graph and fast regardless of the
    underlying algorithm.

    Instances have properties to access statistics. See the specific subclasses for details. Instances can be converted
    to a human readable string: `str(stat)`.
    """
    def __init__(self, pg, *args):
        """
        Compute the appropriate statistics for the algorithm the subclass is associated with.

        :type pg: katana.property_graph.PropertyGraph
        :param pg: The property graph containing the algorithm outputs.
        :param args...: Additional parameters needed to compute statistics, including output property names.
        """
        raise NotImplementedError()
