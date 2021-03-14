Local Clustering Coefficient
================================================================================

DESCRIPTION 
--------------------------------------------------------------------------------

This program computes the local clustering coefficient value for each node
in a given undirected graph. We implement an ordered count algorithm that sorts the nodes by degree before
execution: this has been found to give good performance. We implement the
ordered count algorithm from the following:

http://gap.cs.berkeley.edu/benchmark.html

INPUT
--------------------------------------------------------------------------------

This application takes in symmetric Galois .gr graphs.
You must specify the -symmetricGraph flag when running this benchmark.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/lcc; make -j`

RUN
--------------------------------------------------------------------------------

The following are a few example command lines.

-`$ ./local-clustering-coefficient-cpu <path-symmetric-graph> -t 20 -algo orderedCountAtomics -symmetricGraph`
-`$ ./local-clustering-coefficient-cpu <path-symmetric-graph> -t 20 -algo orderedCountPerThread -symmetricGraph`

PERFORMANCE
--------------------------------------------------------------------------------

* In our experience, orderedCount algorithm with PerThreadStorage implementation gives the best performance.

* The performance of algorithms depend on an optimal choice of the compile 
  time constant, CHUNK_SIZE, the granularity of stolen work when work stealing is 
  enabled (via katana::steal()). The optimal value of the constant might depend on 
  the architecture, so you might want to evaluate the performance over a range of 
  values (say [16-4096]).
