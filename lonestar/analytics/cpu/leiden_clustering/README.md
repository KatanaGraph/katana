Clustering
================================================================================

DESCRIPTION
--------------------------------------------------------------------------------

This directory contains hierarchical community detection algorithm that
recursively merge the communities into a single node and perform clustering on the
coarsened graph until nodes stop changing communities.


* Louvain Clustering: This algorithm uses the modularity function to find
  well-connected communities by maximizing the modularity score, which
  quantifies the quality of node assignments to the communities based on the
  density of connections.


INPUT
--------------------------------------------------------------------------------

This application takes in symmetric Galois .gr graphs.
You must specify the -symmetricGraph flag when running this benchmark.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/clustering; make -j`

RUN
--------------------------------------------------------------------------------

The following are a few example command lines.

-`$ ./louvain-clustering-cpu <path-to-graph> -t 40 -c_threshold=0.01 -threshold=0.000001 -max_iter 1000 -algo=Foreach  -resolution=0.001 -symmetricGraph`

