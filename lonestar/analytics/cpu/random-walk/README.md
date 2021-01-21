Random Walks
================================================================================

DESCRIPTION 
--------------------------------------------------------------------------------

This program computes the random walks in a given directed graph. 2 algorithms are
are available:
1. Node2vec: For homogeneous graphs (https://snap.stanford.edu/node2vec/)
2. Edge2vec: For heterogeneous graphs (https://bmcbioinformatics.biomedcentral.com/articles/10.1186/s12859-019-2914-2)

INPUT
--------------------------------------------------------------------------------

This application takes in symmetric Galois .gr graphs.
You must specify the -symmetricGraph flag when running this benchmark.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/random-walk; make -j`

RUN
--------------------------------------------------------------------------------

The following are a few example command lines.

-`$ ./random-walk-cpu <path-to-graph> -algo Node2vec -numWalk 1  -walkLength 80 --symmetricGraph -t 4`

