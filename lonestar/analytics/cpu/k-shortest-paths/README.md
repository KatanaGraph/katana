Single Source Shortest Path
================================================================================

DESCRIPTION 
--------------------------------------------------------------------------------

This program computes the k shortest paths in a graph, starting from a
source node (specified by -startNode option) and ending at report node (specified by -reportNode option).

INPUT
--------------------------------------------------------------------------------

This application takes in Katana property graphs having non-negative integer edge weights.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/k-shortest-paths; make -j`

RUN
--------------------------------------------------------------------------------

The following are a few example command lines.

-`$ ./k-shortest-paths-cpu <path-to-graph> --algo=deltaStep --delta=13 --edgePropertyName=value --numPaths=10 --startNode=1 --reportNode=100 -t 40 
-`$ ./k-shortest-paths-cpu <path-to-graph> --algo=deltaTile --delta=13 --edgePropertyName=value --numPaths=10 --startNode=1 --reportNode=100 -t 40`

PERFORMANCE  
--------------------------------------------------------------------------------

* deltaStep/deltaTile algorithms typically performs the best on high diameter
  graphs, such as road networks. Its performance is sensitive to the *delta* parameter, which is
  provided as a power-of-2 at the commandline. *delta* parameter should be tuned
  for every input graph
* topo/topoTile algorithms typically perform the best on low diameter graphs, such
  as social networks and RMAT graphs
* All algorithms rely on CHUNK_SIZE for load balancing, 
