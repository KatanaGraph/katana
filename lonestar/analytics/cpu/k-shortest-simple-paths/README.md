Yen k Single Source Simple Shortest Paths
================================================================================

DESCRIPTION 
--------------------------------------------------------------------------------

This program computes the k simple shortest paths in a graph using Yen's 
algorithm [Management Science Journal, 1971], starting from a
source node (specified by -startNode option) and ending at report node (specified by -reportNode option). 


Yen's k shortest path algorithm uses a single shortest path subroutine internally and
we use the Delta-Stepping algorithm by Meyer and Sanders, 2003. For this Delta-Stepping implementation,
we have a variant that implements edge tiling, deltaTile, which divides the edges of high-degree nodes
 into multiple work items for better load balancing.
 
INPUT
--------------------------------------------------------------------------------

This application takes in Katana property graphs having non-negative integer edge weights.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/k-shortest-simple-paths; make -j`

RUN
--------------------------------------------------------------------------------

The following are a few example command lines.

-`$ ./k-shortest-simple-paths-cpu <path-to-graph> --algo=deltaStep --delta=13 --edgePropertyName=value --numPaths=10 --startNode=1 --reportNode=100 -t 40`
-`$ ./k-shortest-simple-paths-cpu <path-to-graph> --algo=deltaTile --delta=13 --edgePropertyName=value --numPaths=10 --startNode=1 --reportNode=100 -t 40`

