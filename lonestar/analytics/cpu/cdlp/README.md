Comunivcv v     y Detection using Label Propagation (CDLP)
================================================================================

DESCRIPTION 
--------------------------------------------------------------------------------

Detect cummunities in a graph by label propagation. Set the same
label to nodes which belong to the same community.

Its goal is that for every node in the graph, it chooses the most
frequent community label of its immediate neighborhood. The algorithm
initializes the community label of each node equal to its node id and
repeats for a specific times equal to the number of iterations passed
by the user; default value is 10; and in each iteration, nodes acquire
the most frequent community in their immediate neighborhood. At the
end the nodes that have the same community id are in the same community.

  - Synchronous: Bulk synchronous data-driven implementation.

INPUT
--------------------------------------------------------------------------------

This application takes in Galois .gr graphs.
User also can speciry the number of iteration otherwise 10 would
be used as defualt.

BUILD
--------------------------------------------------------------------------------

1. Run cmake at BUILD directory (refer to top-level README for cmake instructions).

2. Run `cd <BUILD>/lonestar/analytics/cpu/cdlp; make -j`

RUN
--------------------------------------------------------------------------------

To run cdlp algorithm with default number of iterations equal to 10, use the following:
-`$ ./cdlp-cpu <input-graph> -t=<num-threads>`
To specify a different unmber of iteration, use this option: `--maxIterations=<num_iterations>`. 
