# Metagraph Interoperability for Katana Graph


Overview
========

Metagraph is a plugin-based system for performing graph computations;
Users can use graph algorithms/representations from different graph libraries with the same API interface.

Highlights include:
- Katana Graph built-in algorithms/types which can be used by existing Metagraph API.
- Graph analytics algorithms supported by both existing metagraph plugin graph libraries and Katana Graph.
- The bi-direction translation between Katana Graph format and the existing metagraph data formats (e.g., NetworkX Graph format).



Installation
============

First install the package

```Shell
conda create -n metagraph-test -c conda-forge -c katanagraph/label/dev -c metagraph metagraph-katana
```

Test
====
To check the installation is successful, you can run the test cases by:

```Shell
conda activate metagraph-test
pytest tests
```


Examples
========

Loading An Example Graph
------------------------

```
import metagraph as mg
import pandas
df = pandas.read_csv("tests/data/edge1.csv")
em = mg.wrappers.EdgeMap.PandasEdgeMap(df, "Source", "Destination", "Weight", is_directed=True)
example_graph = mg.algos.util.graph.build(em)
```


Graph Format Conversion 
-----------------------

```
katana_graph = mg.translate(example_graph, mg.wrappers.Graph.KatanaGraph) # translate from NetworkX Graph to Katana Graph
networkx_graph = mg.translate(katana_graph, mg.wrappers.Graph.NetworkXGraph) # translate from Katana Graph to NetworkX Graph
```


Running Graph Analytics Algorithms
----------------------------------

```
import metagraph as mg
import katana.local
katana.local.initialize()
bfs_kg = mg.algos.traversal.bfs_iter(katana_graph, 0) # run bfs using Katana Graph format
bfs_nx = mg.algos.traversal.bfs_iter(networkx_graph, 0) # run bfs using NetworkX Graph format
```


More examples can be found in the metagraph_katana/tests/ folder


