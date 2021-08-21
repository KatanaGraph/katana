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
===============

First install the package

```Shell
conda create -n metagraph-test -c conda-forge -c katanagraph/label/dev -c metagraph metagraph-katana
```

Test
===============
To check the installation is successful, you can run the test cases by:

```Shell
conda activate metagraph-test
pytest tests
```


Examples
===========================

Loading Katana Graph
------------------

```
import metagraph as mg
import katana.local
from katana.example_data import get_input
katana.local.initialize()
pg = katana.local.Graph(get_input('propertygraphs/rmat15_cleaned_symmetric'))
katana_graph = mg.wrappers.Graph.KatanaGraph(pg)
```


Graph Format Conversion 
------------------

```
import metagraph as mg
networkx_graph = mg.translate(katana_graph, mg.wrappers.Graph.NetworkXGraph) # translate from Katana Graph to NetworkX Graph
```

<!-- katana_graph = mg.translate(networkx_graph, mg.wrappers.Graph.KatanaGraph) # translate from NetworkX Graph to Katana Graph -->
<!-- TODO (pengfei): uncomment this after switching to a cleaned graph-->

Running Graph Analytics Algorithms
------------------

```
import metagraph as mg
bfs_kg = mg.algos.traversal.bfs_iter(katana_graph, 0) # run bfs using Katana Graph format
```

<!-- bfs_nx = mg.algos.traversal.bfs_iter(networkxgraph, 0) # run bfs using NetworkX Graph format -->
<!-- TODO (pengfei): uncomment this after switching to a cleaned graph-->

More examples can be found in the metagraph_katana/tests/ folder


