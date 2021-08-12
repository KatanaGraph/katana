# Metagraph Interoperability for Katana Graph


Overview
========

Metagraph is a plugin-based system for performing graph computations;
Users can use graph algorithms/representations from different graph libraries with the same API interface.

Highlights include:
- Katana Graph built-in algorithms/types which can be used by existing Metagraph API.
- Graph analytics algorithms supported by both existing metagraph plugin graph libraries and Katana Graph.
- The bi-direction translation between Katana Graph format and the NetworkX Graph format.



Installation
===============

First install the dependency together with the plugin

```Shell
conda env create -f katana-metagraph.yml
```

Test
===============
To check the installation is successful, you can run the test cases by:

```Shell
conda activate katana-metagraph
pytest metagraph_katana/tests/ -s
```


Examples
===========================

Loading Katana Graph
------------------

```
import metagraph as mg
from katana.property_graph import PropertyGraph
import katana.local
from katana.example_utils import get_input
katana.local.initialize()
pg = PropertyGraph(get_input('propertygraphs/rmat15_cleaned_symmetric'))
katana_graph = mg.wrappers.Graph.KatanaGraph(pg)
```


Graph Format Conversion 
------------------

```
import metagraph as mg
networkx_graph = mg.translate(katana_graph, mg.wrappers.Graph.NetworkXGraph) # translate from Katana Graph to NetworkX Graph
katana_graph = mg.translate(networkx_graph, mg.wrappers.Graph.KatanaGraph) # translate from NetworkX Graph to Katana Graph
```


Running Graph Analytics Algorithms
------------------

```
import metagraph as mg
bfs_nx = mg.algos.traversal.bfs_iter(networkxgraph, 0) # run bfs using NetworkX Graph format
bfs_kg = mg.algos.traversal.bfs_iter(katanagraph, 0) # run bfs using Katana Graph format
```

More examples can be found in the metagraph_katana/tests/ folder


