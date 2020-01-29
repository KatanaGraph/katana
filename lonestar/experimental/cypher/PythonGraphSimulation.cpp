/*
 * This file belongs to the Galois project, a C++ library for exploiting parallelism.
 * The code is being released under the terms of the 3-Clause BSD License (a
 * copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 */

#include "PythonGraph.h"

size_t runAttributedGraphSimulation(AttributedGraph* queryGraph,
                                    AttributedGraph* dataGraph,
                                    EventLimit limit, EventWindow window) {
  runGraphSimulationOld(queryGraph->graph, dataGraph->graph, limit, window, true);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findFilesWithMultipleWrites(AttributedGraph* dataGraph,
                                   EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNodeWithRepeatedActions(dataGraph->graph,
                               getNodeLabelMask(*dataGraph, "file").second.first,
                               getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                               window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findProcessesWithReadFileWriteNetwork(AttributedGraph* dataGraph,
                                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNodeWithTwoActions(dataGraph->graph,
                          getNodeLabelMask(*dataGraph, "process").second.first,
                          getEdgeLabelMask(*dataGraph, "READ").second.first,
                          getNodeLabelMask(*dataGraph, "file").second.first,
                          getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                          getNodeLabelMask(*dataGraph, "network").second.first,
                          window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findProcessesWritingNetworkIndirectly(AttributedGraph* dataGraph,
                                             EventLimit limit,
                                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  Graph queryGraph;
  queryGraph.allocateFrom(4, 6);
  queryGraph.constructNodes();

  queryGraph.getData(0).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(0, 1, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.fixEndEdge(0, 1);

  queryGraph.getData(1).label = getNodeLabelMask(*dataGraph, "file").second.first;
  queryGraph.constructEdge(1, 0, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.constructEdge(2, 2, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.fixEndEdge(1, 3);

  queryGraph.getData(2).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(3, 1, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.constructEdge(4, 3, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(2, 5);

  queryGraph.getData(3).label = getNodeLabelMask(*dataGraph, "network").second.first;
  queryGraph.constructEdge(5, 2, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(3, 6);

  runGraphSimulationOld(queryGraph, dataGraph->graph, limit, window, false);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findProcessesOriginatingFromNetwork(AttributedGraph* dataGraph,
                                           EventLimit limit,
                                           EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("EXECUTE") ==
       dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  Graph queryGraph;
  queryGraph.allocateFrom(4, 6);
  queryGraph.constructNodes();

  queryGraph.getData(0).label = getNodeLabelMask(*dataGraph, "network").second.first;
  queryGraph.constructEdge(0, 1, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.fixEndEdge(0, 1);

  queryGraph.getData(1).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(1, 0, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.constructEdge(2, 2, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.fixEndEdge(1, 3);

  queryGraph.getData(2).label = getNodeLabelMask(*dataGraph, "file").second.first;
  queryGraph.constructEdge(3, 1, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.constructEdge(4, 3,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(2, 5);

  queryGraph.getData(3).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(5, 2,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(3, 6);

  runGraphSimulationOld(queryGraph, dataGraph->graph, limit, window, false);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findProcessesOriginatingFromNetworkIndirectly(AttributedGraph* dataGraph,
                                                     EventLimit limit,
                                                     EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("EXECUTE") ==
       dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  Graph queryGraph;
  queryGraph.allocateFrom(6, 10);
  queryGraph.constructNodes();

  queryGraph.getData(0).label = getNodeLabelMask(*dataGraph, "network").second.first;
  queryGraph.constructEdge(0, 1, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.fixEndEdge(0, 1);

  queryGraph.getData(1).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(1, 0, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.constructEdge(2, 2, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.fixEndEdge(1, 3);

  queryGraph.getData(2).label = getNodeLabelMask(*dataGraph, "file").second.first;
  queryGraph.constructEdge(3, 1, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.constructEdge(4, 3, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(2, 5);

  queryGraph.getData(3).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(5, 2, EdgeData(getEdgeLabelMask(*dataGraph, "READ").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 2));
#else
    ));
#endif
  queryGraph.constructEdge(6, 4, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 3));
#else
    ));
#endif
  queryGraph.fixEndEdge(3, 7);

  queryGraph.getData(4).label = getNodeLabelMask(*dataGraph, "file").second.first;
  queryGraph.constructEdge(7, 3, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 3));
#else
    ));
#endif
  queryGraph.constructEdge(8, 5,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 4));
#else
    ));
#endif
  queryGraph.fixEndEdge(4, 9);

  queryGraph.getData(5).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(9, 4,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 4));
#else
    ));
#endif
  queryGraph.fixEndEdge(5, 10);

  runGraphSimulationOld(queryGraph, dataGraph->graph, limit, window, false);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t findProcessesExecutingModifiedFile(AttributedGraph* dataGraph,
                                          EventLimit limit,
                                          EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("CHMOD") ==
       dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("EXECUTE") ==
       dataGraph->edgeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  Graph queryGraph;
  queryGraph.allocateFrom(4, 6);
  queryGraph.constructNodes();

  queryGraph.getData(0).label = getNodeLabelMask(*dataGraph, "file").second.first;
  queryGraph.constructEdge(0, 1, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.constructEdge(1, 2, EdgeData(getEdgeLabelMask(*dataGraph, "CHMOD").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.constructEdge(2, 3,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(0, 3);

  queryGraph.getData(1).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(3, 0, EdgeData(getEdgeLabelMask(*dataGraph, "WRITE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 0));
#else
    ));
#endif
  queryGraph.fixEndEdge(1, 4);

  queryGraph.getData(2).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(4, 0, EdgeData(getEdgeLabelMask(*dataGraph, "CHMOD").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
    , 1));
#else
    ));
#endif
  queryGraph.fixEndEdge(2, 5);

  queryGraph.getData(3).label = getNodeLabelMask(*dataGraph, "process").second.first;
  queryGraph.constructEdge(5, 0,
                           EdgeData(getEdgeLabelMask(*dataGraph, "EXECUTE").second.first
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
                           , 2));
#else
    ));
#endif
  queryGraph.fixEndEdge(3, 6);

  runGraphSimulationOld(queryGraph, dataGraph->graph, limit, window, true);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedEdges(dataGraph->graph);
#else
  return countMatchedNodes(dataGraph->graph);
#endif
}

size_t processesReadFromFile(AttributedGraph* dataGraph, char* file_uuid,
                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[file_uuid],
                 getNodeLabelMask(*dataGraph, "file").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[file_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[file_uuid]);
#endif
}

size_t processesWroteToFile(AttributedGraph* dataGraph, char* file_uuid,
                            EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[file_uuid],
                 getNodeLabelMask(*dataGraph, "file").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[file_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[file_uuid]);
#endif
}

size_t processesReadFromNetwork(AttributedGraph* dataGraph,
                                char* network_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[network_uuid],
                 getNodeLabelMask(*dataGraph, "network").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[network_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[network_uuid]);
#endif
}

size_t processesWroteToNetwork(AttributedGraph* dataGraph,
                               char* network_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[network_uuid],
                 getNodeLabelMask(*dataGraph, "network").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[network_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[network_uuid]);
#endif
}

size_t processesReadFromRegistry(AttributedGraph* dataGraph,
                                 char* registry_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("registry") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[registry_uuid],
                 getNodeLabelMask(*dataGraph, "registry").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[registry_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[registry_uuid]);
#endif
}

size_t processesWroteToRegistry(AttributedGraph* dataGraph,
                                char* registry_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("registry") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[registry_uuid],
                 getNodeLabelMask(*dataGraph, "registry").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[registry_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[registry_uuid]);
#endif
}

size_t processesReadFromMemory(AttributedGraph* dataGraph, char* memory_uuid,
                               EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("memory") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[memory_uuid],
                 getNodeLabelMask(*dataGraph, "memory").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[memory_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[memory_uuid]);
#endif
}

size_t processesWroteToMemory(AttributedGraph* dataGraph, char* memory_uuid,
                              EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("memory") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[memory_uuid],
                 getNodeLabelMask(*dataGraph, "memory").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "process").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[memory_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[memory_uuid]);
#endif
}

size_t filesReadByProcess(AttributedGraph* dataGraph, char* process_uuid,
                          EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "file").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t filesWrittenByProcess(AttributedGraph* dataGraph, char* process_uuid,
                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("file") == dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "file").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t networksReadByProcess(AttributedGraph* dataGraph, char* process_uuid,
                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "network").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t networksWrittenByProcess(AttributedGraph* dataGraph,
                                char* process_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("network") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "network").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t registriesReadByProcess(AttributedGraph* dataGraph,
                               char* process_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("registry") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "registry").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t registriesWrittenByProcess(AttributedGraph* dataGraph,
                                  char* process_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("registry") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "registry").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t memoriesReadByProcess(AttributedGraph* dataGraph, char* process_uuid,
                             EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("memory") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("READ") == dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "READ").second.first,
                 getNodeLabelMask(*dataGraph, "memory").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}

size_t memoriesWrittenByProcess(AttributedGraph* dataGraph,
                                char* process_uuid, EventWindow window) {
  if ((dataGraph->nodeLabelIDs.find("process") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->nodeLabelIDs.find("memory") ==
       dataGraph->nodeLabelIDs.end()) ||
      (dataGraph->edgeLabelIDs.find("WRITE") ==
       dataGraph->edgeLabelIDs.end())) {
    resetMatchedStatus(dataGraph->graph);
    return 0;
  }

  matchNeighbors(dataGraph->graph, dataGraph->nodeIndices[process_uuid],
                 getNodeLabelMask(*dataGraph, "process").second.first,
                 getEdgeLabelMask(*dataGraph, "WRITE").second.first,
                 getNodeLabelMask(*dataGraph, "memory").second.first, window);
#ifdef USE_QUERY_GRAPH_WITH_TIMESTAMP
  return countMatchedNeighborEdges(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#else 
  return countMatchedNeighbors(dataGraph->graph,
                                   dataGraph->nodeIndices[process_uuid]);
#endif
}
