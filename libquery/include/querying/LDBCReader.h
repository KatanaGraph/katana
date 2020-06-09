/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2020, The University of Texas at Austin. All rights reserved.
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
#ifndef GALOIS_LDBCREADER
#define GALOIS_LDBCREADER

#include "querying/PythonGraph.h"

// TODO figure out how this will end up working with all these typedefs
//#define USE_QUERY_GRAPH_WITH_NODE_LABEL

/**
 *
 * Requires CsvComposite generation
 */
class LDBCReader {
  // type def-ing them here in case they grow past 32 bytes
  //! type of global ids found in ldbc files
  using LDBCNodeType = uint32_t;
  //! type of global ids
  using GIDType = uint32_t;

  //! Underlying attribute graph
  AttributedGraph attGraph;
  //! Path to the generated ldbc social network data
  std::string ldbcDirectory;
  //! Nodes that have been read so far
  GIDType gidOffset;
  //! Total number of nodes to expect during reading
  GIDType totalNodes;
  //! Total number of edges to expect during reading
  GIDType totalEdges;

  //! mapping organization ids to graph's gid
  std::unordered_map<LDBCNodeType, GIDType> organization2GID;

  //! Files in the static directory that represent vertices
  std::vector<std::string> staticNodes{
      "static/organisation_0_0.csv", "static/place_0_0.csv",
      "static/tag_0_0.csv", "static/tagclass_0_0.csv"};

  //! Files in the static directory that represent edges
  std::vector<std::string> staticEdges{
      "static/organisation_isLocatedIn_place_0_0.csv",
      "static/place_isPartOf_place_0_0.csv",
      "static/tag_hasType_tagclass_0_0.csv",
      "static/tagclass_isSubclassOf_tagclass_0_0.csv"};

  // note that original files have label names all lowercased: reason for
  // uppercase first letter is that the LDBC cypher queries all use
  // upper case first letters
  // TODO dynamics
  //! names of node labels in this dataset
  std::vector<std::string> nodeLabelNames{
      "Place",   "City",       "Country", "Continent", "Organisation",
      "Company", "University", "Tag",     "TagClass"};

  // TODO dynamics
  //! names of edge labels in this dataset
  std::vector<std::string> edgeLabelNames{"isSubclassOf", "hasType",
                                          "isLocatedIn", "isPartOf"};

  // TODO dynamics
  //! names of node attributes in this dataset
  std::vector<std::string> nodeAttributeNames{"id", "name", "url"};

  /**
   * Parse the organization file
   */
  void parseOrganization(std::string filepath);

public:
  /**
   * Constructor takes directory location and expected nodes/edges. Allocates
   * the memory required so only 1 pass through the files will be necessary
   * Initializes memory for node/edge labels and attributes.
   */
  LDBCReader(std::string _ldbcDirectory, GIDType _numNodes, uint64_t _numEdges);

  /**
   * Parses the "static" nodes/edges of the dataset
   *
   * @todo more details on what exactly occurs
   */
  void staticParsing();
};

#endif
