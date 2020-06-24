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

#include "galois/graphs/AttributedGraph.h"

/**
 *
 * Requires CsvComposite generation
 */
class LDBCReader {
public:
  // type def-ing them here in case they grow past 32 bytes
  //! type of global ids found in ldbc files
  using LDBCNodeType = uint64_t;
  //! type of global ids
  using GIDType = uint32_t;
  //! edge index type
  using EdgeIndex = uint64_t;
  //! map from an ldbc lid to graph's gid
  using GIDMap = std::unordered_map<LDBCNodeType, GIDType>;

private:
  //! Struct for holding edges read from disk in-memory
  struct SimpleReadEdge {
    //! source of edge
    GIDType src;
    //! dest of edge
    GIDType dest;
    //! Label on edge; set bits indicate which labels edge has
    uint32_t edgeLabel;
    //! simple constructor that just initializes fields
    SimpleReadEdge(GIDType _src, GIDType _dest, uint32_t _edgeLabel)
        : src(_src), dest(_dest), edgeLabel(_edgeLabel) {}
  };
  //! Struct for holding edges read that include an attribute from disk
  //! in-memory
  struct AttributedReadEdge {
    //! source of edge
    GIDType src;
    //! dest of edge
    GIDType dest;
    //! Label on edge; set bits indicate which labels edge has
    uint32_t edgeLabel;
    //! Attribute on edge
    std::string attribute;
    //! Attribute name
    const std::string& attributeName;
    //! constructor that just initializes fields
    AttributedReadEdge(GIDType _src, GIDType _dest, uint32_t _edgeLabel,
                       std::string&& _attribute,
                       const std::string& _attributeName)
        : src(_src), dest(_dest), edgeLabel(_edgeLabel), attribute(_attribute),
          attributeName(_attributeName) {}
  };

  //! enums for all the difference kinds of node labels
  //! granularity is based on the split of tags on disk rather than on
  //! the scheme itself
  enum NodeLabel {
    NL_ORG,
    NL_PLACE,
    NL_TAG,
    NL_TAGCLASS,
    NL_PERSON,
    NL_COMMENT,
    NL_POST,
    NL_FORUM
  };
  //! Two node labels that represent the source and dest type of an edge
  using ToFromMapping = std::pair<NodeLabel, NodeLabel>;
  //! num columns, source start column, attribute column; 0-indexed
  //! used to specify how to parse a file
  using ParseMetadata = std::tuple<size_t, size_t, size_t>;

  //! Underlying attribute graph
  galois::graphs::AttributedGraph attGraph;
  //! Path to the generated ldbc social network data
  std::string ldbcDirectory;
  //! Nodes that have been read so far
  GIDType gidOffset;
  //! All nodes with GIDs before finished nodes are finalized (i.e., edges
  //! all exist)
  GIDType finishedNodes;
  //! Edges that have been added to CSR so far
  EdgeIndex addedEdges;
  //! Total number of nodes to expect during reading
  GIDType totalNodes;
  //! Total number of edges to expect during reading
  EdgeIndex totalEdges;

  //! mapping organization ids to graph's gid
  GIDMap organization2GID;
  //! mapping place ids to graph's gid
  GIDMap place2GID;
  //! mapping tag ids to graph's gid
  GIDMap tag2GID;
  //! mapping tag class ids to graph's gid
  GIDMap tagClass2GID;
  //! mapping person ids to graph's gid
  GIDMap person2GID;
  //! mapping comment ids to graph's gid
  GIDMap comment2GID;
  //! mapping post ids to graph's gid
  GIDMap post2GID;
  //! mapping forum ids to graph's gid
  GIDMap forum2GID;

  // note that original files have label names all lowercased: reason for
  // uppercase first letter is that the LDBC cypher queries all use
  // upper case first letters
  //! strings for node labels in this dataset
  std::vector<std::string> nodeLabelNames{
      "Place",   "City",       "Country", "Continent", "Organisation",
      "Company", "University", "Tag",     "TagClass",  "Person",
      "Forum",   "Message",    "Post",    "Comment"};
  //! names of edge labels in this dataset
  std::vector<std::string> edgeLabelNames{
      "IS_SUBCLASS_OF", "HAS_TYPE",     "IS_LOCATED_IN", "IS_PART_OF",
      "HAS_INTEREST",   "HAS_TAG",      "STUDY_AT",      "WORK_AT",
      "KNOWS",          "LIKES",        "HAS_CREATOR",   "HAS_MEMBER",
      "HAS_MODERATOR",  "CONTAINER_OF", "REPLY_OF"};
  //! names of node attributes in this dataset
  std::vector<std::string> nodeAttributeNames{
      "id",          "name",       "url",      "creationDate", "firstName",
      "lastName",    "gender",     "birthday", "email",        "speaks",
      "browserUsed", "locationIP", "title",    "language",     "imageFile",
      "content",     "length"};
  //! names of edge attributes in this dataset
  std::vector<std::string> edgeAttributeNames{"classYear", "workFrom",
                                              "creationDate", "joinDate"};

  //! Denotes region of nodes in graph that belongs to nodes of a certain type
  struct NodeLabelPosition {
    //! starting point of region
    GIDType offset;
    //! number of nodes associated with the node type
    GIDType count;
    //! simple constructor that just initializes both fields
    NodeLabelPosition(GIDType _offset, GIDType _count)
        : offset(_offset), count(_count) {}
  };
  //! Maps from a node label type to the region of nodes in the GID
  std::unordered_map<NodeLabel, NodeLabelPosition> nodeLabel2Position;

  //////////////////////////////////////////////////////////////////////////////

  /**
   * Given a NodeLabel enum, return the lid -> gid map associated with it
   */
  GIDMap& getGIDMap(NodeLabel nodeType);

  /**
   * Tag attributes with their type
   */
  void setupAttributeTypes();

  /**
   * Parse the organization file: get label (company/university) and save
   * to node + save name and url to attributes as well.
   */
  void parseOrganizationCSV(const std::string filepath);

  /**
   * Parse the place file: get label (country/city/continent) and save
   * to node + save name and url to attributes as well.
   */
  void parsePlaceCSV(const std::string filepath);

  /**
   * Parse the tag file: id, name, url
   */
  void parseTagCSV(const std::string filepath);

  /**
   * Parse the tag class file: id, name, url
   */
  void parseTagClassCSV(const std::string filepath);

  /**
   * Parse the person file:
   * creation|deletion|id|firstName|lastName|gender|birthday|locationIP|browser|
   * language|email
   *
   * deletion is ignored
   */
  void parsePersonCSV(const std::string filepath);

  /**
   * Parse the forum file:
   * creation|id|title|type
   *
   * type is ignored
   */
  void parseForumCSV(const std::string filepath);

  /**
   * Parse the post file:
   * creation|id|image|locationIP|browser|language|content|length
   */
  void parsePostCSV(const std::string filepath);

  /**
   * Parse the comment file:
   * creation|id|locationIP|browser|content|length
   */
  void parseCommentCSV(const std::string filepath);

  /**
   * Parse a simple edge CSV (2 columns, source|destination). Edges with
   * other attributes should not use this function. There is an argument
   * that allows for skipping prefix columns if necessary.
   *
   * @param filepath Path to edge CSV
   * @param edgeType Edge label to give edges parsed by this file
   * @param nodeFrom Source node label
   * @param nodeTo Edge node label
   * @param gidOffset GID offset for source node class (i.e. at what gid
   * do nodes of that class start)
   * @param[input,output] edgesPerNode array that tracks how many edges each
   * node of the source class has
   * @param[input,output] readEdges Contains the edges read from disk
   * with labels
   * @param skipColumns number of columns to skip on each line
   *
   * @return Number of edges parsed from the file
   */
  size_t parseSimpleEdgeCSV(const std::string filepath,
                            const std::string edgeType, NodeLabel nodeFrom,
                            NodeLabel nodeTo, GIDType gidOffset,
                            std::vector<EdgeIndex>& edgesPerNode,
                            std::vector<SimpleReadEdge>& readEdges,
                            const size_t skipColumns);

  /**
   * Construct the edges in the underlying CSR graph.
   *
   * Should handle all edges associated with a node label class.
   *
   * @param gidOffset First node that this edge set should include;
   * first node of the label class
   * @param numReadEdges Number of edges to be added; should be
   * equal to number of edges in readEdges vector
   * @param edgesPerNode Vector telling me how many edges a particular
   * node in this node label class contains.
   * @param readEdges Actual edges to be added to CSR
   * @param readAttEdges Edges with attributes to be added to CSR
   */
  void constructCSREdges(GIDType gidOffset, size_t numReadEdges,
                         std::vector<EdgeIndex>& edgesPerNode,
                         std::vector<SimpleReadEdge>& readEdges,
                         std::vector<AttributedReadEdge>& readAttEdges);

  /**
   * Parses the edges of some file and construct them; only works if
   * (1) edges have no attributes and (2) all edges of a label class are in a
   * single file and not multiple files
   *
   * @param filepath Edge file to parse
   * @param edgeType Label of edge present in the parsed file
   * @param nodeFrom Source node label
   * @param nodeTo Edge node label
   */
  void parseAndConstructSimpleEdges(const std::string filepath,
                                    const std::string edgeType,
                                    NodeLabel nodeFrom, NodeLabel nodeTo);

  /**
   * Parse a specified attributed edge CSV, counts edges read, and saves
   * read edges into memory. Arguments to function specify how to read the CSV
   * e.g., columns to read, attribute name to save, etc.
   *
   * @warning The function only supports reading ONE attribute from the CSV
   *
   * @param filepath Edge file to parse
   * @param edgeType Label of edge present in the parsed file
   * @param nodeFrom Source node label
   * @param nodeTo Edge node label
   * @param gidOffset GID offset for source node class (i.e. at what gid
   * do nodes of that class start)
   * @param howToRead Tuple specifying num of columns and columns to read in the
   * CSV
   * @param attributeName Reference to string that has the name of the attribute
   * to read in this CSV
   * @param edgesPerNode Vector telling me how many edges a particular node has
   * @param readAttEdges Vector to store newly read attributed edges into
   *
   * @returns Number of edges/lines parsed
   */
  size_t parseEdgeCSVSpecified(const std::string filepath,
                               const std::string edgeType, NodeLabel nodeFrom,
                               NodeLabel nodeTo, const GIDType gidOffset,
                               const ParseMetadata howToRead,
                               const std::string& attributeName,
                               std::vector<EdgeIndex>& edgesPerNode,
                               std::vector<AttributedReadEdge>& readAttEdges);

  /**
   * Given data on what files to parse and how to parse them, do the parsing
   * and save the read edges to memory + count how many edges per node
   * there are in this batch
   *
   * @param gidOffset Offset into the class of nodes being handled by this
   * function
   * @param simpleFiles Relative paths of files to parse that are simple,
   * i.e. src/dest only with columns to skip in beginning
   * @param simpleEdgeTypes edge type for each file in simpleFiles
   * @param simpleMappings src/dest labels of edge being read in
   * @param attributedFiles Relative paths of edges files with an attribute
   * @param attributedEdgeTypes Edge type for each file in attributedFiles
   * @param attributedMappings src/dest labels for attributed file edges
   * @param attributeHowToParse Metadata specifying how to handle columns for
   * attributed files
   * @param attributeOnEdge Name of the attribute on the edge of a att. file
   * @param edgesPerNode Vector telling me how many edges a particular node has
   * @param readSimpleEdges Buffer of edges read from simple files
   * @param readAttEdges Buffer of edges read from attributed edge files
   * @param simpleColumnsSkipped Number of columns to skip from beginning when
   * reading a simple edge file to get to the src/dest columns
   */
  size_t doParse(const GIDType gidOffset,
                 const std::vector<std::string>& simpleFiles,
                 const std::vector<std::string>& simpleEdgeTypes,
                 const std::vector<ToFromMapping>& simpleMappings,
                 const std::vector<std::string>& attributedFiles,
                 const std::vector<std::string>& attributedEdgeTypes,
                 const std::vector<ToFromMapping>& attributedMappings,
                 const std::vector<ParseMetadata>& attributeHowToParse,
                 const std::vector<std::string>& attributeOnEdge,
                 std::vector<EdgeIndex>& edgesPerNode,
                 std::vector<SimpleReadEdge>& readSimpleEdges,
                 std::vector<AttributedReadEdge>& readAttEdges,
                 const size_t simpleColumnsSkipped);

  /**
   * Parses all edge files of outgoing edges for the person node class
   * and adds the edges to the underlying CSR graph.
   */
  void parseAndConstructPersonEdges();

  /**
   * Parses all edge files of outgoing edges for the forum node class
   * and adds the edges to the underlying CSR graph.
   */
  void parseAndConstructForumEdges();

  /**
   * Parses all edge files of outgoing edges for the post node class
   * and adds the edges to the underlying CSR graph.
   */
  void parseAndConstructPostEdges();

  /**
   * Parses all edge files of outgoing edges for the comment node class
   * and adds the edges to the underlying CSR graph.
   */
  void parseAndConstructCommentEdges();

  /**
   * Parses the "static" nodes/edges of the dataset. First parses all nodes,
   * then parses all edges of those nodes.
   *
   * Node classes in this include organization, place, tag, and tag class
   */
  void staticParsing();

  /**
   * Parses the "dynamic" nodes/edges of the dataset. First parses all nodes,
   * then parses all edges of those nodes. One major differnece with the
   * static parsing is that edges for a node class are scattered across
   * multiple files; this requires reading all such files before adding
   * the edges to the underlying CSR.
   *
   * Node classes in this include person, forum, comment, post.
   */
  void dynamicParsing();

public:
  /**
   * Constructor takes directory location and expected nodes/edges. Allocates
   * the memory required so only 1 pass through the files will be necessary
   * Initializes memory for node/edge labels and attributes.
   */
  LDBCReader(std::string _ldbcDirectory, GIDType _numNodes, uint64_t _numEdges);

  /**
   * Parses entire LDBC directory and serializes the attributed graph to disk
   * to the specified file.
   *
   * @param outputFile Name of serialized AttributedGraph
   */
  void parseAndSave(std::string outputFile);
};

#endif
