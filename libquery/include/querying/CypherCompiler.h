#pragma once

#include <assert.h>
#include <unordered_map>
#include <iostream>
#include <stack>
#include "querying/GraphSimulation.h"
// cypher parser includes
#include <cypher-parser.h>
#include <astnode.h>
#include <result.h>

using CypherASTNode = const cypher_astnode_t*;
class CypherCompiler {
  unsigned numNodeIDs;
  unsigned numEdgeIDs;
  std::unordered_map<std::string, unsigned> nodeIDs;
  std::unordered_map<CypherASTNode, unsigned> anonNodeIDs;
  std::unordered_map<std::string, unsigned> edgeIDs;
  std::unordered_map<CypherASTNode, unsigned> anonEdgeIDs;
  std::unordered_map<std::string, std::string> contains;
  std::unordered_map<std::string, uint32_t> timestamps;
  std::unordered_map<std::string, std::string> labels;
  std::unordered_map<std::string, std::string> pathConstraints;
  bool shortestPath;
  std::string namedPath;

  //! set of nodes that comprises the query graph
  // Currently, used only when there are no edges in the query graph
  std::vector<MatchedNode> qNodes;
  //! set of edges that comprises the query graph
  std::vector<MatchedEdge> ir;
  //! string filters
  std::vector<const char*> filters;
  std::stack<bool> bin_op; // true => AND, false => OR

  unsigned getNodeID(std::string str);

  /**
   * Given a pointer to some AST node, find its ID (if it exists), else create
   * a mapping for it
   */
  unsigned getAnonNodeID(CypherASTNode node);

  /**
   * Given a string rperesdenting some edge, find its id (if it exists),
   * else create mapping for it
   */
  unsigned getEdgeID(std::string str);

  unsigned getAnonEdgeID(CypherASTNode node);

  /**
   * Compile a node pattern from a pattern path.
   *
   * TODO not necessarily required from a pattern path
   *
   * @param element node pattern AST node
   * @param mn Matched node structure to save results of parse to (id, name)
   */
  void compile_node_pattern_path(CypherASTNode element, MatchedNode& mn);

  //! Processes an edge between 2 nodes in a pattern path
  void compile_rel_pattern_path(CypherASTNode element);

  /**
   * Compile a pattern path which is found in a MATCH, MERGE, or CREATE
   * clause.
   *
   * Pattern paths are node patterns connected by rel patterns.
   *
   * Current implementation assumes that it will be in a MATCH clause;
   * TODO make it work with the rest eventually.
   * @param ast Root of the pattern path AST
   */
  int compile_pattern_path(CypherASTNode ast);

  void compile_binary_operator(CypherASTNode ast, bool negate);

  void compile_comparison(CypherASTNode ast);

  void compile_labels_operator(CypherASTNode ast, std::string prefix);

  void compile_unary_operator(CypherASTNode ast);

  void compile_list_comprehension(CypherASTNode ast, std::string prefix);

  void compile_none(CypherASTNode ast);

  void compile_expression(CypherASTNode ast);

  /**
   * Handle a projection node
   */
  int compile_ast_projection(CypherASTNode projectionAST);

  /**
   * Handle a return node
   *
   * TODO there are a variety of things a return node can have under it; for
   * now just handle projections
   */
  int compile_ast_return(CypherASTNode returnAST);

  /**
   * Recursively handle an AST node and its children.
   *
   * @param ast Root of tree to handle
   */
  int compile_ast_node(CypherASTNode ast);

  /**
   * Descend children of root AST and recursively process them.
   *
   * Depth first search based descent on the tree.
   *
   * @param ast Result of some parse from the cypher
   */
  int compile_ast(const cypher_parse_result_t* ast);

  //! Sets all vars to 0 or clears all data structures
  void init();

public:
  CypherCompiler() {}

  auto& getQNodes() { return qNodes; }

  auto& getIR() { return ir; }

  auto& getFilters() { return filters; }

  //! Given a query string, compile it and store the galois IR results in this
  //! object
  int compile(const char* queryStr);
};
