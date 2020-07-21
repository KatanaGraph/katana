/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file cypher-compile-test.cpp
 *
 * Various unit tests for the cypher compiler. Verifies that what it outputs is
 * sane/expected. Note that there are some assumptions about the order the
 * compiler parses things is present in these tests.
 */

#include "galois/Galois.h"
#include "querying/CypherCompiler.h"
#include "galois/Logging.h"

////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////

//! Verifies number of nodes/edges match some expected value
void assert_query_node_edge_count(galois::CypherCompiler& cc, size_t num_nodes,
                                  size_t num_edges) {
  GALOIS_LOG_ASSERT(cc.getQueryNodes().size() == num_nodes);
  GALOIS_LOG_ASSERT(cc.getQueryEdges().size() == num_edges);
}

//! Assert that the return modifiers did not change from their default values
//! after compilation
void assert_no_return_modifiers(const galois::CypherCompiler& cc) {
  const galois::CompilerReturnMetadata& rm = cc.getReturnMetadata();
  GALOIS_LOG_VASSERT(!rm.return_skip, "Return metadata should not have a skip");
  GALOIS_LOG_VASSERT(!rm.return_limit,
                     "Return metadata should not have a limit");
  GALOIS_LOG_VASSERT(!rm.distinct_return,
                     "Return metadata should not have distinct be true");
}

//! Asserts the following:
//! - only a single return
//! - the single return is just a var name with no property access
//! - not a count
void assert_basic_return_value(const galois::CypherCompiler& cc,
                               std::string var_name) {
  GALOIS_LOG_VASSERT(cc.getReturnValues().size() == 1,
                     "Should only have a single return value");
  const galois::CompilerQueryResult& r = cc.getReturnValues()[0];

  GALOIS_LOG_VASSERT(r.base_name == var_name,
                     "Basic return basename should be {} not {}", var_name,
                     r.base_name);
  GALOIS_LOG_VASSERT(r.property_name.empty(),
                     "Basic return property name should be empty not {}",
                     r.property_name);
  GALOIS_LOG_VASSERT(r.return_name == var_name,
                     "Basic return return name should be {} not {}", var_name,
                     r.return_name);
  GALOIS_LOG_VASSERT(!r.is_count, "Basic return is not a count");
}

//! Wrapper to call the 2 basic return assertion functions; default var name a
void assert_basic_return(const galois::CypherCompiler& cc) {
  assert_basic_return_value(cc, "a");
  assert_no_return_modifiers(cc);
}

//! Wrapper to call the 2 basic return assertion functions
void assert_basic_return(const galois::CypherCompiler& cc,
                         std::string var_name) {
  assert_basic_return_value(cc, var_name);
  assert_no_return_modifiers(cc);
}

//! verifies a parsed node has some expected values
//! TODO need a version that ignores ID because we shouldn't assume that ID
//! is generated the same way every time (future proofing)
void verify_node(const galois::CompilerQueryNode& n,
                 galois::CompilerQueryNode expected) {
  GALOIS_LOG_VASSERT(n.id == expected.id, "Expected node id is {}, found {}",
                     expected.id, n.id);
  GALOIS_LOG_VASSERT(n.labels == expected.labels,
                     "Expected label stringis {}, found {}", expected.labels,
                     n.labels);
  GALOIS_LOG_VASSERT(n.var_name == expected.var_name,
                     "Expected var name is {}, found {}", expected.var_name,
                     n.var_name);
  GALOIS_LOG_VASSERT(n.path_name == expected.path_name,
                     "Expected path name is {}, found {}", expected.path_name,
                     n.path_name);
}

void verify_edge(const galois::CompilerQueryEdge& e,
                 galois::CompilerQueryEdge expected) {
  // check nodes
  verify_node(e.caused_by, expected.caused_by);
  verify_node(e.acted_on, expected.acted_on);
  // verify everything else
  GALOIS_LOG_VASSERT(e.label == expected.label,
                     "Expected label string is {}, found {}", expected.label,
                     e.label);
  GALOIS_LOG_VASSERT(e.var_name == expected.var_name,
                     "Expected var name is {}, found {}", expected.var_name,
                     e.var_name);
  GALOIS_LOG_VASSERT(e.path_name == expected.path_name,
                     "Expected path name is {}, found {}", expected.path_name,
                     e.path_name);
}

void verify_return(const galois::CompilerQueryResult& e,
                   galois::CompilerQueryResult expected) {
  GALOIS_LOG_VASSERT(e.base_name == expected.base_name,
                     "Expected return base name is {}, found {}",
                     expected.base_name, e.base_name);
  GALOIS_LOG_VASSERT(e.property_name == expected.property_name,
                     "Expected return property name is {}, found {}",
                     expected.property_name, e.property_name);
  GALOIS_LOG_VASSERT(e.return_name == expected.return_name,
                     "Expected return return name is {}, found {}",
                     expected.property_name, e.property_name);
  GALOIS_LOG_VASSERT(e.is_count == expected.is_count,
                     "Expected return count status is {}, found {}",
                     expected.is_count, e.is_count);
}

void verify_return_modifier(const galois::CypherCompiler& cc,
                            const galois::CompilerReturnMetadata& expected) {
  const galois::CompilerReturnMetadata& rm = cc.getReturnMetadata();
  GALOIS_LOG_VASSERT(rm.return_skip == expected.return_skip,
                     "Return metadata skip does not match expected");
  GALOIS_LOG_VASSERT(rm.return_limit == expected.return_limit,
                     "Return metadata limit does not match limit");
  GALOIS_LOG_VASSERT(rm.distinct_return == expected.distinct_return,
                     "Return metadata distinct expected is {}, found {}",
                     expected.distinct_return, rm.distinct_return);
}

////////////////////////////////////////////////////////////////////////////////
// Main
////////////////////////////////////////////////////////////////////////////////

int main() {
  //////////////////////////////////////////////////////////////////////////////
  // init
  //////////////////////////////////////////////////////////////////////////////
  galois::SharedMemSys G;
  galois::CypherCompiler cc;

  //////////////////////////////////////////////////////////////////////////////
  // basic node testing first
  //////////////////////////////////////////////////////////////////////////////
  // single node
  GALOIS_LOG_WARN("Basic node 1");
  std::string basic_node1 = "match (a) return a;";
  cc.compile(basic_node1.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  assert_query_node_edge_count(cc, 1, 0);
  assert_basic_return(cc);

  // single node with label
  GALOIS_LOG_WARN("Basic node 2");
  std::string basic_node2 = "match (b:Test) return a;";
  cc.compile(basic_node2.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "Test", "b", ""});
  assert_query_node_edge_count(cc, 1, 0);
  assert_basic_return(cc);

  // single node with 2 labels
  GALOIS_LOG_WARN("Basic node 3");
  std::string basic_node3 = "match (a:Test:Test2) return a;";
  cc.compile(basic_node3.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "Test;Test2", "a", ""});
  assert_query_node_edge_count(cc, 1, 0);
  assert_basic_return(cc);

  // single node bound to a path
  GALOIS_LOG_WARN("Basic node 4");
  std::string basic_node4 = "match path=(a:Test:Test2) return a;";
  cc.compile(basic_node4.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "Test;Test2", "a", "path"});
  assert_query_node_edge_count(cc, 1, 0);
  assert_basic_return(cc);

  //////////////////////////////////////////////////////////////////////////////
  // edge testing next
  //////////////////////////////////////////////////////////////////////////////
  // NOTE: query node contains 0 nodes as all nodes are part of edges
  GALOIS_LOG_WARN("Basic edge 1");
  std::string basic_edge1 = "match ()-[e]->() return e;";
  cc.compile(basic_edge1.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"0", "any", "", ""},
                  galois::CompilerQueryNode{"1", "any", "", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  assert_query_node_edge_count(cc, 0, 1);
  assert_basic_return(cc, "e");

  // edge with label
  GALOIS_LOG_WARN("Basic edge 2");
  std::string basic_edge2 = "match (a:Test)-[e:SOME]->(k) return e;";
  cc.compile(basic_edge2.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "SOME", galois::CompilerQueryNode{"0", "Test", "a", ""},
                  galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  assert_query_node_edge_count(cc, 0, 1);
  assert_basic_return(cc, "e");

  // undirected edge
  GALOIS_LOG_WARN("Basic edge 3");
  std::string basic_edge3 = "match (a:Test)-[e:SOME]-(k) return e;";
  cc.compile(basic_edge3.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "SOME", galois::CompilerQueryNode{"0", "Test", "a", ""},
                  galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::UNDIRECTED_EDGE, "e", ""});
  assert_query_node_edge_count(cc, 0, 1);
  assert_basic_return(cc, "e");

  // assumes creation of nodes in a certain order; note source/dst are flipped
  // from previous tests
  GALOIS_LOG_WARN("Basic edge 4");
  std::string basic_edge4 = "match (a:Test)<-[e:SOME]-(k) return e;";
  cc.compile(basic_edge4.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "SOME", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"0", "Test", "a", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  assert_query_node_edge_count(cc, 0, 1);
  assert_basic_return(cc, "e");

  // path bound edge
  GALOIS_LOG_WARN("Basic edge 5");
  std::string basic_edge5 = "match p=(a:Test)<-[e:SOME]-(k) return e;";
  cc.compile(basic_edge5.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "SOME", galois::CompilerQueryNode{"1", "any", "k", "p"},
                  galois::CompilerQueryNode{"0", "Test", "a", "p"},
                  galois::DIRECTED_EDGE, "e", "p"});
  assert_query_node_edge_count(cc, 0, 1);
  assert_basic_return(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // more than one edge tests
  //////////////////////////////////////////////////////////////////////////////
  // basic 2 edge test
  GALOIS_LOG_WARN("Multi edge 1");
  std::string multi_edge1 = "match ()<-[e]-(k)-[f]->() return e;";
  cc.compile(multi_edge1.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"0", "any", "", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"2", "any", "", ""},
                  galois::DIRECTED_EDGE, "f", ""});
  assert_query_node_edge_count(cc, 0, 2);
  assert_basic_return(cc, "e");

  // 2 edge test with undirected + labels
  GALOIS_LOG_WARN("Multi edge 2");
  std::string multi_edge2 =
      "match (a:Test)-[e]-(k:Test2)<-[f:WELP]-() return e;";
  cc.compile(multi_edge2.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"0", "Test", "a", ""},
                  galois::CompilerQueryNode{"1", "Test2", "k", ""},
                  galois::UNDIRECTED_EDGE, "e", ""});
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "WELP", galois::CompilerQueryNode{"2", "any", "", ""},
                  galois::CompilerQueryNode{"1", "Test2", "k", ""},
                  galois::DIRECTED_EDGE, "f", ""});
  assert_query_node_edge_count(cc, 0, 2);
  assert_basic_return(cc, "e");

  // 3 edge test
  GALOIS_LOG_WARN("Multi edge 3");
  std::string multi_edge3 =
      "match p=(a:Test:Also)-[e:SOME]-(k:Test2)<-[f:WELP]-()-->(noname) return "
      "e;";
  cc.compile(multi_edge3.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "SOME", galois::CompilerQueryNode{"0", "Test;Also", "a", "p"},
                  galois::CompilerQueryNode{"1", "Test2", "k", "p"},
                  galois::UNDIRECTED_EDGE, "e", "p"});
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "WELP", galois::CompilerQueryNode{"2", "any", "", "p"},
                  galois::CompilerQueryNode{"1", "Test2", "k", "p"},
                  galois::DIRECTED_EDGE, "f", "p"});
  verify_edge(cc.getQueryEdges()[2],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"2", "any", "", "p"},
                  galois::CompilerQueryNode{"3", "any", "noname", "p"},
                  galois::DIRECTED_EDGE, "", "p"});
  assert_query_node_edge_count(cc, 0, 3);
  assert_basic_return(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // Split edge
  //////////////////////////////////////////////////////////////////////////////
  // test here is to make sure k is the same id even though it's split
  GALOIS_LOG_WARN("Split edge 1");
  std::string split_edge1 = "match ()<-[e]-(k), (k)-[f]->() return e;";
  cc.compile(split_edge1.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"0", "any", "", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"2", "any", "", ""},
                  galois::DIRECTED_EDGE, "f", ""});
  assert_query_node_edge_count(cc, 0, 2);
  assert_basic_return(cc, "e");

  GALOIS_LOG_WARN("Split edge 2");
  std::string split_edge2 = "match ()<-[e]-(k:Test), (k)-[f]->() return e;";
  cc.compile(split_edge2.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "Test", "k", ""},
                  galois::CompilerQueryNode{"0", "any", "", ""},
                  galois::DIRECTED_EDGE, "e", ""});
  // TODO even though k refers to the same k:Test, label ends up as "any"
  // The difference is fixed during query graph construction if I recall
  // correctly
  // TODO should this be fixed in the compiler end as well?
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                  galois::CompilerQueryNode{"2", "any", "", ""},
                  galois::DIRECTED_EDGE, "f", ""});
  assert_query_node_edge_count(cc, 0, 2);
  assert_basic_return(cc, "e");

  // make sure old node ids are kept if referred to more than once + check paths
  GALOIS_LOG_WARN("Split edge 3");
  std::string split_edge3 =
      "match p=(a)<-[e]-(k), q=(k)-[f]->(b), r=(b)-[g]->(a) return e;";
  cc.compile(split_edge3.c_str());
  verify_edge(cc.getQueryEdges()[0],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", "p"},
                  galois::CompilerQueryNode{"0", "any", "a", "p"},
                  galois::DIRECTED_EDGE, "e", "p"});
  verify_edge(cc.getQueryEdges()[1],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"1", "any", "k", "q"},
                  galois::CompilerQueryNode{"2", "any", "b", "q"},
                  galois::DIRECTED_EDGE, "f", "q"});
  verify_edge(cc.getQueryEdges()[2],
              galois::CompilerQueryEdge{
                  "ANY", galois::CompilerQueryNode{"2", "any", "b", "r"},
                  galois::CompilerQueryNode{"0", "any", "a", "r"},
                  galois::DIRECTED_EDGE, "g", "r"});

  assert_query_node_edge_count(cc, 0, 3);
  assert_basic_return(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // return tests
  //////////////////////////////////////////////////////////////////////////////

  // check if multiple returns are caught
  GALOIS_LOG_WARN("Return 1");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return1 = "match (a) return a, b, c;";
  cc.compile(return1.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "b", false));
  verify_return(cc.getReturnValues()[2],
                galois::CompilerQueryResult("c", "", "c", false));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 3);
  assert_no_return_modifiers(cc);

  // check if count is parsed correctly
  GALOIS_LOG_WARN("Return 2");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return2 = "match (a) return a, count(b), count(c);";
  cc.compile(return2.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "count(b)", true));
  verify_return(cc.getReturnValues()[2],
                galois::CompilerQueryResult("c", "", "count(c)", true));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 3);
  assert_no_return_modifiers(cc);

  // return properties
  GALOIS_LOG_WARN("Return 3");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return3 =
      "match (a) return a.thing1, count(b.thing2), count(c.thing3);";
  cc.compile(return3.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "thing1", "a.thing1", false));
  verify_return(
      cc.getReturnValues()[1],
      galois::CompilerQueryResult("b", "thing2", "count(b.thing2)", true));
  verify_return(
      cc.getReturnValues()[2],
      galois::CompilerQueryResult("c", "thing3", "count(c.thing3)", true));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 3);
  assert_no_return_modifiers(cc);

  // AS clause
  GALOIS_LOG_WARN("Return 4");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return4 = "match (a) return a.thing1 as one, count(b.thing2) as "
                        "two, count(c.thing3) as three;";
  cc.compile(return4.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "thing1", "one", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "thing2", "two", true));
  verify_return(cc.getReturnValues()[2],
                galois::CompilerQueryResult("c", "thing3", "three", true));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 3);
  assert_no_return_modifiers(cc);

  //////////////////////////////////////////////////////////////////////////////
  // return modifiers
  //////////////////////////////////////////////////////////////////////////////

  // distinct
  GALOIS_LOG_WARN("Return Mods 1");
  std::string return_mod1 = "match (a) return distinct a, b;";
  cc.compile(return_mod1.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "b", false));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 2);
  verify_return_modifier(
      cc, galois::CompilerReturnMetadata{std::nullopt, std::nullopt, true});

  // skip
  GALOIS_LOG_WARN("Return Mods 2");
  std::string return_mod2 = "match (a) return a, b skip 3;";
  cc.compile(return_mod2.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "b", false));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 2);
  verify_return_modifier(
      cc, galois::CompilerReturnMetadata{3, std::nullopt, false});

  // limit
  GALOIS_LOG_WARN("Return Mods 3");
  std::string return_mod3 = "match (a) return a, b limit 100;";
  cc.compile(return_mod3.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "b", false));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 2);
  verify_return_modifier(
      cc, galois::CompilerReturnMetadata{std::nullopt, 100, false});

  // all 3 basic mods
  GALOIS_LOG_WARN("Return Mods 4");
  std::string return_mod4 = "match (a) return distinct a, b skip 3 limit 100;";
  cc.compile(return_mod4.c_str());
  verify_node(cc.getQueryNodes()[0],
              galois::CompilerQueryNode{"0", "any", "a", ""});
  verify_return(cc.getReturnValues()[0],
                galois::CompilerQueryResult("a", "", "a", false));
  verify_return(cc.getReturnValues()[1],
                galois::CompilerQueryResult("b", "", "b", false));
  GALOIS_LOG_ASSERT(cc.getReturnValues().size() == 2);
  verify_return_modifier(cc, galois::CompilerReturnMetadata{3, 100, true});

  //////////////////////////////////////////////////////////////////////////////

  // TODO when we get to implementing/reviving
  // - order by
  // - star paths
  // - shortest paths
  // - WHERE
  // - WITH
  // - etc.
  return 0;
}
