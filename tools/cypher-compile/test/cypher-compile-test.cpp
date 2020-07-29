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
void AssertQueryNodeEdgeCount(galois::CypherCompiler& cc, size_t num_nodes,
                              size_t num_edges) {
  GALOIS_LOG_ASSERT(cc.GetQueryNodes().size() == num_nodes);
  GALOIS_LOG_ASSERT(cc.GetQueryEdges().size() == num_edges);
}

//! Assert that the return modifiers did not change from their default values
//! after compilation
void AssertNoReturnModifiers(const galois::CypherCompiler& cc) {
  const galois::CompilerReturnMetadata& rm = cc.GetReturnMetadata();
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
void AssertBasicReturnValue(const galois::CypherCompiler& cc,
                            std::string var_name) {
  GALOIS_LOG_VASSERT(cc.GetReturnValues().size() == 1,
                     "Should only have a single return value");
  const galois::QueryProperty& r = cc.GetReturnValues()[0];

  GALOIS_LOG_VASSERT(r.variable_name == var_name,
                     "Basic return basename should be {} not {}", var_name,
                     r.variable_name);
  GALOIS_LOG_VASSERT(!r.property_name,
                     "Basic return property name should not exist, not be {}",
                     r.property_name.value());
  GALOIS_LOG_VASSERT(!r.function_name,
                     "Basic return function name should not exist, not be {}",
                     r.function_name.value());
  GALOIS_LOG_VASSERT(r.alias.value() == var_name,
                     "Basic return return name should be {} not {}", var_name,
                     r.alias.value());
}

//! Wrapper to call the 2 basic return assertion functions; default var name a
void AssertBasicReturn(const galois::CypherCompiler& cc) {
  AssertBasicReturnValue(cc, "a");
  AssertNoReturnModifiers(cc);
}

//! Wrapper to call the 2 basic return assertion functions
void AssertBasicReturn(const galois::CypherCompiler& cc, std::string var_name) {
  AssertBasicReturnValue(cc, var_name);
  AssertNoReturnModifiers(cc);
}

//! verifies a parsed node has some expected values
//! TODO need a version that ignores ID because we shouldn't assume that ID
//! is generated the same way every time (future proofing)
void VerifyNode(const galois::CompilerQueryNode& n,
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

void VerifyEdge(const galois::CompilerQueryEdge& e,
                galois::CompilerQueryEdge expected) {
  // check nodes
  VerifyNode(e.caused_by, expected.caused_by);
  VerifyNode(e.acted_on, expected.acted_on);
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

void VerifyReturn(const galois::QueryProperty& e,
                  galois::QueryProperty expected) {
  GALOIS_LOG_VASSERT(e.variable_name == expected.variable_name,
                     "Expected return base name is {}, found {}",
                     expected.variable_name, e.variable_name);
  GALOIS_LOG_VASSERT(e.property_name == expected.property_name,
                     "Expected return property name is {}, found {}",
                     expected.property_name.value_or(""),
                     e.property_name.value_or(""));
  GALOIS_LOG_VASSERT(e.function_name == expected.function_name,
                     "Expected return function name is {}, found {}",
                     expected.function_name.value_or(""),
                     e.function_name.value_or(""));
  GALOIS_LOG_VASSERT(e.alias == expected.alias,
                     "Expected return return name is {}, found {}",
                     expected.alias.value_or(""), e.alias.value_or(""));
}

void VerifyOrderByStruct(const galois::CompilerOrderByMetadata& result,
                         const galois::CompilerOrderByMetadata& expected) {
  GALOIS_LOG_VASSERT(
      result.elements_to_order.size() == expected.elements_to_order.size(),
      "Number of elements to order by differs: expect {}, found {}",
      expected.elements_to_order.size(), result.elements_to_order.size());

  for (size_t i = 0; i < expected.elements_to_order.size(); i++) {
    GALOIS_LOG_VASSERT(
        result.elements_to_order[i].Equals(expected.elements_to_order[i]),
        "Order-by element {} differs between found and expected", i);
    GALOIS_LOG_VASSERT(result.is_ascending[i] == expected.is_ascending[i],
                       "Order-by ascend/descend for element {} differs between "
                       "found {} and expected {}",
                       i, result.is_ascending[i], expected.is_ascending[i]);
  }
}

void VerifyReturnModifier(const galois::CypherCompiler& cc,
                          const galois::CompilerReturnMetadata& expected) {
  const galois::CompilerReturnMetadata& rm = cc.GetReturnMetadata();
  GALOIS_LOG_VASSERT(rm.return_skip == expected.return_skip,
                     "Return metadata skip does not match expected");
  GALOIS_LOG_VASSERT(rm.return_limit == expected.return_limit,
                     "Return metadata limit does not match limit");
  GALOIS_LOG_VASSERT(rm.distinct_return == expected.distinct_return,
                     "Return metadata distinct expected is {}, found {}",
                     expected.distinct_return, rm.distinct_return);
  if (expected.order_by) {
    GALOIS_LOG_VASSERT(
        rm.order_by,
        "Order by struct for result does not exist even though it is expected");
    VerifyOrderByStruct(rm.order_by.value(), expected.order_by.value());
  } else {
    GALOIS_LOG_VASSERT(
        !rm.order_by,
        "Order by struct for result exists even though it should not");
  }
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
  cc.Compile(basic_node1.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  AssertQueryNodeEdgeCount(cc, 1, 0);
  AssertBasicReturn(cc);

  // single node with label
  GALOIS_LOG_WARN("Basic node 2");
  std::string basic_node2 = "match (b:Test) return a;";
  cc.Compile(basic_node2.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "Test", "b", ""});
  AssertQueryNodeEdgeCount(cc, 1, 0);
  AssertBasicReturn(cc);

  // single node with 2 labels
  GALOIS_LOG_WARN("Basic node 3");
  std::string basic_node3 = "match (a:Test:Test2) return a;";
  cc.Compile(basic_node3.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "Test;Test2", "a", ""});
  AssertQueryNodeEdgeCount(cc, 1, 0);
  AssertBasicReturn(cc);

  // single node bound to a path
  GALOIS_LOG_WARN("Basic node 4");
  std::string basic_node4 = "match path=(a:Test:Test2) return a;";
  cc.Compile(basic_node4.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "Test;Test2", "a", "path"});
  AssertQueryNodeEdgeCount(cc, 1, 0);
  AssertBasicReturn(cc);

  //////////////////////////////////////////////////////////////////////////////
  // edge testing next
  //////////////////////////////////////////////////////////////////////////////
  // NOTE: query node contains 0 nodes as all nodes are part of edges
  GALOIS_LOG_WARN("Basic edge 1");
  std::string basic_edge1 = "match ()-[e]->() return e;";
  cc.Compile(basic_edge1.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"0", "any", "", ""},
                 galois::CompilerQueryNode{"1", "any", "", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  AssertQueryNodeEdgeCount(cc, 0, 1);
  AssertBasicReturn(cc, "e");

  // edge with label
  GALOIS_LOG_WARN("Basic edge 2");
  std::string basic_edge2 = "match (a:Test)-[e:SOME]->(k) return e;";
  cc.Compile(basic_edge2.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "SOME", galois::CompilerQueryNode{"0", "Test", "a", ""},
                 galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  AssertQueryNodeEdgeCount(cc, 0, 1);
  AssertBasicReturn(cc, "e");

  // undirected edge
  GALOIS_LOG_WARN("Basic edge 3");
  std::string basic_edge3 = "match (a:Test)-[e:SOME]-(k) return e;";
  cc.Compile(basic_edge3.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "SOME", galois::CompilerQueryNode{"0", "Test", "a", ""},
                 galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::UNDIRECTED_EDGE, "e", ""});
  AssertQueryNodeEdgeCount(cc, 0, 1);
  AssertBasicReturn(cc, "e");

  // assumes creation of nodes in a certain order; note source/dst are flipped
  // from previous tests
  GALOIS_LOG_WARN("Basic edge 4");
  std::string basic_edge4 = "match (a:Test)<-[e:SOME]-(k) return e;";
  cc.Compile(basic_edge4.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "SOME", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"0", "Test", "a", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  AssertQueryNodeEdgeCount(cc, 0, 1);
  AssertBasicReturn(cc, "e");

  // path bound edge
  GALOIS_LOG_WARN("Basic edge 5");
  std::string basic_edge5 = "match p=(a:Test)<-[e:SOME]-(k) return e;";
  cc.Compile(basic_edge5.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "SOME", galois::CompilerQueryNode{"1", "any", "k", "p"},
                 galois::CompilerQueryNode{"0", "Test", "a", "p"},
                 galois::DIRECTED_EDGE, "e", "p"});
  AssertQueryNodeEdgeCount(cc, 0, 1);
  AssertBasicReturn(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // more than one edge tests
  //////////////////////////////////////////////////////////////////////////////
  // basic 2 edge test
  GALOIS_LOG_WARN("Multi edge 1");
  std::string multi_edge1 = "match ()<-[e]-(k)-[f]->() return e;";
  cc.Compile(multi_edge1.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"0", "any", "", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"2", "any", "", ""},
                 galois::DIRECTED_EDGE, "f", ""});
  AssertQueryNodeEdgeCount(cc, 0, 2);
  AssertBasicReturn(cc, "e");

  // 2 edge test with undirected + labels
  GALOIS_LOG_WARN("Multi edge 2");
  std::string multi_edge2 =
      "match (a:Test)-[e]-(k:Test2)<-[f:WELP]-() return e;";
  cc.Compile(multi_edge2.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"0", "Test", "a", ""},
                 galois::CompilerQueryNode{"1", "Test2", "k", ""},
                 galois::UNDIRECTED_EDGE, "e", ""});
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "WELP", galois::CompilerQueryNode{"2", "any", "", ""},
                 galois::CompilerQueryNode{"1", "Test2", "k", ""},
                 galois::DIRECTED_EDGE, "f", ""});
  AssertQueryNodeEdgeCount(cc, 0, 2);
  AssertBasicReturn(cc, "e");

  // 3 edge test
  GALOIS_LOG_WARN("Multi edge 3");
  std::string multi_edge3 =
      "match p=(a:Test:Also)-[e:SOME]-(k:Test2)<-[f:WELP]-()-->(noname) return "
      "e;";
  cc.Compile(multi_edge3.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "SOME", galois::CompilerQueryNode{"0", "Test;Also", "a", "p"},
                 galois::CompilerQueryNode{"1", "Test2", "k", "p"},
                 galois::UNDIRECTED_EDGE, "e", "p"});
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "WELP", galois::CompilerQueryNode{"2", "any", "", "p"},
                 galois::CompilerQueryNode{"1", "Test2", "k", "p"},
                 galois::DIRECTED_EDGE, "f", "p"});
  VerifyEdge(cc.GetQueryEdges()[2],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"2", "any", "", "p"},
                 galois::CompilerQueryNode{"3", "any", "noname", "p"},
                 galois::DIRECTED_EDGE, "", "p"});
  AssertQueryNodeEdgeCount(cc, 0, 3);
  AssertBasicReturn(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // Split edge
  //////////////////////////////////////////////////////////////////////////////
  // test here is to make sure k is the same id even though it's split
  GALOIS_LOG_WARN("Split edge 1");
  std::string split_edge1 = "match ()<-[e]-(k), (k)-[f]->() return e;";
  cc.Compile(split_edge1.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"0", "any", "", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"2", "any", "", ""},
                 galois::DIRECTED_EDGE, "f", ""});
  AssertQueryNodeEdgeCount(cc, 0, 2);
  AssertBasicReturn(cc, "e");

  GALOIS_LOG_WARN("Split edge 2");
  std::string split_edge2 = "match ()<-[e]-(k:Test), (k)-[f]->() return e;";
  cc.Compile(split_edge2.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "Test", "k", ""},
                 galois::CompilerQueryNode{"0", "any", "", ""},
                 galois::DIRECTED_EDGE, "e", ""});
  // TODO even though k refers to the same k:Test, label ends up as "any"
  // The difference is fixed during query graph construction if I recall
  // correctly
  // TODO should this be fixed in the compiler end as well?
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", ""},
                 galois::CompilerQueryNode{"2", "any", "", ""},
                 galois::DIRECTED_EDGE, "f", ""});
  AssertQueryNodeEdgeCount(cc, 0, 2);
  AssertBasicReturn(cc, "e");

  // make sure old node ids are kept if referred to more than once + check paths
  GALOIS_LOG_WARN("Split edge 3");
  std::string split_edge3 =
      "match p=(a)<-[e]-(k), q=(k)-[f]->(b), r=(b)-[g]->(a) return e;";
  cc.Compile(split_edge3.c_str());
  VerifyEdge(cc.GetQueryEdges()[0],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", "p"},
                 galois::CompilerQueryNode{"0", "any", "a", "p"},
                 galois::DIRECTED_EDGE, "e", "p"});
  VerifyEdge(cc.GetQueryEdges()[1],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"1", "any", "k", "q"},
                 galois::CompilerQueryNode{"2", "any", "b", "q"},
                 galois::DIRECTED_EDGE, "f", "q"});
  VerifyEdge(cc.GetQueryEdges()[2],
             galois::CompilerQueryEdge{
                 "ANY", galois::CompilerQueryNode{"2", "any", "b", "r"},
                 galois::CompilerQueryNode{"0", "any", "a", "r"},
                 galois::DIRECTED_EDGE, "g", "r"});

  AssertQueryNodeEdgeCount(cc, 0, 3);
  AssertBasicReturn(cc, "e");

  //////////////////////////////////////////////////////////////////////////////
  // return tests
  //////////////////////////////////////////////////////////////////////////////

  // check if multiple returns are caught
  GALOIS_LOG_WARN("Return 1");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return1 = "match (a) return a, b, c;";
  cc.Compile(return1.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, std::nullopt, "b"));
  VerifyReturn(cc.GetReturnValues()[2],
               galois::QueryProperty("c", std::nullopt, std::nullopt, "c"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  AssertNoReturnModifiers(cc);

  // check if count is parsed correctly
  GALOIS_LOG_WARN("Return 2");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return2 = "match (a) return a, count(b), count(c);";
  cc.Compile(return2.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, "count", "count(b)"));
  VerifyReturn(cc.GetReturnValues()[2],
               galois::QueryProperty("c", std::nullopt, "count", "count(c)"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  AssertNoReturnModifiers(cc);

  // return properties
  GALOIS_LOG_WARN("Return 3");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return3 =
      "match (a) return a.thing1, count(b.thing2), count(c.thing3);";
  cc.Compile(return3.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", "thing1", std::nullopt, "a.thing1"));
  VerifyReturn(
      cc.GetReturnValues()[1],
      galois::QueryProperty("b", "thing2", "count", "count(b.thing2)"));
  VerifyReturn(
      cc.GetReturnValues()[2],
      galois::QueryProperty("c", "thing3", "count", "count(c.thing3)"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  AssertNoReturnModifiers(cc);

  // AS clause
  GALOIS_LOG_WARN("Return 4");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return4 = "match (a) return a.thing1 as one, count(b.thing2) as "
                        "two, count(c.thing3) as three;";
  cc.Compile(return4.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", "thing1", std::nullopt, "one"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", "thing2", "count", "two"));
  VerifyReturn(cc.GetReturnValues()[2],
               galois::QueryProperty("c", "thing3", "count", "three"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  AssertNoReturnModifiers(cc);

  // some arbitrary function
  GALOIS_LOG_WARN("Return 5");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string return5 =
      "match (a) return asdf(a.thing1) as one, asdf2(b.thing2) as "
      "two, ASDF3(c.thing3) as three;";
  cc.Compile(return5.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", "thing1", "asdf", "one"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", "thing2", "asdf2", "two"));
  // note compiler makes function name lowercase asdf instead of ASDF as part
  // of normalization
  VerifyReturn(cc.GetReturnValues()[2],
               galois::QueryProperty("c", "thing3", "asdf3", "three"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  AssertNoReturnModifiers(cc);

  //////////////////////////////////////////////////////////////////////////////
  // return modifiers
  //////////////////////////////////////////////////////////////////////////////

  // distinct
  GALOIS_LOG_WARN("Return Mods 1");
  std::string return_mod1 = "match (a) return distinct a, b;";
  cc.Compile(return_mod1.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, std::nullopt, "b"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 2);
  VerifyReturnModifier(
      cc, galois::CompilerReturnMetadata{std::nullopt, std::nullopt, true});

  // skip
  GALOIS_LOG_WARN("Return Mods 2");
  std::string return_mod2 = "match (a) return a, b skip 3;";
  cc.Compile(return_mod2.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, std::nullopt, "b"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 2);
  VerifyReturnModifier(cc,
                       galois::CompilerReturnMetadata{3, std::nullopt, false});

  // limit
  GALOIS_LOG_WARN("Return Mods 3");
  std::string return_mod3 = "match (a) return a, b limit 100;";
  cc.Compile(return_mod3.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, std::nullopt, "b"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 2);
  VerifyReturnModifier(
      cc, galois::CompilerReturnMetadata{std::nullopt, 100, false});

  // all 3 basic mods
  GALOIS_LOG_WARN("Return Mods 4");
  std::string return_mod4 = "match (a) return distinct a, b skip 3 limit 100;";
  cc.Compile(return_mod4.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, std::nullopt, "b"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 2);
  VerifyReturnModifier(cc, galois::CompilerReturnMetadata{3, 100, true});

  //////////////////////////////////////////////////////////////////////////////
  // Order by on return
  //////////////////////////////////////////////////////////////////////////////

  // single
  GALOIS_LOG_WARN("Order by, Return 1");
  std::string order_by1 = "match (a) return a order by a.something;";
  cc.Compile(order_by1.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 1);

  galois::CompilerOrderByMetadata ob1;
  ob1.AddElement(galois::QueryProperty("a", "something", std::nullopt), true);

  VerifyReturnModifier(cc, galois::CompilerReturnMetadata{
                               std::nullopt, std::nullopt, ob1, false});

  // multiple
  GALOIS_LOG_WARN("Order by, Return 2");
  std::string order_by2 =
      "match (a) return a order by a.something, b.more, c.reate;";
  cc.Compile(order_by2.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 1);

  galois::CompilerOrderByMetadata ob2;
  ob2.AddElement(galois::QueryProperty("a", "something", std::nullopt), true);
  ob2.AddElement(galois::QueryProperty("b", "more", std::nullopt), true);
  ob2.AddElement(galois::QueryProperty("c", "reate", std::nullopt), true);

  VerifyReturnModifier(cc, galois::CompilerReturnMetadata{
                               std::nullopt, std::nullopt, ob2, false});

  // ascend, descend
  GALOIS_LOG_WARN("Order by, Return 3");
  std::string order_by3 = "match (a) return a order by a.something desc, "
                          "b.more descending, c.reate asc;";
  cc.Compile(order_by3.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 1);

  galois::CompilerOrderByMetadata ob3;
  ob3.AddElement(galois::QueryProperty("a", "something", std::nullopt), false);
  ob3.AddElement(galois::QueryProperty("b", "more", std::nullopt), false);
  ob3.AddElement(galois::QueryProperty("c", "reate", std::nullopt), true);

  VerifyReturnModifier(cc, galois::CompilerReturnMetadata{
                               std::nullopt, std::nullopt, ob3, false});

  //////////////////////////////////////////////////////////////////////////////
  // MISC
  //////////////////////////////////////////////////////////////////////////////

  // check if count is parsed correctly
  GALOIS_LOG_WARN("Misc 1, distinct count");
  // NOTE: return vars do not necessarily have to exist in the query
  std::string misc1 =
      "match (a) return a, count(distinct b), count(DISTINCT c);";
  cc.Compile(misc1.c_str());
  VerifyNode(cc.GetQueryNodes()[0],
             galois::CompilerQueryNode{"0", "any", "a", ""});
  VerifyReturn(cc.GetReturnValues()[0],
               galois::QueryProperty("a", std::nullopt, std::nullopt, "a"));
  VerifyReturn(cc.GetReturnValues()[1],
               galois::QueryProperty("b", std::nullopt, "distinct count",
                                     "count(distinct b)"));
  VerifyReturn(cc.GetReturnValues()[2],
               galois::QueryProperty("c", std::nullopt, "distinct count",
                                     "count(DISTINCT c)"));
  GALOIS_LOG_ASSERT(cc.GetReturnValues().size() == 3);
  GALOIS_LOG_ASSERT(cc.GetReturnValues()[1].IsDistinctCount());
  GALOIS_LOG_ASSERT(cc.GetReturnValues()[2].IsDistinctCount());
  AssertNoReturnModifiers(cc);

  //////////////////////////////////////////////////////////////////////////////

  // TODO when we get to implementing/reviving
  // - star paths
  // - shortest paths
  // - WHERE
  // - WITH
  // - etc.
  return 0;
}
