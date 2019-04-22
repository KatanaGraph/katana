#include <cypher-parser.h>
#include <astnode.h>
#include <result.h>
#include <assert.h>
#include <unordered_map>
#include <iostream>

class CypherCompiler {
    unsigned numNodeIDs;
    unsigned numEdgeIDs;
    std::ostream& os;
    std::unordered_map<std::string, unsigned> nodeIDs;
    std::unordered_map<const cypher_astnode_t*, unsigned> anonNodeIDs;
    std::unordered_map<std::string, unsigned> edgeIDs;
    std::unordered_map<const cypher_astnode_t*, unsigned> anonEdgeIDs;
    std::unordered_map<std::string, std::string> contains;
    std::unordered_map<std::string, uint32_t> timestamps;
    std::unordered_map<std::string, std::string> labels;
    std::unordered_map<std::string, std::string> pathConstraints;
    bool shortestPath;
    std::string namedPath;
    
    unsigned getNodeID(std::string str) {
        if (nodeIDs.find(str) == nodeIDs.end()) {
            nodeIDs[str] = numNodeIDs++;
        }
        return nodeIDs[str];
    }
    
    unsigned getAnonNodeID(const cypher_astnode_t* node) {
        if (anonNodeIDs.find(node) == anonNodeIDs.end()) {
            anonNodeIDs[node] = numNodeIDs++;
        }
        return anonNodeIDs[node];
    }

    unsigned getEdgeID(std::string str) {
        if (edgeIDs.find(str) == edgeIDs.end()) {
            edgeIDs[str] = numEdgeIDs++;
        }
        return edgeIDs[str];
    }

    unsigned getAnonEdgeID(const cypher_astnode_t* node) {
        if (anonEdgeIDs.find(node) == anonEdgeIDs.end()) {
            anonEdgeIDs[node] = numEdgeIDs++;
        }
        return anonEdgeIDs[node];
    }

    void compile_node_pattern_path(const cypher_astnode_t *element) {
        auto nameNode = cypher_ast_node_pattern_get_identifier(element);
        std::string name;
        if (nameNode != NULL) {
            name = cypher_ast_identifier_get_name(nameNode);
        }

        auto nlabels = cypher_ast_node_pattern_nlabels(element);
        if ((nlabels > 0) || (labels.find(name) != labels.end())) {
          for (unsigned int i = 0; i < nlabels; ++i) {
            if (i > 0) {
              os << ";";
            }
            auto label = cypher_ast_node_pattern_get_label(element, i);
            os << cypher_ast_label_get_name(label);
          }
          if (labels.find(name) != labels.end()) {
            if (nlabels > 0) {
              os << ";";
            }
            os << labels[name];
          }
          os << ",";
        } else {
            os << "any,";
        }
        if (nameNode != NULL) {
            os << getNodeID(name);
            os << ",";
            if (contains.find(name) != contains.end()) {
              os << contains[name];
            }
        } else {
            os << getAnonNodeID(element);
            os << ",";
        }
    }

    void compile_rel_pattern_path(const cypher_astnode_t *element) {
        auto nameNode = cypher_ast_rel_pattern_get_identifier(element);
        std::string name;
        if (nameNode != NULL) {
            name = cypher_ast_identifier_get_name(nameNode);
        }
        auto nlabels = cypher_ast_rel_pattern_nreltypes(element);
        unsigned int repeat = 1;

        auto varlength = cypher_ast_rel_pattern_get_varlength(element);
        if (varlength != NULL) {
          auto start = cypher_ast_range_get_start(varlength);
          auto end = cypher_ast_range_get_end(varlength);
          if ((start == NULL) || (end == NULL)) {
            if (shortestPath) {
              os << "*";
              shortestPath = false;
            } else {
              os << "**"; // all paths; TODO: modify runtime to handle it
            }
            if (pathConstraints.find(namedPath) != pathConstraints.end()) {
              os << "=";
              os << pathConstraints[namedPath];
              namedPath.clear();
            } else if (pathConstraints.find(name) != pathConstraints.end()) {
              os << "=";
              os << pathConstraints[name];
            } else if (nlabels > 0) {
              os << "=";
            }
          } else if (start == end) {
            repeat = atoi(cypher_ast_integer_get_valuestr(start));
          }
        }
        for (unsigned int r = 0; r < repeat; ++r) {
          if (r > 0) {
            os << ",any,";
            os << getAnonNodeID(element);
            os << ",\n";
            os << "any,";
            os << getAnonNodeID(element);
            os << ",,";
          }
          if (nlabels > 0) {
            for (unsigned int i = 0; i < nlabels; ++i) {
              if (i > 0) {
                os << ";";
              }
              auto label = cypher_ast_rel_pattern_get_reltype(element, i);
              os << cypher_ast_reltype_get_name(label);
            }
          }
          if (((varlength == NULL) || (repeat > 1)) && (nlabels == 0)) {
            os << "ANY";
          }
          os << ",";
          if (nameNode != NULL) {
              if (timestamps.find(name) != timestamps.end()) {
                os << timestamps[name];
              } else {
                os << std::numeric_limits<uint32_t>::max();
              }
          } else {
              os << std::numeric_limits<uint32_t>::max();
          }
        }
    }

    int compile_pattern_path(const cypher_astnode_t *ast)
    {
        unsigned int nelements = cypher_ast_pattern_path_nelements(ast);
        assert(nelements > 2);
        assert((nelements % 2) == 1); // odd number of elements
        for (unsigned int i = 1; i < nelements; i+=2) {
          { // source
            auto element = cypher_ast_pattern_path_get_element(ast, i - 1);
            auto element_type = cypher_astnode_type(element);
            assert(element_type == CYPHER_AST_NODE_PATTERN);
            compile_node_pattern_path(element);
          } 
          os << ",";
          { // relation
            auto element = cypher_ast_pattern_path_get_element(ast, i);
            auto element_type = cypher_astnode_type(element);
            assert(element_type == CYPHER_AST_REL_PATTERN);
            compile_rel_pattern_path(element);
          } 
          os << ",";
          { // destination
            auto element = cypher_ast_pattern_path_get_element(ast, i + 1);
            auto element_type = cypher_astnode_type(element);
            assert(element_type == CYPHER_AST_NODE_PATTERN);
            compile_node_pattern_path(element);
          } 
          os << "\n";
        }
        return 0;
    }

    void compile_binary_operator(const cypher_astnode_t *ast)
    {
      auto op = cypher_ast_binary_operator_get_operator(ast);
      auto arg1 = cypher_ast_binary_operator_get_argument1(ast);
      auto arg2 = cypher_ast_binary_operator_get_argument2(ast);
      if (op == CYPHER_OP_AND) {
        compile_expression(arg1);
        compile_expression(arg2);
      } else if (op == CYPHER_OP_CONTAINS) {
        auto arg1_type = cypher_astnode_type(arg1);
        auto arg2_type = cypher_astnode_type(arg2);
        if ((arg1_type == CYPHER_AST_PROPERTY_OPERATOR) &&
           (arg2_type == CYPHER_AST_STRING)) {
          auto prop_id = cypher_ast_property_operator_get_expression(arg1);
          auto prop_name = cypher_ast_property_operator_get_prop_name(arg1);
          if ((prop_id != NULL) && (prop_name != NULL)) {
            auto name = cypher_ast_prop_name_get_value(prop_name);
            if (!strcmp(name, "name")) {
              auto id = cypher_ast_identifier_get_name(prop_id);
              auto value = cypher_ast_string_get_value(arg2);
              assert(contains.find(id) == contains.end());
              contains[id] = value;
            }
          }
        }
      }
    }

    void compile_comparison(const cypher_astnode_t *ast)
    {
      if (cypher_ast_comparison_get_length(ast) == 1) {
        auto arg1 = cypher_ast_comparison_get_argument(ast, 0);
        auto arg2 = cypher_ast_comparison_get_argument(ast, 1);
        auto arg1_type = cypher_astnode_type(arg1);
        auto arg2_type = cypher_astnode_type(arg2);
        if ((arg1_type == CYPHER_AST_PROPERTY_OPERATOR) &&
           (arg2_type == CYPHER_AST_PROPERTY_OPERATOR)) {
          auto prop_name1 = cypher_ast_property_operator_get_prop_name(arg1);
          auto prop_name2 = cypher_ast_property_operator_get_prop_name(arg2);
          if ((prop_name1 != NULL) && (prop_name2 != NULL)) {
            auto name1 = cypher_ast_prop_name_get_value(prop_name1);
            auto name2 = cypher_ast_prop_name_get_value(prop_name2);
            if (!strcmp(name1, "time") && !strcmp(name2, "time")) {
              auto prop_id1 = cypher_ast_property_operator_get_expression(arg1);
              auto prop_id2 = cypher_ast_property_operator_get_expression(arg2);
              auto id1 = cypher_ast_identifier_get_name(prop_id1);
              auto id2 = cypher_ast_identifier_get_name(prop_id2);

              auto op = cypher_ast_comparison_get_operator(ast, 0);
              // TODO: make it more general - topological sort among all timestamp constraints
              if ((op == CYPHER_OP_LT) || (op == CYPHER_OP_LTE)) {
                if (timestamps.find(id1) == timestamps.end()) {
                  if (timestamps.find(id2) == timestamps.end()) {
                    timestamps[id1] = 5;
                    timestamps[id2] = 10;
                  } else {
                    timestamps[id1] = timestamps[id2] - 1;
                  }
                } else {
                  if (timestamps.find(id2) == timestamps.end()) {
                    timestamps[id2] = timestamps[id1] + 1;
                  } else {
                    assert(timestamps[id1] <= timestamps[id2]);
                  }
                }
              } else if ((op == CYPHER_OP_GT) || (op == CYPHER_OP_GTE)) {
                if (timestamps.find(id1) == timestamps.end()) {
                  if (timestamps.find(id2) == timestamps.end()) {
                    timestamps[id1] = 10;
                    timestamps[id2] = 5;
                  } else {
                    timestamps[id1] = timestamps[id2] + 1;
                  }
                } else {
                  if (timestamps.find(id2) == timestamps.end()) {
                    timestamps[id2] = timestamps[id1] - 1;
                  } else {
                    assert(timestamps[id1] >= timestamps[id2]);
                  }
                }
              }
            }
          }
        }
      }
    }

    void compile_labels_operator(const cypher_astnode_t *ast, std::string prefix = "")
    {
      auto labels_id = cypher_ast_labels_operator_get_expression(ast);
      auto id = cypher_ast_identifier_get_name(labels_id);
      for (unsigned int i = 0; i < cypher_ast_labels_operator_nlabels(ast); ++i) {
        auto label = cypher_ast_labels_operator_get_label(ast, i);
        auto name = cypher_ast_label_get_name(label);
        if (labels.find(id) == labels.end()) {
          labels[id] = prefix + name;
        } else {
          labels[id] += ";" + prefix + name; // TODO: fix assumption of AND
        }
      }
    }

    void compile_unary_operator(const cypher_astnode_t *ast)
    {
      auto op = cypher_ast_unary_operator_get_operator(ast);
      if (op == CYPHER_OP_NOT) {
        auto arg = cypher_ast_unary_operator_get_argument(ast);
        auto arg_type = cypher_astnode_type(arg);
        if (arg_type == CYPHER_AST_LABELS_OPERATOR) {
          compile_labels_operator(arg, "~");
        }
      }
    }

    void compile_list_comprehension(const cypher_astnode_t *ast, std::string prefix = "")
    {
      auto list_id = cypher_ast_list_comprehension_get_identifier(ast);
      auto id = cypher_ast_identifier_get_name(list_id);
      auto new_id = id;

      auto expression = cypher_ast_list_comprehension_get_expression(ast);
      auto exp_type = cypher_astnode_type(expression);
      if (exp_type == CYPHER_AST_APPLY_OPERATOR) {
        //auto exp_fn = cypher_ast_apply_operator_get_func_name(expression);
        // assume function is inverse of the other
        auto arg = cypher_ast_apply_operator_get_argument(expression, 0);
        auto arg_type = cypher_astnode_type(arg);
        if (arg_type == CYPHER_AST_IDENTIFIER) {
          new_id = cypher_ast_identifier_get_name(arg);
        }
      } else if (exp_type == CYPHER_AST_IDENTIFIER) {
        new_id = cypher_ast_identifier_get_name(expression);
      }

      auto predicate = cypher_ast_list_comprehension_get_predicate(ast);
      auto pred_type = cypher_astnode_type(predicate);
      if (pred_type == CYPHER_AST_BINARY_OPERATOR) {
        auto op = cypher_ast_binary_operator_get_operator(predicate);
        if (op == CYPHER_OP_EQUAL) {
          auto arg1 = cypher_ast_binary_operator_get_argument1(predicate);
          auto arg2 = cypher_ast_binary_operator_get_argument2(predicate);
          auto arg1_type = cypher_astnode_type(arg1);
          auto arg2_type = cypher_astnode_type(arg2);
          if (arg1_type == CYPHER_AST_APPLY_OPERATOR) {
            //auto arg1_fn = cypher_ast_apply_operator_get_func_name(arg1);
            // assume function is inverse of the other
            auto arg = cypher_ast_apply_operator_get_argument(arg1, 0);
            auto arg_type = cypher_astnode_type(arg);
            if (arg_type == CYPHER_AST_IDENTIFIER) {
              if (!strcmp(id, cypher_ast_identifier_get_name(arg))) {
                if (arg2_type == CYPHER_AST_STRING) {
                  pathConstraints[new_id] = prefix + cypher_ast_string_get_value(arg2);
                }
              }
            }
          }
        }
      }
    }

    void compile_none(const cypher_astnode_t *ast)
    {
      compile_list_comprehension(ast, "~");
    }

    void compile_expression(const cypher_astnode_t *ast)
    {
        auto type = cypher_astnode_type(ast);
        if (type == CYPHER_AST_BINARY_OPERATOR) {
          compile_binary_operator(ast);
        } else if (type == CYPHER_AST_COMPARISON) {
          compile_comparison(ast);
        } else if (type == CYPHER_AST_UNARY_OPERATOR) {
          compile_unary_operator(ast);
        } else if (type == CYPHER_AST_LABELS_OPERATOR) {
          compile_labels_operator(ast);
        } else if (type == CYPHER_AST_NONE) {
          compile_none(ast);
        }
    }

    int compile_ast_node(const cypher_astnode_t *ast)
    {
        auto type = cypher_astnode_type(ast);
        if (type == CYPHER_AST_MATCH) {
            auto predicate = cypher_ast_match_get_predicate(ast);
            if (predicate != NULL) compile_expression(predicate);

            auto pattern = cypher_ast_match_get_pattern(ast);
            if (pattern != NULL) return compile_ast_node(pattern);
            else return 0;
        } else if (type == CYPHER_AST_PATTERN_PATH) {
            return compile_pattern_path(ast);
        } else if (type == CYPHER_AST_SHORTEST_PATH) {
            shortestPath = true;
            assert(cypher_ast_shortest_path_is_single(ast));
            return compile_ast_node(cypher_ast_shortest_path_get_path(ast));
        } else if (type == CYPHER_AST_NAMED_PATH) {
            auto named_id = cypher_ast_named_path_get_identifier(ast);
            namedPath = cypher_ast_identifier_get_name(named_id);

            auto path = cypher_ast_named_path_get_path(ast);
            return compile_ast_node(path);
        }

        for (unsigned int i = 0; i < cypher_astnode_nchildren(ast); ++i)
        {
            const cypher_astnode_t *child = cypher_astnode_get_child(ast, i);
            if (compile_ast_node(child) < 0)
            {
                return -1;
            }
        }
        return 0;
    }

    int compile_ast(const cypher_parse_result_t *ast)
    {
        for (unsigned int i = 0; i < ast->nroots; ++i)
        {
            if (compile_ast_node(ast->roots[i]) < 0)
            {
                return -1;
            }
        }
        return 0;
    }

public:
    CypherCompiler(std::ostream& ostream) : 
      numNodeIDs(0), numEdgeIDs(0), os(ostream), shortestPath(false) {}

    int compile(const char* queryStr)
    {
        std::cout << "Query: " << queryStr << "\n";

        cypher_parse_result_t *result = cypher_parse(queryStr, 
                NULL, NULL, CYPHER_PARSE_ONLY_STATEMENTS);

        if (result == NULL)
        {
            std::cerr << "Critical failure in parsing the cypher query\n";
            return EXIT_FAILURE;
        }

        auto nerrors = cypher_parse_result_nerrors(result);

        static bool skip = galois::substrate::EnvCheck("GALOIS_DEBUG_SKIP");
        if (!skip) {
          std::cout << "Parsed " << cypher_parse_result_nnodes(result) << " AST nodes\n";
          std::cout << "Read " << cypher_parse_result_ndirectives(result) << " statements\n";
          std::cout << "Encountered " << nerrors << " errors\n";
          if (nerrors == 0) {
              cypher_parse_result_fprint_ast(result, stdout, 0, NULL, 0);
          }
        }

        if (nerrors == 0) {
            compile_ast(result);
        }

        cypher_parse_result_free(result);
        
        if (nerrors != 0) {
            std::cerr << "Parsing the cypher query failed with " << nerrors << " errors \n";
            return EXIT_FAILURE;
        }

        return EXIT_SUCCESS;
    }
};
