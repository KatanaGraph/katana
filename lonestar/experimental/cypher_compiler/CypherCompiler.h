#include <cypher-parser.h>
#include <astnode.h>
#include <result.h>
#include <assert.h>
#include <unordered_map>
#include <iostream>

#define CYPHER_DEBUG

class CypherCompiler {
    unsigned numNodeIDs;
    unsigned numEdgeIDs;
    std::ostream& os;
    std::unordered_map<std::string, unsigned> nodeIDs;
    std::unordered_map<const cypher_astnode_t*, unsigned> anonNodeIDs;
    std::unordered_map<std::string, unsigned> edgeIDs;
    std::unordered_map<const cypher_astnode_t*, unsigned> anonEdgeIDs;
    std::unordered_map<std::string, std::string> contains;
    
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
        auto label = cypher_ast_node_pattern_get_label(element, 0);
        if (label != NULL) {
            os << cypher_ast_label_get_name(label);
            os << ",";
        } else {
            os << "any,";
        }
        auto nameNode = cypher_ast_node_pattern_get_identifier(element);
        if (nameNode != NULL) {
            auto name = cypher_ast_identifier_get_name(nameNode);
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
        auto reltype = cypher_ast_rel_pattern_get_reltype(element, 0);
        if (reltype != NULL) {
            os << cypher_ast_reltype_get_name(reltype);
            os << ",";
        } else {
            os << "ANY,";
        }
        auto nameNode = cypher_ast_rel_pattern_get_identifier(element);
        if (nameNode != NULL) {
            auto name = cypher_ast_identifier_get_name(nameNode);
            os << getEdgeID(name);
        } else {
            os << getAnonEdgeID(element);
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

    void compile_expression(const cypher_astnode_t *ast)
    {
        auto type = cypher_astnode_type(ast);
        if (type == CYPHER_AST_BINARY_OPERATOR) {
          compile_binary_operator(ast);
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
    CypherCompiler(std::ostream& ostream) : numNodeIDs(0), numEdgeIDs(0), os(ostream) {}

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

#ifdef CYPHER_DEBUG
        std::cout << "Parsed " << cypher_parse_result_nnodes(result) << " AST nodes\n";
        std::cout << "Read " << cypher_parse_result_ndirectives(result) << " statements\n";
        std::cout << "Encountered " << nerrors << " errors\n";
        if (nerrors == 0) {
            cypher_parse_result_fprint_ast(result, stdout, 0, NULL, 0);
        }
#endif

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
