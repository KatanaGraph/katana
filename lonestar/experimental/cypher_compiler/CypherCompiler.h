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
    std::unordered_map<std::string, std::string> nodeIDs;
    std::unordered_map<std::string, std::string> edgeIDs;
    
    std::string getNodeID(std::string str) {
        if (nodeIDs.find(str) == nodeIDs.end()) {
            nodeIDs[str] = std::to_string(numNodeIDs++);
        }
        return nodeIDs[str];
    }

    std::string getEdgeID(std::string str) {
        if (edgeIDs.find(str) == edgeIDs.end()) {
            edgeIDs[str] = std::to_string(numEdgeIDs++);
        }
        return edgeIDs[str];
    }

    int compile_pattern_path(const cypher_astnode_t *ast)
    {
        unsigned int nelements = cypher_ast_pattern_path_nelements(ast);
        for (unsigned int i = 0; i < nelements; ++i) {
            auto element = cypher_ast_pattern_path_get_element(ast, i);
            auto element_type = cypher_astnode_type(element);
            if (element_type == CYPHER_AST_NODE_PATTERN) {
                auto label = cypher_ast_node_pattern_get_label(element, 0);
                os << cypher_ast_label_get_name(label);
                os << ",";
                auto nameNode = cypher_ast_node_pattern_get_identifier(element);
                if (nameNode != NULL) {
                    auto name = cypher_ast_identifier_get_name(nameNode);
                    os << getNodeID(name);
                } else {
                    os << numNodeIDs++;
                }
            } else if (element_type == CYPHER_AST_REL_PATTERN) {
                auto reltype = cypher_ast_rel_pattern_get_reltype(element, 0);
                os << cypher_ast_reltype_get_name(reltype);
                os << ",";
                auto nameNode = cypher_ast_rel_pattern_get_identifier(element);
                if (nameNode != NULL) {
                    auto name = cypher_ast_identifier_get_name(nameNode);
                    os << getEdgeID(name);
                } else {
                    os << numEdgeIDs++;
                }
            }
            if (i != nelements - 1) {
                os << ",";
            }
        }
        os << "\n";
        return 0;
    }

    int compile_ast_node(const cypher_astnode_t *ast)
    {
        auto type = cypher_astnode_type(ast);
        if (type == CYPHER_AST_PATTERN_PATH) {
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