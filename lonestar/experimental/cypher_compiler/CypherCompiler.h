#include <cypher-parser.h>
#include <astnode.h>
#include <result.h>
#include <assert.h>
#include <unordered_map>
#include <iostream>

class CypherCompiler {
    unsigned numLabels;
    std::ostream& os;
    std::unordered_map<std::string, std::string> nameID;
    
    std::string getID(std::string str) {
        if (nameID.find(str) == nameID.end()) {
            nameID[str] = std::to_string(numLabels++);
        }
        return nameID[str];
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
                auto name = cypher_ast_identifier_get_name(nameNode);
                os << getID(name);
            } else if (element_type == CYPHER_AST_REL_PATTERN) {
                auto reltype = cypher_ast_rel_pattern_get_reltype(element, 0);
                os << cypher_ast_reltype_get_name(reltype);
                os << ",";
                auto nameNode = cypher_ast_rel_pattern_get_identifier(element);
                auto name = cypher_ast_identifier_get_name(nameNode);
                os << getID(name);
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

public:
    CypherCompiler(std::ostream& ostream) : numLabels(0), os(ostream) {}

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
};