#include "CypherCompiler.h"
#include <fstream>

#define drakon

int main(int argc, char *argv[])
{
    std::string queryStr;
    if (argc > 1) {
        queryStr = argv[1];
    } else {
#ifndef drakon
        queryStr = "match P1 = (n1:process)-[e1:WRITE]->(n0:file) \
                    return P1";
#else
        queryStr = "match P1 = (n1:process)-[e1:WRITE]->(n0:file), \
                    P2 = (n2:process)-[e2:CHMOD]->(n0:file), \
                    P3 = (n3:process)-[e3:EXECUTE]->(n0:file) \
                    return P1, P2, P3";
#endif
    }
    cypher_parse_result_t *result = cypher_parse(queryStr.c_str(), 
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
        std::ofstream ofile;
        if (argc > 2) {
            ofile.open(argv[2]);
        }
        CypherCompiler cc((argc > 2) ? ofile : std::cout);
        cc.compile_ast(result);
        if (argc > 2) {
            ofile.close();
        }
    } else {
        std::cerr << "Parsing the cypher query failed with " << nerrors << " errors \n";
        return EXIT_FAILURE;
    }

    cypher_parse_result_free(result);
    return EXIT_SUCCESS;
}