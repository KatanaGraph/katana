#include "CypherCompiler.h"
#include <fstream>

#define drakon

void compileCypherQuery(const char* cypherQueryStr, const char* outputFileName)
{
    std::ofstream ofile;
    ofile.open(outputFileName);
    CypherCompiler cc(ofile);
    cc.compile(cypherQueryStr);
    ofile.close();
}

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

    std::ofstream ofile;
    if (argc > 2) {
        ofile.open(argv[2]);
    }
    CypherCompiler cc((argc > 2) ? ofile : std::cout);
    int ret = cc.compile(queryStr.c_str());
    if (argc > 2) {
        ofile.close();
    }

    return ret;
}