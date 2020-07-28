/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file cypher-compile.cpp
 *
 * Tool to run the cypher compiler and print out the output: used for
 * debugging and development purposes.
 */

#include "galois/Galois.h"
#include "querying/CypherCompiler.h"

#include <llvm/Support/CommandLine.h>

namespace cll = llvm::cl;

////////////////////////////////////////////////////////////////////////////////

static cll::opt<std::string>
    filename("filename", cll::desc("<file with query>"), cll::init(""));

static cll::opt<std::string>
    query("query", cll::desc("Cypher query (higher precedence than filename)"),
          cll::init(""));

////////////////////////////////////////////////////////////////////////////////

int main(int argc, char** argv) {
  // setup
  galois::SharedMemSys G;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  galois::StatTimer totalTime("TimerTotal");
  totalTime.start();

  galois::CypherCompiler cc;
  // get query to parse
  if (query != "") {
    cc.compile(query.c_str());
  } else if (filename != "") {
    // open file, get query
    std::ifstream queryStream(filename);
    if (!queryStream.is_open()) {
      GALOIS_DIE("failed to open query file ", filename);
    }
    std::stringstream querySS;
    // putting into string stream lets you pull a string out of it
    querySS << queryStream.rdbuf();

    cc.compile(querySS.str().c_str());
  } else {
    galois::gError("No query or file with query specified to parse");
  }

  // Print parsed values
  galois::gInfo("Parsed single node values (not attached to an edge) "
                "are as follows:");
  for (const galois::CompilerQueryNode& a : cc.getQueryNodes()) {
    a.printStruct(1);
  }

  galois::gInfo("Parsed edge values and endpoints are as follows:");
  for (const galois::CompilerQueryEdge& a : cc.getQueryEdges()) {
    a.printStruct(1);
    a.caused_by.printStruct(2);
    a.acted_on.printStruct(2);
  }

  galois::gInfo("Parsed return metadata is as follows:");
  cc.getReturnMetadata().printStruct(1);

  galois::gInfo("Parsed return values are as follows:");
  for (const galois::QueryProperty& a : cc.getReturnValues()) {
    galois::gPrint("\t", a.toString(), "\n");
  }

  totalTime.stop();
  return 0;
}
