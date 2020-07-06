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

  querying::CypherCompiler cc;
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

  unsigned r_count = 0;
  galois::gInfo("Parsed return values are as follows:");
  // parsed return values are the following
  for (const querying::ReturnResult& a : cc.getReturnValues()) {
    galois::gInfo("Return value ", r_count++);
    a.printStruct();
  }

  totalTime.stop();
  return 0;
}
