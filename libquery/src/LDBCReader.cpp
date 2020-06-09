#include "querying/LDBCReader.h"

LDBCReader::LDBCReader(std::string _ldbcDirectory, GIDType _numNodes,
                       uint64_t _numEdges)
    : ldbcDirectory(_ldbcDirectory), gidOffset(0), totalNodes(_numNodes),
      totalEdges(_numEdges) {
  // count node/edge labels (pre-defined as we know what we need
  // from the ldbc file)
  size_t nodeLabelCount = nodeLabelNames.size();
  size_t edgeLabelCount = edgeLabelNames.size();

  allocateGraph(&(this->attGraph), this->totalNodes, this->totalEdges,
                nodeLabelCount, edgeLabelCount);
}

void LDBCReader::parseOrganization(std::string filepath) {
  galois::gInfo("Parsing org file at ", filepath);
  // open file
  std::ifstream orgFile(filepath);
  // read header
  std::string header;
  std::getline(orgFile, header);

  // read the rest of the file
  std::string curLine;
  std::string oID;
  std::string oType;
  std::string oName;
  std::string oURL;
  while (std::getline(orgFile, curLine)) {
    // parse org line
    // id|type|name|url
    std::stringstream tokenString(curLine);

    std::getline(tokenString, oID, '|');
    std::getline(tokenString, oType, '|');
    std::getline(tokenString, oName, '|');
    std::getline(tokenString, oURL, '|');

    galois::gDebug(oID, " ", oType, " ", oName, " ", oURL);

    // organization lid to gid mapping save
    organization2GID[oID] = this->gidOffset++;

    // TODO the rest
  }
}

void LDBCReader::staticParsing() {
  for (std::string curFile : this->staticNodes) {
    if (curFile.find("organisation") != std::string::npos) {
      this->parseOrganization(ldbcDirectory + "/" + curFile);
    }
  }
}
