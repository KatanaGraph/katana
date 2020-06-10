#include "querying/LDBCReader.h"
// TODO figure out how this will end up working with all these typedefs
//#define USE_QUERY_GRAPH_WITH_NODE_LABEL

LDBCReader::LDBCReader(std::string _ldbcDirectory, GIDType _numNodes,
                       uint64_t _numEdges)
    : ldbcDirectory(_ldbcDirectory), gidOffset(0), totalNodes(_numNodes),
      totalEdges(_numEdges) {
  // count node/edge labels (pre-defined as we know what we need
  // from the ldbc file)
  size_t nodeLabelCount = this->nodeLabelNames.size();
  size_t edgeLabelCount = this->edgeLabelNames.size();

  AttributedGraph* attGraphPointer = &(this->attGraph);

  // Steps to setting up an attributed graph's metadata
  // (1) allocate memory for it
  galois::gInfo("allocating memory for graph");
  allocateGraph(attGraphPointer, this->totalNodes, this->totalEdges,
                nodeLabelCount, edgeLabelCount);
  // (2) Initialize node and edge label memory/metadata
  galois::gInfo("allocating memory for node and edge labels");
  for (size_t i = 0; i < nodeLabelCount; i++) {
    setNodeLabelMetadata(attGraphPointer, i, this->nodeLabelNames[i].c_str());
  }
  for (size_t i = 0; i < edgeLabelCount; i++) {
    setEdgeLabelMetadata(attGraphPointer, i, this->edgeLabelNames[i].c_str());
  }
  // (3) Initialize node and edge attribute memory/metadata
  // note; node/edge *attributes* are initialized when you set them later
  // if not already initialized; init here to make code easier to understand
  // if debugging
  galois::gInfo("allocating memory for node and edge attributes");
  for (std::string nAttribute : this->nodeAttributeNames) {
    addNodeAttributeMap(attGraphPointer, nAttribute.c_str(), this->totalNodes);
  }
  for (std::string eAttribute : this->edgeAttributeNames) {
    addEdgeAttributeMap(attGraphPointer, eAttribute.c_str(), this->totalEdges);
  }

  // the graph object at this point should no longer need to allocate any
  // extra memory; any additional memory use at this point is for runtime/
  // parsing of the LDBC
  galois::gInfo("Meta-level memory allocation complete");

  // after metadata initialized, need to setup data and links of underlying
  // CSR graph; this will be done as files get parsed

  // NOTE: This entire process will ignore maintaining nodeNames, index2UUID,
  // nodeIndices: will not be using them since (1) there are no uuids in this
  // dataset and (2) name is now stored as an attribute and not a separate
  // thing (because not all nodes necessarily have a single name)
}

void LDBCReader::parseOrganizationCSV(std::string filepath) {
  galois::StatTimer timer("ParseOrganizationCSVTime");
  timer.start();

  galois::gInfo("Parsing org file at ", filepath);
  // open file
  std::ifstream orgFile(filepath);
  // read header
  std::string header;
  std::getline(orgFile, header);

  // get the label for organization and its subtypes
  // assumption here is that they exist and will be found
  // TODO error checking
  uint32_t orgIndex     = this->attGraph.nodeLabelIDs["Organisation"];
  uint32_t uniIndex     = this->attGraph.nodeLabelIDs["University"];
  uint32_t companyIndex = this->attGraph.nodeLabelIDs["Company"];
  galois::gDebug("org: ", orgIndex, " uni: ", uniIndex,
                 " comp: ", companyIndex);
  // create the labels for the 2 possible types of nodes in this file
  uint32_t uniLabel     = (1 << orgIndex) & (1 << uniIndex);
  uint32_t companyLabel = (1 << orgIndex) & (1 << companyIndex);

  // read the rest of the file
  std::string curLine;
  std::string oID;
  std::string oType;
  std::string oName;
  std::string oURL;
  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  while (std::getline(orgFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse org line
    // id|type|name|url
    std::stringstream tokenString(curLine);
    std::getline(tokenString, oID, '|');
    std::getline(tokenString, oType, '|');
    std::getline(tokenString, oName, '|');
    std::getline(tokenString, oURL, '|');
    // galois::gDebug(oID, " ", oType, " ", oName, " ", oURL);

    // organization lid to gid mapping save
    organization2GID[std::stoul(oID)] = thisGID;

    // in addition to being an organization, it is also whatever type
    // is listed in the file
    if (oType.compare("company") == 0) {
      setNodeLabel(attGraphPointer, thisGID, companyLabel);
    } else if (oType.compare("university") == 0) {
      setNodeLabel(attGraphPointer, thisGID, uniLabel);
    } else {
      GALOIS_DIE("invalid organization type ", oType);
    }

    // finally, save all 3 parsed fields to attributes
    setNodeAttribute(attGraphPointer, thisGID, "id", oID.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "name", oName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "url", oURL.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed,
                " in the organization CSV; total so far is ", this->gidOffset);
}

void LDBCReader::staticParsing() {
  for (std::string curFile : this->staticNodes) {
    if (curFile.find("organisation") != std::string::npos) {
      this->parseOrganizationCSV(ldbcDirectory + "/" + curFile);
    }
  }
}
