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
  galois::gInfo("Allocating memory for graph");
  allocateGraph(attGraphPointer, this->totalNodes, this->totalEdges,
                nodeLabelCount, edgeLabelCount);
  // (2) Initialize node and edge label memory/metadata
  galois::gInfo("Allocating memory for node and edge labels");
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
  galois::gInfo("Allocating memory for node and edge attributes");
  for (std::string nAttribute : this->nodeAttributeNames) {
    addNodeAttributeMap(attGraphPointer, nAttribute.c_str(), this->totalNodes);
  }
  for (std::string eAttribute : this->edgeAttributeNames) {
    addEdgeAttributeMap(attGraphPointer, eAttribute.c_str(), this->totalEdges);
  }

  this->setupAttributeTypes();

  // the graph object at this point should no longer need to allocate any
  // extra memory; any additional memory use at this point is for runtime/
  // parsing of the LDBC/actual attributes which are stored as strings
  galois::gInfo("Meta-level preparation complete");

  // after metadata initialized, need to setup data and links of underlying
  // CSR graph; this will be done as files get parsed

  // NOTE: This entire process will ignore maintaining nodeNames, index2UUID,
  // nodeIndices: will not be using them since (1) there are no uuids in this
  // dataset and (2) name is now stored as an attribute and not a separate
  // thing (because not all nodes necessarily have a single name)
}

void LDBCReader::setupAttributeTypes() {
  galois::gInfo("Tagging attributes with types");
  AttributedGraph* attGraphPointer = &(this->attGraph);
  // looping over them via the array rather than hardcode setting them so
  // that it's easier to make sure the ones we want are handled (e.g.
  // if I add a new attribute and try to run, it will fail if I haven't
  // handled it here rather than silently cause issues later)
  for (std::string attName : this->nodeAttributeNames) {
    if (attName == "id" || attName == "name" || attName == "url") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_LONGSTRING);
    } else {
      GALOIS_DIE("unhandled node attribute type");
    }
  }

  for (std::string attName : this->edgeAttributeNames) {
    if (attName == "id" || attName == "name" || attName == "url") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_LONGSTRING);
    } else {
      GALOIS_DIE("unhandled edge attribute type");
    }
  }
}

void LDBCReader::parseOrganizationCSV(const std::string filepath) {
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

void LDBCReader::parsePlaceCSV(const std::string filepath) {
  galois::StatTimer timer("ParsePlaceCSVTime");
  timer.start();

  galois::gInfo("Parsing place file at ", filepath);
  // open file
  std::ifstream placeFile(filepath);
  // read header
  std::string header;
  std::getline(placeFile, header);

  // get the labels for place and its subtypes
  // assumption here is that they exist and will be found
  // TODO error checking
  uint32_t placeIndex     = this->attGraph.nodeLabelIDs["Place"];
  uint32_t cityIndex      = this->attGraph.nodeLabelIDs["City"];
  uint32_t countryIndex   = this->attGraph.nodeLabelIDs["Country"];
  uint32_t continentIndex = this->attGraph.nodeLabelIDs["Continent"];

  galois::gDebug("place: ", placeIndex, " city: ", cityIndex,
                 " country: ", countryIndex, " continent: ", continentIndex);

  // create the labels for the 3 types of nodes in this file
  uint32_t cityLabel      = (1 << placeIndex) & (1 << cityIndex);
  uint32_t countryLabel   = (1 << placeIndex) & (1 << countryIndex);
  uint32_t continentLabel = (1 << placeIndex) & (1 << continentIndex);

  // read the rest of the file
  std::string curLine;
  std::string oID;
  std::string oName;
  std::string oURL;
  std::string oType;
  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  while (std::getline(placeFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse place line
    // id|name|url|type
    std::stringstream tokenString(curLine);
    std::getline(tokenString, oID, '|');
    std::getline(tokenString, oName, '|');
    std::getline(tokenString, oURL, '|');
    std::getline(tokenString, oType, '|');
    // galois::gDebug(oID, " ", oType, " ", oName, " ", oURL);

    // place lid to gid mapping save
    place2GID[std::stoul(oID)] = thisGID;

    // in addition to being an place, it is also whatever type
    // is listed in the file
    if (oType.compare("country") == 0) {
      setNodeLabel(attGraphPointer, thisGID, countryLabel);
    } else if (oType.compare("city") == 0) {
      setNodeLabel(attGraphPointer, thisGID, cityLabel);
    } else if (oType.compare("continent") == 0) {
      setNodeLabel(attGraphPointer, thisGID, continentLabel);
    } else {
      GALOIS_DIE("invalid place type ", oType);
    }

    // finally, save all 3 parsed fields to attributes
    setNodeAttribute(attGraphPointer, thisGID, "id", oID.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "name", oName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "url", oURL.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the place CSV; total so far is ",
                this->gidOffset);
}

void LDBCReader::parseTagCSV(const std::string filepath) {
  galois::StatTimer timer("ParseTagCSVTime");
  timer.start();

  galois::gInfo("Parsing tag file at ", filepath);
  // open file
  std::ifstream tagFile(filepath);
  // read header
  std::string header;
  std::getline(tagFile, header);

  // TODO error checking for non-existence
  uint32_t tagIndex = this->attGraph.nodeLabelIDs["Tag"];
  galois::gDebug("tag: ", tagIndex);
  // create tag label
  uint32_t tagLabel = (1 << tagIndex);

  // read the rest of the file
  std::string curLine;
  std::string oID;
  std::string oName;
  std::string oURL;
  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  while (std::getline(tagFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse place line
    // id|name|url
    std::stringstream tokenString(curLine);
    std::getline(tokenString, oID, '|');
    std::getline(tokenString, oName, '|');
    std::getline(tokenString, oURL, '|');
    // galois::gDebug(oID, " ", oName, " ", oURL);

    // place lid to gid mapping save
    tag2GID[std::stoul(oID)] = thisGID;
    // set tag label
    setNodeLabel(attGraphPointer, thisGID, tagLabel);
    // save all 3 parsed fields to attributes
    setNodeAttribute(attGraphPointer, thisGID, "id", oID.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "name", oName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "url", oURL.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the tag CSV; total so far is ",
                this->gidOffset);
}

void LDBCReader::parseTagClassCSV(const std::string filepath) {
  galois::StatTimer timer("ParseTagClassCSVTime");
  timer.start();

  galois::gInfo("Parsing tag class file at ", filepath);
  // open file
  std::ifstream tagClassFile(filepath);
  // read header
  std::string header;
  std::getline(tagClassFile, header);

  // TODO error checking for non-existence
  uint32_t tagClassIndex = this->attGraph.nodeLabelIDs["TagClass"];
  galois::gDebug("tagclass: ", tagClassIndex);
  // create tag label
  uint32_t tagClassLabel = (1 << tagClassIndex);

  // read the rest of the file
  std::string curLine;
  std::string oID;
  std::string oName;
  std::string oURL;
  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  while (std::getline(tagClassFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse place line
    // id|name|url
    std::stringstream tokenString(curLine);
    std::getline(tokenString, oID, '|');
    std::getline(tokenString, oName, '|');
    std::getline(tokenString, oURL, '|');
    // galois::gDebug(oID, " ", oType, " ", oName);

    // place lid to gid mapping save
    tagClass2GID[std::stoul(oID)] = thisGID;
    // set tag label
    setNodeLabel(attGraphPointer, thisGID, tagClassLabel);
    // save all 3 parsed fields to attributes
    setNodeAttribute(attGraphPointer, thisGID, "id", oID.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "name", oName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "url", oURL.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed,
                " in the tag class CSV; total so far is ", this->gidOffset);
}

void LDBCReader::parseSimpleEdgeCSV(const std::string filepath,
                                    const std::string edgeType,
                                    NodeLabel nodeFrom, NodeLabel nodeTo,
                                    GIDType gidOffset,
                                    std::vector<EdgeIndex>& edgesPerNode,
                                    std::vector<SimpleReadEdge>& readEdges) {
  galois::StatTimer timer("ParseSimpleEdgeTime");
  timer.start();

  galois::gInfo("Parsing simple edge file at ", filepath);
  // open file
  std::ifstream edgeFile(filepath);
  // read/ignore header
  std::string header;
  std::getline(edgeFile, header);

  // TODO error checking for non-existence
  uint32_t edgeTypeIndex = this->attGraph.edgeLabelIDs[edgeType];
  galois::gDebug("edgeclass: ", edgeTypeIndex);
  // create tag label
  uint32_t edgeLabel = (1 << edgeTypeIndex);
  // get gid maps
  GIDMap& srcMap  = getGIDMap(nodeFrom);
  GIDMap& destMap = getGIDMap(nodeTo);

  // read the file
  std::string curLine;
  std::string src;
  std::string dest;
  size_t linesParsed = 0;
  while (std::getline(edgeFile, curLine)) {
    linesParsed++;
    std::stringstream tokenString(curLine);
    // src|dst
    std::getline(tokenString, src, '|');
    std::getline(tokenString, dest, '|');
    // get gids of source and dest
    GIDType srcGID  = srcMap[std::stoul(src)];
    GIDType destGID = destMap[std::stoul(dest)];
    // increment edge count of src gid by one
    edgesPerNode[srcGID - gidOffset]++;
    // save src, dest, and edge label to in-memory edgelist
    readEdges.emplace_back(srcGID, destGID, edgeLabel);
  }

  timer.stop();
  galois::gInfo("Parsed ", linesParsed, " edges");
}

void LDBCReader::staticParsing() {
  for (std::string curFile : this->staticNodes) {
    if (curFile.find("organisation") != std::string::npos) {
      this->parseOrganizationCSV(ldbcDirectory + "/" + curFile);
    } else if (curFile.find("place") != std::string::npos) {
      this->parsePlaceCSV(ldbcDirectory + "/" + curFile);
    } else if (curFile.find("tag_") != std::string::npos) {
      this->parseTagCSV(ldbcDirectory + "/" + curFile);
    } else if (curFile.find("tagclass_") != std::string::npos) {
      this->parseTagClassCSV(ldbcDirectory + "/" + curFile);
    } else {
      GALOIS_DIE("invalid/unparsable static node file ", curFile);
    }
  }

  // There must be an order in which edges are processed:
  for (std::string curFile : this->staticEdges) {
  }
}
