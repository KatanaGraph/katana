#include "querying/LDBCReader.h"
// TODO figure out how this will end up working with all these typedefs
//#define USE_QUERY_GRAPH_WITH_NODE_LABEL

LDBCReader::LDBCReader(std::string _ldbcDirectory, GIDType _numNodes,
                       uint64_t _numEdges)
    : ldbcDirectory(_ldbcDirectory), gidOffset(0), finishedNodes(0),
      addedEdges(0), totalNodes(_numNodes), totalEdges(_numEdges) {
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

LDBCReader::GIDMap& LDBCReader::getGIDMap(NodeLabel nodeType) {
  switch (nodeType) {
  case NL_ORG:
    return this->organization2GID;
  case NL_PLACE:
    return this->place2GID;
  case NL_TAG:
    return this->tag2GID;
  case NL_TAGCLASS:
    return this->tagClass2GID;
  // TODO the new classes
  default:
    GALOIS_DIE("invalid GIDMap type ", nodeType);
    // shouldn't get here
    return this->organization2GID;
  }
}

void LDBCReader::setupAttributeTypes() {
  galois::gInfo("Tagging attributes with types");
  AttributedGraph* attGraphPointer = &(this->attGraph);
  // looping over them via the array rather than hardcode setting them so
  // that it's easier to make sure the ones we want are handled (e.g.
  // if I add a new attribute and try to run, it will fail if I haven't
  // handled it here rather than silently cause issues later)
  for (std::string attName : this->nodeAttributeNames) {
    if (attName == "id" || attName == "name" || attName == "url" ||
        attName == "title") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_LONGSTRING);
    } else if (attName == "creationDate") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_DATETIME);
    } else if (attName == "firstName" || attName == "lastName" ||
               attName == "gender" || attName == "browserUsed" ||
               attName == "locationIP" || attName == "language" ||
               attName == "imageFile") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_STRING);
    } else if (attName == "birthday") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_DATE);
    } else if (attName == "email") {
      addNodeAttributeType(attGraphPointer, attName.c_str(),
                           AT_LONGSTRINGARRAY);
    } else if (attName == "speaks") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_STRINGARRAY);
    } else if (attName == "content") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_TEXT);
    } else if (attName == "length") {
      addNodeAttributeType(attGraphPointer, attName.c_str(), AT_INT32);
    } else {
      GALOIS_DIE("unhandled node attribute type ", attName);
    }
  }

  for (std::string attName : this->edgeAttributeNames) {
    if (attName == "classYear" || attName == "workFrom") {
      addEdgeAttributeType(attGraphPointer, attName.c_str(), AT_INT32);
    } else if (attName == "creationDate" || attName == "joinDate") {
      addEdgeAttributeType(attGraphPointer, attName.c_str(), AT_DATETIME);
    } else {
      GALOIS_DIE("unhandled edge attribute type ", attName);
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
  GIDType beginOffset              = this->gidOffset;
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

  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_ORG, beginOffset, nodesParsed);
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
  GIDType beginOffset              = this->gidOffset;
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

  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_PLACE, beginOffset, nodesParsed);
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
  GIDType beginOffset              = this->gidOffset;
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
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_TAG, beginOffset, nodesParsed);
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
  GIDType beginOffset              = this->gidOffset;
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
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_TAGCLASS, beginOffset, nodesParsed);
}

void LDBCReader::parsePersonCSV(const std::string filepath) {
  galois::StatTimer timer("ParsePersonCSVTime");
  timer.start();

  galois::gInfo("Parsing person file at ", filepath);
  // open file
  std::ifstream nodeFile(filepath);
  // read header
  std::string header;
  std::getline(nodeFile, header);

  // TODO error checking for non-existence
  uint32_t personIndex = this->attGraph.nodeLabelIDs["Person"];
  galois::gDebug("person: ", personIndex);
  // create tag label
  uint32_t personLabel = (1 << personIndex);

  // read the rest of the file
  std::string curLine;

  // fields of the person file in order
  std::string fCreation;
  std::string fDeletion;
  std::string fID;
  std::string fFirstName;
  std::string fLastName;
  std::string fGender;
  std::string fBirthday;
  std::string fLocationIP;
  std::string fBrowser;
  std::string fLanguage;
  std::string fMail;

  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  GIDType beginOffset              = this->gidOffset;

  while (std::getline(nodeFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse person line
    // creation|deletion|id|firstName|lastName|gender|birthday|locationIP|
    // browser|language|email
    std::stringstream tokenString(curLine);
    std::getline(tokenString, fCreation, '|');
    std::getline(tokenString, fDeletion, '|');
    std::getline(tokenString, fID, '|');
    std::getline(tokenString, fFirstName, '|');
    std::getline(tokenString, fLastName, '|');
    std::getline(tokenString, fGender, '|');
    std::getline(tokenString, fBirthday, '|');
    std::getline(tokenString, fLocationIP, '|');
    std::getline(tokenString, fBrowser, '|');
    std::getline(tokenString, fLanguage, '|');
    std::getline(tokenString, fMail, '|');

    // galois::gInfo(fCreation, "|", fDeletion, "|", fID, "|", fFirstName, "|",
    //              fLastName, "|", fGender, "|", fBirthday, "|", fLocationIP,
    //              "|", fBrowser, "|", fLanguage, "|", fMail);

    // place lid to gid mapping save
    person2GID[std::stoul(fID)] = thisGID;
    // set label
    setNodeLabel(attGraphPointer, thisGID, personLabel);

    // save parsed fields into attributes
    setNodeAttribute(attGraphPointer, thisGID, "creationDate",
                     fCreation.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "firstName", fFirstName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "lastName", fLastName.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "gender", fGender.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "birthday", fBirthday.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "email", fMail.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "speaks", fLanguage.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "browserUsed", fBrowser.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "locationIP",
                     fLocationIP.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "id", fID.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the person CSV; total so far is ",
                this->gidOffset);
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_PERSON, beginOffset, nodesParsed);
}

size_t LDBCReader::parseSimpleEdgeCSV(const std::string filepath,
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
  // src gid bounds for correct execution (this is an assumption the code
  // makes that should always hold)
  GIDType rightBound = gidOffset + edgesPerNode.size();

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
    GIDType srcGID = srcMap[std::stoul(src)];
    // make sure src GID is in bounds of this label class
    GALOIS_ASSERT(srcGID >= gidOffset && srcGID < rightBound);
    GIDType destGID = destMap[std::stoul(dest)];

    // increment edge count of src gid by one
    edgesPerNode[srcGID - gidOffset]++;
    // save src, dest, and edge label to in-memory edgelist
    readEdges.emplace_back(srcGID, destGID, edgeLabel);
  }

  timer.stop();
  galois::gInfo("Parsed ", linesParsed, " edges");

  return linesParsed;
}

void LDBCReader::constructCSRSimpleEdges(
    GIDType gidOffset, size_t numReadEdges,
    std::vector<EdgeIndex>& edgesPerNode,
    std::vector<SimpleReadEdge>& readEdges) {
  GALOIS_ASSERT(readEdges.size() == numReadEdges);

  ////////////////////////////////////////
  // Get edge endpoints
  ////////////////////////////////////////
  // partial sum on edge array to get node edgepoints
  galois::ParallelSTL::partial_sum(edgesPerNode.begin(), edgesPerNode.end(),
                                   edgesPerNode.begin());
  // make sure numReadEdges matches last count on prefix sum
  GALOIS_ASSERT(edgesPerNode.back() == numReadEdges);

  // add edges already read to prefix sum vector to get correct edge endpoints
  std::transform(edgesPerNode.begin(), edgesPerNode.end(), edgesPerNode.begin(),
                 [&](EdgeIndex& v) { return v + this->addedEdges; });
  // fix the end edges on the CSR
  AttributedGraph* attGraphPointer = &(this->attGraph);
  galois::do_all(
      galois::iterate((size_t)0, edgesPerNode.size()),
      [&](size_t nodeIndex) {
        // galois::gDebug("Node ", nodeIndex + gidOffset, " edges stop at ",
        //               edgesPerNode[nodeIndex]);
        fixEndEdge(attGraphPointer, nodeIndex + gidOffset,
                   edgesPerNode[nodeIndex]);
      },
      galois::loopname("FixEndEdgeSimple"), galois::no_stats());

  ////////////////////////////////////////
  // add edges proper
  ////////////////////////////////////////
  // this var is to track how many edges have been added to first node of this
  // set; the vector is used for all other edges
  EdgeIndex firstNodeOffset = this->addedEdges;
  // loop over the read edges
  galois::do_all(
      galois::iterate(readEdges),
      [&](SimpleReadEdge e) {
        GIDType src        = e.src;
        GIDType dest       = e.dest;
        GIDType label      = e.edgeLabel;
        GIDType localSrcID = src - gidOffset;

        // get insertion point
        EdgeIndex insertionPoint;
        // if here is to determine where to check to current insertion point for
        // the node
        if (localSrcID != 0) {
          // -1 to account for node 0 not being part of this vector
          insertionPoint =
              __sync_fetch_and_add(&(edgesPerNode[localSrcID - 1]), 1);
        } else {
          // 0th node of this set = use firstNodeOffset
          insertionPoint = __sync_fetch_and_add(&firstNodeOffset, 1);
        }

        // galois::gDebug(src, " ", dest, " ", label, " ", localSrcID, " insert
        // ",
        //               insertionPoint);
        // TODO last arg is timestamp; what to do about it?
        constructNewEdge(attGraphPointer, insertionPoint, dest, label, 0);
      },
      galois::loopname("SaveSimpleEdges"), galois::no_stats());

  // increment number of edges that have been added to the CSR
  this->addedEdges += numReadEdges;
  // this set of edges should have handled all nodes too; update finishedNodes
  this->finishedNodes += edgesPerNode.size();
}

void LDBCReader::parseAndConstructSimpleEdges(const std::string filepath,
                                              const std::string edgeType,
                                              NodeLabel nodeFrom,
                                              NodeLabel nodeTo) {
  // get position data
  NodeLabelPosition& positionData = this->nodeLabel2Position.at(nodeFrom);
  GIDType gidOffset               = positionData.offset;
  GIDType numLabeledNodes         = positionData.count;

  // check to make sure GID offset is equivalent to finishedNodes, i.e. up
  // to this point all edges are handled
  GALOIS_ASSERT(gidOffset == this->finishedNodes);

  // construct vector to hold edge counts of each node
  std::vector<EdgeIndex> edgesPerNode;
  edgesPerNode.assign(numLabeledNodes, 0);
  // vector to hold read edges in memory (so that only one pass over file on
  // storage is necessary)
  std::vector<SimpleReadEdge> readEdges;

  // read the edges into memory + get num edges per node
  size_t numReadEdges = this->parseSimpleEdgeCSV(
      filepath, edgeType, nodeFrom, nodeTo, gidOffset, edgesPerNode, readEdges);
  // construct the edges in the underlying CSR of attributed graph
  this->constructCSRSimpleEdges(gidOffset, numReadEdges, edgesPerNode,
                                readEdges);
}

void LDBCReader::staticParsing() {
  // parse static nodes
  this->parseOrganizationCSV(ldbcDirectory + "/" +
                             "static/organisation_0_0.csv");
  this->parsePlaceCSV(ldbcDirectory + "/" + "static/place_0_0.csv");
  this->parseTagCSV(ldbcDirectory + "/" + "static/tag_0_0.csv");
  this->parseTagClassCSV(ldbcDirectory + "/" + "static/tagclass_0_0.csv");

  // sanity check node label to position mappings
  for (auto mapIter = this->nodeLabel2Position.begin();
       mapIter != this->nodeLabel2Position.end(); mapIter++) {
    galois::gDebug(mapIter->first, " ", mapIter->second.offset, " ",
                   mapIter->second.count);
  }

  // There must be an order in which edges are processed: same order as
  // node read order
  // therefore, hard code handle files to read

  // first is organization
  this->parseAndConstructSimpleEdges(
      ldbcDirectory + "/" + "static/organisation_isLocatedIn_place_0_0.csv",
      "isLocatedIn", NL_ORG, NL_PLACE);
  // next is place edges
  this->parseAndConstructSimpleEdges(ldbcDirectory + "/" +
                                         "static/place_isPartOf_place_0_0.csv",
                                     "isPartOf", NL_PLACE, NL_PLACE);
  // then tag
  this->parseAndConstructSimpleEdges(ldbcDirectory + "/" +
                                         "static/tag_hasType_tagclass_0_0.csv",
                                     "hasType", NL_TAG, NL_TAGCLASS);
  // then tag class
  this->parseAndConstructSimpleEdges(
      ldbcDirectory + "/" + "static/tagclass_isSubclassOf_tagclass_0_0.csv",
      "isSubclassOf", NL_TAGCLASS, NL_TAGCLASS);
}

void LDBCReader::dynamicParsing() {
  // get all nodes in memory first in this order: person, forum
  // post, comment
  this->parsePersonCSV(ldbcDirectory + "/" + "dynamic/person_0_0.csv");

  // handle all person outgoing edges

  // handle all forum outgoing edges

  // handle all post outgoing edges

  // handle all comment outgoing edges
}
