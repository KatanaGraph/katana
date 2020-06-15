#include "querying/LDBCReader.h"
// TODO figure out how this will end up working with all these typedefs
//#define USE_QUERY_GRAPH_WITH_NODE_LABEL

// namespace for various helper functions
namespace internal {
LDBCReader::GIDType getGID(LDBCReader::GIDMap& map2Query,
                           LDBCReader::LDBCNodeType key) {
  auto gidEntry = map2Query.find(key);
  if (gidEntry != map2Query.end()) {
    return gidEntry->second;
  } else {
    GALOIS_DIE(key, " not found in gid mappping");
    // shouldn't get here
    return 0;
  }
}
}; // namespace internal

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
  case NL_PERSON:
    return this->person2GID;
  case NL_COMMENT:
    return this->comment2GID;
  case NL_POST:
    return this->post2GID;
  case NL_FORUM:
    return this->forum2GID;
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
    this->organization2GID[std::stoul(oID)] = thisGID;

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
    this->place2GID[std::stoul(oID)] = thisGID;

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
    this->tag2GID[std::stoul(oID)] = thisGID;
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
    this->tagClass2GID[std::stoul(oID)] = thisGID;
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
  // create label
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
    this->person2GID[std::stoul(fID)] = thisGID;
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

void LDBCReader::parseForumCSV(const std::string filepath) {
  galois::StatTimer timer("ParseForumCSVTime");
  timer.start();

  galois::gInfo("Parsing forum file at ", filepath);
  // open file
  std::ifstream nodeFile(filepath);
  // read header
  std::string header;
  std::getline(nodeFile, header);

  // TODO error checking for non-existence
  uint32_t forumIndex = this->attGraph.nodeLabelIDs["Forum"];
  galois::gDebug("forum: ", forumIndex);
  // create label
  uint32_t forumLabel = (1 << forumIndex);

  // read the rest of the file
  std::string curLine;

  // fields of the forum file in order
  std::string fCreation;
  std::string fID;
  std::string fTitle;
  std::string fType;

  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  GIDType beginOffset              = this->gidOffset;

  while (std::getline(nodeFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse forum line
    // creation|id|title|type
    std::stringstream tokenString(curLine);
    std::getline(tokenString, fCreation, '|');
    std::getline(tokenString, fID, '|');
    std::getline(tokenString, fTitle, '|');
    std::getline(tokenString, fType, '|');

    // galois::gInfo(fCreation, "|", fID, "|", fTitle, "|", fType);

    // place lid to gid mapping save
    this->forum2GID[std::stoul(fID)] = thisGID;
    // set label
    setNodeLabel(attGraphPointer, thisGID, forumLabel);

    // save parsed fields into attributes
    setNodeAttribute(attGraphPointer, thisGID, "creationDate",
                     fCreation.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "title", fTitle.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "id", fID.c_str());
    // fType is ignored
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the forum CSV; total so far is ",
                this->gidOffset);
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_FORUM, beginOffset, nodesParsed);
}

void LDBCReader::parsePostCSV(const std::string filepath) {
  galois::StatTimer timer("ParsePostCSVTime");
  timer.start();

  galois::gInfo("Parsing post file at ", filepath);
  // open file
  std::ifstream nodeFile(filepath);
  // read header
  std::string header;
  std::getline(nodeFile, header);

  // TODO error checking for non-existence
  uint32_t messageIndex = this->attGraph.nodeLabelIDs["Message"];
  uint32_t postIndex    = this->attGraph.nodeLabelIDs["Post"];
  galois::gDebug("message: ", messageIndex, " post: ", postIndex);
  // create label
  uint32_t postLabel = (1 << postIndex) & (1 << messageIndex);

  // read the rest of the file
  std::string curLine;

  // fields of the post file in order
  std::string fCreation;
  std::string fID;
  std::string fImageFile;
  std::string fLocationIP;
  std::string fBrowser;
  std::string fLanguage;
  std::string fContent;
  std::string fLength;

  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  GIDType beginOffset              = this->gidOffset;

  while (std::getline(nodeFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse post line
    // creation|id|image|locationIP|browser|language|content|length
    std::stringstream tokenString(curLine);
    std::getline(tokenString, fCreation, '|');
    std::getline(tokenString, fID, '|');
    std::getline(tokenString, fImageFile, '|');
    std::getline(tokenString, fLocationIP, '|');
    std::getline(tokenString, fBrowser, '|');
    std::getline(tokenString, fLanguage, '|');
    std::getline(tokenString, fContent, '|');
    std::getline(tokenString, fLength, '|');

    // galois::gInfo(fCreation, "|", fID, "|", fImageFile, "|", fLocationIP,
    // "|",
    //              fBrowser, "|", fLanguage, "|", fContent, "|", fLength);

    // place lid to gid mapping save
    this->post2GID[std::stoul(fID)] = thisGID;
    // set label
    setNodeLabel(attGraphPointer, thisGID, postLabel);

    // save parsed fields into attributes
    // message specific
    setNodeAttribute(attGraphPointer, thisGID, "creationDate",
                     fCreation.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "browserUsed", fBrowser.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "locationIP",
                     fLocationIP.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "content", fContent.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "length", fLength.c_str());
    // post specific
    setNodeAttribute(attGraphPointer, thisGID, "language", fLanguage.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "imageFile", fImageFile.c_str());

    setNodeAttribute(attGraphPointer, thisGID, "id", fID.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the post CSV; total so far is ",
                this->gidOffset);
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_POST, beginOffset, nodesParsed);
}

void LDBCReader::parseCommentCSV(const std::string filepath) {
  galois::StatTimer timer("ParseCommentCSVTime");
  timer.start();

  galois::gInfo("Parsing comment file at ", filepath);
  // open file
  std::ifstream nodeFile(filepath);
  // read header
  std::string header;
  std::getline(nodeFile, header);

  // TODO error checking for non-existence
  uint32_t messageIndex = this->attGraph.nodeLabelIDs["Message"];
  uint32_t commentIndex = this->attGraph.nodeLabelIDs["Comment"];
  galois::gDebug("message: ", messageIndex, " comment: ", commentIndex);
  // create label
  uint32_t commentLabel = (1 << commentIndex) & (1 << messageIndex);

  // read the rest of the file
  std::string curLine;

  // fields of the post file in order
  std::string fCreation;
  std::string fID;
  std::string fLocationIP;
  std::string fBrowser;
  std::string fContent;
  std::string fLength;

  AttributedGraph* attGraphPointer = &(this->attGraph);
  size_t nodesParsed               = 0;
  GIDType beginOffset              = this->gidOffset;

  while (std::getline(nodeFile, curLine)) {
    GIDType thisGID = this->gidOffset++;
    nodesParsed++;
    // parse comment line
    // creation|id|locationIP|browser|content|length
    std::stringstream tokenString(curLine);
    std::getline(tokenString, fCreation, '|');
    std::getline(tokenString, fID, '|');
    std::getline(tokenString, fLocationIP, '|');
    std::getline(tokenString, fBrowser, '|');
    std::getline(tokenString, fContent, '|');
    std::getline(tokenString, fLength, '|');

    // galois::gInfo(fCreation, "|", fID, "|", fLocationIP, "|", fBrowser, "|",
    //              fContent, "|", fLength);

    // place lid to gid mapping save
    this->comment2GID[std::stoul(fID)] = thisGID;
    // set label
    setNodeLabel(attGraphPointer, thisGID, commentLabel);

    // save parsed fields into attributes
    // message specific
    setNodeAttribute(attGraphPointer, thisGID, "creationDate",
                     fCreation.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "browserUsed", fBrowser.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "locationIP",
                     fLocationIP.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "content", fContent.c_str());
    setNodeAttribute(attGraphPointer, thisGID, "length", fLength.c_str());

    setNodeAttribute(attGraphPointer, thisGID, "id", fID.c_str());
  }

  timer.stop();
  GALOIS_ASSERT(this->gidOffset <= this->totalNodes);
  galois::gInfo("Parsed ", nodesParsed, " in the comment CSV; total so far is ",
                this->gidOffset);
  // adds position info for this class of labels
  this->nodeLabel2Position.try_emplace(NL_COMMENT, beginOffset, nodesParsed);
}

size_t LDBCReader::parseSimpleEdgeCSV(
    const std::string filepath, const std::string edgeType, NodeLabel nodeFrom,
    NodeLabel nodeTo, GIDType gidOffset, std::vector<EdgeIndex>& edgesPerNode,
    std::vector<SimpleReadEdge>& readEdges, const size_t skipColumns = 0) {
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

    // columns to skip per line as necessary for some files
    if (skipColumns > 0) {
      // read data into src that will be overwritten later
      for (size_t i = 0; i < skipColumns; i++) {
        std::getline(tokenString, src, '|');
      }
    }
    // src|dst
    std::getline(tokenString, src, '|');
    std::getline(tokenString, dest, '|');
    // get gids of source and dest
    GIDType srcGID = internal::getGID(srcMap, std::stoul(src));

    // make sure src GID is in bounds of this label class
    GALOIS_ASSERT(srcGID >= gidOffset && srcGID < rightBound,
                  "left: ", gidOffset, " right: ", rightBound,
                  " offender: ", srcGID);

    GIDType destGID = internal::getGID(destMap, std::stoul(dest));

    /// galois::gDebug(srcGID, " ", destGID);

    // increment edge count of src gid by one
    edgesPerNode[srcGID - gidOffset]++;
    // save src, dest, and edge label to in-memory edgelist
    readEdges.emplace_back(srcGID, destGID, edgeLabel);
  }

  timer.stop();
  galois::gInfo("Parsed ", linesParsed, " edges");

  return linesParsed;
}

void LDBCReader::constructCSREdges(
    GIDType gidOffset, size_t numReadEdges,
    std::vector<EdgeIndex>& edgesPerNode,
    std::vector<SimpleReadEdge>& readEdges,
    std::vector<AttributedReadEdge>& readAttEdges) {
  galois::gInfo("Constructing ", numReadEdges, " in the CSR");
  GALOIS_ASSERT((readEdges.size() + readAttEdges.size()) == numReadEdges);

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

        // galois::gDebug(src, " ", dest, " ", label, " insert ",
        // insertionPoint);
        // TODO last arg is timestamp; what to do about it?
        constructNewEdge(attGraphPointer, insertionPoint, dest, label, 0);
      },
      galois::loopname("SaveSimpleEdges"), galois::no_stats());

  galois::do_all(
      galois::iterate(readAttEdges),
      [&](AttributedReadEdge& e) {
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

        // galois::gDebug(src, " ", dest, " ", label, " insert ",
        // insertionPoint);
        // TODO last arg is timestamp; what to do about it?
        constructNewEdge(attGraphPointer, insertionPoint, dest, label, 0);

        // handle attribute to save as well
        const std::string& attributeName = e.attributeName;
        std::string& attribute           = e.attribute;
        // TODO make sure attribute name already exists when inserting
        setEdgeAttribute(attGraphPointer, insertionPoint, attributeName.c_str(),
                         attribute.c_str());
      },
      galois::loopname("SaveAttributedEdges"), galois::no_stats());

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
  // dummy vector to pass to construct function
  std::vector<AttributedReadEdge> dummy;
  // construct the edges in the underlying CSR of attributed graph
  this->constructCSREdges(gidOffset, numReadEdges, edgesPerNode, readEdges,
                          dummy);
}

size_t LDBCReader::parseEdgeCSVSpecified(
    const std::string filepath, const std::string edgeType, NodeLabel nodeFrom,
    NodeLabel nodeTo, const GIDType gidOffset, const ParseMetadata howToRead,
    const std::string& attributeName, std::vector<EdgeIndex>& edgesPerNode,
    std::vector<AttributedReadEdge>& readAttEdges) {
  galois::StatTimer timer("ParseEdgeCSVSpecifiedTime");
  timer.start();

  galois::gInfo("Parsing attributed edge file at ", filepath);
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

  size_t numColumns = std::get<0>(howToRead);
  size_t srcStart   = std::get<1>(howToRead);
  size_t attColumn  = std::get<2>(howToRead);

  // read the file
  std::string curLine;
  std::string src;
  std::string dest;
  std::string attribute;
  std::string dummy;
  size_t linesParsed = 0;

  while (std::getline(edgeFile, curLine)) {
    linesParsed++;
    std::stringstream tokenString(curLine);

    for (size_t i = 0; i < numColumns; i++) {
      if (i == attColumn) {
        // get the attribute
        std::getline(tokenString, attribute, '|');
      } else if (i == srcStart) {
        // read source and dest (assumption is that they are in successive
        // columns)
        std::getline(tokenString, src, '|');
        std::getline(tokenString, dest, '|');
        // addition increment to account for fact that we are reading 2 lines
        i++;
      } else {
        std::getline(tokenString, dummy, '|');
      }
    }
    // galois::gInfo(src, " ", dest, " attribute ", attribute);

    // get gids of source and dest
    GIDType srcGID = internal::getGID(srcMap, std::stoul(src));
    // make sure src GID is in bounds of this label class
    GALOIS_ASSERT(srcGID >= gidOffset && srcGID < rightBound,
                  "left: ", gidOffset, " right: ", rightBound,
                  " offender: ", srcGID);
    GIDType destGID = internal::getGID(destMap, std::stoul(dest));
    // increment edge count of src gid by one
    edgesPerNode[srcGID - gidOffset]++;

    // save edge to read edges
    readAttEdges.emplace_back(srcGID, destGID, edgeLabel, std::move(attribute),
                              attributeName);
  }

  timer.stop();
  galois::gInfo("Parsed ", linesParsed, " edges");
  return linesParsed;
}

size_t
LDBCReader::doParse(const GIDType gidOffset,
                    const std::vector<std::string>& simpleFiles,
                    const std::vector<std::string>& simpleEdgeTypes,
                    const std::vector<ToFromMapping>& simpleMappings,
                    const std::vector<std::string>& attributedFiles,
                    const std::vector<std::string>& attributedEdgeTypes,
                    const std::vector<ToFromMapping>& attributedMappings,
                    const std::vector<ParseMetadata>& attributeHowToParse,
                    const std::vector<std::string>& attributeOnEdge,
                    std::vector<EdgeIndex>& edgesPerNode,
                    std::vector<SimpleReadEdge>& readSimpleEdges,
                    std::vector<AttributedReadEdge>& readAttEdges,
                    const size_t simpleColumnsSkipped) {
  size_t totalEdges = 0;
  for (size_t i = 0; i < simpleFiles.size(); i++) {
    const std::string& toRead    = simpleFiles[i];
    const std::string& edgeType  = simpleEdgeTypes[i];
    const ToFromMapping& mapping = simpleMappings[i];
    totalEdges += this->parseSimpleEdgeCSV(
        this->ldbcDirectory + toRead, edgeType, mapping.first, mapping.second,
        gidOffset, edgesPerNode, readSimpleEdges, simpleColumnsSkipped);
  }

  for (size_t i = 0; i < attributedFiles.size(); i++) {
    const std::string& toRead        = attributedFiles[i];
    const std::string& edgeType      = attributedEdgeTypes[i];
    const ToFromMapping& mapping     = attributedMappings[i];
    const ParseMetadata& parseInfo   = attributeHowToParse[i];
    const std::string& attributeName = attributeOnEdge[i];
    totalEdges += this->parseEdgeCSVSpecified(
        this->ldbcDirectory + toRead, edgeType, mapping.first, mapping.second,
        gidOffset, parseInfo, attributeName, edgesPerNode, readAttEdges);
  }
  return totalEdges;
}

void LDBCReader::parseAndConstructPersonEdges() {
  // get position data
  NodeLabelPosition& positionData = this->nodeLabel2Position.at(NL_PERSON);
  GIDType gidOffset               = positionData.offset;
  GIDType numLabeledNodes         = positionData.count;

  // check to make sure GID offset is equivalent to finishedNodes, i.e. up
  // to this point all edges are handled
  GALOIS_ASSERT(gidOffset == this->finishedNodes);

  // construct vector to hold edge counts of each node
  std::vector<EdgeIndex> edgesPerNode;
  edgesPerNode.assign(numLabeledNodes, 0);
  // vectors to hold read edges in memory (so that only one pass over file on
  // storage is necessary); one type has no attribute, the other one does
  std::vector<SimpleReadEdge> readSimpleEdges;
  std::vector<AttributedReadEdge> readAttEdges;

  // files that need to be read for person edges and their to-from mappings
  std::vector<std::string> simpleFiles{
      "/dynamic/person_hasInterest_tag_0_0.csv",
      "/dynamic/person_isLocatedIn_place_0_0.csv"};

  std::vector<std::string> attributedFiles{
      "/dynamic/person_knows_person_0_0.csv",
      "/dynamic/person_likes_comment_0_0.csv",
      "/dynamic/person_likes_post_0_0.csv",
      "/dynamic/person_studyAt_organisation_0_0.csv",
      "/dynamic/person_workAt_organisation_0_0.csv"};

  // edge types for both
  std::vector<std::string> simpleEdgeTypes{"hasInterest", "isLocatedIn"};
  std::vector<std::string> attributedEdgeTypes{"knows", "likes", "likes",
                                               "studyAt", "workAt"};
  // name of the attribute on the attributed edge
  std::vector<std::string> attributeOnEdge{
      "creationDate", "creationDate", "creationDate", "classYear", "workFrom"};

  // initialize edge src/dest classes; note that order is important
  std::vector<ToFromMapping> simpleMappings(simpleFiles.size());
  simpleMappings[0] = std::move(std::make_pair(NL_PERSON, NL_TAG));
  simpleMappings[1] = std::move(std::make_pair(NL_PERSON, NL_PLACE));
  std::vector<ToFromMapping> attributedMappings(attributedFiles.size());
  attributedMappings[0] = std::move(std::make_pair(NL_PERSON, NL_PERSON));
  attributedMappings[1] = std::move(std::make_pair(NL_PERSON, NL_COMMENT));
  attributedMappings[2] = std::move(std::make_pair(NL_PERSON, NL_POST));
  attributedMappings[3] = std::move(std::make_pair(NL_PERSON, NL_ORG));
  attributedMappings[4] = std::move(std::make_pair(NL_PERSON, NL_ORG));
  // setup metadata that tells us how to parse each attribute edge file
  // (has to be hardcoded; headers are not consistent/wrong)
  std::vector<ParseMetadata> attributeHowToParse(attributedFiles.size());
  // create|delete|src|dest
  attributeHowToParse[0] = std::move(std::make_tuple(4, 2, 0));
  // create|src|dest
  attributeHowToParse[1] = std::move(std::make_tuple(3, 1, 0));
  // create|src|dest
  attributeHowToParse[2] = std::move(std::make_tuple(3, 1, 0));
  // create|delete|src|dest|classyear
  attributeHowToParse[3] = std::move(std::make_tuple(5, 2, 4));
  // create|delete|src|dest|workfrom
  attributeHowToParse[4] = std::move(std::make_tuple(5, 2, 4));

  size_t totalEdges = 0;

  // skip 2 columns: create|delete|src|dst
  totalEdges = this->doParse(
      gidOffset, simpleFiles, simpleEdgeTypes, simpleMappings, attributedFiles,
      attributedEdgeTypes, attributedMappings, attributeHowToParse,
      attributeOnEdge, edgesPerNode, readSimpleEdges, readAttEdges, 2);

  galois::gInfo("Person nodes have a total of ", totalEdges, " outgoing edges");
  // construct both simple and attributed edges
  this->constructCSREdges(gidOffset, totalEdges, edgesPerNode, readSimpleEdges,
                          readAttEdges);
}

void LDBCReader::parseAndConstructForumEdges() {
  // get position data
  NodeLabelPosition& positionData = this->nodeLabel2Position.at(NL_FORUM);
  GIDType gidOffset               = positionData.offset;
  GIDType numLabeledNodes         = positionData.count;

  // check to make sure GID offset is equivalent to finishedNodes, i.e. up
  // to this point all edges are handled
  GALOIS_ASSERT(gidOffset == this->finishedNodes);

  // construct vector to hold edge counts of each node
  std::vector<EdgeIndex> edgesPerNode;
  edgesPerNode.assign(numLabeledNodes, 0);
  // vectors to hold read edges in memory (so that only one pass over file on
  // storage is necessary); one type has no attribute, the other one does
  std::vector<SimpleReadEdge> readSimpleEdges;
  std::vector<AttributedReadEdge> readAttEdges;

  // files that need to be read for person edges and their to-from mappings
  std::vector<std::string> simpleFiles{
      "/dynamic/forum_hasModerator_person_0_0.csv",
      "/dynamic/forum_hasTag_tag_0_0.csv",
      "/dynamic/forum_containerOf_post_0_0.csv"};

  std::vector<std::string> attributedFiles{
      "/dynamic/forum_hasMember_person_0_0.csv"};

  // edge types for both
  std::vector<std::string> simpleEdgeTypes{"hasModerator", "hasTag",
                                           "containerOf"};
  std::vector<std::string> attributedEdgeTypes{"hasMember"};

  // name of the attribute on the attributed edge
  std::vector<std::string> attributeOnEdge{"joinDate"};

  // initialize edge src/dest classes; note that order is important
  std::vector<ToFromMapping> simpleMappings(simpleFiles.size());
  simpleMappings[0] = std::move(std::make_pair(NL_FORUM, NL_PERSON));
  simpleMappings[1] = std::move(std::make_pair(NL_FORUM, NL_TAG));
  simpleMappings[2] = std::move(std::make_pair(NL_FORUM, NL_POST));
  std::vector<ToFromMapping> attributedMappings(attributedFiles.size());
  attributedMappings[0] = std::move(std::make_pair(NL_FORUM, NL_PERSON));

  // setup metadata that tells us how to parse each attribute edge file
  // (has to be hardcoded; headers are not consistent/wrong)
  std::vector<ParseMetadata> attributeHowToParse(attributedFiles.size());
  // creation|src|dst|type|joindate(?)
  attributeHowToParse[0] = std::move(std::make_tuple(5, 1, 4));

  size_t totalEdges = 0;

  // skip 1 column: create|src|dst
  totalEdges = this->doParse(
      gidOffset, simpleFiles, simpleEdgeTypes, simpleMappings, attributedFiles,
      attributedEdgeTypes, attributedMappings, attributeHowToParse,
      attributeOnEdge, edgesPerNode, readSimpleEdges, readAttEdges, 1);

  galois::gInfo("Forum nodes have a total of ", totalEdges, " outgoing edges");
  // construct both simple and attributed edges
  this->constructCSREdges(gidOffset, totalEdges, edgesPerNode, readSimpleEdges,
                          readAttEdges);
}

void LDBCReader::parseAndConstructPostEdges() {
  // get position data
  NodeLabelPosition& positionData = this->nodeLabel2Position.at(NL_POST);
  GIDType gidOffset               = positionData.offset;
  GIDType numLabeledNodes         = positionData.count;

  // check to make sure GID offset is equivalent to finishedNodes, i.e. up
  // to this point all edges are handled
  GALOIS_ASSERT(gidOffset == this->finishedNodes);

  // construct vector to hold edge counts of each node
  std::vector<EdgeIndex> edgesPerNode;
  edgesPerNode.assign(numLabeledNodes, 0);
  // vectors to hold read edges in memory (so that only one pass over file on
  // storage is necessary); one type has no attribute, the other one does
  std::vector<SimpleReadEdge> readSimpleEdges;
  std::vector<AttributedReadEdge> readAttEdges;

  // files that need to be read for person edges and their to-from mappings
  std::vector<std::string> simpleFiles{
      "/dynamic/post_hasCreator_person_0_0.csv",
      "/dynamic/post_hasTag_tag_0_0.csv",
      "/dynamic/post_isLocatedIn_place_0_0.csv"};

  // edge types for both
  std::vector<std::string> simpleEdgeTypes{"hasCreator", "hasTag",
                                           "isLocatedIn"};
  // initialize edge src/dest classes; note that order is important
  std::vector<ToFromMapping> simpleMappings(simpleFiles.size());
  simpleMappings[0] = std::move(std::make_pair(NL_POST, NL_PERSON));
  simpleMappings[1] = std::move(std::make_pair(NL_POST, NL_TAG));
  simpleMappings[2] = std::move(std::make_pair(NL_POST, NL_PLACE));

  // post edge have no attributed edge files
  std::vector<std::string> attributedFiles;
  std::vector<std::string> attributedEdgeTypes;
  std::vector<std::string> attributeOnEdge;
  std::vector<ToFromMapping> attributedMappings;
  std::vector<ParseMetadata> attributeHowToParse;

  size_t totalEdges = 0;
  // skip 1 column: create|src|dst
  totalEdges = this->doParse(
      gidOffset, simpleFiles, simpleEdgeTypes, simpleMappings, attributedFiles,
      attributedEdgeTypes, attributedMappings, attributeHowToParse,
      attributeOnEdge, edgesPerNode, readSimpleEdges, readAttEdges, 1);

  galois::gInfo("Post nodes have a total of ", totalEdges, " outgoing edges");
  // construct both simple and attributed edges
  this->constructCSREdges(gidOffset, totalEdges, edgesPerNode, readSimpleEdges,
                          readAttEdges);
}

void LDBCReader::parseAndConstructCommentEdges() {
  // get position data
  NodeLabelPosition& positionData = this->nodeLabel2Position.at(NL_COMMENT);
  GIDType gidOffset               = positionData.offset;
  GIDType numLabeledNodes         = positionData.count;

  // check to make sure GID offset is equivalent to finishedNodes, i.e. up
  // to this point all edges are handled
  GALOIS_ASSERT(gidOffset == this->finishedNodes);

  // construct vector to hold edge counts of each node
  std::vector<EdgeIndex> edgesPerNode;
  edgesPerNode.assign(numLabeledNodes, 0);
  // vectors to hold read edges in memory (so that only one pass over file on
  // storage is necessary); one type has no attribute, the other one does
  std::vector<SimpleReadEdge> readSimpleEdges;
  std::vector<AttributedReadEdge> readAttEdges;

  // files that need to be read for person edges and their to-from mappings
  std::vector<std::string> simpleFiles{
      "/dynamic/comment_hasCreator_person_0_0.csv",
      "/dynamic/comment_hasTag_tag_0_0.csv",
      "/dynamic/comment_isLocatedIn_place_0_0.csv",
      "/dynamic/comment_replyOf_comment_0_0.csv",
      "/dynamic/comment_replyOf_post_0_0.csv"};

  // edge types for both
  std::vector<std::string> simpleEdgeTypes{"hasCreator", "hasTag",
                                           "isLocatedIn", "replyOf", "replyOf"};
  // initialize edge src/dest classes; note that order is important
  std::vector<ToFromMapping> simpleMappings(simpleFiles.size());
  simpleMappings[0] = std::move(std::make_pair(NL_COMMENT, NL_PERSON));
  simpleMappings[1] = std::move(std::make_pair(NL_COMMENT, NL_TAG));
  simpleMappings[2] = std::move(std::make_pair(NL_COMMENT, NL_PLACE));
  simpleMappings[3] = std::move(std::make_pair(NL_COMMENT, NL_COMMENT));
  simpleMappings[4] = std::move(std::make_pair(NL_COMMENT, NL_POST));

  // post edge have no attributed edge files
  std::vector<std::string> attributedFiles;
  std::vector<std::string> attributedEdgeTypes;
  std::vector<std::string> attributeOnEdge;
  std::vector<ToFromMapping> attributedMappings;
  std::vector<ParseMetadata> attributeHowToParse;

  size_t totalEdges = 0;
  // skip 1 column: create|src|dst
  totalEdges = this->doParse(
      gidOffset, simpleFiles, simpleEdgeTypes, simpleMappings, attributedFiles,
      attributedEdgeTypes, attributedMappings, attributeHowToParse,
      attributeOnEdge, edgesPerNode, readSimpleEdges, readAttEdges, 1);

  galois::gInfo("Comment nodes have a total of ", totalEdges,
                " outgoing edges");
  // construct both simple and attributed edges
  this->constructCSREdges(gidOffset, totalEdges, edgesPerNode, readSimpleEdges,
                          readAttEdges);
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
  this->parseForumCSV(ldbcDirectory + "/" + "dynamic/forum_0_0.csv");
  this->parsePostCSV(ldbcDirectory + "/" + "dynamic/post_0_0.csv");
  this->parseCommentCSV(ldbcDirectory + "/" + "dynamic/comment_0_0.csv");

  // handle all person outgoing edges
  this->parseAndConstructPersonEdges();
  // handle all forum outgoing edges
  this->parseAndConstructForumEdges();
  // handle all post outgoing edges
  this->parseAndConstructPostEdges();
  // handle all comment outgoing edges
  this->parseAndConstructCommentEdges();

  galois::gInfo("Total of ", this->finishedNodes, " and ", this->addedEdges,
                " edges");
}
