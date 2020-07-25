################################################################################
#
# <Description>
#
# Import an RDF serialized text to a neo4j, and
# export to GraphML format.
# User can specify either an individual RDF file or
# a directory having multiple RDFs.
# Note that if a directory is specified, then RDFs on the directory should have
# the same format.
#
# If a direcotry is specified, this script iteratively attempts to
# import RDFs on there.
# If one of the RDFs is failed to be imported, this case is logged and printed
# out after the execution. Finally, this script copies all the failed RDFs to
# backup/ directory. Users can retry the import after they fix the RDFs.
#
#
# <Prerequisite>
#
#  (1) neo4j plugin: You need following neo4j plugins.
#
#      apoc (https://neo4j.com/developer/neo4j-apoc)
#      neosemantic (https://neo4j.com/labs/neosemantics-rdf)
#
#  (2) activate neo4j server:
#      This script does not activate a neo4j server.
#      Neo4j should be running in background.
#
# <Parameters>
#
#  Note that either -i or -d option is required for source RDFs.
#
#  -i (--input): pass a source RDF to be imported
#
#  -d (--directory): pass a directory having source RDFs
#
#  -f (--source): pass a RDF format
#                 (0: Turtle, 1: N-triples, 2: RDF/XML (default))
#
#  -t (--type): specify if export GraphML with type information
#               (0: False, 1: True (default)) 
#
#  -r (--reset): delete/initialize neo4j DB
#                (0: False (default), 1: True)
#
#  -u (--user): pass an user ID (default: neo4j)
#
#  -p (--pw): pass an user PW (default: neo4j)
#
#  --uri: pass an activated neo4j server URI
#         (default: neo4j://localhost:7687)
#
# <Examples>
#
# $ python rdfConverter.py -i [RDFfile].nt -f 1 -t 1
#
# $ python rdfConverter.py -d [Directory having multiple turtle RDFs] -f 0 -t 1
#
################################################################################

import argparse
import os
import sys
import shutil
from neo4j import GraphDatabase

TURTLE = 0
NT     = 1
RDFXML = 2

RDFString = [ "Turtle", "N-Triples", "RDF/XML" ]

"""
Remove all existing neo4j data.
NOTE that directly remove neo4j db directory from local FS
is much faster than querying deletion through this function.
"""
def delete_existing_db(session):
  print("Delete existing data..")
  session.run("MATCH (resource) DETACH DELETE resource;")
  print("Deletion done..\n")

"""
Set up db constraints and neosemantic configurations.
"""
def init_env(session):
  delete_existing_db(session)
  session.run("CALL n10s.graphconfig.init("+
              "{handleRDFTypes: 'LABELS',"+
              " handleMultival: 'ARRAY'});")

  # Check if unique uri constraint is already enabled.
  uniqConstExt = False
  for constraint in session.run("call db.constraints()"):
    if (constraint[0] == "n10s_unique_uri"):
      uniqConstExt = True

  if uniqConstExt:
    print("Unique URI constraint is already enabled.")
  else:
    print("Enable an unique URI constraint..")
    session.run("CREATE CONSTRAINT n10s_unique_uri ON (r:Resource)"+
                "ASSERT r.uri IS UNIQUE")

"""
Import a specified RDF input to neo4j.

TODO should allow users to specify configurations.
     For example, users would can store multi-valued properties as
     an array or overwrite the latest value.
"""
def import_RDF_to_PG(session, input, rdfFormatIdx):
  print("Import the input RDF :"+input)
  importCypher = ("CALL n10s.rdf.import.fetch('file:///"+input+"', '"      \
                 +RDFString[rdfFormatIdx]+"')"                             \
                 "YIELD terminationStatus, triplesLoaded, triplesParsed, " \
                 "extraInfo RETURN extraInfo")
  with session.begin_transaction() as tx:
    res = tx.run(importCypher)
    # printout the result of the query.
    for r in res:
      print(r["extraInfo"])
      continue;
    print("Importing done.")
  return r["extraInfo"]

"""
Export imported PG to Cypher.
"""
def exportToCypher(session, output):
	session.run("CALL apoc.export.cypher.all('"+output+".cypher')");

"""  
Export imported PG to GraphML.
"""
def exportToGraphML(session, output, printType):
  if (printType == 1):
      session.run("CALL apoc.export.graphml.all('"+output+".graphml',"+
                  "{useTypes:true, readLabels:true})")
  else:
      session.run("CALL apoc.export.graphml.all('"+output+".graphml',"+
                  "{readLabels:true})")
	
"""
Get the number of nodes and the relationships.
"""
def get_num_nodes_edges(session):
  nNodes = session.run("MATCH (n) RETURN count(n)")
  print(str(nNodes.single()[0]) + " nodes exist.")
  nEdges = session.run("MATCH ()-[r]->() RETURN count(r)")
  print(str(nEdges.single()[0]) + " edge exist.")

def main():
  optParser = argparse.ArgumentParser(
              description=''' [Prerequisite]
                          You need `neo4j` plugin. Please intall the plugin
                          by running the following command:
                          `$ pip install neo4j`.
                          This script does not `activate` a neo4j server.
                          Please run the command, `$ sudo neo4j start`, then
                          start a neo4j server before run this script.
                          ''')
  # Either one file or a directory could be accepted.
  optParser.add_argument('-i', '--input', type=str,
                         help='Specify an input file name having '
                               'RDF formmated texts.',
                         dest='inputRDFFile', required=False)

  optParser.add_argument('-d', '--dir', type=str,
                         help='Specify an input directory having '
                               'RDF formmated texts.',
                         dest='inputDirectory', required=False)

  optParser.add_argument('-f', '--format', type=int,
                         help='Specify an input RDF format to be converted '
                              '(Turtle: 0, NTriple: 1, RDF/XML:2, '
                              'default=RDF/XML)', dest='rdfFormat', default=2)

  optParser.add_argument('-t', '--type', type=int,
                         help="If you want to print out type information, "
                               "please specify '-t' or '--type' (default: 0)",
                         dest='printType', default=1)

  optParser.add_argument('-r', '--reset', type=int,
                         help="If you want to delete/initialize neo4j DB, "
                               "please specify '-r' or '--reset' (default: 0)",
                         dest='resetDB', default=0)

  optParser.add_argument('-u', '--user', type=str,
                         help="Specify an neo4j user ID (default: neo4j)",
                         dest='usrID', default="neo4j")

  optParser.add_argument('-p', '--pw', type=str,
                         help="Specify an neo4j user password (default: neo4j)",
                         dest='usrPW', default="neo4j")

  optParser.add_argument('--uri', type=str,
                         help=("Specify an activated neo4j URI."
                               "(default: neo4j://localhost:7687)"),
                         dest='uri', default="neo4j://localhost:7687")


  args = optParser.parse_args()
  inputRDFFile = args.inputRDFFile
  inputDirectory = args.inputDirectory
  printType = args.printType
  resetDB = args.resetDB
  inputRDFFormat = args.rdfFormat
  usrID = args.usrID
  usrPW = args.usrPW
  uri   = args.uri

  if not (inputRDFFile or inputDirectory):
    print("Either input rdf file or input rdf directory is required\n")
    exit()

  # Convert relative to absolute path.
  currPath = os.path.abspath(os.path.dirname(__file__))

  if inputRDFFile:
    inputRDFFile = os.path.join(currPath, inputRDFFile)

  if inputDirectory:
    inputDirectory = os.path.join(currPath, inputDirectory)

  print("\n** Passed arguments ***************************\n ")
  print("\tNeo4j server URI: "+uri)
  print("\tNeo4j user ID: "+usrID)
  if inputRDFFile:
    print("\tInput file name: "+inputRDFFile)
  if inputDirectory:
    print("\tInput Dir: "+inputDirectory)
  print("\tInput RDF format: "+RDFString[inputRDFFormat])
  if printType == 1:
    print("\tPrint types to GraphML")
  print("\n*********************************************** ")

  # Construct source RDF file lists.
  fileList = []
  if inputDirectory:
    for rdfFile in os.listdir(inputDirectory):
      rdfFname = os.fsdecode(rdfFile)
      fileList.append(inputDirectory+"/"+rdfFile)
  if inputRDFFile:
    fileList.append(inputRDFFile)

  driver = GraphDatabase.driver(uri, auth=(usrID, usrPW))
  convertLog = {}
  importSuccess = True
  with driver.session() as session:
    # Only delete/initialize neo4j DB if user specifies.
    if resetDB == 1:
      init_env(session)

    for rdfPath in fileList:
      print(rdfPath)
      rdfFile = os.path.basename(rdfPath)
      extraInfo = import_RDF_to_PG(session, rdfPath, inputRDFFormat)
      get_num_nodes_edges(session)
      if extraInfo != "": # extraInfo should be empty if a import succeeded.
        importSuccess = False
      convertLog[rdfFile] = extraInfo

  print("\n\n **** Import results **** \n")
  failedFileList = []
  if importSuccess == True:
    print("Importing succeeded.")
    print("Exporting started.")
    # Only export GraphML if all RDFs are successfully imported.
    exportToGraphML(session, rdfFile+".graphML", printType)
    print("Exporting done.")
  else:
    for fpath in fileList:
      fname = os.path.basename(fpath)
      if convertLog[fname] != "":
        print(fname+":"+str(convertLog[fname]))
        failedFileList.append(fpath)
      else:
        print(fname+": successfully imported")

  for fpath in failedFileList:
    if os.path.isfile(fpath):
      fname = os.path.basename(fpath)
      if not os.path.isdir("backup"):
        os.mkdir("backup")
      # Aggregate failed RDFs to backup/ directory.
      shutil.copy(fpath, "backup/"+fname)
      print("\n Failed RDFs are copied to backup/ direcotry.")
    else:
      print("File does not exist", fpath)

if __name__ == "__main__":
  main()
