/** dGraph loader -*- C++ -*-
 * @file
 * dGraphLoader.h
 * @section License
 *
 * Galois, a framework to exploit amorphous data-parallelism in irregular
 * programs.
 *
 * Copyright (C) 2017, The University of Texas at Austin. All rights reserved.
 * UNIVERSITY EXPRESSLY DISCLAIMS ANY AND ALL WARRANTIES CONCERNING THIS
 * SOFTWARE AND DOCUMENTATION, INCLUDING ANY WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR ANY PARTICULAR PURPOSE, NON-INFRINGEMENT AND WARRANTIES OF
 * PERFORMANCE, AND ANY WARRANTY THAT MIGHT OTHERWISE ARISE FROM COURSE OF
 * DEALING OR USAGE OF TRADE.  NO WARRANTY IS EITHER EXPRESS OR IMPLIED WITH
 * RESPECT TO THE USE OF THE SOFTWARE OR DOCUMENTATION. Under no circumstances
 * shall University be liable for incidental, special, indirect, direct or
 * consequential damages or loss of profits, interruption of business, or
 * related expenses which may arise from use of Software or Documentation,
 * including but not limited to those resulting from defects in Software and/or
 * Documentation, or loss or inaccuracy of data of any kind.
 *
 * @section Description
 *
 * Command line arguments and functions for loading dGraphs into memory
 *
 * @author Loc Hoang <l_hoang@utexas.edu>
 */

#ifndef D_GRAPH_LOADER
#define D_GRAPH_LOADER

//#include "Lonestar/BoilerPlate.h"
#include "llvm/Support/CommandLine.h"
#include "galois/runtime/dGraph_edgeCut.h"
#include "galois/runtime/dGraph_cartesianCut.h"
#include "galois/runtime/dGraph_hybridCut.h"
#include "galois/runtime/dGraph_jaggedCut.h"

/*******************************************************************************
 * Supported partitioning schemes
 ******************************************************************************/
enum PARTITIONING_SCHEME {
  OEC, IEC, HOVC, HIVC, BOARD2D_VCUT, CART_VCUT, JAGGED_CYCLIC_VCUT, 
  JAGGED_BLOCKED_VCUT, OVER_DECOMPOSE_2_VCUT, OVER_DECOMPOSE_4_VCUT
};

/*******************************************************************************
 * Graph-loading-related command line arguments
 ******************************************************************************/
namespace cll = llvm::cl;

extern cll::opt<std::string> inputFile;
extern cll::opt<std::string> inputFileTranspose;
extern cll::opt<bool> inputFileSymmetric;
extern cll::opt<std::string> partFolder;
extern cll::opt<PARTITIONING_SCHEME> partitionScheme;
extern cll::opt<unsigned int> VCutThreshold;

/*******************************************************************************
 * Graph-loading functions
 ******************************************************************************/
/**
 * Loads a symmetric graph file (i.e. directed graph with edges in both 
 * directions)
 *
 * @tparam NodeData node data to store in graph
 * @tparam EdgeData edge data to store in graph
 * @param scaleFactor How to split nodes among hosts
 * @returns a pointer to a newly allocated hGraph based on the command line
 * loaded based on command line arguments
 */
template<typename NodeData, typename EdgeData>
hGraph<NodeData, EdgeData>* constructSymmetricGraph(std::vector<unsigned> 
                                                    scaleFactor) {
  if (!inputFileSymmetric) {
    GALOIS_DIE("Calling constructSymmetricGraph without inputFileSymmetric "
               "flag");
  }

  typedef hGraph_edgeCut<NodeData, EdgeData> Graph_edgeCut;
  typedef hGraph_vertexCut<NodeData, EdgeData> Graph_vertexCut;
  typedef hGraph_cartesianCut<NodeData, EdgeData> Graph_cartesianCut;
  typedef hGraph_cartesianCut<NodeData, EdgeData, true> Graph_checkerboardCut;
  typedef hGraph_jaggedCut<NodeData, EdgeData> Graph_jaggedCut;
  typedef hGraph_jaggedCut<NodeData, EdgeData, true> Graph_jaggedBlockedCut;
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, false, false, false, 2> Graph_cartesianCut_overDecomposeBy2;
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, false, false, false, 4> Graph_cartesianCut_overDecomposeBy4;

  auto& net = galois::runtime::getSystemNetworkInterface();
  
  switch(partitionScheme) {
    case OEC:
    case IEC:
      return new Graph_edgeCut(inputFile, partFolder, net.ID, net.Num, 
                               scaleFactor, false);
    case HOVC:
    case HIVC:
      return new Graph_vertexCut(inputFile, partFolder, net.ID, net.Num, 
                                 scaleFactor, false, VCutThreshold);
    case BOARD2D_VCUT:
      return new Graph_checkerboardCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case CART_VCUT:
      return new Graph_cartesianCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case JAGGED_CYCLIC_VCUT:
      return new Graph_jaggedCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case JAGGED_BLOCKED_VCUT:
      return new Graph_jaggedBlockedCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case OVER_DECOMPOSE_2_VCUT:
      return new Graph_cartesianCut_overDecomposeBy2(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case OVER_DECOMPOSE_4_VCUT:
      return new Graph_cartesianCut_overDecomposeBy4(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    default:
      GALOIS_DIE("Error: partition scheme specified is invalid");
      return nullptr;
  }
}

/**
 * Loads a graph file with the purpose of iterating over the out edges
 * of the graph.
 *
 * @tparam NodeData node data to store in graph
 * @tparam EdgeData edge data to store in graph
 * @tparam iterateOut says if you want to iterate over out edges or not; if
 * false, will iterate over in edgse
 * @tparam enable_if this function  will only be enabled if iterateOut is true
 * @param scaleFactor How to split nodes among hosts
 * @returns a pointer to a newly allocated hGraph based on the command line
 * loaded based on command line arguments
 */
template<typename NodeData, typename EdgeData, bool iterateOut = true,
         typename std::enable_if<iterateOut>::type* = nullptr>
hGraph<NodeData, EdgeData>* constructGraph(std::vector<unsigned> scaleFactor) {
  typedef hGraph_edgeCut<NodeData, EdgeData> Graph_edgeCut;
  typedef hGraph_vertexCut<NodeData, EdgeData> Graph_vertexCut;
  typedef hGraph_cartesianCut<NodeData, EdgeData> Graph_cartesianCut; // assumes push-style
  typedef hGraph_cartesianCut<NodeData, EdgeData, true> Graph_checkerboardCut; // assumes push-style
  typedef hGraph_jaggedCut<NodeData, EdgeData> Graph_jaggedCut; // assumes push-style
  typedef hGraph_jaggedCut<NodeData, EdgeData, true> Graph_jaggedBlockedCut; // assumes push-style
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, false, false, false, 2> Graph_cartesianCut_overDecomposeBy2;
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, false, false, false, 4> Graph_cartesianCut_overDecomposeBy4;

  auto& net = galois::runtime::getSystemNetworkInterface();

  // 1 host = no concept of cut; just load from edgeCut, no transpose
  if (net.Num == 1) {
    return new Graph_edgeCut(inputFile, partFolder, net.ID, net.Num, 
                             scaleFactor, false);
  }

  switch(partitionScheme) {
    case OEC:
      return new Graph_edgeCut(inputFile, partFolder, net.ID, net.Num, 
                               scaleFactor, false);
    case IEC:
      if (inputFileTranspose.size()) {
        return new Graph_edgeCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                 scaleFactor, true);
      } else {
        GALOIS_DIE("Error: attempting incoming edge cut without transpose "
                   "graph");
        break;
      }
    case HOVC:
      return new Graph_vertexCut(inputFile, partFolder, net.ID, net.Num, 
                                 scaleFactor, false, VCutThreshold);
    case HIVC:
      if (inputFileTranspose.size()) {
        return new Graph_vertexCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                 scaleFactor, true, VCutThreshold);
      } else {
        GALOIS_DIE("Error: attempting incoming hybrid cut without transpose "
                   "graph");
        break;
      }
    case BOARD2D_VCUT:
      return new Graph_checkerboardCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case CART_VCUT:
      return new Graph_cartesianCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case JAGGED_CYCLIC_VCUT:
      return new Graph_jaggedCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case JAGGED_BLOCKED_VCUT:
      return new Graph_jaggedBlockedCut(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case OVER_DECOMPOSE_2_VCUT:
      return new Graph_cartesianCut_overDecomposeBy2(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    case OVER_DECOMPOSE_4_VCUT:
      return new Graph_cartesianCut_overDecomposeBy4(inputFile, partFolder, net.ID, net.Num, 
                                    scaleFactor, false);
    default:
      GALOIS_DIE("Error: partition scheme specified is invalid");
      return nullptr;
  }
}

/**
 * Loads a graph file with the purpose of iterating over the in edges
 * of the graph.
 *
 * @tparam NodeData node data to store in graph
 * @tparam EdgeData edge data to store in graph
 * @tparam iterateOut says if you want to iterate over out edges or not; if
 * false, will iterate over in edgse
 * @tparam enable_if this function  will only be enabled if iterateOut is false
 * (i.e. iterate over in-edges)
 * @param scaleFactor How to split nodes among hosts
 * @returns a pointer to a newly allocated hGraph based on the command line
 * loaded based on command line arguments
 */
template<typename NodeData, typename EdgeData, bool iterateOut = true,
         typename std::enable_if<!iterateOut>::type* = nullptr>
hGraph<NodeData, EdgeData>* constructGraph(std::vector<unsigned> scaleFactor) {
  typedef hGraph_edgeCut<NodeData, EdgeData> Graph_edgeCut;
  typedef hGraph_vertexCut<NodeData, EdgeData> Graph_vertexCut;
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, true> Graph_cartesianCut; // assumes pull-style
  typedef hGraph_cartesianCut<NodeData, EdgeData, true, true> Graph_checkerboardCut; // assumes pull-style
  typedef hGraph_jaggedCut<NodeData, EdgeData, false, true> Graph_jaggedCut; // assumes pull-style
  typedef hGraph_jaggedCut<NodeData, EdgeData, true, true> Graph_jaggedBlockedCut; // assumes pull-style
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, true, false, false, 2> Graph_cartesianCut_overDecomposeBy2; // assumes pull-style
  typedef hGraph_cartesianCut<NodeData, EdgeData, false, true, false, false, 4> Graph_cartesianCut_overDecomposeBy4; // assumes pull-style

  auto& net = galois::runtime::getSystemNetworkInterface();

  // 1 host = no concept of cut; just load from edgeCut
  if (net.Num == 1) {
    if (inputFileTranspose.size()) {
      return new Graph_edgeCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                               scaleFactor, false);
    } else {
      fprintf(stderr, "WARNING: Loading transpose graph through in-memory "
                      "transpose to iterate over in-edges: pass in transpose "
                      "graph with -graphTranspose to avoid unnecessary "
                      "overhead.\n");
      return new Graph_edgeCut(inputFile, partFolder, net.ID, net.Num, 
                               scaleFactor, true);
    }
  }


  switch(partitionScheme) {
    case OEC:
      return new Graph_edgeCut(inputFile, partFolder, net.ID, net.Num, 
                               scaleFactor, true);
    case IEC:
      if (inputFileTranspose.size()) {
        return new Graph_edgeCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                 scaleFactor, false);
      } else {
        GALOIS_DIE("Error: attempting incoming edge cut without transpose "
                   "graph");
        break;
      }
    case HOVC:
      return new Graph_vertexCut(inputFile, partFolder, net.ID, net.Num, 
                                 scaleFactor, true, VCutThreshold);
    case HIVC:
      if (inputFileTranspose.size()) {
        return new Graph_vertexCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                 scaleFactor, false, VCutThreshold);
      } else {
        GALOIS_DIE("Error: (hivc) iterate over in-edges without transpose graph");
        break;
      }

    case BOARD2D_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_checkerboardCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                      scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (cvc) iterate over in-edges without transpose graph");
        break;
      }

    case CART_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_cartesianCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                      scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (cvc) iterate over in-edges without transpose graph");
        break;
      }
    case JAGGED_CYCLIC_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_jaggedCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                      scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (jcvc) iterate over in-edges without transpose graph");
        break;
      }
    case JAGGED_BLOCKED_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_jaggedBlockedCut(inputFileTranspose, partFolder, net.ID, net.Num, 
                                      scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (jbvc) iterate over in-edges without transpose graph");
        break;
      }
    case OVER_DECOMPOSE_2_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_cartesianCut_overDecomposeBy2(inputFileTranspose, partFolder, net.ID, 
                                                        net.Num, scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (od2vc) iterate over in-edges without transpose graph");
        break;
      }
    case OVER_DECOMPOSE_4_VCUT:
      if (inputFileTranspose.size()) {
        return new Graph_cartesianCut_overDecomposeBy4(inputFileTranspose, partFolder, net.ID, 
                                                        net.Num, scaleFactor, false);
      } else {
        GALOIS_DIE("Error: (od4vc) iterate over in-edges without transpose graph");
        break;
      }

    default:
      GALOIS_DIE("Error: partition scheme specified is invalid");
      return nullptr;
  }
}
#endif
