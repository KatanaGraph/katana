#include <iomanip>
#include <iostream>

#include "BCEdge.h"
#include "BCNode.h"
#include "katana/Bag.h"
#include "katana/BufferedGraph.h"
#include "katana/LC_CSR_CSC_Graph.h"

// WARNING: optimal chunk size may differ depending on input graph
constexpr static const unsigned ASYNC_CHUNK_SIZE = 64U;
using NodeType = BCNode<BC_USE_MARKING, BC_CONCURRENT>;
using AsynchronousGraph =
    katana::LC_CSR_CSC_Graph<NodeType, BCEdge, false, true>;

// Work items for the forward phase
struct ForwardPhaseWorkItem {
  uint32_t nodeID;
  uint32_t distance;
  ForwardPhaseWorkItem() : nodeID(kInfinity), distance(kInfinity){};
  ForwardPhaseWorkItem(uint32_t _n, uint32_t _d) : nodeID(_n), distance(_d){};
};

// grabs distance from a forward phase work item
struct FPWorkItemIndexer {
  uint32_t operator()(const ForwardPhaseWorkItem& it) const {
    return it.distance;
  }
};

// obim worklist type declaration
using PSchunk = katana::PerSocketChunkFIFO<ASYNC_CHUNK_SIZE>;
using OBIM = katana::OrderedByIntegerMetric<FPWorkItemIndexer, PSchunk>;

template <typename T, bool enable>
struct Counter : public T {
  std::string name;

  Counter(std::string s) : name(std::move(s)) {}

  ~Counter() { katana::ReportStatSingle("(NULL)", name, this->reduce()); }
};

template <typename T>
struct Counter<T, false> {
  Counter(std::string) {}

  template <typename... Args>
  void update(Args...) {}
};

struct BetweenessCentralityAsynchronous {
  AsynchronousGraph& graph;

  BetweenessCentralityAsynchronous(AsynchronousGraph& _graph) : graph(_graph) {}

  using SumCounter =
      Counter<katana::GAccumulator<unsigned long>, BC_COUNT_ACTIONS>;
  SumCounter spfuCount{"SP&FU"};
  SumCounter updateSigmaP1Count{"UpdateSigmaBefore"};
  SumCounter updateSigmaP2Count{"RealUS"};
  SumCounter firstUpdateCount{"First Update"};
  SumCounter correctNodeP1Count{"CorrectNodeBefore"};
  SumCounter correctNodeP2Count{"Real CN"};
  SumCounter noActionCount{"NoAction"};

  using MaxCounter =
      Counter<katana::GReduceMax<unsigned long>, BC_COUNT_ACTIONS>;
  MaxCounter largestNodeDist{"Largest node distance"};

  using LeafCounter =
      Counter<katana::GAccumulator<unsigned long>, BC_COUNT_LEAVES>;

  void CorrectNode(uint32_t dstID, BCEdge&) {
    NodeType& dstData = graph.getData(dstID);

    // loop through in edges
    for (auto e : graph.in_edges(dstID)) {
      BCEdge& inEdgeData = graph.getInEdgeData(e);

      uint32_t srcID = graph.getInEdgeDst(e);
      if (srcID == dstID)
        continue;

      NodeType& srcData = graph.getData(srcID);

      // lock in right order
      if (srcID < dstID) {
        srcData.lock();
        dstData.lock();
      } else {
        dstData.lock();
        srcData.lock();
      }

      const unsigned edgeLevel = inEdgeData.level;

      // Correct Node
      if (srcData.distance >= dstData.distance) {
        correctNodeP1Count.update(1);
        dstData.unlock();

        if (edgeLevel != kInfinity) {
          inEdgeData.level = kInfinity;
          if (edgeLevel == srcData.distance) {
            correctNodeP2Count.update(1);
            srcData.nsuccs--;
          }
        }
        srcData.unlock();
      } else {
        srcData.unlock();
        dstData.unlock();
      }
    }
  }

  template <typename CTXType>
  void SpAndFU(uint32_t srcID, uint32_t dstID, BCEdge& ed, CTXType& ctx) {
    spfuCount.update(1);

    NodeType& srcData = graph.getData(srcID);
    NodeType& dstData = graph.getData(dstID);

    // make dst a successor of src, src predecessor of dst
    srcData.nsuccs++;
    const ShortPathType srcSigma = srcData.sigma;
    KATANA_LOG_DEBUG_ASSERT(srcSigma > 0);
    NodeType::predTY& dstPreds = dstData.preds;
    bool dstPredsNotEmpty = !dstPreds.empty();
    dstPreds.clear();
    dstPreds.push_back(srcID);
    dstData.distance = srcData.distance + 1;

    largestNodeDist.update(dstData.distance);

    dstData.nsuccs = 0;        // SP
    dstData.sigma = srcSigma;  // FU
    ed.val = srcSigma;
    ed.level = srcData.distance;
    srcData.unlock();
    if (!dstData.isAlreadyIn())
      ctx.push(ForwardPhaseWorkItem(dstID, dstData.distance));
    dstData.unlock();
    if (dstPredsNotEmpty) {
      correctNode(dstID, ed);
    }
  }

  template <typename CTXType>
  void UpdateSigma(uint32_t srcID, uint32_t dstID, BCEdge& ed, CTXType& ctx) {
    updateSigmaP1Count.update(1);

    NodeType& srcData = graph.getData(srcID);
    NodeType& dstData = graph.getData(dstID);

    const ShortPathType srcSigma = srcData.sigma;
    const ShortPathType eval = ed.val;
    const ShortPathType diff = srcSigma - eval;

    srcData.unlock();
    // greater than 0.0001 instead of 0 due to floating point imprecision
    if (diff > 0.0001) {
      updateSigmaP2Count.update(1);
      ed.val = srcSigma;

      // ShortPathType old = dstData.sigma;
      dstData.sigma += diff;

      // if (old >= dstData.sigma) {
      //  katana::gDebug("Overflow detected; capping at max uint64_t");
      //  dstData.sigma = std::numeric_limits<uint64_t>::max();
      //}

      int nbsuccs = dstData.nsuccs;

      if (nbsuccs > 0) {
        if (!dstData.isAlreadyIn())
          ctx.push(ForwardPhaseWorkItem(dstID, dstData.distance));
      }
      dstData.unlock();
    } else {
      dstData.unlock();
    }
  }

  template <typename CTXType>
  void FirstUpdate(uint32_t srcID, uint32_t dstID, BCEdge& ed, CTXType& ctx) {
    firstUpdateCount.update(1);

    NodeType& srcData = graph.getData(srcID);
    srcData.nsuccs++;
    const ShortPathType srcSigma = srcData.sigma;

    NodeType& dstData = graph.getData(dstID);
    dstData.preds.push_back(srcID);

    const ShortPathType dstSigma = dstData.sigma;

    // ShortPathType old = dstData.sigma;
    dstData.sigma = dstSigma + srcSigma;
    // if (old >= dstData.sigma) {
    //  katana::gDebug("Overflow detected; capping at max uint64_t");
    //  dstData.sigma = std::numeric_limits<uint64_t>::max();
    //}

    ed.val = srcSigma;
    ed.level = srcData.distance;
    srcData.unlock();
    int nbsuccs = dstData.nsuccs;
    if (nbsuccs > 0) {
      if (!dstData.isAlreadyIn())
        ctx.push(ForwardPhaseWorkItem(dstID, dstData.distance));
    }
    dstData.unlock();
  }

  void DagConstruction(katana::InsertBag<ForwardPhaseWorkItem>& wl) {
    katana::for_each(
        katana::iterate(wl),
        [&](ForwardPhaseWorkItem& wi, auto& ctx) {
          uint32_t srcID = wi.nodeID;
          NodeType& srcData = graph.getData(srcID);
          srcData.markOut();

          // loop through all edges
          for (auto e : graph.edges(srcID)) {
            BCEdge& edgeData = graph.getEdgeData(e);
            uint32_t dstID = graph.getEdgeDst(e);
            NodeType& dstData = graph.getData(dstID);

            if (srcID == dstID)
              continue;  // ignore self loops

            // lock in set order to prevent deadlock (lower id
            // first)
            // TODO run even in serial version; find way to not
            // need to run
            if (srcID < dstID) {
              srcData.lock();
              dstData.lock();
            } else {
              dstData.lock();
              srcData.lock();
            }

            const int elevel = edgeData.level;
            const int ADist = srcData.distance;
            const int BDist = dstData.distance;

            if (BDist - ADist > 1) {
              // Shortest Path + First Update (and Correct Node)
              this->spAndFU(srcID, dstID, edgeData, ctx);
            } else if (elevel == ADist && BDist == ADist + 1) {
              // Update Sigma
              this->updateSigma(srcID, dstID, edgeData, ctx);
            } else if (BDist == ADist + 1 && elevel != ADist) {
              // First Update not combined with Shortest Path
              this->firstUpdate(srcID, dstID, edgeData, ctx);
            } else {  // No Action
              noActionCount.update(1);
              srcData.unlock();
              dstData.unlock();
            }
          }
        },
        katana::wl<OBIM>(FPWorkItemIndexer()),
        katana::disable_conflict_detection(), katana::loopname("ForwardPhase"));
  }

  void DependencyBackProp(katana::InsertBag<uint32_t>& wl) {
    katana::for_each(
        katana::iterate(wl),
        [&](uint32_t srcID, auto& ctx) {
          NodeType& srcData = graph.getData(srcID);
          srcData.lock();

          if (srcData.nsuccs == 0) {
            const double srcDelta = srcData.delta;
            srcData.bc += srcDelta;

            srcData.unlock();

            NodeType::predTY& srcPreds = srcData.preds;

            // loop through src's predecessors
            for (unsigned i = 0; i < srcPreds.size(); i++) {
              uint32_t predID = srcPreds[i];
              NodeType& predData = graph.getData(predID);

              KATANA_LOG_DEBUG_ASSERT(srcData.sigma >= 1);
              const double term =
                  (double)predData.sigma * (1.0 + srcDelta) / srcData.sigma;
              // if (std::isnan(term)) {
              //  katana::gPrint(predData.sigma, " ", srcDelta, "
              //  ", srcData.sigma, "\n");
              //}
              predData.lock();
              predData.delta += term;
              const unsigned prevPdNsuccs = predData.nsuccs;
              predData.nsuccs--;

              if (prevPdNsuccs == 1) {
                predData.unlock();
                ctx.push(predID);
              } else {
                predData.unlock();
              }
            }

            // reset data in preparation for next source
            srcData.reset();
            for (auto e : graph.edges(srcID)) {
              graph.getEdgeData(e).reset();
            }
          } else {
            srcData.unlock();
          }
        },
        katana::disable_conflict_detection(),
        katana::loopname("BackwardPhase"));
  }

  void FindLeaves(katana::InsertBag<uint32_t>& fringeWL, unsigned nnodes) {
    LeafCounter leafCount{"leaf nodes in DAG"};
    katana::do_all(
        katana::iterate(0u, nnodes),
        [&](auto i) {
          NodeType& n = graph.getData(i);

          if (n.nsuccs == 0 && n.distance < kInfinity) {
            leafCount.update(1);
            fringeWL.push(i);
          }
        },
        katana::loopname("LeafFind"));
  }
};

void
AsynchronousSanity(AsynchronousGraph& graph) {
  katana::GReduceMax<float> accum_max;
  katana::GReduceMin<float> accum_min;
  katana::GAccumulator<float> accum_sum;
  accum_max.reset();
  accum_min.reset();
  accum_sum.reset();

  // get max, min, sum of BC values using accumulators and reducers
  katana::do_all(
      katana::iterate(graph),
      [&](unsigned n) {
        auto& nodeData = graph.getData(n);
        accum_max.update(nodeData.bc);
        accum_min.update(nodeData.bc);
        accum_sum += nodeData.bc;
      },
      katana::no_stats(), katana::loopname("AsynchronousSanity"));

  katana::gPrint("Max BC is ", accum_max.reduce(), "\n");
  katana::gPrint("Min BC is ", accum_min.reduce(), "\n");
  katana::gPrint("BC sum is ", accum_sum.reduce(), "\n");
}
////////////////////////////////////////////////////////////////////////////////

//! runs asynchronous BC
void
BetweennessCentralityAsynchronous() {
  if (BC_CONCURRENT) {
    katana::gInfo("Running in concurrent mode with ", numThreads, " threads");
  } else {
    katana::gInfo("Running in serial mode");
  }

  katana::gInfo("Constructing async BC graph");
  // create bidirectional graph
  AsynchronousGraph bcGraph;

  katana::StatTimer graph_construct_timer("GRAPH_CONSTRUCT");
  graph_construct_timer.start();

  katana::FileGraph fileReader;
  fileReader.fromFile(inputFile);
  bcGraph.allocateFrom(fileReader.size(), fileReader.sizeEdges());
  bcGraph.constructNodes();

  katana::do_all(katana::iterate(fileReader), [&](uint32_t i) {
    auto b = fileReader.edge_begin(i);
    auto e = fileReader.edge_end(i);

    bcGraph.fixEndEdge(i, *e);

    while (b < e) {
      bcGraph.constructEdge(*b, fileReader.getEdgeDst(*b));
      b++;
    }
  });
  bcGraph.constructIncomingEdges();

  graph_construct_timer.stop();

  BetweenessCentralityAsynchronous bcExecutor(bcGraph);

  unsigned nnodes = bcGraph.size();
  uint64_t nedges = bcGraph.sizeEdges();
  katana::gInfo("Num nodes is ", nnodes, ", num edges is ", nedges);
  katana::gInfo("Using OBIM chunk size: ", ASYNC_CHUNK_SIZE);
  katana::gInfo(
      "Note that optimal chunk size may differ depending on input "
      "graph");
  katana::ReportStatSingle(
      "BetweennessCentralityAsynchronous", "ChunkSize", ASYNC_CHUNK_SIZE);

  katana::EnsurePreallocated(
      std::min(
          static_cast<uint64_t>(
              std::min(katana::getActiveThreads(), 100U) *
              std::max((nnodes / 4500000), unsigned{5}) *
              std::max((nedges / 30000000), uint64_t{5}) * 2.5),
          uint64_t{1500}) +
      5);
  katana::ReportPageAllocGuard page_alloc;

  // reset everything in preparation for run
  katana::do_all(
      katana::iterate(0u, nnodes), [&](auto i) { bcGraph.getData(i).reset(); });
  katana::do_all(katana::iterate(UINT64_C(0), nedges), [&](auto i) {
    bcGraph.getEdgeData(i).reset();
  });

  // reading in list of sources to operate on if provided
  std::ifstream sourceFile;
  std::vector<uint64_t> sourceVector;
  if (sourcesToUse != "") {
    sourceFile.open(sourcesToUse);
    std::vector<uint64_t> t(
        std::istream_iterator<uint64_t>{sourceFile},
        std::istream_iterator<uint64_t>{});
    sourceVector = t;
    sourceFile.close();
  }

  if (numOfSources == 0) {
    numOfSources = nnodes;
  }

  // if user does specifes a certain number of out sources (i.e. only sources
  // with outgoing edges), we need to loop over the entire node set to look for
  // good sources to use
  uint32_t goodSource = 0;
  if (iterLimit != 0) {
    numOfSources = nnodes;
  }

  // only use at most the number of sources in the passed in source file (if
  // such a file was actually pass in)
  if (sourceVector.size() != 0) {
    if (numOfSources > sourceVector.size()) {
      numOfSources = sourceVector.size();
    }
  }

  katana::InsertBag<ForwardPhaseWorkItem> forwardPhaseWL;
  katana::InsertBag<uint32_t> backwardPhaseWL;

  katana::gInfo("Beginning execution");

  katana::StatTimer exec_time("BetweennessCentralityAsynchronous");
  exec_time.start();
  for (uint32_t i = 0; i < numOfSources; ++i) {
    uint32_t sourceToUse = i;
    if (sourceVector.size() != 0) {
      sourceToUse = sourceVector[i];
    }

    // ignore nodes with no neighbors
    if (!std::distance(
            bcGraph.edge_begin(sourceToUse), bcGraph.edge_end(sourceToUse))) {
      katana::gDebug(sourceToUse, " has no outgoing edges");
      continue;
    }

    forwardPhaseWL.push_back(ForwardPhaseWorkItem(sourceToUse, 0));
    NodeType& active = bcGraph.getData(sourceToUse);
    active.initAsSource();
    katana::gDebug("Source is ", sourceToUse);

    bcExecutor.dagConstruction(forwardPhaseWL);
    forwardPhaseWL.clear();

    bcExecutor.findLeaves(backwardPhaseWL, nnodes);

    double backupSrcBC = active.bc;
    bcExecutor.dependencyBackProp(backwardPhaseWL);

    active.bc = backupSrcBC;  // current source BC should not get updated

    backwardPhaseWL.clear();

    // break out once number of sources user specified to do (if any) has been
    // reached
    goodSource++;
    if (iterLimit != 0 && goodSource >= iterLimit)
      break;
  }
  exec_time.stop();

  katana::gInfo("Number of sources with outgoing edges was ", goodSource);

  page_alloc.Report();

  // sanity
  AsynchronousSanity(bcGraph);

  // prints out first 10 node BC values
  if (!skipVerify) {
    int count = 0;
    for (unsigned i = 0; i < nnodes && count < 10; ++i, ++count) {
      katana::gPrint(
          count, ": ", std::setiosflags(std::ios::fixed), std::setprecision(6),
          bcGraph.getData(i).bc, "\n");
    }
  }

  if (output) {
    std::cerr << "Writting out bc values...\n";
    std::stringstream outfname;
    outfname << "certificate"
             << "_" << numThreads << ".txt";
    std::string fname = outfname.str();
    std::ofstream outfile(fname.c_str());
    for (unsigned i = 0; i < nnodes; ++i) {
      outfile << i << " " << std::setiosflags(std::ios::fixed)
              << std::setprecision(9) << bcGraph.getData(i).bc << "\n";
    }
    outfile.close();
  }
}
