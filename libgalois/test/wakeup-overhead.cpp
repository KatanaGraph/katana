/*
 * This file belongs to the Galois project, a C++ library for exploiting
 * parallelism. The code is being released under the terms of the 3-Clause BSD
 * License (a copy is located in LICENSE.txt at the top-level directory).
 *
 * Copyright (C) 2018, The University of Texas at Austin. All rights reserved.
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
 */

#include <chrono>
#include <cmath>
#include <iostream>
#include <vector>

#include <boost/iterator/counting_iterator.hpp>
#include <llvm/Support/CommandLine.h>

#include "katana/Galois.h"
#include "katana/Reduction.h"
#include "katana/Timer.h"

typedef katana::GAccumulator<double> AccumDouble;

namespace cll = llvm::cl;

static cll::opt<int> size(
    "size", cll::desc("length of vectors"), cll::init(1000));
static cll::opt<int> rounds(
    "rounds", cll::desc("number of rounds"), cll::init(10000));
static cll::opt<int> trials(
    "trials", cll::desc("number of trials"), cll::init(1));
static cll::opt<unsigned> threads(
    "threads", cll::desc("number of threads"), cll::init(2));

void
runDoAllBurn(int num) {
  katana::GetThreadPool().burnPower(katana::getActiveThreads());

  for (int r = 0; r < rounds; ++r) {
    katana::do_all(katana::iterate(0, num), [&](int) {
      asm volatile("" ::: "memory");
    });
  }

  katana::GetThreadPool().beKind();
}

void
runDoAll(int num) {
  for (int r = 0; r < rounds; ++r) {
    katana::do_all(katana::iterate(0, num), [&](int) {
      asm volatile("" ::: "memory");
    });
  }
}

void
runExplicitThread(int num) {
  katana::Barrier& barrier = katana::GetBarrier(katana::getActiveThreads());

  katana::on_each([&](unsigned tid, unsigned total) {
    auto range = katana::block_range(
        boost::counting_iterator<int>(0), boost::counting_iterator<int>(num),
        tid, total);
    for (int r = 0; r < rounds; ++r) {
      for (auto ii = range.first, ei = range.second; ii != ei; ++ii) {
        asm volatile("" ::: "memory");
      }
      barrier.Wait();
    }
  });
}

void
run(std::function<void(int)> fn, std::string name) {
  katana::Timer t;
  t.start();
  fn(size);
  t.stop();
  std::cout << name << " time: " << t.get() << "\n";
}

std::atomic<int> EXIT;

int
main(int argc, char* argv[]) {
  katana::GaloisRuntime Katana_runtime;
  llvm::cl::ParseCommandLineOptions(argc, argv);

  katana::setActiveThreads(threads);

  EXIT = 0;
  std::function<void(void)> f = []() {
    while (!EXIT) {
      std::cerr << ".";
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
  };
  katana::GetThreadPool().runDedicated(f);

  for (int t = 0; t < trials; ++t) {
    run(runDoAll, "DoAll");
    run(runDoAllBurn, "DoAllBurn");
    run(runExplicitThread, "ExplicitThread");
  }
  EXIT = 1;

  std::cout << "threads: " << katana::getActiveThreads() << " usable threads: "
            << katana::GetThreadPool().getMaxUsableThreads()
            << " rounds: " << rounds << " size: " << size << "\n";

  return 0;
}
