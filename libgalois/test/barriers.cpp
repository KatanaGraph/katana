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

#include <unistd.h>

#include <cstdlib>
#include <iostream>

#include "katana/Barrier.h"
#include "katana/Galois.h"
#include "katana/Timer.h"

unsigned iter = 0;
unsigned numThreads = 0;

char bname[100];

struct emp {
  katana::Barrier& b;

  void go() {
    for (unsigned i = 0; i < iter; ++i) {
      b.Wait();
    }
  }

  template <typename T>
  void operator()(const T&) {
    go();
  }

  template <typename T, typename C>
  void operator()(const T&, const C&) {
    go();
  }
};

void
test(std::unique_ptr<katana::Barrier> b) {
  if (b == nullptr) {
    std::cout << "skipping " << bname << "\n";
    return;
  }

  unsigned M = numThreads;
  if (M > 16)
    M /= 2;
  while (M) {
    katana::setActiveThreads(M);
    b->Reinit(M);
    katana::Timer t;
    t.start();
    emp e{*b.get()};
    katana::on_each(e);
    t.stop();
    std::cout << bname << "," << b->name() << "," << M << "," << t.get()
              << "\n";
    M -= 1;
  }
}

int
main(int argc, char** argv) {
  katana::GaloisRuntime Katana_runtime;
  if (argc > 1)
    iter = atoi(argv[1]);
  else
    iter = 16 * 1024;
  if (argc > 2)
    numThreads = atoi(argv[2]);
  else
    numThreads = katana::GetThreadPool().getMaxThreads();

  gethostname(bname, sizeof(bname));
  using namespace katana;
  test(CreateCountingBarrier(1));
  test(CreateMCSBarrier(1));
  test(CreateTopoBarrier(1));
  test(CreateDisseminationBarrier(1));
  // TODO(amp): Reenable when SimpleBarrier is fixed. It is broken and deadlocks.
  //test(CreateSimpleBarrier(1));
  return 0;
}
