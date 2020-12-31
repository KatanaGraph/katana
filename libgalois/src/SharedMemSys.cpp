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

#include "galois/SharedMemSys.h"

#include "galois/CommBackend.h"
#include "galois/Logging.h"
#include "galois/Statistics.h"
#include "galois/substrate/SharedMem.h"
#include "tsuba/FileStorage.h"
#include "tsuba/tsuba.h"

namespace {

galois::NullCommBackend comm_backend;

}  // namespace

struct galois::SharedMemSys::Impl {
  galois::substrate::SharedMem shared_mem;
  galois::StatManager stat_manager;
};

galois::SharedMemSys::SharedMemSys() : impl_(std::make_unique<Impl>()) {
  if (auto init_good = tsuba::Init(&comm_backend); !init_good) {
    GALOIS_LOG_FATAL("tsuba::Init: {}", init_good.error());
  }

  galois::internal::setSysStatManager(&impl_->stat_manager);
}

galois::SharedMemSys::~SharedMemSys() {
  galois::PrintStats();
  galois::internal::setSysStatManager(nullptr);

  if (auto fini_good = tsuba::Fini(); !fini_good) {
    GALOIS_LOG_ERROR("tsuba::Fini: {}", fini_good.error());
  }
}
