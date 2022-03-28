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

#include "katana/SharedMemSys.h"

#include "katana/CommBackend.h"
#include "katana/Experimental.h"
#include "katana/FileStorage.h"
#include "katana/Galois.h"
#include "katana/GaloisRuntime.h"
#include "katana/Logging.h"
#include "katana/Plugin.h"
#include "katana/Strings.h"
#include "katana/TextTracer.h"
#include "katana/tsuba.h"

namespace {

katana::NullCommBackend comm_backend;

}  // namespace

struct katana::SharedMemSys::Impl {
  katana::GaloisRuntime galois_rt;
};

katana::SharedMemSys::SharedMemSys(std::unique_ptr<ProgressTracer> tracer)
    : impl_(std::make_unique<Impl>()) {
  katana::ProgressTracer::Set(std::move(tracer));
  LoadPlugins();
  if (auto init_good = katana::InitTsuba(&comm_backend); !init_good) {
    KATANA_LOG_FATAL("katana::InitTsuba: {}", init_good.error());
  }

  auto features_on = katana::internal::ExperimentalFeature::ReportEnabled();
  if (!features_on.empty()) {
    auto feature_string = katana::Join(features_on, ",");
    ProgressTracer::Get().GetActiveSpan().SetTags(
        {{"experimental_features_enabled", feature_string}});
  }

  auto unrecognized =
      katana::internal::ExperimentalFeature::ReportUnrecognized();
  if (!unrecognized.empty()) {
    KATANA_LOG_WARN(
        "these values from KATANA_ENABLE_EXPERIMENTAL did not match any "
        "features:\n\t{}",
        katana::Join(unrecognized, " "));
  }
}

katana::SharedMemSys::~SharedMemSys() {
  if (auto fini_good = katana::FiniTsuba(); !fini_good) {
    KATANA_LOG_ERROR("katana::FiniTsuba: {}", fini_good.error());
  }
  katana::GetTracer().Finish();
  // This will finalize plugins irreversibly, reinitialization may not work.
  FinalizePlugins();
}
