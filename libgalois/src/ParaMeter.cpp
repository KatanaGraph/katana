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

#include <ctime>

#include "katana/Env.h"
#include "katana/Executor_ParaMeter.h"
#include "katana/gIO.h"

struct StatsFileManager {
  bool init = false;
  bool isOpen = false;
  FILE* statsFH = nullptr;
  // char statsFileName[FNAME_SIZE];
  std::string statsFileName;

  ~StatsFileManager(void) { close(); }

  static void getTimeStampedName(std::string& statsFileName) {
    constexpr unsigned FNAME_SIZE = 256;
    char buf[FNAME_SIZE];

    time_t rawtime;
    struct tm* timeinfo;

    time(&rawtime);
    timeinfo = localtime(&rawtime);

    strftime(
        buf, FNAME_SIZE, "ParaMeter-Stats-%Y-%m-%d--%H-%M-%S.csv", timeinfo);
    statsFileName = buf;
  }

  FILE* get(void) {
    if (!init) {
      init = true;

      if (!katana::GetEnv("KATANA_PARAMETER_OUTFILE", &statsFileName)) {
        // statsFileName = "ParaMeter-Stats.csv";
        getTimeStampedName(statsFileName);
      }

      statsFH = fopen(statsFileName.c_str(), "w");
      KATANA_LOG_VASSERT(statsFH != nullptr, "ParaMeter stats file error");

      katana::parameter::StepStatsBase::printHeader(statsFH);

      fclose(statsFH);
    }

    if (!isOpen) {
      statsFH = fopen(statsFileName.c_str(), "a");  // open in append mode
      KATANA_LOG_VASSERT(statsFH != nullptr, "ParaMeter stats file error");

      isOpen = true;
    }

    return statsFH;
  }

  void close(void) {
    if (isOpen) {
      fclose(statsFH);
      isOpen = false;
      statsFH = nullptr;
    }
  }
};

static StatsFileManager&
getStatsFileManager(void) {
  static StatsFileManager s;
  return s;
}

FILE*
katana::parameter::getStatsFile(void) {
  return getStatsFileManager().get();
}

void
katana::parameter::closeStatsFile(void) {
  getStatsFileManager().close();
}
