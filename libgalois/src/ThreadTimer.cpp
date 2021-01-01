#include "katana/ThreadTimer.h"

#include <ctime>
#include <limits>

#include "katana/Executor_OnEach.h"
#include "katana/Statistics.h"

void
katana::ThreadTimer::start() {
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &start_);
}

void
katana::ThreadTimer::stop() {
  clock_gettime(CLOCK_THREAD_CPUTIME_ID, &stop_);
  nsec_ += (stop_.tv_nsec - start_.tv_nsec);
  nsec_ += ((stop_.tv_sec - start_.tv_sec) * 1000000000);
}

void
katana::ThreadTimers::reportTimes(const char* category, const char* region) {
  uint64_t minTime = std::numeric_limits<uint64_t>::max();

  for (unsigned i = 0; i < timers_.size(); ++i) {
    auto ns = timers_.getRemote(i)->get_nsec();
    minTime = std::min(minTime, ns);
  }

  std::string timeCat = category + std::string("PerThreadTimes");
  std::string lagCat = category + std::string("PerThreadLag");

  on_each_gen(
      [&](auto, auto) {
        auto ns = timers_.getLocal()->get_nsec();
        auto lag = ns - minTime;
        assert(lag > 0 && "negative time lag from min is impossible");

        ReportStatMax(region, timeCat.c_str(), ns / 1000000);
        ReportStatMax(region, lagCat.c_str(), lag / 1000000);
      },
      std::make_tuple());
}
