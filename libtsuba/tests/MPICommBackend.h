#ifndef GALOIS_LIBTSUBA_MPICOMMBACKEND_H_
#define GALOIS_LIBTSUBA_MPICOMMBACKEND_H_

#include <mpi.h>

#include <cassert>

#include "galois/CommBackend.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/tsuba.h"

namespace tsuba {

struct MPICommBackend : public galois::CommBackend {
  MPICommBackend() {
    int support_provided;
    int init_success =
        MPI_Init_thread(NULL, NULL, MPI_THREAD_MULTIPLE, &support_provided);
    if (init_success != MPI_SUCCESS) {
      GALOIS_LOG_ERROR("MPI_Init failed");
      MPI_Abort(MPI_COMM_WORLD, init_success);
    }
    if (support_provided != MPI_THREAD_MULTIPLE) {
      GALOIS_LOG_FATAL("MPI_THREAD_MULTIPLE not supported.");
    }

    int num_tasks_val;
    if (MPI_Comm_size(MPI_COMM_WORLD, &num_tasks_val) != 0) {
      GALOIS_LOG_FATAL("MPI_Comm_size failed");
    }

    int task_rank_val;
    if (MPI_Comm_rank(MPI_COMM_WORLD, &task_rank_val) != 0) {
      GALOIS_LOG_FATAL("MPI_Comm_rank failed");
    }

    assert(task_rank_val >= 0);
    assert(num_tasks_val > 0);
    Num = num_tasks_val;
    ID = task_rank_val;
  }

  void Barrier() override {
    if (MPI_Barrier(MPI_COMM_WORLD) != 0) {
      GALOIS_LOG_FATAL("MPI_Barrier failed");
    }
  }

  void NotifyFailure() override {
    if (MPI_Abort(MPI_COMM_WORLD, 1) != 0) {
      GALOIS_LOG_FATAL("MPI_Abort failed");
    }
  }
};

static tsuba::MPICommBackend test_backend{};

static galois::Result<void>
InitWithMPI() {
  return Init(&test_backend);
}

static galois::Result<void>
FiniWithMPI() {
  auto ret = Fini();

  int finalize_success = MPI_Finalize();
  if (finalize_success != MPI_SUCCESS) {
    GALOIS_LOG_ERROR("MPI_Finalize failed");
    MPI_Abort(MPI_COMM_WORLD, finalize_success);
  }
  return ret;
}

}  // namespace tsuba

#endif
