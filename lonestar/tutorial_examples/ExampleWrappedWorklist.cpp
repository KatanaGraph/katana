#include <fstream>
#include <iostream>

#include "Lonestar/BoilerPlate.h"
#include "katana/Bag.h"
#include "katana/PerThreadStorage.h"
#include "katana/SharedMemSys.h"
#include "katana/UserContext.h"

class ExampleWrappedWorklist {
private:
  katana::InsertBag<int> bag;
  katana::PerThreadStorage<katana::UserContext<int>*> ctxPtr;
  bool inParallelPhase;

private:
  void reset() {
    bag.clear();
    for (unsigned i = 0; i < ctxPtr.size(); i++) {
      *(ctxPtr.getRemote(i)) = nullptr;
    }
  }

public:
  ExampleWrappedWorklist() : inParallelPhase(false) { reset(); }

  void enqueue(int item) {
    if (inParallelPhase) {
      (*(ctxPtr.getLocal()))->push(item);
    } else {
      bag.push(item);
    }
  }

  void execute() {
    inParallelPhase = true;

    katana::for_each(
        katana::iterate(bag),
        [&](int item, auto& ctx) {
          if (nullptr == *(ctxPtr.getLocal())) {
            *(ctxPtr.getLocal()) = &ctx;
          }

          std::cout << item << std::endl;

          if (item < 2000) {
            this->enqueue(item + item);
          }
        },
        katana::loopname("execute"), katana::disable_conflict_detection());

    inParallelPhase = false;
    reset();
  }
};

int
main(int argc, char* argv[]) {
  std::unique_ptr<katana::SharedMemSys> G = LonestarStart(argc, argv);

  ExampleWrappedWorklist q;
  for (unsigned i = 0; i < katana::getActiveThreads(); i++) {
    q.enqueue(i + 1);
  }
  q.execute();

  return 0;
}
