#include "katana/PropertyIndex.h"

namespace katana {

katana::Result<std::unique_ptr<PropertyIndex>> PropertyIndex::Make(
  PropertyGraph* pg, std::string column) {
    (void)pg;
    (void)column;
    auto index = std::make_unique<BasicPropertyIndex<double>>();
    return index;
  }

}