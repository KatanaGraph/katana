#include "katana/Properties.h"

#include "katana/Result.h"

namespace katana {

Result<BooleanPropertyReadOnlyView>
BooleanPropertyReadOnlyView::Make(const arrow::BooleanArray& array) {
  return BooleanPropertyReadOnlyView(array);
}

}  // namespace katana
