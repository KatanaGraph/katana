#include "katana/ArrowInterchange.h"

#include <iostream>
#include <iterator>
#include <sstream>

#include <arrow/array/concatenate.h>

#include "katana/Random.h"
#include "katana/Result.h"

namespace {

uint64_t
ApproxArrayDataMemUse(const std::shared_ptr<arrow::ArrayData>& data) {
  uint64_t total_mem_use = 0;
  const auto& buffers = data->buffers;
  const auto& layout = data->type->layout();

  KATANA_LOG_ASSERT(layout.buffers.size() == buffers.size());

  for (int i = 0, num_buffers = buffers.size(); i < num_buffers; ++i) {
    if (!buffers[i]) {
      continue;
    }
    switch (layout.buffers[i].kind) {
    case arrow::DataTypeLayout::FIXED_WIDTH:
      total_mem_use += layout.buffers[i].byte_width * data->length;
      break;

    // TODO(thunt) get a better estimate for these types, based on my
    // read of the arrow source they don't follow the rules use the whole
    // buffer size as an over estimate
    case arrow::DataTypeLayout::VARIABLE_WIDTH:
    case arrow::DataTypeLayout::BITMAP:
    case arrow::DataTypeLayout::ALWAYS_NULL:
      total_mem_use += buffers[i]->size();
    }
  }
  if (data->dictionary) {
    total_mem_use += ApproxArrayDataMemUse(data->dictionary);
  }
  for (const auto& child_data : data->child_data) {
    total_mem_use += ApproxArrayDataMemUse(child_data);
  }

  return total_mem_use;
}

}  // anonymous namespace

katana::Result<std::shared_ptr<arrow::Table>>
katana::TakeRows(
    const std::shared_ptr<arrow::Table>& original,
    const std::shared_ptr<arrow::BooleanArray>& picker) {
  arrow::compute::FilterOptions opts(arrow::compute::FilterOptions::DROP);
  arrow::Datum filtered =
      KATANA_CHECKED(arrow::compute::Filter(original, picker, opts));
  return filtered.table();
}

katana::Result<std::shared_ptr<arrow::ChunkedArray>>
katana::NullChunkedArray(
    const std::shared_ptr<arrow::DataType>& type, int64_t length) {
  auto array = KATANA_CHECKED(arrow::MakeArrayOfNull(type, length));
  std::vector<std::shared_ptr<arrow::Array>> chunks{array};
  return std::make_shared<arrow::ChunkedArray>(chunks);
}

std::shared_ptr<arrow::Table>
katana::MakeEmptyArrowTable() {
  return arrow::Table::Make(
      arrow::schema({}), std::vector<std::shared_ptr<arrow::ChunkedArray>>());
}

void
katana::DiffFormatTo(
    fmt::memory_buffer& buf, const std::shared_ptr<arrow::ChunkedArray>& a0,
    const std::shared_ptr<arrow::ChunkedArray>& a1,
    size_t approx_total_characters) {
  if (a0->type() != a1->type()) {
    fmt::format_to(
        std::back_inserter(buf), "Arrays are different types {}/{}\n",
        a0->type()->name(), a1->type()->name());
    return;
  }

  auto maybe_b0 = arrow::Concatenate(a0->chunks());
  if (!maybe_b0.ok()) {
    fmt::format_to(
        std::back_inserter(buf),
        "failed conversion of chunked array to array type: {} reason: {}",
        a0->type()->name(), maybe_b0.status().message());
    return;
  }
  auto b0 = maybe_b0.ValueUnsafe();
  auto maybe_b1 = arrow::Concatenate(a1->chunks());
  if (!maybe_b1.ok()) {
    fmt::format_to(
        std::back_inserter(buf),
        "failed conversion of chunked array to array type: {} reason: {}",
        a1->type()->name(), maybe_b1.status().message());
  }
  auto b1 = maybe_b1.ValueUnsafe();

  arrow::EqualOptions equal_options;
  // TODO (witchel) create a bounded length streambuf so this won't waste memory when
  // the diff is large
  std::ostringstream ss;
  equal_options = equal_options.diff_sink(&ss);
  if (!arrow::ArrayEquals(*b0, *b1, equal_options)) {
    auto str = ss.str();
    // Arrow output starts with newline for some reason
    const auto after = str.find_first_not_of('\n');
    auto orig_len = str.size() - after;
    if (orig_len <= approx_total_characters) {
      fmt::format_to(std::back_inserter(buf), "{}", str.substr(after));
    } else {
      // Cut it off at next newline, but +1 to keep that newline
      str = str.substr(
          after, str.find_first_of('\n', approx_total_characters + after) + 1);
      // Indicator that we have truncated the output
      str += "...\n";
      fmt::format_to(std::back_inserter(buf), "{}", str);
    }
  }
}

uint64_t
katana::ApproxArrayMemUse(const std::shared_ptr<arrow::Array>& array) {
  return ApproxArrayDataMemUse(array->data());
}

uint64_t
katana::ApproxTableMemUse(const std::shared_ptr<arrow::Table>& table) {
  uint64_t total_mem_use = 0;
  for (const auto& chunked_array : table->columns()) {
    for (const auto& array : chunked_array->chunks()) {
      total_mem_use += ApproxArrayDataMemUse(array->data());
    }
  }
  return total_mem_use;
}
