#ifndef KATANA_LIBGALOIS_KATANA_ANALYTICS_UTILS_H_
#define KATANA_LIBGALOIS_KATANA_ANALYTICS_UTILS_H_

#include <algorithm>
#include <random>
#include <utility>

#include <arrow/type.h>

#include "katana/ErrorCode.h"
#include "katana/Properties.h"
#include "katana/PropertyGraph.h"
#include "katana/Result.h"

namespace katana::analytics {

// TODO(amp): This file should be disbanded and its functions moved to
// PropertyGraph.h or other more specific places.

//! Used to pick random non-zero degree starting points for search algorithms
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/benchmark.h)
class KATANA_EXPORT SourcePicker {
  const PropertyGraph& graph;

public:
  explicit SourcePicker(const PropertyGraph& g) : graph(g) {}

  uint32_t PickNext();
};

//! Used to determine if a graph has power-law degree distribution or not
//! by sampling some of the vertices in the graph randomly
//! This code has been copied from GAP benchmark suite
//! (https://github.com/sbeamer/gapbs/blob/master/src/tc.cc WorthRelabelling())
KATANA_EXPORT bool IsApproximateDegreeDistributionPowerLaw(
    const PropertyGraph& graph);

template <typename Props>
std::vector<std::string>
DefaultPropertyNames() {
  auto num_tuple_elem = std::tuple_size<Props>::value;
  std::vector<std::string> names(num_tuple_elem);

  for (size_t i = 0; i < names.size(); ++i) {
    names[i] = "Column_" + std::to_string(i);
  }
  return names;
}

template <typename NodeProps>
inline katana::Result<void>
ConstructNodeProperties(
    PropertyGraph* pg,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto res_table = katana::AllocateTable<NodeProps>(pg->num_nodes(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddNodeProperties(res_table.value());
}

inline std::shared_ptr<arrow::ChunkedArray>
ApplyBitMask(
    const std::shared_ptr<arrow::ChunkedArray>& chunked_array,
    const uint8_t* bit_mask) {
  uint32_t num_chunks = chunked_array->num_chunks();

  uint32_t offset{0};

  std::vector<std::shared_ptr<arrow::Array>> new_chunks;

  katana::gPrint("\n inside: {}", num_chunks);

  for (uint32_t i = 0; i < num_chunks; i++) {
    std::shared_ptr<arrow::Array> array = chunked_array->chunk(i);
    std::shared_ptr<arrow::Buffer> mask =
        std::make_shared<arrow::Buffer>(bit_mask + offset, array->length());
    std::shared_ptr<arrow::ArrayData> data = array->data()->Copy();

    uint32_t p{0};
    for (uint32_t j = 0; j < array->length(); j++) {
      if ((*mask)[j]) {
        p++;
      }
    }

    katana::gPrint("\n p : {}", p);
    data->buffers[0] = mask;
    data->null_count = arrow::kUnknownNullCount;

    p = 0;
    for (uint32_t j = 0; j < array->length(); j++) {
      if ((*data->buffers[0])[j]) {
        p++;
      }
    }

    katana::gPrint("\n p : {}", p);
    auto array_ptr = arrow::MakeArray(data);
    uint32_t num{0};

    katana::gPrint("\n length: {}", array_ptr->length());

    katana::gPrint("\n offset: {} ", array_ptr->offset());

    for (uint32_t len = 0; len < array_ptr->length(); len++) {
      if (array_ptr->IsValid(len)) {
        num++;
      }
    }

    katana::gPrint("\n num: {}", num);
    new_chunks.emplace_back(std::move(arrow::MakeArray(data)));
    offset += array_ptr->length();
  }

  auto res_new_chunked_array = arrow::ChunkedArray::Make(new_chunks);
  if (res_new_chunked_array.ok()) {
    return res_new_chunked_array.ValueOrDie();
  } else {
    return nullptr;
  }
}

inline katana::Result<std::shared_ptr<arrow::Table>>
AddBitMaskToTable(
    std::shared_ptr<arrow::Table> table, const uint8_t* bit_mask) {
  std::vector<std::shared_ptr<arrow::Field>> fields;
  std::vector<std::shared_ptr<arrow::ChunkedArray>> columns;

  for (const auto& field : table->fields()) {
    fields.emplace_back(field);
  }

  for (const auto& col : table->columns()) {
    auto new_col = ApplyBitMask(col, bit_mask);

    uint32_t num_chunks = new_col->num_chunks();

    katana::gPrint("\n num chunks: {}", num_chunks);

    uint32_t total_length{0};
    for (uint32_t chunk = 0; chunk < num_chunks; chunk++) {
      auto node_prop_array = new_col->chunk(chunk);
      uint32_t array_length = node_prop_array->length();

      total_length += array_length;
      uint32_t valid{0};
      for (uint32_t len = 0; len < array_length; len++) {
        if (node_prop_array->IsValid(len)) {
          valid++;
        }
      }

      katana::gPrint("\n valid: {}", valid);
    }

    katana::gPrint("\n total length {}", total_length);
    columns.emplace_back(std::move(new_col));
  }

  return arrow::Table::Make(arrow::schema(fields), columns);
}

template <typename PGView, typename NodeProps>
inline katana::Result<void>
ConstructNodeProperties(
    PropertyGraph* pg, const PGView& pg_view,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto res_table = katana::AllocateTable<NodeProps>(pg->num_nodes(), names);
  if (!res_table) {
    return res_table.error();
  }

  auto bit_mask = pg_view.node_bitmask();

  uint32_t valid{0};
  for (uint32_t i = 0; i < pg->num_nodes(); i++) {
    if (bit_mask[i]) {
      valid++;
    }
  }

  katana::gPrint("\n inside vaid: {}", valid);

  res_table = AddBitMaskToTable(res_table.value(), bit_mask);

  if (!res_table) {
    return res_table.error();
  }

  return pg->AddNodeProperties(res_table.value());
}

template <typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    PropertyGraph* pg,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto res_table = katana::AllocateTable<EdgeProps>(pg->num_edges(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddEdgeProperties(res_table.value());
}

template <typename PGView, typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    PropertyGraph* pg, const PGView& pg_view,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto res_table = katana::AllocateTable<EdgeProps>(pg->num_edges(), names);
  if (!res_table) {
    return res_table.error();
  }

  auto bit_mask = pg_view.edge_bitmask();
  res_table = AddBitMaskToTable(res_table.value(), bit_mask);

  if (!res_table) {
    return res_table.error();
  }

  return pg->AddEdgeProperties(res_table.value());
}

class KATANA_EXPORT TemporaryPropertyGuard {
  static thread_local int temporary_property_counter;

  std::optional<katana::PropertyGraph::MutablePropertyView> property_view_ =
      std::nullopt;
  std::string name_;

  std::string GetPropertyName() {
    // Use a thread local counter and the thread ID to get a unique name.
    // `this` is not unique because we support moves.
    return fmt::format(
        "__katana_temporary_property_{}_{}", std::this_thread::get_id(),
        temporary_property_counter++);
  }

  void Deinit() {
    if (!property_view_) {
      return;
    }

    if (auto r = property_view_->RemoveProperty(name_); !r) {
      if (r.error() != ErrorCode::PropertyNotFound) {
        // Log an error if something goes wrong other than the property not
        // existing.
        KATANA_LOG_WARN("Failed to remove temporary property: {}", r.error());
      }
    }
    Clear();
  }

  void Clear() { property_view_ = std::nullopt; }

public:
  TemporaryPropertyGuard() = default;

  // TODO(amp): Remove old constructors. They were left to avoid simultainious
  //  changes to enterprise.
  TemporaryPropertyGuard(PropertyGraph* pv, std::string name)
      : TemporaryPropertyGuard(pv->NodeMutablePropertyView(), std::move(name)) {
  }
  TemporaryPropertyGuard(PropertyGraph* pv)
      : TemporaryPropertyGuard(pv->NodeMutablePropertyView()) {}

  TemporaryPropertyGuard(
      PropertyGraph::MutablePropertyView pv, std::string name)
      : property_view_(pv), name_(std::move(name)) {}

  explicit TemporaryPropertyGuard(PropertyGraph::MutablePropertyView pv)
      : TemporaryPropertyGuard(pv, GetPropertyName()) {}

  const TemporaryPropertyGuard& operator=(const TemporaryPropertyGuard&) =
      delete;
  TemporaryPropertyGuard(const TemporaryPropertyGuard&) = delete;

  TemporaryPropertyGuard(TemporaryPropertyGuard&& rhs) noexcept
      : property_view_(rhs.property_view_), name_(std::move(rhs.name_)) {
    rhs.Clear();
  }

  TemporaryPropertyGuard& operator=(TemporaryPropertyGuard&& rhs) noexcept {
    Deinit();
    property_view_ = rhs.property_view_;
    name_ = std::move(rhs.name_);
    rhs.Clear();
    return *this;
  }

  std::string name() const { return name_; }

  ~TemporaryPropertyGuard() { Deinit(); }
};

}  // namespace katana::analytics

#endif
