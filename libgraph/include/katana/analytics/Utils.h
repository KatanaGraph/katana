#ifndef KATANA_LIBGRAPH_KATANA_ANALYTICS_UTILS_H_
#define KATANA_LIBGRAPH_KATANA_ANALYTICS_UTILS_H_

#include <algorithm>
#include <random>
#include <utility>

#include <arrow/type.h>

#include "arrow/util/bitmap.h"
#include "katana/ErrorCode.h"
#include "katana/Properties.h"
#include "katana/Result.h"
#include "katana/TransformationView.h"

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
    PropertyGraph* pg, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto res_table = katana::AllocateTable<NodeProps>(pg->NumNodes(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddNodeProperties(res_table.value(), txn_ctx);
}

template <typename NodeProps>
inline katana::Result<void>
ConstructNodeProperties(
    TransformationView* pg, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto bit_mask = pg->NodeBitmask();
  auto res_table =
      katana::AllocateTable<NodeProps>(pg->NumOriginalNodes(), names, bit_mask);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddNodeProperties(res_table.value(), txn_ctx);
}

/// TODO(udit) here pg_view which is a const object
/// is modified to add properties
template <typename PGView, typename NodeProps>
inline katana::Result<void>
ConstructNodeProperties(
    const PGView& pg_view, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<NodeProps>()) {
  auto pg = const_cast<PropertyGraph*>(pg_view.property_graph());
  auto bit_mask = pg_view.node_bitmask();
  auto res_table =
      katana::AllocateTable<NodeProps>(pg->NumNodes(), names, bit_mask);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddNodeProperties(res_table.value(), txn_ctx);
}

template <typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    PropertyGraph* pg, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto res_table = katana::AllocateTable<EdgeProps>(pg->NumEdges(), names);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddEdgeProperties(res_table.value(), txn_ctx);
}

template <typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    TransformationView* pg, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto bit_mask = pg->EdgeBitmask();
  auto res_table =
      katana::AllocateTable<EdgeProps>(pg->NumOriginalEdges(), names, bit_mask);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddEdgeProperties(res_table.value(), txn_ctx);
}

template <typename PGView, typename EdgeProps>
inline katana::Result<void>
ConstructEdgeProperties(
    const PGView& pg_view, katana::TxnContext* txn_ctx,
    const std::vector<std::string>& names = DefaultPropertyNames<EdgeProps>()) {
  auto pg = const_cast<PropertyGraph*>(pg_view.property_graph());
  auto bit_mask = pg_view.edge_bitmask();
  auto res_table =
      katana::AllocateTable<EdgeProps>(pg->NumEdges(), names, bit_mask);
  if (!res_table) {
    return res_table.error();
  }

  return pg->AddEdgeProperties(res_table.value(), txn_ctx);
}

class KATANA_EXPORT TemporaryPropertyGuard {
  static thread_local int temporary_property_counter;

  std::optional<katana::PropertyGraph::MutablePropertyView> property_view_ =
      std::nullopt;
  std::string name_;
  std::unique_ptr<katana::TxnContext> txn_ctx_;  // Temporary TxnContext

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

    // Since the property is a temporary, thread-local one, we don't need
    // to pass the TxnContext to the caller. Hence, use a local TxnContext.
    if (auto r = property_view_->RemoveProperty(name_, txn_ctx_.get()); !r) {
      if (r.error() != ErrorCode::PropertyNotFound) {
        // Log an error if something goes wrong other than the property not
        // existing.
        KATANA_LOG_WARN("Failed to remove temporary property: {}", r.error());
      }
    }
    Clear();
  }

  void Clear() {
    property_view_ = std::nullopt;
    txn_ctx_.reset();
  }

public:
  TemporaryPropertyGuard() = default;

  TemporaryPropertyGuard(PropertyGraph* pv)
      : TemporaryPropertyGuard(pv->NodeMutablePropertyView()) {}

  explicit TemporaryPropertyGuard(PropertyGraph::MutablePropertyView pv)
      : property_view_(pv) {
    name_ = GetPropertyName();
    txn_ctx_ = std::make_unique<katana::TxnContext>();
  }

  const TemporaryPropertyGuard& operator=(const TemporaryPropertyGuard&) =
      delete;
  TemporaryPropertyGuard(const TemporaryPropertyGuard&) = delete;

  TemporaryPropertyGuard(TemporaryPropertyGuard&& rhs) noexcept
      : property_view_(rhs.property_view_), name_(std::move(rhs.name_)) {
    txn_ctx_ = std::make_unique<katana::TxnContext>();
    rhs.Clear();
  }

  TemporaryPropertyGuard& operator=(TemporaryPropertyGuard&& rhs) noexcept {
    Deinit();
    property_view_ = rhs.property_view_;
    name_ = std::move(rhs.name_);
    txn_ctx_ = std::make_unique<katana::TxnContext>();
    rhs.Clear();
    return *this;
  }

  std::string name() const { return name_; }

  ~TemporaryPropertyGuard() { Deinit(); }
};

}  // namespace katana::analytics

#endif
