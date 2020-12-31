#ifndef GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYVIEWS_H_
#define GALOIS_LIBGALOIS_GALOIS_GRAPHS_PROPERTYVIEWS_H_

#include <galois/Properties.h>
#include <galois/graphs/PropertyFileGraph.h>

namespace galois::graphs::internal {

/// ExtractArrays returns the array for each column of a table. It returns an
/// error if there is more than one array for any column.
Result<std::vector<arrow::Array*>> GALOIS_EXPORT ExtractArrays(
    const arrow::Table* table, const std::vector<std::string>& properties);

template <typename PropTuple>
Result<galois::PropertyViewTuple<PropTuple>>
MakePropertyViews(
    const arrow::Table* table, const std::vector<std::string>& properties) {
  auto arrays_result = ExtractArrays(table, properties);
  if (!arrays_result) {
    return arrays_result.error();
  }

  auto arrays = std::move(arrays_result.value());

  if (arrays.size() < std::tuple_size_v<PropTuple>) {
    return std::errc::invalid_argument;
  }

  auto views_result = ConstructPropertyViews<PropTuple>(arrays);
  if (!views_result) {
    return views_result.error();
  }
  return views_result.value();
}

/// MakeNodePropertyViews asserts a typed view on top of runtime properties.
/// This version selects a specific set of properties to include in the typed
/// view.
///
/// It returns an error if there are fewer properties than elements of the
/// view or if the underlying arrow::ChunkedArray has more than one
/// arrow::Array.
template <typename PropTuple>
static Result<galois::PropertyViewTuple<PropTuple>>
MakeNodePropertyViews(
    const PropertyFileGraph* pfg, const std::vector<std::string>& properties) {
  return MakePropertyViews<PropTuple>(pfg->node_table().get(), properties);
}

/// MakeNodePropertyViews asserts a typed view on top of runtime properties.
///
/// It returns an error if there are fewer properties than elements of the
/// view or if the underlying arrow::ChunkedArray has more than one
/// arrow::Array.
template <typename PropTuple>
static Result<galois::PropertyViewTuple<PropTuple>>
MakeNodePropertyViews(const PropertyFileGraph* pfg) {
  return MakeNodePropertyViews<PropTuple>(
      pfg, pfg->node_schema()->field_names());
}

/// MakeEdgePropertyViews asserts a typed view on top of runtime properties.
/// This version selects a specific set of properties to include in the typed
/// view.
///
/// \see MakeNodePropertyViews
template <typename PropTuple>
static Result<galois::PropertyViewTuple<PropTuple>>
MakeEdgePropertyViews(
    const PropertyFileGraph* pfg, const std::vector<std::string>& properties) {
  return MakePropertyViews<PropTuple>(pfg->edge_table().get(), properties);
}

/// MakeEdgePropertyViews asserts a typed view on top of runtime properties.
///
/// \see MakeNodePropertyViews
template <typename PropTuple>
static Result<galois::PropertyViewTuple<PropTuple>>
MakeEdgePropertyViews(const PropertyFileGraph* pfg) {
  return MakeEdgePropertyViews<PropTuple>(
      pfg, pfg->edge_schema()->field_names());
}

}  // namespace galois::graphs::internal

#endif
