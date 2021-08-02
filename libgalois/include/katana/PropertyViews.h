#ifndef KATANA_LIBGALOIS_KATANA_PROPERTYVIEWS_H_
#define KATANA_LIBGALOIS_KATANA_PROPERTYVIEWS_H_

#include "katana/Properties.h"
#include "katana/PropertyGraph.h"

namespace katana::internal {

/// ExtractArrays returns the array for each column of a table. It returns an
/// error if there is more than one array for any column.
KATANA_EXPORT Result<std::vector<arrow::Array*>> ExtractArrays(
    const arrow::Table* table, const std::vector<std::string>& properties);
KATANA_EXPORT Result<std::vector<arrow::Array*>> ExtractArrays(
    const PropertyGraph::ReadOnlyPropertyView& pview,
    const std::vector<std::string>& properties);

template <typename PropTuple>
Result<katana::PropertyViewTuple<PropTuple>>
PropertyViewsFromArrays(std::vector<arrow::Array*> arrays) {
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
Result<katana::PropertyViewTuple<PropTuple>>
MakePropertyViews(
    const arrow::Table* table, const std::vector<std::string>& properties) {
  auto arrays = KATANA_CHECKED(ExtractArrays(table, properties));
  return PropertyViewsFromArrays<PropTuple>(arrays);
}

template <typename PropTuple>
Result<katana::PropertyViewTuple<PropTuple>>
MakePropertyViews(
    const PropertyGraph::ReadOnlyPropertyView& pview,
    const std::vector<std::string>& properties) {
  auto arrays = KATANA_CHECKED(ExtractArrays(pview, properties));
  return PropertyViewsFromArrays<PropTuple>(arrays);
}

/// MakeNodePropertyViews asserts a typed view on top of runtime properties.
///
/// It returns an error if there are fewer properties than elements of the
/// view or if the underlying arrow::ChunkedArray has more than one
/// arrow::Array.
template <typename PropTuple>
static Result<katana::PropertyViewTuple<PropTuple>>
MakeNodePropertyViews(
    const PropertyGraph* pg, const std::vector<std::string>& properties) {
  return MakePropertyViews<PropTuple>(
      pg->NodeReadOnlyPropertyView(), properties);
}

/// MakeNodePropertyViews asserts a typed view on top of runtime properties.
///
/// \see MakeNodePropertyViews
template <typename PropTuple>
static Result<katana::PropertyViewTuple<PropTuple>>
MakeNodePropertyViews(const PropertyGraph* pg) {
  return MakeNodePropertyViews<PropTuple>(
      pg, pg->loaded_node_schema()->field_names());
}

/// MakeEdgePropertyViews asserts a typed view on top of runtime properties.
/// This version selects a specific set of properties to include in the typed
/// view.
///
/// \see MakeNodePropertyViews
template <typename PropTuple>
static Result<katana::PropertyViewTuple<PropTuple>>
MakeEdgePropertyViews(
    const PropertyGraph* pg, const std::vector<std::string>& properties) {
  return MakePropertyViews<PropTuple>(
      pg->EdgeReadOnlyPropertyView(), properties);
}

/// MakeEdgePropertyViews asserts a typed view on top of runtime properties.
///
/// \see MakeNodePropertyViews
template <typename PropTuple>
static Result<katana::PropertyViewTuple<PropTuple>>
MakeEdgePropertyViews(const PropertyGraph* pg) {
  return MakeEdgePropertyViews<PropTuple>(
      pg, pg->loaded_edge_schema()->field_names());
}

}  // namespace katana::internal

#endif
