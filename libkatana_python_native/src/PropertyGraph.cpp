#define NPY_NO_DEPRECATED_API NPY_1_11_API_VERSION

#include <numpy/ndarrayobject.h>

// Must come after numpy/ndarrayobject.h since that is required, but
// not included by arrow/python headers.
#include <arrow/python/numpy_convert.h>
#include <arrow/python/numpy_to_arrow.h>
#include <arrow/python/python_to_arrow.h>
#include <pybind11/numpy.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include "katana/python/Conventions.h"
#include "katana/python/CythonIntegration.h"
#include "katana/python/EntityTypeManagerPython.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/FunctionUtils.h"
#include "katana/python/NumbaSupport.h"
#include "katana/python/PropertyGraphPython.h"
#include "katana/python/PythonModuleInitializers.h"
#include "katana/python/TemplateSupport.h"
#include "katana/python/TypeBans.h"

namespace py = pybind11;

/// Utility to convert Python argument to an Arrow Table for insertion into a
/// graph as properties.
katana::Result<std::shared_ptr<arrow::Table>>
katana::python::PythonArgumentsToTable(
    const pybind11::object& table, const pybind11::dict& kwargs) {
  std::shared_ptr<arrow::Table> arrow_table;

  if (arrow::py::is_table(table.ptr())) {
    arrow_table = KATANA_CHECKED(arrow::py::unwrap_table(table.ptr()));
  } else if (!table.is_none()) {
    arrow_table = KATANA_CHECKED(arrow::py::unwrap_table(
        py::module::import("pyarrow").attr("table")(table).ptr()));
  }

  for (const auto& [k, v] : kwargs) {
    std::shared_ptr<arrow::ChunkedArray> array;
    if (py::isinstance<py::array>(v)) {
      // Convert a numpy array.
      py::array pyarray = py::cast<py::array>(v);
      std::shared_ptr<arrow::DataType> dtype;
      KATANA_CHECKED(
          arrow::py::NumPyDtypeToArrow(pyarray.dtype().ptr(), &dtype));
      KATANA_CHECKED(arrow::py::NdarrayToArrow(
          arrow::default_memory_pool(), v.ptr(), nullptr, true, dtype,
          arrow::compute::CastOptions::Safe(dtype), &array));
      KATANA_LOG_DEBUG_ASSERT(array);
    } else {
      // Convert any other python sequence.
      arrow::py::PyConversionOptions options;
      options.from_pandas = true;
      array = KATANA_CHECKED(
          arrow::py::ConvertPySequence(v.ptr(), nullptr, options));
    }
    auto field = arrow::field(py::cast<py::str>(k), array->type());
    if (arrow_table) {
      arrow_table = KATANA_CHECKED(
          arrow_table->AddColumn(arrow_table->num_columns(), field, array));
    } else {
      arrow_table = arrow::Table::Make(arrow::schema({field}), {array});
    }
  }

  if (!arrow_table) {
    return KATANA_ERROR(
        katana::ErrorCode::InvalidArgument,
        "A table argument (a dict or arrow table) or a keyword argument must "
        "be provided.");
  }

  return arrow_table;
}

/// Base class analogous to the Python katana.dataframe.LazyDataAccessor.
class LazyDataAccessor {
public:
  virtual ~LazyDataAccessor();
  virtual py::object at(ssize_t i) const = 0;
  virtual py::object array(const py::slice& slice) const = 0;
};

LazyDataAccessor::~LazyDataAccessor() = default;

template <typename T>
class LazyDataAccessorTyped : public LazyDataAccessor {
public:
  virtual T at_typed(ssize_t i) const = 0;
  py::object at(ssize_t i) const final { return py::cast(at_typed(i)); }
  py::object array(const py::slice& slice) const override {
    auto begin = py::cast<ssize_t>(slice.attr("start"));
    auto end = py::cast<ssize_t>(slice.attr("stop"));
    auto step = py::cast<ssize_t>(slice.attr("step"));
    py::array_t<T> out{(end - begin) / step};
    for (ssize_t j = 0, i = begin; i < end; j += 1, i += step) {
      out.mutable_at(j) = at_typed(i);
    }
    return std::move(out);
  }
};

// LazyDataAccessors for edge destination and source

class GraphBaseEdgeDestAccessor final
    : public LazyDataAccessorTyped<katana::PropertyGraph::Node> {
  const katana::PropertyGraph* pg_;

public:
  virtual ~GraphBaseEdgeDestAccessor();

  GraphBaseEdgeDestAccessor(const katana::PropertyGraph* pg) : pg_(pg) {}

  katana::PropertyGraph::Node at_typed(ssize_t i) const final {
    return *pg_->OutEdgeDst(i);
  }
};
GraphBaseEdgeDestAccessor::~GraphBaseEdgeDestAccessor() = default;

class GraphBaseEdgeSourceAccessor final
    : public LazyDataAccessorTyped<katana::PropertyGraph::Node> {
  const katana::PropertyGraphViews::BiDirectional view_;

public:
  virtual ~GraphBaseEdgeSourceAccessor();

  GraphBaseEdgeSourceAccessor(katana::PropertyGraph* pg)
      : view_(pg->BuildView<katana::PropertyGraphViews::BiDirectional>()) {}

  katana::PropertyGraph::Node at_typed(ssize_t i) const final {
    return view_.GetEdgeSrc(i);
  }
};
GraphBaseEdgeSourceAccessor::~GraphBaseEdgeSourceAccessor() = default;

namespace {
// Custom Python methods on PropertyGraph that are used from Numba so cannot be
// lambdas until C++20

katana::GraphTopologyTypes::Node
out_edge_dst(katana::PropertyGraph* pg, katana::GraphTopologyTypes::Edge e) {
  return *pg->OutEdgeDst(e);
}

auto
PropertyGraphTopologyOutEdges(katana::PropertyGraph* pg) {
  return pg->topology().OutEdges();
}

auto
PropertyGraphTopologyOutEdgesForNode(
    katana::PropertyGraph* pg, katana::GraphTopologyTypes::Node n) {
  return pg->topology().OutEdges(n);
}

class PropertyGraphNumbaReplacement {
  using Edge = katana::GraphTopologyTypes::Edge;
  using Node = katana::GraphTopologyTypes::Node;

  const std::shared_ptr<katana::PropertyGraph> graph_;
  std::optional<katana::PropertyGraphViews::Transposed> transposed_;
  std::optional<katana::PropertyGraphViews::Undirected> undirected_;
  std::optional<katana::PropertyGraphViews::BiDirectional> bi_directional_;
  std::optional<katana::PropertyGraphViews::EdgeTypeAwareBiDir>
      type_aware_bi_dir_;

  // Wrappers exist to provide asserts. They are private.

  auto& graph() { return *graph_.get(); }

  auto& transposed() {
    KATANA_LOG_ASSERT(transposed_.has_value());
    return transposed_.value();
  }

  auto& undirected() {
    KATANA_LOG_ASSERT(undirected_.has_value());
    return undirected_.value();
  }

  auto& bi_directional() {
    KATANA_LOG_ASSERT(bi_directional_.has_value());
    return bi_directional_.value();
  }

  auto& type_aware_bi_dir() {
    KATANA_LOG_ASSERT(type_aware_bi_dir_.has_value());
    return type_aware_bi_dir_.value();
  }

public:
  explicit PropertyGraphNumbaReplacement(
      std::shared_ptr<katana::PropertyGraph> graph)
      : graph_(std::move(graph)) {}

  void WithTransposed() {
    // Put these together since bi_directional is just a combination of the
    // default (free) topology and transposed.
    transposed_ = graph_->BuildView<katana::PropertyGraphViews::Transposed>();
    bi_directional_ =
        graph_->BuildView<katana::PropertyGraphViews::BiDirectional>();
  }

  void WithUndirected() {
    undirected_ = graph_->BuildView<katana::PropertyGraphViews::Undirected>();
  }

  void WithEdgeTypeAwareBiDirectional() {
    type_aware_bi_dir_ =
        graph_->BuildView<katana::PropertyGraphViews::EdgeTypeAwareBiDir>();
  }

  auto NumNodes() { return graph().NumNodes(); }
  auto NumEdges() { return graph().NumEdges(); }

  auto OutEdgeDst(Edge e) { return *graph().OutEdgeDst(e); }
  auto GetEdgeSrc(Edge e) { return bi_directional().GetEdgeSrc(e); }

  auto OutEdges() { return graph().OutEdges(); }
  auto OutEdgesForNode(Node n) { return graph().OutEdges(n); }
  auto OutEdgesForNodeAndType(Node n, katana::EntityTypeID t) {
    return type_aware_bi_dir().OutEdges(n, t);
  }
  auto OutDegree(Node n) { return graph().topology().OutDegree(n); }
  auto OutDegreeForType(Node n, katana::EntityTypeID t) {
    return type_aware_bi_dir().OutDegree(n, t);
  }

  auto InEdges() { return transposed().OutEdges(); }
  auto InEdgesForNode(Node n) { return transposed().OutEdges(n); }
  auto InEdgesForNodeAndType(Node n, katana::EntityTypeID t) {
    return type_aware_bi_dir().InEdges(n, t);
  }
  auto InDegree(Node n) { return transposed().OutDegree(n); }
  auto InDegreeForType(Node n, katana::EntityTypeID t) {
    return type_aware_bi_dir().InDegree(n, t);
  }
  auto InEdgeSrc(Edge e) { return transposed().OutEdgeDst(e); }

  auto UndirectedEdges() { return undirected().OutEdges(); }
  auto UndirectedEdgesForNode(Node n) {
    return undirected().UndirectedEdges(n);
  }
  auto UndirectedDegree(Node n) { return undirected().UndirectedDegree(n); }
  auto UndirectedEdgeNeighbor(Edge e) {
    return undirected().UndirectedEdgeNeighbor(e);
  }
};

template <auto iterator_func, typename Cls, typename... Args>
class DefCompactIteratorWithNumbaImpl {
  static auto GetRange(Cls* self, Args... args) {
    return std::invoke(iterator_func, self, args...);
  }

  static auto Begin(Cls* self, Args... args) {
    return *GetRange(self, args...).begin();
  }
  static auto End(Cls* self, Args... args) {
    return *GetRange(self, args...).end();
  }

public:
  template <typename... ClsExtra>
  static void Def(
      py::class_<Cls, ClsExtra...>& cls, const char* name,
      const std::string& scab) {
    std::string begin_name = "_" + std::string(name) + "_" + scab + "_begin";
    std::string end_name = "_" + std::string(name) + "_" + scab + "_end";
    katana::DefWithNumba<&Begin>(cls, begin_name.c_str(), katana::numba_only());
    katana::DefWithNumba<&End>(cls, end_name.c_str(), katana::numba_only());
    auto numba_support =
        pybind11::module::import("katana.native_interfacing.numba_support");
    numba_support.attr("register_compact_range_method")(
        cls, name, begin_name, end_name,
        katana::PythonTypeTraits<
            katana::detail::remove_cvref_t<Args>>::ctypes_type()...);
  }
};

template <
    auto iterator_func, typename Cls, typename Return, typename... Args,
    typename... ClsExtra>
void
DefCompactIteratorWithNumbaInferer(
    Return (Cls::*)(Args...), py::class_<Cls, ClsExtra...>& cls,
    const char* name, const std::string& scab) {
  DefCompactIteratorWithNumbaImpl<iterator_func, Cls, Args...>::Def(
      cls, name, scab);
}

template <
    auto iterator_func, typename Cls, typename Return, typename... Args,
    typename... ClsExtra>
void
DefCompactIteratorWithNumbaInferer(
    Return (Cls::*)(Args...) const, py::class_<Cls, ClsExtra...>& cls,
    const char* name, const std::string& scab) {
  DefCompactIteratorWithNumbaImpl<iterator_func, const Cls, Args...>::Def(
      cls, name, scab);
}

template <
    auto iterator_func, typename Cls, typename Return, typename... Args,
    typename... ClsExtra>
void
DefCompactIteratorWithNumbaInferer(
    Return (*)(Cls*, Args...), py::class_<Cls, ClsExtra...>& cls,
    const char* name, const std::string& scab) {
  DefCompactIteratorWithNumbaImpl<iterator_func, Cls, Args...>::Def(
      cls, name, scab);
}

template <
    auto iterator_func, typename Cls, typename Return, typename... Args,
    typename... ClsExtra>
void
DefCompactIteratorWithNumbaInferer(
    Return (*)(const Cls*, Args...), py::class_<Cls, ClsExtra...>& cls,
    const char* name, const std::string& scab) {
  DefCompactIteratorWithNumbaImpl<iterator_func, const Cls, Args...>::Def(
      cls, name, scab);
}

template <auto iterator_func, typename Cls, typename... ClsExtra>
void
DefCompactIteratorWithNumba(
    py::class_<Cls, ClsExtra...>& cls, const char* name,
    const std::string& scab) {
  DefCompactIteratorWithNumbaInferer<iterator_func>(
      iterator_func, cls, name, scab);
}

// Functions which define specific types or groups of types. These are all
// called from InitPropertyGraph.

void
DefPropertyGraph(py::module& m) {
  using namespace katana;
  using namespace katana::python;

  py::class_<PropertyGraph, std::shared_ptr<PropertyGraph>> cls(
      m, "Graph",
      R"""(
      A distributed Katana graph.
      )""");
  py::class_<PropertyGraphNumbaReplacement> cls_numba_replacement(
      m, "PropertyGraphNumbaReplacement");

  katana::DefCythonSupport(cls);
  katana::DefConventions(cls);
  katana::RegisterNumbaClass(cls);

  katana::RegisterNumbaClass(cls_numba_replacement);

  cls.def(
      py::init(
          [](py::object path,
             std::optional<std::vector<std::string>> node_properties,
             std::optional<std::vector<std::string>> edge_properties,
             TxnContext* txn_ctx) -> std::shared_ptr<PropertyGraph> {
            auto path_str = py::str(path).cast<std::string>();
            py::gil_scoped_release guard;
            katana::RDGLoadOptions options = katana::RDGLoadOptions::Defaults();
            options.node_properties = node_properties;
            options.edge_properties = edge_properties;
            TxnContextArgumentHandler txn_context_handler(txn_ctx);
            KATANA_LOG_DEBUG("{}", reinterpret_cast<uintptr_t>(path.ptr()));
            return PythonChecked(PropertyGraph::Make(
                path_str, txn_context_handler.get(), options));
          }),
      py::arg("path"), py::kw_only(), py::arg("node_properties") = std::nullopt,
      py::arg("edge_properties") = std::nullopt,
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value,
      R"""(
      Load a property graph.

      :param path: the path or URL from which to load the graph.
      :type path: Union[str, Path]
      :param node_properties: A list of node property names to load into
          memory. If this is None (default), then all properties are loaded.
      :param edge_properties: A list of edge property names to load into
          memory. If this is None (default), then all properties are loaded.
      )""");

  // Below are the view API methods that we have in mind (not all of them may end up being exposed to Python). We differentiate between 3 classes of identifiers for nodes and edges:
  // Local ID: This is an ID produced by a specific partitioning of a distributed graph.
  // Topology handle: It is a handle used to relate to an entity in a topology data structure. It is a handle that various topology-related methods return.
  // Property index: This is used to access that entity’s properties.
  // We allow conversions between them in one direction: topology handle -> local ID -> property index.

  katana::DefWithNumba<&PropertyGraph::NumNodes>(cls, "num_nodes");
  katana::DefWithNumba<&PropertyGraph::NumEdges>(cls, "num_edges");
  katana::DefWithNumba<&PropertyGraphNumbaReplacement::NumNodes>(
      cls_numba_replacement, "num_nodes", numba_only());
  katana::DefWithNumba<&PropertyGraphNumbaReplacement::NumEdges>(
      cls_numba_replacement, "num_edges", numba_only());

  cls.def(
      "project",
      [](PropertyGraph& self, py::object node_types,
         py::object edge_types) -> std::shared_ptr<PropertyGraph> {
        std::optional<katana::SetOfEntityTypeIDs> node_type_ids;
        if (!node_types.is_none()) {
          node_type_ids = katana::SetOfEntityTypeIDs();
          node_type_ids->resize(self.GetNodeTypeManager().GetNumEntityTypes());
          for (auto& t : node_types) {
            node_type_ids->set(py::cast<EntityType>(t).type_id);
          }
        }
        std::optional<katana::SetOfEntityTypeIDs> edge_type_ids;
        if (!edge_types.is_none()) {
          edge_type_ids = katana::SetOfEntityTypeIDs();
          edge_type_ids->resize(self.GetEdgeTypeManager().GetNumEntityTypes());
          for (auto& t : edge_types) {
            edge_type_ids->set(py::cast<EntityType>(t).type_id);
          }
        }

        py::gil_scoped_release
            guard;  // graph projection may copy or load data.
        // is_none is safe without the GIL because it is just a pointer compare.
        return PythonChecked(PropertyGraph::MakeProjectedGraph(
            self, node_type_ids, edge_type_ids));
      },
      py::arg("node_types") = py::none(), py::arg("edge_types") = py::none(),
      py::return_value_policy::reference_internal,
      R"""(
      Get a projected view of the graph which only contains nodes or edges of
      specific types.

      Args:
        node_types (Optional[Iterable[EntityType]]): A set of node types to include in the projected graph,
            or ``None`` to keep all nodes.
        edge_types (Optional[Iterable[EntityType]]): A set of edge types to include in the projected graph,
            or ``None`` to keep all edges on the selected nodes.
      )""");

  // GetLocalNodeID(NodeHandle) -> LocalNodeID  - local node ID
  cls.def(
      "get_local_node_id",
      [](const PropertyGraph& self, GraphTopologyTypes::Node n) {
        return self.topology().GetLocalNodeID(n);
      });

  // GetNodePropertyIndex(LocalNodeID) -> NodePropertyIndex  - index into the property table for a node
  cls.def(
      "get_node_property_index",
      py::overload_cast<const GraphTopologyTypes::Node&>(
          &PropertyGraph::GetNodePropertyIndex, py::const_));

  // GetLocalEdgeID(OutEdgeHandle) -> LocalEdgeID  - local edge ID
  cls.def(
      "get_local_edge_id_from_out_edge",
      [](const PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.topology().GetLocalEdgeIDFromOutEdge(e);
      });

  // GetLocalEdgeID(InEdgeHandle) -> LocalEdgeID  - local edge ID
  cls.def(
      "get_local_edge_id_from_in_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::BiDirectional>()
            .GetLocalEdgeIDFromInEdge(e);
      },
      py::call_guard<py::gil_scoped_release>());

  // GetLocalEdgeID(UndirectedEdgeHandle) -> LocalEdgeID  - local edge ID
  cls.def(
      "get_local_edge_id_from_undirected_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::Undirected>()
            .GetLocalEdgeIDFromUndirectedEdge(e);
      },
      py::call_guard<py::gil_scoped_release>());

  // GetEdgePropertyIndex(OutEdgeHandle) -> EdgePropertyIndex  - index into the property table for an edge
  cls.def(
      "get_edge_property_index_from_out_edge",
      &GraphTopology::GetEdgePropertyIndexFromOutEdge);

  // GetEdgePropertyIndex(InEdgeHandle) -> EdgePropertyIndex  - index into the property table for an edge
  cls.def(
      "get_edge_property_index_from_in_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::BiDirectional>()
            .GetEdgePropertyIndexFromInEdge(e);
      },
      py::call_guard<py::gil_scoped_release>());

  // GetEdgePropertyIndex(UndirectedEdgeHandle) -> EdgePropertyIndex  - index into the property table for an edge
  cls.def(
      "get_edge_property_index_from_undirected_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::Undirected>()
            .GetEdgePropertyIndexFromUndirectedEdge(e);
      },
      py::call_guard<py::gil_scoped_release>());

  // GetEdgePropertyIndex(LocalEdgeID) -> EdgePropertyIndex  - index into the property table for an edge

  // Nodes() -> NodeHandle iterator  - iterator over all graph nodes
  cls.def("nodes", [](PropertyGraph& self) { return self.Nodes(); });

  {
    py::options options;
    options.disable_function_signatures();

    // OutEdges() -> OutEdgeHandle iterator  - iterator over all graph out-edges
    cls.def(
        "out_edge_ids",
        py::overload_cast<>(&PropertyGraph::OutEdges, py::const_),
        R"""(
        out_edge_ids(node: Optional[NodeID] = None, edge_type: Optional[EntityType] = None)

        Get out-edges from the graph; either all out-edges, or a subset based on
        destination node and edge type. |lazy_compute|

        Returns:
            Iterable[NodeID]: An iterable over in-edges in the graph.

        Args:
            node (Optional[NodeID]): A node ID whose in-edges should be returned.
                If this is not provided, all in-edges in the graph are returned.
            edge_type (Optional[EntityType]): The type of edges to return; other
                edges are ignored. If this is not provided, edges of all types are
                returned.

        .. note::

            |supports_compiled_operator| To call this method with ``edge_type``
            from compiled operators, call
            :py:func:`~Graph.with_edge_type_lookup` and pass the result to the
            compiled function. When using this method from compiled operators,
            you must call it as ``out_edge_ids_for_node`` if using ``node``
            only, and ``out_edge_ids_for_node_and_type`` if using ``node`` and
            ``edge_type``.
        )""");

    DefCompactIteratorWithNumba<&PropertyGraphTopologyOutEdges>(
        cls, "out_edge_ids", "for_node");
    DefCompactIteratorWithNumba<&PropertyGraphNumbaReplacement::OutEdges>(
        cls_numba_replacement, "out_edge_ids", "all");

    cls.def(
        "out_edge_ids",
        py::overload_cast<GraphTopologyTypes::Node>(
            &PropertyGraph::OutEdges, py::const_),
        py::call_guard<py::gil_scoped_release>());
    cls.attr("out_edge_ids_for_node") = cls.attr("out_edge_ids");
    DefCompactIteratorWithNumba<&PropertyGraphTopologyOutEdgesForNode>(
        cls, "out_edge_ids_for_node", "for_node");
    DefCompactIteratorWithNumba<
        &PropertyGraphNumbaReplacement::OutEdgesForNode>(
        cls_numba_replacement, "out_edge_ids_for_node", "for_node");

    cls.def(
        "out_edge_ids",
        [](PropertyGraph& self, GraphTopologyTypes::Node n,
           const EntityType& ty) {
          return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>()
              .OutEdges(n, ty.type_id);
        },
        py::call_guard<py::gil_scoped_release>());
    cls.attr("out_edge_ids_for_node_and_type") = cls.attr("out_edge_ids");
    DefCompactIteratorWithNumba<
        &PropertyGraphNumbaReplacement::OutEdgesForNodeAndType>(
        cls_numba_replacement, "out_edge_ids_for_node_and_type",
        "for_node_and_type");

    cls.def(
        "out_degree",
        [](PropertyGraph& self, GraphTopologyTypes::Node n) {
          return self.topology().OutDegree(n);
        },
        py::call_guard<py::gil_scoped_release>(),
        R"""(
        out_degree(node: NodeID, edge_type: Optional[EntityType] = None)

        Get out-degree of a node, possibly filtered by edge type. |lazy_compute|

        Returns:
            int: The degree of the node.

        Args:
            node (NodeID): A node ID whose in-degree should be returned.
            edge_type (Optional[EntityType]): The type of edges to return; other
              edges are ignored. If this is not provided, edges of all types are
              returned.

        .. note::

            |supports_compiled_operator| To call this method with ``edge_type``
            from compiled operators, call
            :py:func:`~Graph.with_edge_type_lookup` and pass the result to the
            compiled function. When using this method from compiled operators,
            you must call it as ``out_edge_ids_for_node`` if using ``node``
            only, and ``out_edge_ids_for_node_and_type`` if using ``node`` and
            ``edge_type``.
        )""");
    // TODO(amp): Decide if this kind of function composition is actually better
    //  than boilerplate declarations.
    DefWithNumba<&SubObjectCall<
        &PropertyGraph::topology, &GraphTopology::OutDegree>::Func>(
        cls, "out_degree", katana::numba_only());
    DefWithNumba<&PropertyGraphNumbaReplacement::OutDegree>(
        cls_numba_replacement, "out_degree");

    // OutDegree(NodeHandle, EdgeEntityTypeID)-> size_t  - number of out-edges of a type for a node
    cls.def(
        "out_degree",
        [](PropertyGraph& self, GraphTopologyTypes::Node n,
           const EntityType& ty) {
          return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>()
              .OutDegree(n, ty.type_id);
        },
        py::call_guard<py::gil_scoped_release>());
    cls.attr("out_degree_for_type") = cls.attr("out_degree");
    DefWithNumba<&PropertyGraphNumbaReplacement::OutDegreeForType>(
        cls_numba_replacement, "out_degree_for_type");
  }

  // OutEdgeDst(OutEdgeHandle)-> NodeHandle - destination of an out-edge
  katana::DefWithNumba<&out_edge_dst>(cls, "out_edge_dst");

  cls.def(
      "with_edge_type_lookup",
      [](std::shared_ptr<PropertyGraph> self) {
        auto r = std::make_unique<PropertyGraphNumbaReplacement>(self);
        r->WithEdgeTypeAwareBiDirectional();
        return r;
      },
      py::call_guard<py::gil_scoped_release>(),
      R"""(
      Returns: A view on this graph with edge type lookup available via
      `~Graph.in_edge_ids` and others.
      )""");

  cls_numba_replacement.def(
      "with_edge_type_lookup",
      [](py::object& self) {
        py::cast<PropertyGraphNumbaReplacement*>(self)
            ->WithEdgeTypeAwareBiDirectional();
        return self;
      },
      py::call_guard<py::gil_scoped_release>(),
      R"""(
      Returns: A view on this graph with edge type lookup available via
      `~Graph.in_edge_ids` and others.
      )""");

  cls.def(
      "with_in_edges",
      [](std::shared_ptr<PropertyGraph> self) {
        auto r = std::make_unique<PropertyGraphNumbaReplacement>(self);
        r->WithTransposed();
        return r;
      },
      py::call_guard<py::gil_scoped_release>(),
      R"""(
      Returns: A view on this graph with in-edge information available via
      `~Graph.in_edge_ids` and others. This view can be augemented with
      additional information using other ``with_`` method, but may not be
      )""");
  cls_numba_replacement.def(
      "with_in_edges",
      [](py::object& self) {
        py::cast<PropertyGraphNumbaReplacement*>(self)->WithTransposed();
        return self;
      },

      py::call_guard<py::gil_scoped_release>(),
      R"""(
      Returns: A view on this graph with in-edge information available via
      `~Graph.in_edge_ids` and others.
      )""");

  {
    py::options options;
    options.disable_function_signatures();

    cls.def(
        "in_edge_ids",
        [](PropertyGraph& self) {
          return self.BuildView<PropertyGraphViews::Transposed>().OutEdges();
        },
        py::call_guard<py::gil_scoped_release>(),
        R"""(
        in_edge_ids(node: Optional[NodeID] = None, edge_type: Optional[EntityType] = None)

        Get in-edges from the graph; either all in-edges, or a subset based on
        destination node and edge type. |lazy_compute|

        Returns:
            Iterable[NodeID]: An iterable over in-edges in the graph.

        Args:
            node (Optional[NodeID]): A node ID whose in-edges should be returned.
              If this is not provided, all in-edges in the graph are returned.
            edge_type (Optional[EntityType]): The type of edges to return; other
              edges are ignored. If this is not provided, edges of all types are
              returned.

        .. note::

            |supports_compiled_operator| To call this method from compiled
            operators call :py:func:`~Graph.with_in_edges` and
            :py:func:`~Graph.with_edge_type_lookup` if using the ``edge_type``
            argument. When using this method from compiled operators, you must
            call it as ``in_edge_ids_for_node`` if using ``node`` only, and
            ``in_edge_ids_for_node_and_type`` if using ``node`` and
            ``edge_type``.
        )""");

    DefCompactIteratorWithNumba<&PropertyGraphNumbaReplacement::InEdges>(
        cls_numba_replacement, "in_edge_ids", "all");

    // InEdges(NodeHandle)-> InEdgeHandle iterator - iterator over in-edges for a node
    cls.def(
        "in_edge_ids",
        [](PropertyGraph& self, GraphTopologyTypes::Node n) {
          return self.BuildView<PropertyGraphViews::Transposed>().OutEdges(n);
        },
        py::call_guard<py::gil_scoped_release>());
    cls.attr("in_edge_ids_for_node") = cls.attr("in_edge_ids");

    // TODO(amp, KAT-4362): Make all these scabbed methods into overloads.
    DefCompactIteratorWithNumba<&PropertyGraphNumbaReplacement::InEdgesForNode>(
        cls_numba_replacement, "in_edge_ids_for_node", "for_node");

    // InEdges(NodeHandle, EdgeEntityTypeID)-> InEdgeHandle iterator - iterator over in-edges of a type for a node
    cls.def(
        "in_edge_ids",
        [](PropertyGraph& self, GraphTopologyTypes::Node n,
           const EntityType& ty) {
          return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>()
              .InEdges(n, ty.type_id);
        },
        py::call_guard<py::gil_scoped_release>());
    cls.attr("in_edge_ids_for_node_and_type") = cls.attr("in_edge_ids");

    DefCompactIteratorWithNumba<
        &PropertyGraphNumbaReplacement::InEdgesForNodeAndType>(
        cls_numba_replacement, "in_edge_ids_for_node_and_type",
        "for_node_and_type");

    cls.def(
        "in_degree",
        [](PropertyGraph& self, GraphTopologyTypes::Node n) {
          return self.BuildView<PropertyGraphViews::Transposed>().OutDegree(n);
        },
        py::call_guard<py::gil_scoped_release>(),
        R"""(
        in_degree(node: NodeID, edge_type: Optional[EntityType] = None)

        Get in-degree of a node, possibly filtered by edge type. |lazy_compute|

        Returns:
            int: The degree of the node.

        Args:
            node (NodeID): A node ID whose in-degree should be returned.
            edge_type (Optional[EntityType]): The type of edges to return; other
              edges are ignored. If this is not provided, edges of all types are
              returned.

        .. note::

            |supports_compiled_operator| To call this method from compiled
            operators call :py:func:`~Graph.with_in_edges` and
            :py:func:`~Graph.with_edge_type_lookup` if using the ``edge_type``
            argument. When using this method from compiled operators, you must
            call it  ``in_degree_for_type`` if using ``edge_type``.
        )""");

    katana::DefWithNumba<&PropertyGraphNumbaReplacement::InDegree>(
        cls_numba_replacement, "in_degree");

    cls.def(
        "in_degree",
        [](PropertyGraph& self, GraphTopologyTypes::Node n,
           const EntityType& ty) {
          return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>()
              .InDegree(n, ty.type_id);
        },
        py::call_guard<py::gil_scoped_release>());
    cls.attr("in_degree_for_type") = cls.attr("in_degree");

    katana::DefWithNumba<&PropertyGraphNumbaReplacement::InDegreeForType>(
        cls_numba_replacement, "in_degree_for_type");
  }

  // InEdgeSrc(InEdgeHandle)-> NodeHandle - source of an in-edge
  cls.def(
      "in_edge_src",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::Transposed>().OutEdgeDst(e);
      },
      py::call_guard<py::gil_scoped_release>());

  katana::DefWithNumba<&PropertyGraphNumbaReplacement::InEdgeSrc>(
      cls_numba_replacement, "in_edge_src");

  // FindAllEdges(NodeHandle src_node, NodeHandle dst_node) -> LocalEdgeID iterator - iterator over out-edges between src and dst nodes
  cls.def(
      "find_all_edge_ids",
      [](PropertyGraph& self, GraphTopologyTypes::Node src,
         GraphTopologyTypes::Node dst) {
        return self.BuildView<PropertyGraphViews::EdgesSortedByDestID>()
            .FindAllEdges(src, dst);
      },
      py::call_guard<py::gil_scoped_release>());

  // FindAllEdges(NodeHandle src_node, NodeHandle dst_node, EdgeEntityTypeID)-> LocalEdgeID iterator - iterator over out-edges of a type between src and dst nodes
  cls.def(
      "find_all_edge_ids",
      [](PropertyGraph& self, GraphTopologyTypes::Node src,
         GraphTopologyTypes::Node dst, const EntityType& ty) {
        return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>()
            .FindAllEdges(src, dst, ty.type_id);
      },
      py::call_guard<py::gil_scoped_release>());

  // HasEdge(NodeHandle src, NodeHandle dst) -> bool  - are source and destination nodes connected by some edge
  cls.def(
      "has_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Node src,
         GraphTopologyTypes::Node dst) {
        return self.BuildView<PropertyGraphViews::EdgesSortedByDestID>()
            .HasEdge(src, dst);
      },
      py::call_guard<py::gil_scoped_release>());
  // HasEdge(NodeHandle src, NodeHandle dst, EdgeEntityTypeID)-> bool - are source and destination nodes connected by some edge of a given type
  cls.def(
      "has_edge",
      [](PropertyGraph& self, GraphTopologyTypes::Node src,
         GraphTopologyTypes::Node dst, const EntityType& ty) {
        return self.BuildView<PropertyGraphViews::EdgeTypeAwareBiDir>().HasEdge(
            src, dst, ty.type_id);
      },
      py::call_guard<py::gil_scoped_release>());

  // These methods are needed in addition to the above (for querying/mining that stores matched edges):

  // GetEdgeSrc(LocalEdgeID)-> NodeHandle - source of an edge
  cls.def(
      "get_edge_src",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return self.BuildView<PropertyGraphViews::BiDirectional>().GetEdgeSrc(
            e);
      },
      py::call_guard<py::gil_scoped_release>());
  katana::DefWithNumba<&PropertyGraphNumbaReplacement::GetEdgeSrc>(
      cls_numba_replacement, "get_edge_src");

  // GetEdgeDst(LocalEdgeID)-> NodeHandle - destination of an edge
  cls.def("get_edge_dst", [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
    return self.BuildView<PropertyGraphViews::BiDirectional>().OutEdgeDst(e);
  });
  katana::DefWithNumba<&PropertyGraphNumbaReplacement::OutEdgeDst>(
      cls_numba_replacement, "get_edge_dst");

  // In addition, all access views will support property and type queries:

  // GetNodeProperty(string) -> PropertyArray - property array for all nodes
  cls.def(
      "get_node_property",
      [](PropertyGraph& self,
         const std::string& name) -> katana::Result<py::object> {
        KATANA_CHECKED(self.EnsureNodePropertyLoaded(name));
        return py::reinterpret_steal<py::object>(arrow::py::wrap_chunked_array(
            KATANA_CHECKED(self.GetNodeProperty(name))));
      },
      py::call_guard<py::gil_scoped_release>());
  // GetEdgeProperty(string) -> PropertyArray - property array for all edges
  cls.def(
      "get_edge_property",
      [](PropertyGraph& self,
         const std::string& name) -> katana::Result<py::object> {
        KATANA_CHECKED(self.EnsureEdgePropertyLoaded(name));
        return py::reinterpret_steal<py::object>(arrow::py::wrap_chunked_array(
            KATANA_CHECKED(self.GetEdgeProperty(name))));
      },
      py::call_guard<py::gil_scoped_release>());

  cls.def(
      "unload_node_property", &PropertyGraph::UnloadNodeProperty,
      py::call_guard<py::gil_scoped_release>());
  cls.def(
      "unload_edge_property", &PropertyGraph::UnloadEdgeProperty,
      py::call_guard<py::gil_scoped_release>());

  cls.def("loaded_node_schema", [](PropertyGraph& self) {
    return py::reinterpret_steal<py::object>(
        arrow::py::wrap_schema(self.loaded_node_schema()));
  });
  cls.def("loaded_edge_schema", [](PropertyGraph& self) {
    return py::reinterpret_steal<py::object>(
        arrow::py::wrap_schema(self.loaded_edge_schema()));
  });

  // GetNodeEntityType(NodePropertyIndex)-> EntityTypeID - entity type for a node
  cls.def(
      "get_node_type",
      [](PropertyGraph& self, GraphTopologyTypes::Node n) {
        return EntityType::Make(
            &self.GetNodeTypeManager(), self.GetTypeOfNode(n));
      },
      py::return_value_policy::reference_internal);
  cls.def(
      "does_node_have_type", [](PropertyGraph& self, GraphTopologyTypes::Node n,
                                const EntityType& type) {
        return self.DoesNodeHaveType(n, type.type_id);
      });
  cls.def_property_readonly("node_types", &PropertyGraph::GetNodeTypeManager);

  // GetEdgeEntityType(EdgePropertyIndex)-> EntityTypeID - entity type for an edge
  cls.def(
      "get_edge_type",
      [](PropertyGraph& self, GraphTopologyTypes::Edge e) {
        return EntityType::Make(
            &self.GetEdgeTypeManager(), self.GetTypeOfEdgeFromTopoIndex(e));
      },
      py::return_value_policy::reference_internal);
  cls.def(
      "does_edge_have_type", [](PropertyGraph& self, GraphTopologyTypes::Edge e,
                                const EntityType& type) {
        return self.DoesEdgeHaveTypeFromTopoIndex(e, type.type_id);
      });
  cls.def_property_readonly("edge_types", &PropertyGraph::GetEdgeTypeManager);

  // Mutators

  cls.def(
      "add_node_property",
      [](PropertyGraph& self, const py::object& table, TxnContext* txn_ctx,
         const py::kwargs& kwargs) -> Result<void> {
        std::shared_ptr<arrow::Table> arrow_table =
            KATANA_CHECKED(PythonArgumentsToTable(table, kwargs));
        py::gil_scoped_release guard;
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.AddNodeProperties(arrow_table, txn_handler.get());
      },
      py::arg("table") = py::none(),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value);
  cls.def(
      "upsert_node_property",
      [](PropertyGraph& self, const py::object& table, TxnContext* txn_ctx,
         const py::kwargs& kwargs) -> Result<void> {
        std::shared_ptr<arrow::Table> arrow_table =
            KATANA_CHECKED(PythonArgumentsToTable(table, kwargs));
        py::gil_scoped_release guard;
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.UpsertNodeProperties(arrow_table, txn_handler.get());
      },
      py::arg("table") = py::none(),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value);
  cls.def(
      "remove_node_property",
      [](PropertyGraph& self, const std::string& name, TxnContext* txn_ctx) {
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.RemoveNodeProperty(name, txn_handler.get());
      },
      py::arg("name"),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value,
      py::call_guard<py::gil_scoped_release>());

  cls.def(
      "add_edge_property",
      [](PropertyGraph& self, const py::object& table, TxnContext* txn_ctx,
         const py::kwargs& kwargs) -> Result<void> {
        std::shared_ptr<arrow::Table> arrow_table =
            KATANA_CHECKED(PythonArgumentsToTable(table, kwargs));
        py::gil_scoped_release guard;
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.AddEdgeProperties(arrow_table, txn_handler.get());
      },
      py::arg("table") = py::none(),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value);
  cls.def(
      "upsert_edge_property",
      [](PropertyGraph& self, const py::object& table, TxnContext* txn_ctx,
         const py::kwargs& kwargs) -> Result<void> {
        std::shared_ptr<arrow::Table> arrow_table =
            KATANA_CHECKED(PythonArgumentsToTable(table, kwargs));
        py::gil_scoped_release guard;
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.UpsertEdgeProperties(arrow_table, txn_handler.get());
      },
      py::arg("table") = py::none(),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value);

  cls.def(
      "remove_edge_property",
      [](PropertyGraph& self, const std::string& name, TxnContext* txn_ctx) {
        TxnContextArgumentHandler txn_handler(txn_ctx);
        return self.RemoveEdgeProperty(name, txn_handler.get());
      },
      py::arg("name"),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value,
      py::call_guard<py::gil_scoped_release>());

  cls.def("has_node_index", &PropertyGraph::HasNodeIndex, py::arg("name"));
  cls.def(
      "get_node_index",
      [](PropertyGraph& self, const std::string& name)
          -> std::shared_ptr<katana::EntityIndex<katana::GraphTopology::Node>> {
        if (!self.HasNodeIndex(name)) {
          PythonChecked(self.MakeNodeIndex(name));
        }
        return PythonChecked(self.GetNodeIndex(name));
      },
      py::arg("name"), py::return_value_policy::reference_internal);
  cls.def("has_edge_index", &PropertyGraph::HasEdgeIndex, py::arg("name"));
  cls.def(
      "get_edge_index",
      [](PropertyGraph& self, const std::string& name)
          -> std::shared_ptr<katana::EntityIndex<katana::GraphTopology::Edge>> {
        if (!self.HasEdgeIndex(name)) {
          PythonChecked(self.MakeEdgeIndex(name));
        }
        return PythonChecked(self.GetEdgeIndex(name));
      },
      py::arg("name"), py::return_value_policy::reference_internal);

  cls.def("unload_topologies", &PropertyGraph::DropAllTopologies);

  cls.def(
      "write",
      [](PropertyGraph& self, const std::string* path,
         const std::string& provenance, TxnContext* txn_ctx) {
        TxnContextArgumentHandler txn_handler(txn_ctx);
        if (path) {
          return self.Write(*path, provenance, txn_handler.get());
        }
        return self.Commit(provenance, txn_handler.get());
      },
      py::arg("path") = py::none(),
      py::arg("provenance") = py::str("katana.local"),
      py::arg("txn_ctx") = TxnContextArgumentHandler::default_value,
      py::call_guard<py::gil_scoped_release>(),
      R"""(
      Write the property graph to the specified path or URL (or the original path it was loaded from if path is
      not provided). Provide lineage information in the form of a command line.

      :param path: The path to which to write or None to use `self.path`.
      :type path: str or Path
      :param command_line: Lineage information in the form of a command line.
      :type command_line: str
      )""");
  cls.def_property_readonly("path", &PropertyGraph::rdg_dir);
}

template <typename node_or_edge>
struct WrapPrimitiveEntityIndex {
  py::class_<
      katana::EntityIndex<node_or_edge>,
      std::shared_ptr<katana::EntityIndex<node_or_edge>>>
      base_cls;

  template <typename T>
  py::object instantiate(py::module& m, const char* name) {
    using Cls = katana::PrimitiveEntityIndex<node_or_edge, T>;
    py::class_<Cls, std::shared_ptr<Cls>> cls(m, name, base_cls);

    cls.template def(
        "__getitem__", [](Cls& self, const T& v) { return *self.Find(v); });
    cls.template def("find_all", [](Cls& self, const T& v) {
      return py::make_iterator(self.LowerBound(v), self.UpperBound(v));
    });

    //    katana::DefConventions(cls);

    return std::move(cls);
  }
};

template <typename node_or_edge>
struct WrapStringEntityIndex {
  py::class_<
      katana::EntityIndex<node_or_edge>,
      std::shared_ptr<katana::EntityIndex<node_or_edge>>>
      base_cls;

  py::object instantiate(py::module& m, const char* name) {
    using Cls = katana::StringEntityIndex<node_or_edge>;
    py::class_<Cls, std::shared_ptr<Cls>> cls(m, name, base_cls);

    cls.template def("__getitem__", [](Cls& self, const std::string& v) {
      return *self.Find(v);
    });
    cls.template def("find_all", [](Cls& self, const std::string& v) {
      return py::make_iterator(self.LowerBound(v), self.UpperBound(v));
    });

    //    katana::DefConventions(cls);

    return std::move(cls);
  }
};

template <typename node_or_edge>
void
DefEntityIndex(py::module& m) {
  using EntityIndex = katana::EntityIndex<node_or_edge>;
  constexpr bool is_node =
      std::is_same_v<node_or_edge, katana::GraphTopologyTypes::Node>;
  auto cls_name = std::string(is_node ? "Node" : "Edge") + "Index";
  py::class_<EntityIndex, std::shared_ptr<EntityIndex>> cls(
      m, cls_name.c_str());

  cls.template def("property_name", &EntityIndex::property_name);
  katana::DefContainer(cls);

  katana::InstantiateForTypes<bool, uint8_t, int64_t, uint64_t, double_t>(
      m, ("Primitive" + cls_name).c_str(),
      WrapPrimitiveEntityIndex<node_or_edge>{cls});
  WrapStringEntityIndex<node_or_edge>{cls}.instantiate(
      m, ("String" + cls_name).c_str());
}

void
DefTxnContext(py::module& m) {
  py::class_<katana::TxnContext> cls(m, "TxnContext");
  cls.def(py::init<>());
  cls.def("commit", &katana::TxnContext::Commit);
  katana::DefKatanaAddress(cls);
}

void
DefRanges(py::module& m) {
  py::class_<katana::GraphTopologyTypes::nodes_range> nodes_range_cls(
      m, "NodeRange");
  katana::DefRange(nodes_range_cls);
  py::class_<katana::GraphTopologyTypes::edges_range> edges_range_cls(
      m, "EdgeRange");
  katana::DefRange(edges_range_cls);
}

void
DefAccessors(py::module& m) {
  py::class_<GraphBaseEdgeDestAccessor>(m, "GraphBaseEdgeDestAccessor")
      .def(py::init<katana::PropertyGraph*>())
      .def("__getitem__", &GraphBaseEdgeDestAccessor::at)
      .def("array", &GraphBaseEdgeDestAccessor::array);
  py::class_<GraphBaseEdgeSourceAccessor>(m, "GraphBaseEdgeSourceAccessor")
      .def(py::init<katana::PropertyGraph*>())
      .def("__getitem__", &GraphBaseEdgeSourceAccessor::at)
      .def("array", &GraphBaseEdgeSourceAccessor::array);
}

}  // namespace

void
katana::python::InitPropertyGraph(py::module& m) {
  DefAccessors(m);
  DefEntityIndex<katana::GraphTopologyTypes::Node>(m);
  DefEntityIndex<katana::GraphTopologyTypes::Edge>(m);
  DefTxnContext(m);
  DefRanges(m);
  DefPropertyGraph(m);
}
