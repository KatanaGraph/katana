#include <pybind11/pybind11.h>

#include "katana/RDGPythonInterface.h"

#include "katana/python/CythonIntegration.h"
#include "katana/python/ErrorHandling.h"
#include "katana/python/PythonModuleInitializers.h"
#include "katana/tsuba.h"

namespace py = pybind11;

// NB: This interface is only needed for the out-of-core import code path
// This should NOT be used by anyone else for any kind of purpose since it
// exposes low level details that users do not need to be concerned with.
void
katana::python::InitRDGInterface(py::module& m) {
  // Define the wrapped interface for PropStorageInfo - needed for RDGPartHeader
  // Only need the initial constructor since properites will be in memory
  py::class_<katana::RDGPropInfo> rdg_prop_info_cls(
      m, "RDGPropInfo");
  rdg_prop_info_cls.def(py::init([](std::string name, std::string path) {
    return katana::RDGPropInfo{name, path};
  }));

//   // Define the wrapped interface for RDGPartHeader
//   py::class_<katana::RDGPartHeader> rdg_part_header_cls(m, "RDGPartHeader");

//   katana::DefConventions(rdg_part_header_cls);
//   rdg_part_header_cls
//       .def(py::init([](const std::string& path) {
//         auto uri_path = katana::Uri::Make(path).value();
//         return katana::RDGPartHeader::Make(uri_path).value();
//       }))
//       .def(
//           "set_node_prop_info_list",
//           &katana::RDGPartHeader::set_node_prop_info_list)
//       .def(
//           "set_edge_prop_info_list",
//           &katana::RDGPartHeader::set_edge_prop_info_list)
//       .def(
//           "store_node_entity_type_manager",
//           &katana::RDGPartHeader::StoreNodeEntityTypeManager)
//       .def(
//           "store_edge_entity_type_manager",
//           &katana::RDGPartHeader::StoreEdgeEntityTypeManager)
//       .def(
//           "make_partition_topology_metadata_entry",
//           [](const katana::RDGPartHeader* self, const std::string& topo_path) {
//             self->MakePartitionTopologyMetadataEntry(
//                 topo_path);  // We don't need to return the value from here
//           })
//       .def(
//           "write",
//           [](const katana::RDGPartHeader* self, const std::string& rdg_dir) -> Result<void> {
//             // We need to create an RDGHandle
//             // Need to call the WriteGroup?
//             // Pass in one more thing about versioning?
//             std::unique_ptr<WriteGroup> write_group =
//                 KATANA_CHECKED(WriteGroup::Make());
//             auto policy = katana::RDG::RDGVersioningPolicy::RetainVersion;
//             auto uri_dir = KATANA_CHECKED(katana::Uri::Make(rdg_dir));
//             auto manifest = katana::RDGManifest(
//                 1, 0, 1, 0, false, uri_dir, katana::RDGLineage());
//             katana::RDGHandle handle = KATANA_CHECKED(katana::Open(std::move(manifest), katana::kReadWrite));
//             self->Write(handle, write_group.get(), policy);
//           });
}
