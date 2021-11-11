#ifndef KATANA_LIBTSUBA_TSUBA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_TSUBA_TXNCONTEXT_H_

#include <string>

namespace tsuba {

class KATANA_EXPORT TxnContext {
public:
  TxnContext(){};
  ~TxnContext() = default;

  void InsertNodePropertyRead(std::string name) {
    node_properties_read.push_back(name);
  }

  void InsertNodePropertyRead(std::vector<std::string> names) {
    node_properties_read.insert(
        node_properties_read.end(), names.begin(), names.end());
  }

  void InsertNodePropertyWrite(std::string name) {
    node_properties_write.push_back(name);
  }

  void InsertNodePropertyWrite(std::vector<std::string> names) {
    node_properties_write.insert(
        node_properties_write.end(), names.begin(), names.end());
  }

  void InsertEdgePropertyRead(std::string name) {
    edge_properties_read.push_back(name);
  }

  void InsertEdgePropertyRead(std::vector<std::string> names) {
    edge_properties_read.insert(
        edge_properties_read.end(), names.begin(), names.end());
  }

  void InsertEdgePropertyWrite(std::string name) {
    edge_properties_write.push_back(name);
  }

  void InsertEdgePropertyWrite(std::vector<std::string> names) {
    edge_properties_write.insert(
        edge_properties_write.end(), names.begin(), names.end());
  }

  void SetAllPropertiesRead() { all_properties_read = true; }

  void SetAllPropertiesWrite() { all_properties_write = true; }

  void SetTopologyRead() { topology_read = true; }

  void SetTopologyWrite() { topology_write = true; }

private:
  std::vector<std::string> node_properties_read{};
  std::vector<std::string> node_properties_write{};
  std::vector<std::string> edge_properties_read{};
  std::vector<std::string> edge_properties_write{};
  bool all_properties_read{false};
  bool all_properties_write{false};
  bool topology_read{false};
  bool topology_write{false};
};

}  // namespace tsuba

#endif
