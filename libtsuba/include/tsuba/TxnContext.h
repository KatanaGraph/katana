#ifndef KATANA_LIBTSUBA_TSUBA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_TSUBA_TXNCONTEXT_H_

namespace tsuba {

class KATANA_EXPORT TxnContext {
public:
  void InsertNodePropertyRead(std::string name) {
    node_properties_read_.insert(name);
  }

  void InsertNodePropertyRead(std::unordered_set<std::string> names) {
    node_properties_read_.insert(names.begin(), names.end());
  }

  void InsertNodePropertyRead(std::vector<std::string> names) {
    node_properties_read_.insert(names.begin(), names.end());
  }

  void InsertNodePropertyWrite(std::string name) {
    node_properties_write_.insert(name);
  }

  void InsertNodePropertyWrite(std::unordered_set<std::string> names) {
    node_properties_write_.insert(names.begin(), names.end());
  }

  void InsertNodePropertyWrite(std::vector<std::string> names) {
    node_properties_write_.insert(names.begin(), names.end());
  }

  void InsertEdgePropertyRead(std::string name) {
    edge_properties_read_.insert(name);
  }

  void InsertEdgePropertyRead(std::unordered_set<std::string> names) {
    edge_properties_read_.insert(names.begin(), names.end());
  }

  void InsertEdgePropertyRead(std::vector<std::string> names) {
    edge_properties_read_.insert(names.begin(), names.end());
  }

  void InsertEdgePropertyWrite(std::string name) {
    edge_properties_write_.insert(name);
  }

  void InsertEdgePropertyWrite(std::unordered_set<std::string> names) {
    edge_properties_write_.insert(names.begin(), names.end());
  }

  void InsertEdgePropertyWrite(std::vector<std::string> names) {
    edge_properties_write_.insert(names.begin(), names.end());
  }

  void SetAllPropertiesRead() { all_properties_read_ = true; }

  void SetAllPropertiesWrite() { all_properties_write_ = true; }

  void SetTopologyRead() { topology_read_ = true; }

  void SetTopologyWrite() { topology_write_ = true; }

  const std::unordered_set<std::string>& GetNodePropertyRead() const {
    return node_properties_read_;
  }

  const std::unordered_set<std::string>& GetNodePropertyWrite() const {
    return node_properties_write_;
  }

  const std::unordered_set<std::string>& GetEdgePropertyRead() const {
    return edge_properties_read_;
  }

  const std::unordered_set<std::string>& GetEdgePropertyWrite() const {
    return edge_properties_write_;
  }

  bool GetAllPropertiesRead() const { return all_properties_read_; }

  bool GetAllPropertiesWrite() const { return all_properties_write_; }

  bool GetTopologyRead() const { return topology_read_; }

  bool GetTopologyWrite() const { return topology_write_; }

private:
  std::unordered_set<std::string> node_properties_read_;
  std::unordered_set<std::string> node_properties_write_;
  std::unordered_set<std::string> edge_properties_read_;
  std::unordered_set<std::string> edge_properties_write_;
  bool all_properties_read_{false};
  bool all_properties_write_{false};
  bool topology_read_{false};
  bool topology_write_{false};
};

}  // namespace tsuba

#endif
