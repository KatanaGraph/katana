#ifndef KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_

#include <set>

#include "katana/config.h"

namespace katana {

class KATANA_EXPORT TxnContext {
public:
  void InsertNodePropertyRead(std::string rdg_dir, std::string name) {
    node_properties_read_.insert(ConcatRDGProperty(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyRead(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_read_.insert(ConcatRDGProperty(rdg_dir, name));
    }
  }

  void InsertNodePropertyWrite(std::string rdg_dir, std::string name) {
    node_properties_write_.insert(ConcatRDGProperty(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyWrite(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_write_.insert(ConcatRDGProperty(rdg_dir, name));
    }
  }

  void InsertEdgePropertyRead(std::string rdg_dir, std::string name) {
    edge_properties_read_.insert(ConcatRDGProperty(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyRead(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_read_.insert(ConcatRDGProperty(rdg_dir, name));
    }
  }

  void InsertEdgePropertyWrite(std::string rdg_dir, std::string name) {
    edge_properties_write_.insert(ConcatRDGProperty(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyWrite(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_write_.insert(ConcatRDGProperty(rdg_dir, name));
    }
  }

  void SetAllPropertiesRead() { all_properties_read_ = true; }

  void SetAllPropertiesWrite() { all_properties_write_ = true; }

  void SetTopologyRead() { topology_read_ = true; }

  void SetTopologyWrite() { topology_write_ = true; }

  const std::set<std::string>& GetNodePropertyRead() const {
    return node_properties_read_;
  }

  const std::set<std::string>& GetNodePropertyWrite() const {
    return node_properties_write_;
  }

  const std::set<std::string>& GetEdgePropertyRead() const {
    return edge_properties_read_;
  }

  const std::set<std::string>& GetEdgePropertyWrite() const {
    return edge_properties_write_;
  }

  bool GetAllPropertiesRead() const { return all_properties_read_; }

  bool GetAllPropertiesWrite() const { return all_properties_write_; }

  bool GetTopologyRead() const { return topology_read_; }

  bool GetTopologyWrite() const { return topology_write_; }

private:
  inline std::string ConcatRDGProperty(std::string rdg_dir, std::string prop) {
    return Uri::JoinPath(rdg_dir, prop);
  }

  static constexpr char kPropSeparator[] = "/propertyseparator/";

  std::set<std::string> node_properties_read_;
  std::set<std::string> node_properties_write_;
  std::set<std::string> edge_properties_read_;
  std::set<std::string> edge_properties_write_;
  bool all_properties_read_{false};
  bool all_properties_write_{false};
  bool topology_read_{false};
  bool topology_write_{false};
};

}  // namespace katana

#endif
