#ifndef KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_

#include <set>

#include "katana/URI.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT TxnContext {
public:
  TxnContext() {}

  TxnContext(bool commit_manifest) : commit_manifest_(commit_manifest) {}

  void InsertNodePropertyRead(std::string rdg_dir, std::string name) {
    node_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyRead(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertNodePropertyWrite(std::string rdg_dir, std::string name) {
    node_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyWrite(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertEdgePropertyRead(std::string rdg_dir, std::string name) {
    edge_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyRead(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertEdgePropertyWrite(std::string rdg_dir, std::string name) {
    edge_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyWrite(std::string rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void SetAllPropertiesRead() { all_properties_read_ = true; }

  void SetAllPropertiesWrite() { all_properties_write_ = true; }

  void SetTopologyRead() { topology_read_ = true; }

  void SetTopologyWrite() { topology_write_ = true; }

  void DelayManifestCommit() { commit_manifest_ = false; }

  const std::set<std::string>& NodePropertyRead() const {
    return node_properties_read_;
  }

  const std::set<std::string>& NodePropertyWrite() const {
    return node_properties_write_;
  }

  const std::set<std::string>& EdgePropertyRead() const {
    return edge_properties_read_;
  }

  const std::set<std::string>& EdgePropertyWrite() const {
    return edge_properties_write_;
  }

  bool AllPropertiesRead() const { return all_properties_read_; }

  bool AllPropertiesWrite() const { return all_properties_write_; }

  bool TopologyRead() const { return topology_read_; }

  bool TopologyWrite() const { return topology_write_; }

  bool CommitManifest() const { return commit_manifest_; }

private:
  std::set<std::string> node_properties_read_;
  std::set<std::string> node_properties_write_;
  std::set<std::string> edge_properties_read_;
  std::set<std::string> edge_properties_write_;
  bool all_properties_read_{false};
  bool all_properties_write_{false};
  bool topology_read_{false};
  bool topology_write_{false};

  bool commit_manifest_{true};
};

}  // namespace katana

#endif
