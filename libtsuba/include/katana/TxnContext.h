#ifndef KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_

#include <set>

#include "katana/RDGManifest.h"
#include "katana/URI.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT TxnContext {
public:
  /// Create a transaction context. By default it commits changes when the context is destroyed. This is useful when calling from transaction unaware code like tests.
  TxnContext() {}

  /// @brief Create a transaction context.
  /// @param auto_commit :: if false, changes are only committed when Commit is called; if true, changes are committed also when the context is destroyed.
  explicit TxnContext(bool auto_commit) : auto_commit_(auto_commit) {}

  ~TxnContext() {
    if (auto_commit_) {
      KATANA_LOG_ASSERT(Commit());
    }
  }

  void InsertNodePropertyRead(
      const std::string& rdg_dir, const std::string& name) {
    node_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyRead(
      const std::string& rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertNodePropertyWrite(
      const std::string& rdg_dir, const std::string& name) {
    node_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertNodePropertyWrite(
      const std::string& rdg_dir, const Container& names) {
    for (const auto& name : names) {
      node_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertEdgePropertyRead(
      const std::string& rdg_dir, const std::string& name) {
    edge_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyRead(
      const std::string& rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_read_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void InsertEdgePropertyWrite(
      const std::string& rdg_dir, const std::string& name) {
    edge_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
  }

  template <typename Container>
  void InsertEdgePropertyWrite(
      const std::string& rdg_dir, const Container& names) {
    for (const auto& name : names) {
      edge_properties_write_.insert(Uri::JoinPath(rdg_dir, name));
    }
  }

  void SetAllPropertiesRead() { all_properties_read_ = true; }

  void SetAllPropertiesWrite() { all_properties_write_ = true; }

  void SetTopologyRead() { topology_read_ = true; }

  void SetTopologyWrite() { topology_write_ = true; }

  void SetManifestFile(const Uri& manifest_file) {
    manifest_file_ = manifest_file;
  }

  void SetManifest(const RDGManifest& rdg_manifest) {
    rdg_manifest_ = rdg_manifest;
    manifest_cached_ = true;
    manifest_uptodate_ = false;
  }

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

  bool ManifestCached() const { return manifest_cached_; }

  const Uri& ManifestFile() const { return manifest_file_; };

  const RDGManifest& Manifest() const { return rdg_manifest_; }

  katana::Result<void> Commit();

private:
  std::set<std::string> node_properties_read_;
  std::set<std::string> node_properties_write_;
  std::set<std::string> edge_properties_read_;
  std::set<std::string> edge_properties_write_;
  bool all_properties_read_{false};
  bool all_properties_write_{false};
  bool topology_read_{false};
  bool topology_write_{false};

  bool auto_commit_{true};
  bool manifest_cached_{false};
  bool manifest_uptodate_{true};
  Uri manifest_file_;
  RDGManifest rdg_manifest_;
};

}  // namespace katana

#endif
