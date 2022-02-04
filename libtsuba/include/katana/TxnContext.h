#ifndef KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_
#define KATANA_LIBTSUBA_KATANA_TXNCONTEXT_H_

#include <set>

#include "katana/RDGManifest.h"
#include "katana/URI.h"
#include "katana/config.h"

namespace katana {

class KATANA_EXPORT TxnContext {
public:
  TxnContext() {}

  explicit TxnContext(bool auto_commit) : auto_commit_(auto_commit) {}

  ~TxnContext() {
    if (auto_commit_) {
      KATANA_LOG_DEBUG_ASSERT(Commit());
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

  const Uri& ManifestFile() const { return manifest_file_; };

  const RDGManifest& Manifest() const { return rdg_manifest_; }

  katana::Result<void> Commit() const;

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

  Uri manifest_file_;
  RDGManifest rdg_manifest_;
};

/** I'm putting it here only for review purpose.
  * C++ self-define conversion can only convert the case
  *     AutoCommit<TxnContext> a;
  *     TxnContext b = a;
  * with the below defined converter.
  * But the followed is not allowed because &a is not a member variable
  *     AutoCommit<TxnContext> a;
  *     TxnContext *b = &a; // not possible
  * And we need the second case because we're calling functions in tools
  * and tests like the followed:
  *     TxnContext txn_ctx;
  *     func(..., &txn_ctx);
  * which needs conversion from AutoCommit* to txn_ctx* if we use AutoCommit

template <typename C>
class AutoCommit {  // Maybe call this AutoCommit?
  C ctx_;

public:
  AutoCommit() {}

  ~AutoCommit() {
    // Yes this is violent but AutoCommit is only for tests
    KATANA_LOG_ASSERT(ctx_.Commit());
  }

  operator C&() { return ctx_; }
};
*/

}  // namespace katana

#endif
