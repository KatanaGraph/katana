#include "RDGMeta.h"

#include "Constants.h"
#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "galois/JSON.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/tsuba.h"

template <typename T>
using Result = galois::Result<T>;
using json = nlohmann::json;

namespace {

Result<uint64_t>
Parse(const std::string& str) {
  uint64_t val = strtoul(str.c_str(), nullptr, 10);
  if (errno == ERANGE) {
    GALOIS_LOG_ERROR("meta file found with out of range version");
    return galois::ResultErrno();
  }
  return val;
}

}  // namespace

namespace tsuba {

// galois::RandomAlphanumericString does not include _, making this pattern robust
// TODO (witchel) meta with no _[0-9]+ is deprecated and should be
// eliminated eventually
const std::regex RDGMeta::kMetaVersion("meta(?:_([0-9]+)|)(?:-[0-9A-Za-z]+|)$");

Result<tsuba::RDGMeta>
RDGMeta::MakeFromStorage(const galois::Uri& uri) {
  tsuba::FileView fv;
  if (auto res = fv.Bind(uri.string(), true); !res) {
    GALOIS_LOG_DEBUG("RDGMeta::MakeFromStorage bind failed: {}", res.error());
    return res.error();
  }
  tsuba::RDGMeta meta(uri.DirName());
  auto meta_res = galois::JsonParse<tsuba::RDGMeta>(fv, &meta);
  if (!meta_res) {
    GALOIS_LOG_ERROR("cannot parse: {}", uri.string());
    return meta_res.error();
  }
  return meta;
}

Result<RDGMeta>
RDGMeta::Make(const galois::Uri& uri, uint64_t version) {
  return MakeFromStorage(FileName(uri, version));
}

Result<RDGMeta>
RDGMeta::Make(RDGHandle handle) {
  return handle.impl_->rdg_meta();
}

Result<RDGMeta>
RDGMeta::Make(const galois::Uri& uri) {
  if (!IsMetaUri(uri)) {
    auto ns_res = NS()->Get(uri);
    if (!ns_res) {
      GALOIS_LOG_DEBUG("NS->Get failed: {}", ns_res.error());
      return ns_res.error();
    }
    if (ns_res) {
      ns_res.value().dir_ = uri;
    }
    return ns_res;
  }
  return MakeFromStorage(uri);
}

std::string
RDGMeta::PartitionFileName(uint32_t node_id, uint64_t version) {
  return fmt::format("meta_{}_{}", node_id, version);
}

galois::Uri
RDGMeta::PartitionFileName(
    const galois::Uri& uri, uint32_t node_id, uint64_t version) {
  assert(!IsMetaUri(uri));
  return uri.Join(PartitionFileName(node_id, version));
}

galois::Uri
RDGMeta::PartitionFileName(uint32_t node_id) const {
  return RDGMeta::PartitionFileName(dir_, node_id, version());
}

std::string
RDGMeta::ToJsonString() const {
  // POSIX specifies that text files end in a newline
  std::string s = json(*this).dump() + '\n';
  return s;
}

// e.g., rdg_dir == s3://witchel-tests-east2/fault/simple/
galois::Uri
RDGMeta::FileName(const galois::Uri& uri, uint64_t version) {
  assert(uri.empty() || !IsMetaUri(uri));

  return uri.Join(fmt::format("meta_{}", version));
}

// if it doesn't name a meta file, assume it's meant to be a managed URI
bool
RDGMeta::IsMetaUri(const galois::Uri& uri) {
  return std::regex_match(uri.BaseName(), kMetaVersion);
}

Result<uint64_t>
RDGMeta::ParseVersionFromName(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kMetaVersion)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  return Parse(sub_match[1]);
}

// Return the set of file names that hold this RDG's data by reading partition files
// Useful to garbage collect unused files
Result<std::set<std::string>>
RDGMeta::FileNames() {
  std::set<std::string> fnames{};
  fnames.emplace(FileName().BaseName());
  for (auto i = 0U; i < num_hosts(); ++i) {
    // All other file names are directory-local, so we pass an empty
    // directory instead of handle.impl_->rdg_meta.path for the partition files
    fnames.emplace(PartitionFileName(i, version()));

    auto header_res =
        RDGPartHeader::Make(PartitionFileName(dir(), i, version()));

    if (!header_res) {
      GALOIS_LOG_DEBUG(
          "problem uri: {} host: {} ver: {} : {}", dir(), i, version(),
          header_res.error());
    } else {
      auto header = std::move(header_res.value());
      for (const auto& node_prop : header.node_prop_info_list()) {
        fnames.emplace(node_prop.path);
      }
      for (const auto& edge_prop : header.edge_prop_info_list()) {
        fnames.emplace(edge_prop.path);
      }
      for (const auto& part_prop : header.part_prop_info_list()) {
        fnames.emplace(part_prop.path);
      }
      // Duplicates eliminated by set
      fnames.emplace(header.topology_path());
    }
  }
  return fnames;
}

}  // namespace tsuba

void
tsuba::to_json(json& j, const tsuba::RDGMeta& meta) {
  j = json{
      {"magic", kRDGMagicNo},
      {"version", meta.version_},
      {"previous_version", meta.previous_version_},
      {"num_hosts", meta.num_hosts_},
      {"policy_id", meta.policy_id_},
      {"transpose", meta.transpose_},
      {"lineage", meta.lineage_},
  };
}

void
tsuba::from_json(const json& j, tsuba::RDGMeta& meta) {
  uint32_t magic;
  j.at("magic").get_to(magic);
  j.at("version").get_to(meta.version_);
  j.at("num_hosts").get_to(meta.num_hosts_);

  // these values are temporarily optional
  if (auto it = j.find("previous_version"); it != j.end()) {
    it->get_to(meta.previous_version_);
  }
  if (auto it = j.find("policy_id"); it != j.end()) {
    it->get_to(meta.policy_id_);
  }
  if (auto it = j.find("transpose"); it != j.end()) {
    it->get_to(meta.transpose_);
  }
  if (auto it = j.find("lineage"); it != j.end()) {
    it->get_to(meta.lineage_);
  }

  if (magic != kRDGMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("RDG Magic number mismatch");
  }
}
