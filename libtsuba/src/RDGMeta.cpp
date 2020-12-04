#include "tsuba/RDGMeta.h"

#include "Constants.h"
#include "GlobalState.h"
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
    return res.error();
  }
  tsuba::RDGMeta meta(uri.DirName());
  auto meta_res = galois::JsonParse<tsuba::RDGMeta>(fv, &meta);
  if (!meta_res) {
    return meta_res.error();
  }
  return meta;
}

Result<RDGMeta>
RDGMeta::Make(const galois::Uri& uri, uint64_t version) {
  return MakeFromStorage(FileName(uri, version));
}

Result<RDGMeta>
RDGMeta::Make(const galois::Uri& uri) {
  if (!IsMetaUri(uri)) {
    auto ns_res = NS()->Get(uri);
    if (!ns_res) {
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

Result<galois::Uri>
RDGMeta::PartitionFileName(bool intend_partial_read) const {
  if (intend_partial_read) {
    if (num_hosts() != 1) {
      GALOIS_LOG_ERROR("cannot partially read partitioned graph");
      return ErrorCode::InvalidArgument;
    }
    return PartitionFileName(dir(), 0, version());
  }
  if (policy_id() != 0 && num_hosts() != 0 && num_hosts() != Comm()->Num) {
    GALOIS_LOG_ERROR(
        "number of hosts for partitioned graph {} does not "
        "match number of current hosts {}",
        num_hosts(), Comm()->Num);
    // Query depends on being able to load a graph this way
    if (num_hosts() == 1) {
      // TODO(thunt) eliminate this special case after query is updated not
      // to depend on this behavior
      return PartitionFileName(dir(), 0, version());
    }
    return ErrorCode::InvalidArgument;
  }
  return RDGMeta::PartitionFileName(dir(), Comm()->ID, version());
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
