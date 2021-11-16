#include "tsuba/RDGManifest.h"

#include <sstream>

#include "Constants.h"
#include "GlobalState.h"
#include "RDGHandleImpl.h"
#include "RDGPartHeader.h"
#include "katana/JSON.h"
#include "katana/Result.h"
#include "tsuba/Errors.h"
#include "tsuba/FileView.h"
#include "tsuba/ParquetReader.h"
#include "tsuba/tsuba.h"
template <typename T>
using Result = katana::Result<T>;
using json = nlohmann::json;

namespace {

Result<uint64_t>
Parse(const std::string& str) {
  uint64_t val = strtoul(str.c_str(), nullptr, 10);
  if (errno == ERANGE) {
    return KATANA_ERROR(
        katana::ResultErrno(), "manifest file found with out of range version");
  }
  return val;
}

const int MANIFEST_MATCH_VERS_INDEX = 1;
const int MANIFEST_MATCH_VIEW_INDEX = 2;

}  // namespace

namespace {
const int NODE_ZERO_PADDING_LENGTH = 5;
const int VERS_ZERO_PADDING_LENGTH = 20;
std::string
ToVersionString(uint64_t version) {
  return fmt::format("vers{0:0{1}d}", version, VERS_ZERO_PADDING_LENGTH);
}
std::string
ToNodeString(uint32_t node_id) {
  return fmt::format("node{0:0{1}d}", node_id, NODE_ZERO_PADDING_LENGTH);
}
}  // namespace
namespace tsuba {

const std::regex RDGManifest::kManifestVersion(
    "katana_vers(?:([0-9]+))_(?:([0-9A-Za-z-]+))\\.manifest$");

Result<tsuba::RDGManifest>
RDGManifest::MakeFromStorage(const katana::Uri& uri) {
  tsuba::FileView fv;

  KATANA_CHECKED(fv.Bind(uri.string(), true));

  tsuba::RDGManifest manifest(uri.DirName());
  auto manifest_res = katana::JsonParse<tsuba::RDGManifest>(fv, &manifest);

  if (!manifest_res) {
    return manifest_res.error().WithContext("cannot parse {}", uri.string());
  }

  auto manifest_name = uri.BaseName();
  auto view_name = ParseViewNameFromName(manifest_name);
  auto view_args = ParseViewArgsFromName(manifest_name);

  if (view_name) {
    manifest.set_viewtype(view_name.value());
  }

  if (view_args) {
    manifest.set_viewargs(view_args.value());
  } else {
    manifest.set_viewargs(std::vector<std::string>());
  }
  return manifest;
}

Result<RDGManifest>
RDGManifest::Make(
    const katana::Uri& uri, const std::string& view_type, uint64_t version) {
  return MakeFromStorage(FileName(uri, view_type, version));
}

Result<RDGManifest>
RDGManifest::Make(RDGHandle handle) {
  return handle.impl_->rdg_manifest();
}

Result<RDGManifest>
RDGManifest::Make(const katana::Uri& uri) {
  auto rdg_manifest = MakeFromStorage(uri);
  return rdg_manifest;
}

std::string
RDGManifest::PartitionFileName(
    const std::string& view_type, uint32_t node_id, uint64_t version) {
  KATANA_LOG_ASSERT(!view_type.empty());
  return fmt::format(
      "part_{}_{}_{}", ToVersionString(version), view_type,
      ToNodeString(node_id));
}

katana::Uri
RDGManifest::PartitionFileName(
    const katana::Uri& uri, uint32_t node_id, uint64_t version) {
  return uri.Join(
      PartitionFileName(tsuba::kDefaultRDGViewType, node_id, version));
}

katana::Uri
RDGManifest::PartitionFileName(
    const std::string& view_type, const katana::Uri& uri, uint32_t node_id,
    uint64_t version) {
  KATANA_LOG_DEBUG_ASSERT(!IsManifestUri(uri));
  return uri.Join(PartitionFileName(view_type, node_id, version));
}

katana::Uri
RDGManifest::PartitionFileName(uint32_t host_id) const {
  return RDGManifest::PartitionFileName(
      view_specifier(), dir_, host_id, version());
}

std::string
RDGManifest::ToJsonString() const {
  // POSIX specifies that text files end in a newline
  std::string s = json(*this).dump() + '\n';
  return s;
}

// e.g., rdg_dir == s3://witchel-tests-east2/fault/simple/
katana::Uri
RDGManifest::FileName(
    const katana::Uri& uri, const std::string& view_name, uint64_t version) {
  KATANA_LOG_DEBUG_ASSERT(uri.empty() || !IsManifestUri(uri));
  KATANA_LOG_ASSERT(!view_name.empty());
  return uri.Join(fmt::format(
      "katana_{}_{}.manifest", ToVersionString(version), view_name));
}

// if it doesn't name a manifest file, assume it's meant to be a managed URI
bool
RDGManifest::IsManifestUri(const katana::Uri& uri) {
  bool res = std::regex_match(uri.BaseName(), kManifestVersion);
  return res;
}

Result<uint64_t>
RDGManifest::ParseVersionFromName(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kManifestVersion)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  //Manifest file
  return Parse(sub_match[MANIFEST_MATCH_VERS_INDEX]);
}

Result<std::string>
RDGManifest::ParseViewNameFromName(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kManifestVersion)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  std::string view_specifier = sub_match[MANIFEST_MATCH_VIEW_INDEX];
  std::istringstream iss(view_specifier);
  std::vector<std::string> view_args;
  std::string s = "";
  std::string view_type = "";
  while (std::getline(iss, s, '-')) {
    view_args.push_back(s);
  }
  if (view_args.size() == 0) {  //arg-less view
    view_type = view_specifier;
  } else {  //with args present first token is view name and r  est are args
    view_type = view_args[0];
    view_args.erase(view_args.begin());
  }
  return view_type;
}

Result<std::vector<std::string>>
RDGManifest::ParseViewArgsFromName(const std::string& file) {
  std::smatch sub_match;
  if (!std::regex_match(file, sub_match, kManifestVersion)) {
    return tsuba::ErrorCode::InvalidArgument;
  }
  std::string view_specifier = sub_match[MANIFEST_MATCH_VIEW_INDEX];
  std::istringstream iss(view_specifier);
  std::vector<std::string> view_args;
  std::string s = "";
  std::string view_type = "";
  while (std::getline(iss, s, '-')) {
    view_args.push_back(s);
  }
  if (view_args.size() == 0) {  //arg-less view
    view_type = view_specifier;
  } else {  //with args present first token is view name and r  est are args
    view_type = view_args[0];
    view_args.erase(view_args.begin());
  }
  return view_args;
}

Result<void>
AddPropertySubFiles(std::set<std::string>& fnames, std::string full_path) {
  auto reader = KATANA_CHECKED(tsuba::ParquetReader::Make());
  auto uri_path = KATANA_CHECKED(katana::Uri::Make(full_path));
  auto sub_files = KATANA_CHECKED(reader->GetFiles(uri_path));
  for (const auto& sub_file : sub_files) {
    auto sub_file_uri = KATANA_CHECKED(katana::Uri::Make(sub_file));
    fnames.emplace(
        sub_file_uri.BaseName());  // Only want the file name without dir
  }
  return katana::ResultSuccess();
}

// Return the set of file names that hold this RDG's data by reading partition files
// Useful to garbage collect unused files
Result<std::set<std::string>>
RDGManifest::FileNames() {
  std::set<std::string> fnames{};
  fnames.emplace(FileName().BaseName());
  for (auto i = 0U; i < num_hosts(); ++i) {
    // All other file names are directory-local, so we pass an empty
    // directory instead of handle.impl_->rdg_manifest.path for the partition files
    fnames.emplace(PartitionFileName(view_specifier(), i, version()));

    auto header_uri = KATANA_CHECKED(katana::Uri::Make(fmt::format(
        "{}/{}", dir(), PartitionFileName(view_specifier(), i, version()))));
    auto header_res = RDGPartHeader::Make(header_uri);

    if (!header_res) {
      KATANA_LOG_WARN(
          "problem uri: {} host: {} ver: {} view_name: {}  : {}", header_uri, i,
          version(), view_specifier(), header_res.error());
    } else {
      auto header = std::move(header_res.value());
      for (const auto& node_prop : header.node_prop_info_list()) {
        fnames.emplace(node_prop.path());
        KATANA_CHECKED(AddPropertySubFiles(
            fnames, katana::Uri::JoinPath(dir().string(), node_prop.path())));
      }
      for (const auto& edge_prop : header.edge_prop_info_list()) {
        fnames.emplace(edge_prop.path());
        KATANA_CHECKED(AddPropertySubFiles(
            fnames, katana::Uri::JoinPath(dir().string(), edge_prop.path())));
      }
      for (const auto& part_prop : header.part_prop_info_list()) {
        fnames.emplace(part_prop.path());
        KATANA_CHECKED(AddPropertySubFiles(
            fnames, katana::Uri::JoinPath(dir().string(), part_prop.path())));
      }
      // Duplicates eliminated by set
      if (const auto& n = header.node_entity_type_id_array_path(); !n.empty()) {
        fnames.emplace(n);
      }
      if (const auto& n = header.edge_entity_type_id_array_path(); !n.empty()) {
        fnames.emplace(n);
      }

      for (size_t i = 0; i < header.topology_metadata()->num_entries(); i++) {
        fnames.emplace(header.topology_metadata()->Entries().at(i).path_);
      }
    }
  }
  return fnames;
}

}  // namespace tsuba

void
tsuba::to_json(json& j, const tsuba::RDGManifest& manifest) {
  j = json{
      {"magic", kRDGMagicNo},
      {"version", manifest.version_},
      {"previous_version", manifest.previous_version_},
      {"num_hosts", manifest.num_hosts_},
      {"policy_id", manifest.policy_id_},
      {"transpose", manifest.transpose_},
      {"lineage", manifest.lineage_},
  };
}

void
tsuba::from_json(const json& j, tsuba::RDGManifest& manifest) {
  uint32_t magic;
  j.at("magic").get_to(magic);
  j.at("version").get_to(manifest.version_);
  j.at("num_hosts").get_to(manifest.num_hosts_);

  // these values are temporarily optional
  if (auto it = j.find("previous_version"); it != j.end()) {
    it->get_to(manifest.previous_version_);
  }
  if (auto it = j.find("policy_id"); it != j.end()) {
    it->get_to(manifest.policy_id_);
  }
  if (auto it = j.find("transpose"); it != j.end()) {
    it->get_to(manifest.transpose_);
  }
  if (auto it = j.find("lineage"); it != j.end()) {
    it->get_to(manifest.lineage_);
  }

  if (magic != kRDGMagicNo) {
    // nlohmann::json reports errors using exceptions
    throw std::runtime_error("RDG Manifest Magic number mismatch");
  }
}
