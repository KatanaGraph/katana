#ifndef KATANA_LIBGLUON_STORAGEHELPERS_H_
#define KATANA_LIBGLUON_STORAGEHELPERS_H_

#include <algorithm>
#include <memory>

#include "katana/ReadGroup.h"
#include "katana/URI.h"
#include "katana/WriteGroup.h"
#include "storage_operations_generated.h"

namespace katana {

/// seraialize `uri_to_serialize` into a flat buffer, if `storage_location`
/// is a prefix of the URI: remove that prefix and store a "relative" URI
/// if it's not, store an absolute URI
inline std::pair<std::string, fbs::URIType>
TryToShorten(const URI& storage_location, const URI& uri_to_serialize) {
  auto [pfx_it, uri_it] = std::mismatch(
      storage_location.path().begin(), storage_location.path().end(),
      uri_to_serialize.path().begin(), uri_to_serialize.path().end());

  auto res = std::make_unique<fbs::StorageLocationT>();
  if (storage_location.scheme() == uri_to_serialize.scheme() ||
      pfx_it == storage_location.path().end()) {
    return std::make_pair(
        std::string(uri_it, uri_to_serialize.path().end()),
        fbs::URIType::Relative);
  }
  return std::make_pair(uri_to_serialize.string(), fbs::URIType::Absolute);
}

/// seraialize `uri_to_serialize` into a flat buffer, if `storage_location`
/// is a prefix of the URI: remove that prefix and store a "relative" URI
/// if it's not, store an absolute URI
inline std::unique_ptr<fbs::StorageLocationT>
UriToFB(const URI& storage_location, const URI& uri_to_serialize) {
  auto res = std::make_unique<fbs::StorageLocationT>();
  std::tie(res->uri, res->location_type) =
      TryToShorten(storage_location, uri_to_serialize);
  return res;
}

inline flatbuffers::Offset<fbs::StorageLocation>
UriToFB(
    const URI& storage_location, const URI& uri_to_serialize,
    flatbuffers::FlatBufferBuilder* builder) {
  auto [new_uri, type] = TryToShorten(storage_location, uri_to_serialize);
  auto new_uri_buf = builder->CreateString(new_uri.c_str());
  return fbs::CreateStorageLocation(*builder, type, new_uri_buf);
}

inline Result<URI>
UriFromFB(const URI& storage_location, const fbs::StorageLocationT& fb_uri) {
  if (fb_uri.location_type == fbs::URIType::Absolute) {
    return URI::Make(fb_uri.uri);
  }
  return storage_location.Join(fb_uri.uri);
}

inline Result<URI>
UriFromFB(const URI& storage_location, const fbs::StorageLocation& fb_uri) {
  if (fb_uri.location_type() == fbs::URIType::Absolute) {
    return URI::Make(fb_uri.uri()->str());
  }
  return storage_location.Join(fb_uri.uri()->string_view());
}

inline Result<void>
PersistFB(
    const flatbuffers::FlatBufferBuilder& finished_builder, const URI& uri,
    WriteGroup* wg) {
  if (!wg) {
    return FileStore(
        uri.string(), finished_builder.GetBufferPointer(),
        finished_builder.GetSize());
  }

  auto ff = std::make_shared<FileFrame>();
  KATANA_CHECKED(ff->Init(finished_builder.GetSize()));
  std::memcpy(
      KATANA_CHECKED(ff->ptr<uint8_t>()), finished_builder.GetBufferPointer(),
      finished_builder.GetSize());
  KATANA_CHECKED(ff->SetCursor(finished_builder.GetSize()));
  ff->Bind(uri.string());
  wg->StartStore(std::move(ff));

  return ResultSuccess();
}

template <typename FlatBufferType>
inline Result<void>
PersistFB(FlatBufferType* thing_to_write, const URI& uri, WriteGroup* wg) {
  using TableType = typename FlatBufferType::TableType;

  flatbuffers::FlatBufferBuilder fbb;
  fbb.Finish(TableType::Pack(fbb, thing_to_write));
  return PersistFB(fbb, uri, wg);
}

// fn should be a callable thing that returns a
// katana Result<T>
// AsyncGroupType should be a `WriteGroup` or a `ReadGroup`
template <
    typename Fn, typename AsyncGroupType,
    typename T =
        typename std::invoke_result<Fn, AsyncGroupType*>::type::value_type>
inline Result<T>
CreateOrJoinAsyncGroup(AsyncGroupType* ag, Fn&& fn) {
  std::unique_ptr<AsyncGroupType> new_ag;
  if (!ag) {
    new_ag = KATANA_CHECKED(AsyncGroupType::Make());
    ag = new_ag.get();
  }

  auto res = std::invoke(fn, ag);
  if (!res) {
    return res;
  }

  if (new_ag) {
    KATANA_CHECKED(new_ag->Finish());
  }
  return res;
}

// store the contents of a vector directly to storage,
// if writegroup is passed, assumes that the vector will live longer than
// the writegroup
template <typename VectorOfPODs>
Result<void>
PersistVector(const URI& uri, const VectorOfPODs& pods, WriteGroup* wg) {
  const auto* start = reinterpret_cast<const uint8_t*>(pods.data());
  size_t size = pods.size() * sizeof(typename VectorOfPODs::value_type);
  if (wg) {
    wg->StartStore(uri.string(), start, size);
  } else {
    KATANA_CHECKED(FileStore(uri.string(), start, size));
  }
  return ResultSuccess();
}

// store the contents of a vector directly to storage, resizing the vector
// to match the exact file size on storage
template <typename VectorOfPODs>
Result<void>
FillVector(const URI& uri, VectorOfPODs* pods, ReadGroup* rg = nullptr) {
  using ValueType = typename VectorOfPODs::value_type;
  auto future = std::async(std::launch::async, [=]() -> CopyableResult<void> {
    StatBuf sbuf;
    KATANA_CHECKED(FileStat(uri.string(), &sbuf));
    if (sbuf.size % sizeof(ValueType) != 0) {
      return KATANA_ERROR(
          ErrorCode::AssertionFailed, "file length not aligned with type");
    }
    pods->resize(sbuf.size / sizeof(ValueType));
    KATANA_CHECKED(FileGet(uri.string(), pods->data(), 0, sbuf.size));
    return CopyableResultSuccess();
  });

  if (!rg) {
    KATANA_CHECKED(future.get());
    return ResultSuccess();
  }

  rg->AddOp(std::move(future), uri.string(), []() {
    return CopyableResultSuccess();
  });
  return ResultSuccess();
}

}  // namespace katana

#endif
