#include "azure.h"

#include <storage_errno.h>
#include <storage_outcome.h>
#include <storage_stream.h>

#include <memory>
#include <string_view>

#include <blob/blob_client.h>

#include "galois/FileSystem.h"
#include "galois/GetEnv.h"
#include "galois/Logging.h"
#include "galois/Result.h"
#include "tsuba/Errors.h"

namespace az = azure::storage_lite;

namespace {

const int kAzureMaxConcurrency = 16;

void
WarnAboutCreds(std::string_view key) {
  GALOIS_WARN_ONCE(
      "\n"
      "  Missing \"{}\" value in the environment. You will not be able\n"
      "  To use Azure blob storage until you configure AZURE_ACCOUNT_NAME\n"
      "  and AZURE_ACCOUNT_KEY in your environment\n",
      key);
}

galois::Result<std::unique_ptr<az::blob_client>>
GetClient() {
  std::string account_name;
  std::string account_key;
  if (!galois::GetEnv("AZURE_ACCOUNT_NAME", &account_name)) {
    WarnAboutCreds("AZURE_ACCOUNT_NAME");
    return tsuba::ErrorCode::NoCredentials;
  }
  if (!galois::GetEnv("AZURE_ACCOUNT_KEY", &account_key)) {
    WarnAboutCreds("AZURE_ACCOUNT_KEY");
    return tsuba::ErrorCode::NoCredentials;
  }
  auto cred =
      std::make_shared<az::shared_key_credential>(account_name, account_key);
  auto account = std::make_shared<az::storage_account>(
      account_name, cred,
      /* use_https */ true);
  return std::make_unique<az::blob_client>(account, kAzureMaxConcurrency);
}

}  // namespace

galois::Result<void>
tsuba::AzureInit() {
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::AzureFini() {
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::AzureGetSize(
    const std::string& container, const std::string& blob, uint64_t* size) {
  auto client_res = GetClient();
  if (!client_res) {
    return client_res.error();
  }

  std::unique_ptr<az::blob_client> client = std::move(client_res.value());

  auto blob_property_res = client->get_blob_properties(container, blob).get();
  if (!blob_property_res.success()) {
    return ErrorCode::AzureError;
  }
  const az::blob_property& properties = blob_property_res.response();
  *size = properties.size;
  return galois::ResultSuccess();
}

galois::Result<bool>
tsuba::AzureExists(const std::string& container, const std::string& blob) {
  auto client_res = GetClient();
  if (!client_res) {
    return client_res.error();
  }

  std::unique_ptr<az::blob_client> client = std::move(client_res.value());

  auto blob_property_res = client->get_blob_properties(container, blob).get();
  if (!blob_property_res.success()) {
    // Code seems to be the only value populated here
    if (blob_property_res.error().code == "404") {
      return false;
    }
    GALOIS_LOG_DEBUG("azure failed, code: {}", blob_property_res.error().code);
    return ErrorCode::AzureError;
  }
  return true;
}

galois::Result<void>
tsuba::AzureGetSync(
    const std::string& container, const std::string& blob, uint64_t start,
    uint64_t size, char* result_buf) {
  auto client_res = GetClient();
  if (!client_res) {
    return client_res.error();
  }

  std::unique_ptr<az::blob_client> client = std::move(client_res.value());

  auto ret = client
                 ->download_blob_to_buffer(
                     container, blob, start, size, result_buf, INT_MAX)
                 .get();
  if (!ret.success()) {
    return ErrorCode::AzureError;
  }
  return galois::ResultSuccess();
}

galois::Result<void>
tsuba::AzurePutSync(
    const std::string& container, const std::string& blob, const char* data,
    uint64_t size) {
  auto client_res = GetClient();
  if (!client_res) {
    return client_res.error();
  }

  std::unique_ptr<az::blob_client> client = std::move(client_res.value());
  std::vector<std::pair<std::string, std::string>> metadata{};
  auto ret = client
                 ->upload_block_blob_from_buffer(
                     container, blob, data, metadata, size, INT_MAX)
                 .get();
  if (!ret.success()) {
    return ErrorCode::AzureError;
  }
  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::AzureGetAsync(
    const std::string& container, const std::string& blob, uint64_t start,
    uint64_t size, char* result_buf) {
  auto future = std::async([=]() -> galois::Result<void> {
    auto client_res = GetClient();
    if (!client_res) {
      return client_res.error();
    }

    std::unique_ptr<az::blob_client> client = std::move(client_res.value());

    auto res = client
                   ->download_blob_to_buffer(
                       container, blob, start, size, result_buf, INT_MAX)
                   .get();
    if (!res.success()) {
      return ErrorCode::AzureError;
    }
    return galois::ResultSuccess();
  });
  return std::make_unique<tsuba::FileAsyncWork>(std::move(future));
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::AzurePutAsync(
    const std::string& container, const std::string& blob, const char* data,
    uint64_t size) {
  auto future = std::async([=]() -> galois::Result<void> {
    auto client_res = GetClient();
    if (!client_res) {
      return client_res.error();
    }

    std::unique_ptr<az::blob_client> client = std::move(client_res.value());
    std::vector<std::pair<std::string, std::string>> metadata{};
    auto res =
        client
            ->upload_block_blob_from_buffer(
                container, blob, data, metadata, size, kAzureMaxConcurrency)
            .get();
    if (!res.success()) {
      return ErrorCode::AzureError;
    }
    return galois::ResultSuccess();
  });
  return std::make_unique<FileAsyncWork>(std::move(future));
}

galois::Result<std::unique_ptr<tsuba::FileAsyncWork>>
tsuba::AzureListAsync(
    const std::string& container, const std::string& blob,
    std::vector<std::string>* list, std::vector<uint64_t>* size) {
  auto future = std::async([=]() -> galois::Result<void> {
    auto client_res = GetClient();
    if (!client_res) {
      return client_res.error();
    }
    std::unique_ptr<az::blob_client> client = std::move(client_res.value());

    std::string token;
    do {
      auto list_future =
          client->list_blobs_segmented(container, "", token, blob);
      auto result = list_future.get();
      if (!result.success()) {
        return tsuba::ErrorCode::AzureError;
      }

      // populate the list based on the response
      const az::list_blobs_segmented_response& response = result.response();
      for (const az::list_blobs_segmented_item& item : response.blobs) {
        std::string short_name = item.name;
        size_t begin = short_name.find(blob);
        assert(begin == 0);
        assert(short_name[blob.length()] == '/');
        short_name.erase(begin, blob.length() + 1);
        list->emplace_back(short_name);
        if (size) {
          size->emplace_back(item.content_length);
        }
      }
      token = response.next_marker;
    } while (!token.empty());
    return galois::ResultSuccess();
  });
  return std::make_unique<tsuba::FileAsyncWork>(std::move(future));
}

galois::Result<void>
tsuba::AzureDelete(
    const std::string& container, const std::string& blob,
    const std::unordered_set<std::string>& files) {
  auto client_res = GetClient();
  if (!client_res) {
    return client_res.error();
  }
  std::unique_ptr<az::blob_client> client = std::move(client_res.value());
  std::vector<std::future<az::storage_outcome<void>>> futures;
  futures.reserve(files.size());
  for (const std::string& file : files) {
    futures.emplace_back(client->delete_blob(
        container, galois::JoinPath(blob, file),
        /* delete_snapshots */ false));
  }
  for (std::future<az::storage_outcome<void>>& future : futures) {
    auto res = future.get();
    if (!res.success()) {
      // only return the first error; other operations will be waited on by
      // the destructor
      return ErrorCode::AzureError;
    }
  }
  return galois::ResultSuccess();
}
