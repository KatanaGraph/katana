#include "s3.h"

#include <memory>
#include <string_view>
#include <algorithm>
#include <unistd.h>
#include <stack>

#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/transfer/TransferManager.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/auth/STSCredentialsProvider.h>
#include <fmt/core.h>

#include "galois/FileSystem.h"
#include "galois/GetEnv.h"
#include "galois/Result.h"
#include "galois/Logging.h"
#include "tsuba/Errors.h"
#include "tsuba_internal.h"
#include "tsuba/s3_internal.h"
#include "tsuba/FaultTest.h"
#include "SegmentedBufferView.h"

namespace tsuba {

static Aws::SDKOptions sdk_options;

static constexpr const char* kDefaultS3Region = "us-east-1";
static constexpr const char* kAwsTag          = "TsubaS3Client";
// TODO: Could explore policies
// Limits come from here.
//   https://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
// We use these defaults (from aws s3 cli)
//   https://docs.aws.amazon.com/cli/latest/topic/s3-config.html
static constexpr const uint64_t kS3MinBufSize     = MB(5);
static constexpr const uint64_t kS3DefaultBufSize = MB(8);
static constexpr const uint64_t kS3MaxBufSize     = GB(5);
static constexpr const uint64_t kS3MaxMultiPart   = 10000;
// TODO: How to set this number?  Base it on cores on machines and/or memory
// use?
static constexpr const uint64_t kNumS3Threads = 36;

// Initialized in S3Init
static std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>
    default_executor{nullptr};
static std::shared_ptr<Aws::S3::S3Client> async_s3_client{nullptr};
static bool library_init{false};

// TODO(thunt) 2020-08-11 this was taken pretty much verbatim from the current
// AWS SDK source. We should revisit this when the version of the SDK is updated
// in conan (currently at 1.7)
class TsubaCredentialsChain : public Aws::Auth::AWSCredentialsProviderChain {
public:
  TsubaCredentialsChain() : AWSCredentialsProviderChain() {
    AddProvider(
        Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>(kAwsTag));
    AddProvider(
        Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>(
            kAwsTag));
    AddProvider(
        Aws::MakeShared<Aws::Auth::ProcessCredentialsProvider>(kAwsTag));
    AddProvider(
        Aws::MakeShared<Aws::Auth::STSAssumeRoleWebIdentityCredentialsProvider>(
            kAwsTag));

    // Based on source code from the Default provider chain
    std::string relative_uri;
    std::string absolute_uri;
    bool ec2_metadata_disabled = false;

    galois::GetEnv("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", &relative_uri);
    galois::GetEnv("AWS_CONTAINER_CREDENTIALS_FULL_URI", &absolute_uri);
    galois::GetEnv("AWS_EC2_METADATA_DISABLED", &ec2_metadata_disabled);

    if (!relative_uri.empty()) {
      AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
          kAwsTag, relative_uri.c_str()));
    } else if (!absolute_uri.empty()) {
      std::string token;
      galois::GetEnv("AWS_CONTAINER_AUTHORIZATION_TOKEN", &token);
      AddProvider(Aws::MakeShared<Aws::Auth::TaskRoleCredentialsProvider>(
          kAwsTag, absolute_uri.c_str(), token.c_str()));
    } else if (!ec2_metadata_disabled) {
      AddProvider(
          Aws::MakeShared<Aws::Auth::InstanceProfileCredentialsProvider>(
              kAwsTag));
    }
  }
};

/// GetS3Client returns a configured S3 client.
///
/// The client pulls its configuration from the environment using the same
/// environment variables and configuration paths as the AWS CLI, although with
/// a subset of the configurability.
///
/// The AWS region is determined by:
///
/// 1. The region associated with the default profile in $HOME/.aws/config The
///    location configuration file can be overriden by env[AWS_CONFIG_FILE].
/// 2. Otherwise, env[AWS_DEFAULT_REGION]
/// 3. Otherwise, us-east-1
///
/// The credentials are determined by (in order of precedence):
///
/// 1. The environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
/// 2. The credentials associcated with the default profile in
/// $HOME/.aws/credentials
/// 3. An external credential provider command as noted in the config file.
/// 4. STS assume role credenials
/// https://docs.aws.amazon.com/STS/latest/APIReference/API_AssumeRole.html
/// 5. IAM roles for tasks (containers)
/// https://docs.aws.amazon.com/AmazonECS/latest/developerguide/task-iam-roles.html
/// 6. The machine's account (via EC2 metadata service) if on EC2
static inline std::shared_ptr<Aws::S3::S3Client>
GetS3Client(const std::shared_ptr<Aws::Utils::Threading::PooledThreadExecutor>&
                executor) {
  Aws::Client::ClientConfiguration cfg("default");

  bool use_virtual_addressing = true;

  std::string region;
  if (galois::GetEnv("AWS_DEFAULT_REGION", &region)) {
    cfg.region = region;
  }

  if (cfg.region.empty()) {
    // The AWS SDK says the default region is us-east-1 but it appears we need
    // to set it ourselves.
    cfg.region = kDefaultS3Region;
  }

  cfg.executor = executor;

  std::string test_endpoint;
  // No official AWS environment analog so use GALOIS prefix
  galois::GetEnv("GALOIS_AWS_TEST_ENDPOINT", &test_endpoint);
  if (!test_endpoint.empty()) {
    cfg.endpointOverride = test_endpoint;
    cfg.scheme           = Aws::Http::Scheme::HTTP;

    // if false SDK will build "path-style" URLs if true the URLs will be
    // "virtual-host-style" URLs LocalStack only supports the former but they
    // are deprecated for new buckets in s3
    use_virtual_addressing = false;
  }

  return Aws::MakeShared<Aws::S3::S3Client>(
      kAwsTag, Aws::MakeShared<TsubaCredentialsChain>(kAwsTag), cfg,
      Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never,
      use_virtual_addressing);
}

static inline std::shared_ptr<Aws::S3::S3Client> GetS3Client() {
  GALOIS_LOG_VASSERT(library_init == true,
                     "Must call tsuba::Init before S3 interaction");
  return GetS3Client(default_executor);
}

template <class OutcomeType>
static galois::Result<void> CheckS3Error(const OutcomeType& outcome) {
  if (outcome.IsSuccess()) {
    return galois::ResultSuccess();
  }
  const auto& error = outcome.GetError();
  if (error.GetResponseCode() ==
      Aws::Http::HttpResponseCode::MOVED_PERMANENTLY) {
    return ErrorCode::AWSWrongRegion;
  }
  return ErrorCode::S3Error;
}

static SegmentedBufferView SegmentBuf(uint64_t start, const uint8_t* data,
                                      uint64_t size) {
  uint64_t segment_size = kS3DefaultBufSize;
  if ((size / kS3DefaultBufSize) > kS3MaxMultiPart) {
    // I can't find anything that says this needs to be an "even" number
    // Add one because integer arithmetic is floor
    segment_size = size / (kS3MaxMultiPart + 1);
    GALOIS_LOG_VASSERT(
        (segment_size > kS3MinBufSize) && (segment_size < kS3MaxBufSize),
        "\n  Min {:d} Max {:d} Default {:d} Request (too big) {:d} Segment "
        "{:d}",
        kS3MinBufSize, kS3MaxBufSize, kS3DefaultBufSize, size, segment_size);
  }
  return SegmentedBufferView(start, const_cast<uint8_t*>(data), size,
                             segment_size);
}

galois::Result<void> S3Init() {
  library_init = true;
  Aws::InitAPI(sdk_options);
  default_executor =
      Aws::MakeShared<Aws::Utils::Threading::PooledThreadExecutor>(
          kAwsTag, kNumS3Threads);
  // Need context for async uploads
  async_s3_client = GetS3Client();
  return galois::ResultSuccess();
}

galois::Result<void> S3Fini() {
  Aws::ShutdownAPI(sdk_options);
  return galois::ResultSuccess();
}

static inline std::string BucketAndObject(const std::string& bucket,
                                          const std::string& object) {
  return std::string(bucket).append("/").append(object);
}

galois::Result<void> S3GetSize(const std::string& bucket,
                               const std::string& object, uint64_t* size) {
  auto s3_client = GetS3Client();
  /* skip all of the thread management overhead if we only have one request */
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetKey(ToAwsString(object));
  Aws::S3::Model::HeadObjectOutcome outcome = s3_client->HeadObject(request);

  if (auto res = CheckS3Error(outcome); !res) {
    if (res.error() == ErrorCode::S3Error) {
      // couldn't classify dump some extra info to the log
      const auto& error = outcome.GetError();
      GALOIS_LOG_ERROR("S3GetSize\n  [{}] {}\n  {}: {} {}\n", bucket, object,
                       error.GetResponseCode(), error.GetExceptionName(),
                       error.GetMessage());
    }
    return res;
  }
  *size = outcome.GetResult().GetContentLength();
  return galois::ResultSuccess();
}

/// Return 1 if bucket/object exists, 0 otherwise
galois::Result<bool> S3Exists(const std::string& bucket,
                              const std::string& object) {
  auto s3_client = GetS3Client();
  /* skip all of the thread management overhead if we only have one request */
  Aws::S3::Model::HeadObjectRequest request;
  request.SetBucket(ToAwsString(bucket));
  request.SetKey(ToAwsString(object));
  Aws::S3::Model::HeadObjectOutcome outcome = s3_client->HeadObject(request);
  if (!outcome.IsSuccess()) {
    return false;
  }
  return true;
}

galois::Result<void> internal::S3PutSingleSync(const std::string& bucket,
                                               const std::string& object,
                                               const uint8_t* data,
                                               uint64_t size) {
  auto s3_client = GetS3Client();
  Aws::S3::Model::PutObjectRequest object_request;

  object_request.SetBucket(ToAwsString(bucket));
  object_request.SetKey(ToAwsString(object));
  auto streamBuf = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
      kAwsTag, (uint8_t*)data, static_cast<size_t>(size));
  auto preallocatedStreamReader =
      Aws::MakeShared<Aws::IOStream>(kAwsTag, streamBuf);

  object_request.SetBody(preallocatedStreamReader);
  object_request.SetContentType("application/octet-stream");

  tsuba::internal::PtP();
  auto outcome = s3_client->PutObject(object_request);
  tsuba::internal::PtP();
  if (!outcome.IsSuccess()) {
    /* TODO there are likely some errors we can handle gracefully
     * i.e., with retries */
    const auto& error = outcome.GetError();
    GALOIS_LOG_ERROR("\n  Upload failed: {}: {}\n  [{}] {}",
                     error.GetExceptionName(), error.GetMessage(), bucket,
                     object);
    return ErrorCode::S3Error;
  }
  return galois::ResultSuccess();
}

galois::Result<void> S3UploadOverwrite(const std::string& bucket,
                                       const std::string& object,
                                       const uint8_t* data, uint64_t size) {
  // Any small size put, do synchronously
  if (size < kS3DefaultBufSize) {
    GALOIS_LOG_DEBUG("S3 Put {:d} bytes, less than {:d}, doing sync", size,
                     kS3DefaultBufSize);
    return internal::S3PutSingleSync(bucket, object, data, size);
  }

  auto s3_client = GetS3Client();

  Aws::S3::Model::CreateMultipartUploadRequest createMpRequest;
  createMpRequest.WithBucket(ToAwsString(bucket));
  createMpRequest.WithContentType("application/octet-stream");
  createMpRequest.WithKey(ToAwsString(object));

  auto createMpResponse = s3_client->CreateMultipartUpload(createMpRequest);
  tsuba::internal::PtP();
  if (auto res = CheckS3Error(createMpResponse); !res) {
    if (res.error() == ErrorCode::S3Error) {
      const auto& error = createMpResponse.GetError();
      GALOIS_LOG_ERROR("Transfer failed to create a multi-part upload request\n"
                       "  [{}] {}\n  {}: {}\n",
                       bucket, object, error.GetExceptionName(),
                       error.GetMessage());
    }
    return res.error();
  }

  auto upload_id              = createMpResponse.GetResult().GetUploadId();
  SegmentedBufferView bufView = SegmentBuf(0UL, data, size);
  std::vector<SegmentedBufferView::BufPart> parts(bufView.begin(),
                                                  bufView.end());
  // Because zero-length upload handled above, parts should not be empty
  GALOIS_LOG_ASSERT(!parts.empty());

  std::vector<std::string> part_e_tags(parts.size());

  std::mutex m;
  std::condition_variable cv;
  Aws::S3::Model::CompletedMultipartUpload completedUpload;
  uint64_t finished = 0;
  tsuba::internal::PtP();
  for (unsigned i = 0; i < parts.size(); ++i) {
    auto& part         = parts[i];
    auto lengthToWrite = part.end - part.start;
    auto streamBuf     = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
        kAwsTag, part.dest, static_cast<size_t>(lengthToWrite));
    auto preallocatedStreamReader =
        Aws::MakeShared<Aws::IOStream>(kAwsTag, streamBuf);
    Aws::S3::Model::UploadPartRequest uploadPartRequest;

    uploadPartRequest.WithBucket(ToAwsString(bucket))
        .WithContentLength(static_cast<long long>(lengthToWrite))
        .WithKey(ToAwsString(object))
        .WithPartNumber(i + 1) /* part numbers start at 1 */
        .WithUploadId(upload_id);

    uploadPartRequest.SetBody(preallocatedStreamReader);
    uploadPartRequest.SetContentType("application/octet-stream");
    auto callback =
        [i, bucket, object, &part_e_tags, &cv, &m, &finished](
            const Aws::S3::S3Client* client,
            const Aws::S3::Model::UploadPartRequest& request,
            const Aws::S3::Model::UploadPartOutcome& outcome,
            const std::shared_ptr<const Aws::Client::AsyncCallerContext>&
                context) {
          /* we're not using these but they need to be here to preserve the
           * signature
           */
          (void)(client);
          (void)(request);
          (void)(context);
          tsuba::internal::PtP();
          if (outcome.IsSuccess()) {
            std::lock_guard<std::mutex> lk(m);
            tsuba::internal::PtP();
            part_e_tags[i] = outcome.GetResult().GetETag();
            finished++;
            cv.notify_one();
            tsuba::internal::PtP();
          } else {
            const auto& error = outcome.GetError();
            GALOIS_LOG_FATAL(
                "Upload multi callback failure\n  {}: {}\n  [{}] {}",
                error.GetExceptionName(), error.GetMessage(), bucket, object);
          }
        };
    s3_client->UploadPartAsync(uploadPartRequest, callback);
    tsuba::internal::PtP();
  }
  std::unique_lock<std::mutex> lk(m);
  tsuba::internal::PtP();
  cv.wait(lk, [&] {
    tsuba::internal::PtP();
    return finished >= parts.size();
  });

  for (unsigned i = 0; i < part_e_tags.size(); ++i) {
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.WithPartNumber(i + 1).WithETag(ToAwsString(part_e_tags[i]));
    completedUpload.AddParts(completedPart);
    tsuba::internal::PtP();
  }

  Aws::S3::Model::CompleteMultipartUploadRequest completeMultipartUploadRequest;
  completeMultipartUploadRequest.WithBucket(ToAwsString(bucket))
      .WithKey(ToAwsString(object))
      .WithUploadId(upload_id)
      .WithMultipartUpload(completedUpload);

  tsuba::internal::PtP();
  auto completeUploadOutcome =
      s3_client->CompleteMultipartUpload(completeMultipartUploadRequest);

  if (!completeUploadOutcome.IsSuccess()) {
    const auto& error = completeUploadOutcome.GetError();
    GALOIS_LOG_ERROR(
        "\n  Failed to complete multipart upload\n  {}: {}\n  [{}] {}",
        error.GetExceptionName(), error.GetMessage(), bucket, object);
    return ErrorCode::S3Error;
  }
  return galois::ResultSuccess();
}

// This stuff is too complex to fold into S3AsyncWork.  At least for now
enum class Xfer {
  One,   // Ready to start
  Two,   // CreateMulti pending
  Three, // Xfer started
  Four   // Xfer finished, completion pending
};
// ew: There is probably a way to combine declaration and labels,
// but all the techniques I found were over the top
static const std::unordered_map<Xfer, std::string> xfer_label{
    {Xfer::One, "Xfer_1"},
    {Xfer::Two, "Xfer_2"},
    {Xfer::Three, "Xfer_3"},
    {Xfer::Four, "Xfer_4"},
};
struct PutMulti {
  // Modify xfer_ with lock held.  Only code for that Xfer state should
  // read/write data.
  Xfer xfer_{Xfer::One};
  std::vector<SegmentedBufferView::BufPart> parts_{};
  std::future<Aws::S3::Model::CreateMultipartUploadOutcome> create_fut_{};
  std::future<Aws::S3::Model::CompleteMultipartUploadOutcome> outcome_fut_{};
  std::vector<std::string> part_e_tags_{};
  uint64_t finished_{0UL};
  std::string upload_id_{""};
  // Construct by assigning elements, since that is the general case.
  PutMulti() {}
};

static std::unordered_map<std::string, PutMulti> xferm;
static std::mutex xfer_mutex;
static std::condition_variable xfer_cv;

galois::Result<void> internal::S3PutMultiAsync1(S3AsyncWork& s3aw,
                                                const uint8_t* data,
                                                uint64_t size) {
  std::string bucket = s3aw.GetBucket();
  std::string object = s3aw.GetObject();
  GALOIS_LOG_VASSERT(library_init == true,
                     "Must call tsuba::Init before S3 interaction");
  // We don't expect this function to be called directly, it is part of
  // s3_internal S3PutAsync checks the size and never calls S3PutMultiAsync1
  // unless the size is larger than kDefaultS3Region
  GALOIS_LOG_VASSERT(size > 0,
                     "MultiAsync is a bad choice for a zero size file");

  Aws::S3::Model::CreateMultipartUploadRequest createMpRequest;
  createMpRequest.WithBucket(ToAwsString(bucket));
  createMpRequest.WithContentType("application/octet-stream");
  createMpRequest.WithKey(ToAwsString(object));

  SegmentedBufferView bufView = SegmentBuf(0UL, data, size);

  std::string bno = BucketAndObject(bucket, object);
  {
    std::lock_guard<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    if (it == xferm.end()) {
      xferm.try_emplace(bno);
      // Now make the iterator point to the emplaced struct
      it = xferm.find(bno);
    }
    GALOIS_LOG_VASSERT(
        it->second.xfer_ == Xfer::One,
        "{:<30} PutMultiAsync1 before previous finished, state is {}\n", bno,
        xfer_label.at(it->second.xfer_));
    it->second.xfer_  = Xfer::Two;
    it->second.parts_ = std::vector<SegmentedBufferView::BufPart>(
        bufView.begin(), bufView.end());
    it->second.create_fut_ =
        async_s3_client->CreateMultipartUploadCallable(createMpRequest);
    // it->second.outcome_fut_ // assumed invalid
    it->second.part_e_tags_.resize(bufView.NumSegments());
    it->second.finished_  = 0UL;
    it->second.upload_id_ = "";

    GALOIS_LOG_DEBUG(
        "{:<30} PutMultiAsync1 size {:#x} nSeg {:d} parts_.size() {:d}", bno,
        size, bufView.NumSegments(), it->second.parts_.size());
  }
  return galois::ResultSuccess();
}

galois::Result<void> internal::S3PutMultiAsync2(S3AsyncWork& s3aw) {
  std::string bucket = s3aw.GetBucket();
  std::string object = s3aw.GetObject();
  std::string bno    = BucketAndObject(bucket, object);
  // Standard says we can keep a pointer to value that remains valid even if
  // iterator is invalidated.  Iterators can be invalidated because of "rehash"
  // https://en.cppreference.com/w/cpp/container/unordered_map (Iterator
  // invalidation)
  PutMulti* pm{nullptr};
  {
    std::lock_guard<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    GALOIS_LOG_VASSERT(
        it != xferm.end(),
        "{:<30} PutMultiAsync2 callback no bucket/object in map\n", bno);
    GALOIS_LOG_VASSERT(it->second.xfer_ == Xfer::Two,
                       "{:<30} PutMultiAsync2 but state is {}\n", bno,
                       xfer_label.at(it->second.xfer_));
    it->second.xfer_ = Xfer::Three;
    pm               = &it->second;
  }

  auto createMpResponse = pm->create_fut_.get(); // Blocking call
  if (!createMpResponse.IsSuccess()) {
    const auto& error = createMpResponse.GetError();
    GALOIS_LOG_ERROR("Failed to create a multi-part upload request.\n  Bucket: "
                     "[{}] Key: [{}]\n  {}: {}\n",
                     bucket, object, error.GetExceptionName(),
                     error.GetMessage());
    return ErrorCode::S3Error;
  }

  pm->upload_id_ = createMpResponse.GetResult().GetUploadId();
  GALOIS_LOG_DEBUG("{:<30} PutMultiAsync2 B parts.size() {:d}\n  upload id {}",
                   bno, pm->parts_.size(), pm->upload_id_);

  Aws::S3::Model::CompletedMultipartUpload completedUpload;
  for (unsigned i = 0; i < pm->parts_.size(); ++i) {
    auto& part         = pm->parts_[i];
    auto lengthToWrite = part.end - part.start;
    auto streamBuf     = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
        kAwsTag, part.dest, static_cast<size_t>(lengthToWrite));
    auto preallocatedStreamReader =
        Aws::MakeShared<Aws::IOStream>(kAwsTag, streamBuf);
    Aws::S3::Model::UploadPartRequest uploadPartRequest;

    uploadPartRequest.WithBucket(ToAwsString(bucket))
        .WithContentLength(static_cast<long long>(lengthToWrite))
        .WithKey(ToAwsString(object))
        .WithPartNumber(i + 1) /* part numbers start at 1 */
        .WithUploadId(ToAwsString(pm->upload_id_));

    uploadPartRequest.SetBody(preallocatedStreamReader);
    uploadPartRequest.SetContentType("application/octet-stream");

    // References to locals will go out of scope
    auto callback = [i, bucket, object](
                        const Aws::S3::S3Client* /*client*/,
                        const Aws::S3::Model::UploadPartRequest& request,
                        const Aws::S3::Model::UploadPartOutcome& outcome,
                        const std::shared_ptr<
                            const Aws::Client::AsyncCallerContext>& /*ctx*/) {
      std::string bno = BucketAndObject(bucket, object);
      if (outcome.IsSuccess()) {
        {
          std::lock_guard<std::mutex> lk(xfer_mutex);
          auto it = xferm.find(bno);
          GALOIS_LOG_VASSERT(
              it != xferm.end(),
              "{:<30} PutMultiAsync2 callback no bucket/object in map\n", bno);
          GALOIS_LOG_VASSERT(it->second.xfer_ == Xfer::Three,
                             "{:<30} PutMultiAsync2 callback but state is {}\n",
                             bno, xfer_label.at(it->second.xfer_));
          it->second.part_e_tags_[i] = outcome.GetResult().GetETag();
          it->second.finished_++;
          GALOIS_LOG_DEBUG(
              "{:<30} PutMultiAsync2 i {:d} finished {:d}\n etag {}", bno, i,
              it->second.finished_, outcome.GetResult().GetETag());
        }
        // Notify does not require lock
        xfer_cv.notify_one();
      } else {
        /* TODO there are likely some errors we can handle gracefully
         * i.e., with retries */
        const auto& error = outcome.GetError();
        GALOIS_LOG_FATAL(
            "\n  Upload failed: {}: {}\n  upload_id: {}\n  [{}] {}",
            error.GetExceptionName(), error.GetMessage(), request.GetUploadId(),
            bucket, object);
      }
    };
    async_s3_client->UploadPartAsync(uploadPartRequest, callback);
  }

  return galois::ResultSuccess();
}

galois::Result<void> internal::S3PutMultiAsync3(S3AsyncWork& s3aw) {
  std::string bucket = s3aw.GetBucket();
  std::string object = s3aw.GetObject();
  std::string bno    = BucketAndObject(bucket, object);
  PutMulti* pm{nullptr};
  {
    std::unique_lock<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    GALOIS_LOG_VASSERT(it != xferm.end(),
                       "{:<30} PutMultiAsync3 no bucket/object in map\n", bno);
    pm = &it->second;
    GALOIS_LOG_VASSERT(pm->xfer_ == Xfer::Three,
                       "{:<30} PutMultiAsync3 but state is {}\n", bno,
                       xfer_label.at(it->second.xfer_));

    // Possibly blocking call
    xfer_cv.wait(lk, [pm] { return pm->finished_ >= pm->parts_.size(); });
    pm->xfer_ = Xfer::Four;
  }

  // GALOIS_LOG_VERBOSE("{:<30} PutMultiAsync3 B wait resolved finished {:d}
  // parts size {:d} etags size {:d}\n",
  //      bno, pm->finished_, pm->parts_.size(), pm->part_e_tags_.size());

  Aws::S3::Model::CompletedMultipartUpload completedUpload;
  for (unsigned i = 0; i < pm->part_e_tags_.size(); ++i) {
    Aws::S3::Model::CompletedPart completedPart;
    completedPart.WithPartNumber(i + 1).WithETag(
        ToAwsString(pm->part_e_tags_[i]));
    completedUpload.AddParts(completedPart);
  }

  Aws::S3::Model::CompleteMultipartUploadRequest completeMultipartUploadRequest;
  completeMultipartUploadRequest.WithBucket(ToAwsString(bucket))
      .WithKey(ToAwsString(object))
      .WithUploadId(ToAwsString(pm->upload_id_))
      .WithMultipartUpload(completedUpload);

  {
    std::lock_guard<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    GALOIS_LOG_VASSERT(it != xferm.end(),
                       "{:<30} PutMultiAsync3 no bucket/object in map\n", bno);
    it->second.outcome_fut_ = async_s3_client->CompleteMultipartUploadCallable(
        completeMultipartUploadRequest);
  }

  return galois::ResultSuccess();
}

galois::Result<void> internal::S3PutMultiAsyncFinish(S3AsyncWork& s3aw) {
  std::string bucket = s3aw.GetBucket();
  std::string object = s3aw.GetObject();
  std::string bno    = BucketAndObject(bucket, object);
  PutMulti* pm{nullptr};
  {
    std::lock_guard<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    GALOIS_LOG_VASSERT(it != xferm.end(),
                       "{:<30} PutMultiAsyncFinish no bucket/object in map\n",
                       bno);
    pm = &it->second;
    GALOIS_LOG_VASSERT(it->second.xfer_ == Xfer::Four,
                       "{:<30} PutMultiAsyncFinish but state is {}\n", bno,
                       xfer_label.at(it->second.xfer_));
  }

  auto completeUploadOutcome = pm->outcome_fut_.get(); // Blocking call

  // MultiStagePut is complete
  {
    std::lock_guard<std::mutex> lk(xfer_mutex);
    auto it = xferm.find(bno);
    GALOIS_LOG_VASSERT(
        it != xferm.end(),
        "{:<30} PutMultiAsync2 callback no bucket/object in map\n", bno);
    it->second.xfer_ = Xfer::One;
  }

  if (!completeUploadOutcome.IsSuccess()) {
    /* TODO there are likely some errors we can handle gracefully */
    const auto& error = completeUploadOutcome.GetError();
    GALOIS_LOG_ERROR("\n  Failed to complete mutipart upload\n  {}: {}\n  "
                     "upload id: {}\n [{}] {}",
                     error.GetExceptionName(), error.GetMessage(),
                     pm->upload_id_, bucket, object);
    return ErrorCode::S3Error;
  }
  return galois::ResultSuccess();
}

galois::Result<void> internal::S3PutSingleAsync(S3AsyncWork& s3aw,
                                                const uint8_t* data,
                                                uint64_t size) {
  Aws::S3::Model::PutObjectRequest object_request;
  GALOIS_LOG_VASSERT(library_init == true,
                     "Must call tsuba::Init before S3 interaction");

  object_request.SetBucket(ToAwsString(s3aw.GetBucket()));
  object_request.SetKey(ToAwsString(s3aw.GetObject()));
  auto streamBuf = Aws::New<Aws::Utils::Stream::PreallocatedStreamBuf>(
      kAwsTag, (uint8_t*)data, static_cast<size_t>(size));
  auto preallocatedStreamReader =
      Aws::MakeShared<Aws::IOStream>(kAwsTag, streamBuf);

  object_request.SetBody(preallocatedStreamReader);
  object_request.SetContentType("application/octet-stream");

  s3aw.SetGoal(1);

  auto callback = [&s3aw](const Aws::S3::S3Client* /*client*/,
                          const Aws::S3::Model::PutObjectRequest& /*request*/,
                          const Aws::S3::Model::PutObjectOutcome& outcome,
                          const std::shared_ptr<
                              const Aws::Client::AsyncCallerContext>& /*ctx*/) {
    if (outcome.IsSuccess()) {
      s3aw.GoalMinusOne();
    } else {
      /* TODO there are likely some errors we can handle gracefully
       * i.e., with retries */
      const auto& error = outcome.GetError();
      GALOIS_LOG_FATAL(
          "\n  Failed to complete single async upload\n  {}: {}\n  [{}] {}",
          error.GetExceptionName(), error.GetMessage(), s3aw.GetBucket(),
          s3aw.GetObject());
    }
  };
  async_s3_client->PutObjectAsync(object_request, callback);

  return galois::ResultSuccess();
}

galois::Result<void>
internal::S3PutSingleAsyncFinish(internal::S3AsyncWork& s3aw) {
  s3aw.WaitGoal();
  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<FileAsyncWork>>
S3PutAsync(const std::string& bucket, const std::string& object,
           const uint8_t* data, uint64_t size) {
  std::unique_ptr<internal::S3AsyncWork> s3aw =
      std::make_unique<internal::S3AsyncWork>(bucket, object);
  if (size < kS3DefaultBufSize) {
    auto res = internal::S3PutSingleAsync(*s3aw, data, size);
    if (!res) {
      return res.error();
    }
    s3aw->Push(internal::S3PutSingleAsyncFinish);
  } else {
    auto res = internal::S3PutMultiAsync1(*s3aw, data, size);
    if (!res) {
      return res.error();
    }
    s3aw->Push(internal::S3PutMultiAsyncFinish);
    s3aw->Push(internal::S3PutMultiAsync3);
    s3aw->Push(internal::S3PutMultiAsync2);
  }
  return std::unique_ptr<FileAsyncWork>(std::move(s3aw));
}

static void
PrepareObjectRequest(Aws::S3::Model::GetObjectRequest* object_request,
                     const std::string& bucket, const std::string& object,
                     SegmentedBufferView::BufPart part) {
  object_request->SetBucket(ToAwsString(bucket));
  object_request->SetKey(ToAwsString(object));
  std::ostringstream range;
  /* Knock one byte off the end because range in AWS S3 API is inclusive */
  range << "bytes=" << part.start << "-" << part.end - 1;
  object_request->SetRange(ToAwsString(range.str()));

  object_request->SetResponseStreamFactory([part]() {
    auto* bufferStream = Aws::New<Aws::Utils::Stream::DefaultUnderlyingStream>(
        kAwsTag, Aws::MakeUnique<Aws::Utils::Stream::PreallocatedStreamBuf>(
                     kAwsTag, part.dest, part.end - part.start + 1));
    if (bufferStream == nullptr) {
      abort();
    }
    return bufferStream;
  });
}

galois::Result<void> internal::S3GetMultiAsync(S3AsyncWork& s3aw,
                                               uint64_t start, uint64_t size,
                                               uint8_t* result_buf) {
  SegmentedBufferView bufView = SegmentBuf(start, result_buf, size);
  std::vector<SegmentedBufferView::BufPart> parts(bufView.begin(),
                                                  bufView.end());
  if (parts.empty()) {
    return galois::ResultSuccess();
  }

  s3aw.SetGoal(parts.size());

  auto callback = [&s3aw](const Aws::S3::S3Client* /*client*/,
                          const Aws::S3::Model::GetObjectRequest& /*request*/,
                          const Aws::S3::Model::GetObjectOutcome& outcome,
                          const std::shared_ptr<
                              const Aws::Client::AsyncCallerContext>& /*ctx*/) {
    if (outcome.IsSuccess()) {
      s3aw.GoalMinusOne();
    } else {
      /* TODO there are likely some errors we can handle gracefully
       * i.e., with retries */
      const auto& error = outcome.GetError();
      GALOIS_LOG_FATAL(
          "\n  Failed to complete multi async download\n  {}: {}\n  [{}] {}",
          error.GetExceptionName(), error.GetMessage(), s3aw.GetBucket(),
          s3aw.GetObject());
    }
  };

  for (auto& part : parts) {
    Aws::S3::Model::GetObjectRequest request;
    PrepareObjectRequest(&request, s3aw.GetBucket(), s3aw.GetObject(), part);
    async_s3_client->GetObjectAsync(request, callback);
  }
  return galois::ResultSuccess();
}

galois::Result<void> internal::S3GetMultiAsyncFinish(S3AsyncWork& s3aw) {
  s3aw.WaitGoal();
  // result_buf should have the data here
  return galois::ResultSuccess();
}

galois::Result<std::unique_ptr<FileAsyncWork>>
S3GetAsync(const std::string& bucket, const std::string& object, uint64_t start,
           uint64_t size, uint8_t* result_buf) {
  if (size == (uint64_t)0) {
    return galois::ResultSuccess();
  }
  std::unique_ptr<internal::S3AsyncWork> s3aw =
      std::make_unique<internal::S3AsyncWork>(bucket, object);

  if (auto res = internal::S3GetMultiAsync(*s3aw, start, size, result_buf);
      !res) {
    return res.error();
  }
  s3aw->Push(internal::S3GetMultiAsyncFinish);
  return std::unique_ptr<FileAsyncWork>(std::move(s3aw));
}

galois::Result<void> S3DownloadRange(const std::string& bucket,
                                     const std::string& object, uint64_t start,
                                     uint64_t size, uint8_t* result_buf) {
  auto s3_client              = GetS3Client();
  SegmentedBufferView bufView = SegmentBuf(start, result_buf, size);
  std::vector<SegmentedBufferView::BufPart> parts(bufView.begin(),
                                                  bufView.end());
  if (parts.empty()) {
    return galois::ResultSuccess();
  }

  if (parts.size() == 1) {
    /* skip all of the thread management overhead if we only have one request */
    Aws::S3::Model::GetObjectRequest request;
    PrepareObjectRequest(&request, bucket, object, parts[0]);
    Aws::S3::Model::GetObjectOutcome outcome = s3_client->GetObject(request);
    if (auto res = CheckS3Error(outcome); !res) {
      if (res.error() != ErrorCode::S3Error) {
        // TODO there are likely some errors we can handle gracefully
        // i.e., with retries
        const auto& error = outcome.GetError();
        GALOIS_LOG_ERROR("\n  Failed S3DownloadRange\n  {}: {}\n [{}] {}",
                         error.GetExceptionName(), error.GetMessage(), bucket,
                         object);
      }
      return res.error();
    }
    // result_buf should have the data here
    return galois::ResultSuccess();
  }

  std::mutex m;
  std::condition_variable cv;
  uint64_t finished = 0;
  auto callback =
      [&](const Aws::S3::S3Client* /*clnt*/,
          const Aws::S3::Model::GetObjectRequest& /*req*/,
          const Aws::S3::Model::GetObjectOutcome& get_object_outcome,
          const std::shared_ptr<
              const Aws::Client::AsyncCallerContext>& /*ctx*/) {
        if (get_object_outcome.IsSuccess()) {
          /* result_buf should have our data here */
          std::unique_lock<std::mutex> lk(m);
          finished++;
          cv.notify_one();
        } else {
          /* TODO there are likely some errors we can handle gracefully
           * i.e., with retries */
          const auto& error = get_object_outcome.GetError();
          GALOIS_LOG_FATAL(
              "\n  Failed S3DownloadRange callback\n  {}: {}\n  [{}] {}",
              error.GetExceptionName(), error.GetMessage(), bucket, object);
        }
      };
  for (auto& part : parts) {
    Aws::S3::Model::GetObjectRequest object_request;
    PrepareObjectRequest(&object_request, bucket, object, part);
    s3_client->GetObjectAsync(object_request, callback);
  }

  std::unique_lock<std::mutex> lk(m);
  cv.wait(lk, [&] { return finished >= parts.size(); });

  return galois::ResultSuccess();
}

} /* namespace tsuba */
