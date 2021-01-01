#ifndef KATANA_LIBTSUBA_TSUBA_FILEVIEW_H_
#define KATANA_LIBTSUBA_TSUBA_FILEVIEW_H_

#include <cstdint>
#include <future>
#include <string>

#include <parquet/arrow/reader.h>

#include "katana/Logging.h"
#include "katana/Result.h"
#include "katana/config.h"

namespace tsuba {

class KATANA_EXPORT FileView : public arrow::io::RandomAccessFile {
  struct FillingRange {
    uint64_t first_page;
    uint64_t last_page;
    std::future<katana::Result<void>> work;
  };

  uint8_t* map_start_;
  int64_t file_size_;
  uint8_t page_shift_;
  int64_t cursor_;
  int64_t mem_start_;
  std::string filename_;
  bool valid_ = false;
  std::vector<uint64_t> filling_;
  std::unique_ptr<std::vector<FillingRange>> fetches_;

public:
  FileView() = default;
  FileView(const FileView&) = delete;
  FileView& operator=(const FileView&) = delete;

  FileView(FileView&& other) noexcept
      : map_start_(other.map_start_),
        file_size_(other.file_size_),
        page_shift_(other.page_shift_),
        cursor_(other.cursor_),
        mem_start_(other.mem_start_),
        filename_(std::move(other.filename_)),
        valid_(other.valid_),
        filling_(std::move(other.filling_)),
        fetches_(std::move(other.fetches_)) {
    other.valid_ = false;
  }

  FileView& operator=(FileView&& other) noexcept {
    if (&other != this) {
      if (auto res = Unbind(); !res) {
        KATANA_LOG_ERROR("Unbind: {}", res.error());
      }
      map_start_ = other.map_start_;
      file_size_ = other.file_size_;
      page_shift_ = other.page_shift_;
      cursor_ = other.cursor_;
      mem_start_ = other.mem_start_;
      filename_ = std::move(other.filename_);
      valid_ = other.valid_;
      filling_ = std::move(other.filling_);
      fetches_ =
          std::unique_ptr<std::vector<FillingRange>>(std::move(other.fetches_));
      other.valid_ = false;
    }
    return *this;
  }

  ~FileView();

  bool Equals(const FileView& other) const;

  /// \param resolve determines whether the bound region is loaded
  /// asynchronously or synchronously.
  /// \param filename path to the file to load
  /// \param begin first byte of file to load at this time
  /// \param end last byte of file to load at this time
  /// Calls to Read will handle asynchronous
  /// reads internally, but if you intend to use ptr(), you should pass
  /// resolve=true.
  katana::Result<void> Bind(
      std::string_view filename, uint64_t begin, uint64_t end, bool resolve);
  katana::Result<void> Bind(
      std::string_view filename, uint64_t stop, bool resolve) {
    return Bind(filename, 0, stop, resolve);
  }
  katana::Result<void> Bind(std::string_view filename, bool resolve) {
    return Bind(filename, 0, std::numeric_limits<uint64_t>::max(), resolve);
  }

  katana::Result<void> Fill(uint64_t begin, uint64_t end, bool resolve);

  bool Valid() const { return valid_; }

  katana::Result<void> Unbind();

  /// Be very careful with this function. It is the caller's responsibility to
  /// ensure that the region returned holds meaningful data.
  ///
  /// \param offset is an offset into the file.
  ///
  /// \returns a pointer to the virtual memory region reserved for holding the
  /// file
  template <typename T>
  const T* ptr(uint64_t offset) const {
    return reinterpret_cast<T*>(map_start_ + offset); /* NOLINT */
  }

  template <typename T>
  const T* ptr() const {
    return ptr<T>(0);
  }

  /// This version is a little safer than ptr. It will return a pointer to the
  /// first bytes of the file that are present in memory, if there are any. It
  /// will return  nullptr otherwise.
  template <typename T>
  const T* valid_ptr() const {
    if (mem_start_ < 0) {
      return nullptr;
    }
    return reinterpret_cast<T*>(map_start_ + mem_start_);
  }

  uint64_t size() const { return file_size_; }

  // support iterating through characters
  const char* begin() { return ptr<char>(); }
  const char* end() { return ptr<char>() + size(); }

  ///// Begin arrow::io::RandomAccessFile methods ///////

  virtual arrow::Status Close();
  virtual arrow::Result<int64_t> Tell() const;
  virtual bool closed() const;
  virtual arrow::Status Seek(int64_t);
  virtual arrow::Result<int64_t> Read(int64_t, void*);
  virtual arrow::Result<std::shared_ptr<arrow::Buffer>> Read(int64_t);
  virtual arrow::Result<int64_t> GetSize();

  ///// End arrow::io::RandomAccessFile methods ///////

private:
  // Given the size of some region, how many pages does it take up?
  uint64_t page_number(uint64_t size);

  // helper functions for MustFill
  /// These functions do not validate their input. In particular, if
  /// bitmap[block_num] contains no zeroes, they may never terminate
  inline uint64_t LastPage(
      uint64_t* bitmap, uint64_t block_num, uint64_t start, uint64_t end);
  inline uint64_t FirstPage(
      uint64_t* bitmap, uint64_t block_num, uint64_t start, uint64_t end);

  // Given a starting and ending page, return an inclusive region that describes
  // which pages must be fetched from storage. Or an empty std::optional if no
  // pages need to be fetched.
  std::optional<std::pair<uint64_t, uint64_t>> MustFill(
      uint64_t* bitmap, uint64_t begin, uint64_t end);

  katana::Result<void> MarkFilled(
      uint64_t* bitmap, uint64_t begin, uint64_t end);

  // Resolve all outstanding reads that overlap with the range [cursor_, nbytes]
  katana::Result<void> Resolve(int64_t start, int64_t size);

  // Start asynchronously fetching data that we think we might need from storage
  // @start and @size give the location and range of the previous read
  katana::Result<void> PreFetch(int64_t start, int64_t size);
};
}  // namespace tsuba

#endif
