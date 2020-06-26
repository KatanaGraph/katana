/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file IndexableChunkedArray.cpp
 *
 * Implementation of IndexableChunkedArray.
 */
#include "querying/IndexableChunkedArray.h"
#include "galois/Logging.h"

IndexableChunkedArray::IndexableChunkedArray(
    std::shared_ptr<arrow::ChunkedArray> _original_array)
    : original_array(_original_array), num_elements(_original_array->length()),
      num_chunks(_original_array->num_chunks()) {
  // prefix sum starting at 0
  this->chunk_prefix_sum.resize(this->num_chunks + 1);

  // get number of elements in each chunk
  for (int i = 0; i < this->num_chunks; i++) {
    int64_t chunk_size            = this->original_array->chunk(i)->length();
    this->chunk_prefix_sum[i + 1] = chunk_size;
  }

  // get prefix sum by adding them up from left to right
  for (int i = 1; i < this->num_chunks + 1; i++) {
    this->chunk_prefix_sum[i] += this->chunk_prefix_sum[i - 1];
  }
}

int64_t IndexableChunkedArray::size() { return this->num_elements; }

IndexableChunkedArray::ChunkIndexPair
IndexableChunkedArray::getChunkAndIndex(int64_t global_index) {
  // find which chunk the global index is in via prefix sum
  int found_chunk = -1;
  // assumption is that there aren't many chunks, else a serial scan of the
  // prefix sum would be pretty expensive
  for (int i = 0; i < this->num_chunks; i++) {
    int64_t lower_bound = this->chunk_prefix_sum[i];
    int64_t upper_bound = this->chunk_prefix_sum[i + 1];
    // check if it fits in this chunk
    if (lower_bound <= global_index && global_index < upper_bound) {
      found_chunk = i;
      break;
    }
  }

  if (found_chunk != -1) {
    // get offset into the chunk, which is just # elements in chunks before it
    // subtracted from it
    int64_t offset = global_index - this->chunk_prefix_sum[found_chunk];
    return std::make_pair(found_chunk, offset);
  } else {
    // not found; return -1, -1
    // TODO maybe return an error via Outcome?
    GALOIS_LOG_ERROR("global index is out of bounds of chunked array");
    return std::make_pair(-1, -1);
  }
}

const std::shared_ptr<arrow::ChunkedArray>&
IndexableChunkedArray::getChunkedArray() {
  return this->original_array;
}
