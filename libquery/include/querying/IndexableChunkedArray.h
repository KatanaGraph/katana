#ifndef KATANA_INDEXABLE_CHUNKED_ARRAY
#define KATANA_INDEXABLE_CHUNKED_ARRAY
/**
 * Copyright (C) 2020, KatanaGraph
 */

/**
 * @file IndexableChunkedArray.h
 *
 * Contains declaration for IndexableChunkedArray, which wraps an arrow Chunked
 * Array and gives an interface for easy indexing by returning a chunk and an
 * index in the chunk given a global index. (A chunked array contains multiple
 * arrays, so index into i requires determining which chunk index i is in first
 * as well as the offset into that chunk.)
 *
 * Accesses to the array itself should still be done via ChunkedArray.
 */

#include "galois/gstl.h"
#include "arrow/api.h"
#include <utility>

/**
 * Wrapper around an arrow chunked arrow that allows users to calculate the
 * chunk and offset into the chunk given some global index into the array.
 */
class IndexableChunkedArray {
public:
  //! Chunk and index into an array: types used are the same types used by the
  //! arrow interface
  using ChunkIndexPair = std::pair<int, int64_t>;

private:
  //! Pointer to the arrow chunked array that the user wants to index into
  std::shared_ptr<arrow::ChunkedArray> original_array;
  //! Total number of elements in chunked array
  int64_t num_elements;
  //! Number of chunks in the array
  int num_chunks;
  //! Prefix sum of chunks (how many elements are in each chunk)
  galois::gstl::Vector<int64_t> chunk_prefix_sum;

public:
  //! default constructor doesn't exist; needs a pointer to the chunked array
  IndexableChunkedArray() = delete;
  //! Makes a copy of the chunked array pointer and calculates the prefix sum
  //! of elements in chunks
  IndexableChunkedArray(std::shared_ptr<arrow::ChunkedArray>);
  // TODO copy/move constructor as necessary/for completeness?

  //! Returns number of elements in the chunked array
  int64_t size();

  //! Given a global index, return the chunk and the offset into that chunk
  //! that corresponds to that global index into this chunked array
  ChunkIndexPair getChunkAndIndex(int64_t global_index);

  //! Returns the underlying chunked array.
  const std::shared_ptr<arrow::ChunkedArray>& getChunkedArray();
};
#endif
