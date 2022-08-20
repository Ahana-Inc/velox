/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "velox/exec/RowContainer.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {
/// This class is used by the Window operator to store and retrieve
/// the input rows it receives during processing.
/// The WindowRowStore could use a sorting or hash-based approach for
/// its functions.
/// The current implementation is fully in-memory. But could be
/// enhanced for Spill to disk semantics in the future.
class WindowRowStore {
 public:
  WindowRowStore(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      memory::MappedMemory* mappedMemory);

  int32_t estimatedNumRowsPerBatch(int32_t batchSizeInBytes) {
    return (batchSizeInBytes / data_->fixedRowSize()) +
        ((batchSizeInBytes % data_->fixedRowSize()) ? 1 : 0);
  }

  const WindowPartition* getWindowPartition(vector_size_t /*partitionNumber*/) {
    return windowPartition_.get();
  }

  void addInput(RowVectorPtr input);

  void noMoreInput();

  void
  getRows(vector_size_t startRow, vector_size_t numRows, RowVectorPtr result);

  /// Returns the next partition number to be processed.
  /// Returns -1 if no more partitions.
  vector_size_t nextPartition();

  vector_size_t numPartitionRows(vector_size_t partitionNumber);

  /// This function compares the rows at lhs and rhs in the
  /// current partition for equality on the order by keys.
  /// Rows with the same ORDER BY keys in the partition are
  /// considered peers in the Window function calculations.
  bool peerCompare(vector_size_t lhs, vector_size_t rhs);

 private:
  // Helper function to compare the rows at lhs and rhs pointers
  // using the keyInfo in keys. This can be used to compare the
  // rows for partitionKeys, orderByKeys or a combination of both.
  inline bool compareRowsWithKeys(
      const char* lhs,
      const char* rhs,
      const std::vector<std::pair<column_index_t, core::SortOrder>>& keys);

  // This function is invoked after receiving all the input data.
  // The input data needs to be separated into partitions and
  // ordered within it (as that is the order in which the rows
  // will be output for the partition).
  // This function achieves this by ordering the input rows by
  // (partition keys + order by keys). Doing so orders all rows
  // of a partition adjacent to each other and sorted by the
  // ORDER BY clause.
  void sortPartitions();

  // Function to compute the partitionStartRows_ structure.
  // partitionStartRows_ is vector of the starting rows index
  // of each partition in the data. This is an auxiliary
  // structure that helps simplify partition traversals
  // in the RowStore.
  void computePartitionStartRows();

  // This function initializes the new partition rows in the
  // WindowPartition.
  void advanceWindowPartition();

  const vector_size_t numInputColumns_;

  // Number of input rows.
  vector_size_t numRows_ = 0;

  // The Window operator needs to see all the input rows before starting
  // any function computation. As the Window operators gets input rows
  // we store the rows in the RowContainer (data_).
  std::unique_ptr<RowContainer> data_;
  // The decodedInputVectors_ are reused across addInput() calls to decode
  // the partition and sort keys for the above RowContainer.
  std::vector<DecodedVector> decodedInputVectors_;

  // This SelectivityVector is used across addInput calls for decoding.
  SelectivityVector inputRows_;

  // The below 3 vectors represent the ChannelIndex of the partition keys,
  // the order by keys and the concatenation of the 2. These keyInfo are
  // used for sorting by those key combinations during the processing.
  // partitionKeyInfo_ is used to separate partitions in the rows.
  // sortKeyInfo_ is used to identify peer rows in a partition.
  // allKeyInfo_ is a combination of (partitionKeyInfo_ and sortKeyInfo_).
  // It is used to perform a full sorting of the input rows to be able to
  // separate partitions and sort the rows in it. The rows are output in
  // this order by the operator.
  std::vector<std::pair<column_index_t, core::SortOrder>> partitionKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> sortKeyInfo_;
  std::vector<std::pair<column_index_t, core::SortOrder>> allKeyInfo_;

  // Vector of pointers to each input row in the data_ RowContainer.
  // The rows are sorted by partitionKeys + sortKeys. This total
  // ordering can be used to split partitions (with the correct
  // order by) for the processing.
  std::vector<char*> sortedRows_;
  // This is a vector that gives the index of the start row
  // (in sortedRows_) of each partition in the RowContainer data_.
  // This auxiliary structure helps demarcate partitions in
  // getOutput calls.
  std::vector<vector_size_t> partitionStartRows_;

  // Current partition being output.
  vector_size_t currentPartition_;

  // Window partition object used to provide per-partition
  // data to the window function.
  std::unique_ptr<WindowPartition> windowPartition_;
};

} // namespace facebook::velox::exec