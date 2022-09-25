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

#include "velox/exec/Operator.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/WindowRowStore.h"

namespace facebook::velox::exec {

/// This is a very simple in-Memory implementation of a Window Operator
/// to compute window functions.
///
/// This operator uses a very naive algorithm that sorts all the input
/// data with a combination of the (partition_by keys + order_by keys)
/// to obtain a full ordering of the input. We can easily identify
/// partitions while traversing this sorted data in order.
/// It is also sorted in the order required for the WindowFunction
/// to process it.
///
/// We will revise this algorithm in the future using a HashTable based
/// approach pending some profiling results.
class Window : public Operator {
 public:
  Window(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::WindowNode>& windowNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* /* unused */) override {
    return BlockingReason::kNotBlocked;
  }

  bool isFinished() override {
    return finished_;
  }

 private:
  // Structure for the window frame for each function.
  struct WindowFrame {
    const core::WindowNode::WindowType type;
    const core::WindowNode::BoundType startType;
    const core::WindowNode::BoundType endType;
    const std::optional<bool> isStartBoundConstant;
    const std::optional<vector_size_t> startValue;
    const std::optional<bool> isEndBoundConstant;
    const std::optional<vector_size_t> endValue;
  };

  // Helper function to create WindowFunction and frame objects
  // for this operator.
  void createWindowFunctions(
      const std::shared_ptr<const core::WindowNode>& windowNode,
      const RowTypePtr& inputType);

  // Helper function to create the buffers for peer and frame
  // row indices to send in window function apply invocations.
  void createPeerAndFrameBuffers();

  // Helper function to call WindowFunction::resetPartition() for
  // all WindowFunctions.
  void callResetPartition(vector_size_t partitionNumber);

  // Helper method to call WindowFunction::apply to all the rows
  // of a partition between startRow and endRow. The outputs
  // will be written to the vectors in windowFunctionOutputs
  // starting at offset resultIndex.
  void callApplyForPartitionRows(
      vector_size_t startRow,
      vector_size_t endRow,
      const std::vector<VectorPtr>& result,
      vector_size_t resultOffset);

  // Return the vector pointers to
  std::pair<VectorPtr, VectorPtr> computeVariableFrameBounds(
      VectorPtr& precedingVector,
      VectorPtr& followingVector,
      vector_size_t& startOffset,
      vector_size_t& endOffset);

  // This function is to find the frame end points for the current row
  // being output.
  // @param functionNumber  Index of the window function whose frame we
  // are computing.
  // @param partitionStartRow  Index of the start row of the current
  // partition being output.
  // @param partitionEndRow  Index of the end row of the current
  // partition being output.
  // @param currentRow  Index of the current row.
  // partitionStartRow, partitionEndRow and currentRow are indexes in
  // the sortedRows_ ordering of input rows.
  std::pair<vector_size_t, vector_size_t> findFrameEndPoints(
      vector_size_t functionNumber,
      vector_size_t partitionStartRow,
      vector_size_t partitionEndRow,
      vector_size_t currentRow,
      std::optional<vector_size_t> peerStart,
      std::optional<vector_size_t> peerEnd,
      std::optional<vector_size_t> offsetStart,
      std::optional<vector_size_t> offsetEnd);

  // Function to compute window function values for the current output
  // buffer. The buffer has numOutputRows number of rows. windowOutputs
  // has the vectors for window function columns.
  void callApplyLoop(
      vector_size_t numOutputRows,
      const std::vector<VectorPtr>& windowOutputs);

  bool finished_ = false;
  const vector_size_t numInputColumns_;

  const vector_size_t outputBatchSizeInBytes_;
  // Number of rows that be fit into an output block.
  vector_size_t numRowsPerOutput_;

  // The Window operator needs to accumulate all input rows,
  // process them into partitions, order the partitions and
  // output them. The WindowRowStore is used to achieve these
  // functions.
  std::unique_ptr<WindowRowStore> rowStore_;

  // Vector of WindowFunction objects required by this operator.
  // WindowFunction is the base API implemented by all the window functions.
  // The functions are ordered by their positions in the output columns.
  std::vector<std::unique_ptr<exec::WindowFunction>> windowFunctions_;
  // Vector of WindowFrames corresponding to each windowFunction above.
  // It represents the frame spec for the function computation.
  std::vector<WindowFrame> windowFrames_;

  // The following 4 Buffers are used to pass peer and frame start and
  // end values to the WindowFunction::apply method. These
  // buffers can be allocated once and reused across all the getOutput
  // calls.
  // Only a single peer start and peer end buffer is needed across all
  // functions (as the peer values are based on the ORDER BY clause).
  BufferPtr peerStartBuffer_;
  BufferPtr peerEndBuffer_;
  // A separate BufferPtr is required for the frame indexes of each
  // function. Each function has its own frame clause and style. So we
  // have as many buffers as the number of functions.
  std::vector<BufferPtr> frameStartBuffers_;
  std::vector<BufferPtr> frameEndBuffers_;

  // Number of input rows.
  vector_size_t numRows_ = 0;

  // Number of rows output from the WindowOperator so far.
  vector_size_t numProcessedRows_ = 0;

  // The below 2 variables are related to the current partition
  // being output. As the partition might be output across
  // multiple getOutput() calls, so they are tracked in the operator.
  // Total number of rows in the current partition.
  vector_size_t numPartitionRows_;
  // Number of rows of the current partition that have been output.
  vector_size_t numPartitionProcessedRows_;

  // When traversing input partition rows, the peers are the rows
  // with the same values for the ORDER BY clause. These rows
  // are equal in some ways and affect the results of ranking functions.
  // Since all rows between the peerStartRow_ and peerEndRow_ have the same
  // values for peerStartRow_ and peerEndRow_, we needn't compute
  // them for each row independently. Since these rows might
  // cross getOutput boundaries they are saved in the operator.
  vector_size_t peerStartRow_ = 0;
  vector_size_t peerEndRow_ = 0;
};

} // namespace facebook::velox::exec
