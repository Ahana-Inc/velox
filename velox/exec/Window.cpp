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
#include "velox/exec/Window.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/exec/Task.h"
#include "velox/exec/WindowPartition.h"

namespace facebook::velox::exec {

namespace {
vector_size_t findFrameEndPointUnboundedPreceding(
    core::WindowNode::WindowType /*type*/,
    vector_size_t /*functionNumber*/,
    vector_size_t partitionStartRow,
    vector_size_t /*partitionEndRow*/,
    vector_size_t /*currentRow*/,
    std::optional<column_index_t> channel) {
  return partitionStartRow;
}

vector_size_t findFrameEndPointKPreceding(
    core::WindowNode::WindowType type,
    vector_size_t functionNumber,
    vector_size_t partitionStartRow,
    vector_size_t /*partitionEndRow*/,
    vector_size_t currentRow,
    std::optional<column_index_t> channel) {
  auto frameStartValue = channel;
  if (!frameStartValue || type != core::WindowNode::WindowType::kRows) {
    VELOX_FAIL("k preceding as frame start is allowed only in ROWS mode");
  }
  return std::max(
      currentRow - vector_size_t(*frameStartValue), partitionStartRow);
}

vector_size_t findFrameEndPointCurrentRow(
    core::WindowNode::WindowType /*type*/,
    vector_size_t /*functionNumber*/,
    vector_size_t /*partitionStartRow*/,
    vector_size_t /*partitionEndRow*/,
    vector_size_t currentRow,
    std::optional<column_index_t> channel) {
  return currentRow;
}

vector_size_t findFrameEndPointKFollowing(
    core::WindowNode::WindowType type,
    vector_size_t functionNumber,
    vector_size_t /*partitionStartRow*/,
    vector_size_t partitionEndRow,
    vector_size_t currentRow,
    std::optional<column_index_t> channel) {
  auto frameEndValue = channel;
  if (!frameEndValue || type != core::WindowNode::WindowType::kRows) {
    VELOX_FAIL("k preceding as frame end is allowed only in ROWS mode");
  }
  return std::min(
      currentRow + vector_size_t(*frameEndValue), partitionEndRow - 1);
}

vector_size_t findFrameEndPointUnboundedFollowing(
    core::WindowNode::WindowType /*type*/,
    vector_size_t /*functionNumber*/,
    vector_size_t /*partitionStartRow*/,
    vector_size_t partitionEndRow,
    vector_size_t /*currentRow*/,
    std::optional<column_index_t> channel) {
  return partitionEndRow - 1;
}

}; // namespace

Window::Window(
    int32_t operatorId,
    DriverCtx* driverCtx,
    const std::shared_ptr<const core::WindowNode>& windowNode)
    : Operator(
          driverCtx,
          windowNode->outputType(),
          operatorId,
          windowNode->id(),
          "Window"),
      outputBatchSizeInBytes_(
          driverCtx->queryConfig().preferredOutputBatchSize()),
      numInputColumns_(windowNode->sources()[0]->outputType()->size()),
      rowStore_(std::make_unique<WindowRowStore>(
          windowNode,
          operatorCtx_->mappedMemory())) {
  auto inputType = windowNode->sources()[0]->outputType();
  createWindowFunctions(windowNode, inputType);
}

void Window::createWindowFunctions(
    const std::shared_ptr<const core::WindowNode>& windowNode,
    const RowTypePtr& inputType) {
  auto fieldArgToChannel =
      [&](const core::TypedExprPtr arg) -> std::optional<column_index_t> {
    if (arg) {
      std::optional<column_index_t> argChannel =
          exprToChannel(arg.get(), inputType);
      VELOX_CHECK(
          argChannel.value() != kConstantChannel,
          "Window doesn't allow constant arguments or frame end-points");
      return argChannel;
    }
    return std::nullopt;
  };

  for (const auto& windowNodeFunction : windowNode->windowFunctions()) {
    std::vector<TypePtr> argTypes;
    std::vector<column_index_t> argIndices;
    argTypes.reserve(windowNodeFunction.functionCall->inputs().size());
    argIndices.reserve(windowNodeFunction.functionCall->inputs().size());
    for (auto& arg : windowNodeFunction.functionCall->inputs()) {
      argTypes.push_back(arg->type());
      argIndices.push_back(fieldArgToChannel(arg).value());
    }

    windowFunctions_.push_back(WindowFunction::create(
        windowNodeFunction.functionCall->name(),
        argTypes,
        argIndices,
        windowNodeFunction.functionCall->type(),
        operatorCtx_->pool()));

    windowFrames_.push_back(
        {windowNodeFunction.frame.type,
         windowNodeFunction.frame.startType,
         windowNodeFunction.frame.endType,
         fieldArgToChannel(windowNodeFunction.frame.startValue),
         fieldArgToChannel(windowNodeFunction.frame.endValue)});
  }
}

void Window::addInput(RowVectorPtr input) {
  rowStore_->addInput(input);
  numRows_ += input->size();
}

void setWindowFrameStart(
    core::WindowNode::BoundType startType,
    windowFrameFunctionPtr& windowFrameFunction) {
  switch (startType) {
    case core::WindowNode::BoundType::kUnboundedPreceding:
      windowFrameFunction = &findFrameEndPointUnboundedPreceding;
      break;
    case core::WindowNode::BoundType::kPreceding:
      windowFrameFunction = &findFrameEndPointKPreceding;
      break;
    case core::WindowNode::BoundType::kCurrentRow:
      windowFrameFunction = findFrameEndPointCurrentRow;
      break;
    case core::WindowNode::BoundType::kFollowing:
      windowFrameFunction = &findFrameEndPointKFollowing;
      break;
    default:
      VELOX_FAIL("Invalid frame start value");
  }
}

void setWindowFrameEnd(
    core::WindowNode::BoundType endType,
    windowFrameFunctionPtr& windowFrameFunction) {
  switch (endType) {
    case core::WindowNode::BoundType::kPreceding:
      windowFrameFunction = &findFrameEndPointKPreceding;
      break;
    case core::WindowNode::BoundType::kCurrentRow:
      windowFrameFunction = &findFrameEndPointCurrentRow;
      break;
    case core::WindowNode::BoundType::kFollowing:
      windowFrameFunction = &findFrameEndPointKFollowing;
      break;
    case core::WindowNode::BoundType::kUnboundedFollowing:
      windowFrameFunction = &findFrameEndPointUnboundedFollowing;
      break;
    default:
      VELOX_FAIL("Invalid frame end value");
  }
}

void Window::createPeerAndFrameBuffers() {
  // TODO: This computation needs to be revised. It only takes into account
  // the input columns size. We need to also account for the output columns.
  numRowsPerOutput_ =
      rowStore_->estimatedNumRowsPerBatch(outputBatchSizeInBytes_);

  peerStartBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());
  peerEndBuffer_ = AlignedBuffer::allocate<vector_size_t>(
      numRowsPerOutput_, operatorCtx_->pool());

  auto numFuncs = windowFunctions_.size();
  frameStartBuffers_.reserve(numFuncs);
  frameEndBuffers_.reserve(numFuncs);

  for (auto i = 0; i < numFuncs; i++) {
    BufferPtr frameStartBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    BufferPtr frameEndBuffer = AlignedBuffer::allocate<vector_size_t>(
        numRowsPerOutput_, operatorCtx_->pool());
    frameStartBuffers_.push_back(frameStartBuffer);
    frameEndBuffers_.push_back(frameEndBuffer);

    windowFrameFunctionPtr windowFrameStart;
    windowFrameFunctionPtr windowFrameEnd;
    setWindowFrameStart(windowFrames_[i].startType, windowFrameStart);
    setWindowFrameEnd(windowFrames_[i].endType, windowFrameEnd);
    windowFunctions_[i]->setFrameStartBoundFunction(windowFrameStart);
    windowFunctions_[i]->setFrameEndBoundFunction(windowFrameEnd);
  }
}

void Window::noMoreInput() {
  Operator::noMoreInput();
  // No data.
  if (numRows_ == 0) {
    finished_ = true;
    return;
  }

  // At this point we have seen all the input rows. We can start
  // outputting rows now.
  // Finalize the rowStore which can be used subsequently to
  // process the rows in order.
  rowStore_->noMoreInput();

  createPeerAndFrameBuffers();

  callResetPartition(0);
}

void Window::callResetPartition(vector_size_t partitionNumber) {
  auto windowPartition = rowStore_->getWindowPartition(partitionNumber);
  for (int i = 0; i < windowFunctions_.size(); i++) {
    windowFunctions_[i]->resetPartition(windowPartition);
  }
  numPartitionProcessedRows_ = 0;
  numPartitionRows_ = rowStore_->numPartitionRows(partitionNumber);
}

void Window::callApplyForPartitionRows(
    vector_size_t startRow,
    vector_size_t endRow,
    const std::vector<VectorPtr>& result,
    vector_size_t resultOffset) {
  vector_size_t numRows = endRow - startRow;
  vector_size_t numFuncs = windowFunctions_.size();

  // Size buffers for the call to WindowFunction::apply.
  auto bufferSize = numRows * sizeof(vector_size_t);
  peerStartBuffer_->setSize(bufferSize);
  peerEndBuffer_->setSize(bufferSize);
  auto rawPeerStartBuffer = peerStartBuffer_->asMutable<vector_size_t>();
  auto rawPeerEndBuffer = peerEndBuffer_->asMutable<vector_size_t>();

  std::vector<vector_size_t*> rawFrameStartBuffers;
  std::vector<vector_size_t*> rawFrameEndBuffers;
  rawFrameStartBuffers.reserve(numFuncs);
  rawFrameEndBuffers.reserve(numFuncs);
  for (auto w = 0; w < numFuncs; w++) {
    frameStartBuffers_[w]->setSize(bufferSize);
    frameEndBuffers_[w]->setSize(bufferSize);

    auto rawFrameStartBuffer =
        frameStartBuffers_[w]->asMutable<vector_size_t>();
    auto rawFrameEndBuffer = frameEndBuffers_[w]->asMutable<vector_size_t>();
    rawFrameStartBuffers.push_back(rawFrameStartBuffer);
    rawFrameEndBuffers.push_back(rawFrameEndBuffer);
  }

  // Setup values in the peer and frame buffers.
  auto firstPartitionRow = 0;
  auto lastPartitionRow = numPartitionRows_ - 1;
  for (auto i = startRow, j = 0; i < endRow; i++, j++) {
    // When traversing input partition rows, the peers are the rows
    // with the same values for the ORDER BY clause. These rows
    // are equal in some ways and affect the results of ranking functions.
    // This logic exploits the fact that all rows between the peerStartRow_
    // and peerEndRow_ have the same values for peerStartRow_ and peerEndRow_.
    // So we can compute them just once and reuse across the rows in that peer
    // interval.

    // Compute peerStart and peerEnd rows for the first row of the partition or
    // when past the previous peerGroup.
    if (i == firstPartitionRow || i >= peerEndRow_) {
      peerStartRow_ = i;
      peerEndRow_ = i;
      while (peerEndRow_ <= lastPartitionRow) {
        if (rowStore_->peerCompare(peerStartRow_, peerEndRow_)) {
          break;
        }
        peerEndRow_++;
      }
    }

    rawPeerStartBuffer[j] = peerStartRow_;
    rawPeerEndBuffer[j] = peerEndRow_ - 1;

    for (auto w = 0; w < numFuncs; w++) {
      rawFrameStartBuffers[w][j] =
          windowFunctions_[w]->getFrameStartBoundFunction()(
              windowFrames_[w].type,
              w,
              0,
              numPartitionRows_,
              i,
              windowFrames_[w].startChannel);
      rawFrameEndBuffers[w][j] =
          windowFunctions_[w]->getFrameEndBoundFunction()(
              windowFrames_[w].type,
              w,
              0,
              numPartitionRows_,
              i,
              windowFrames_[w].endChannel);
    }
  }

  // Invoke the apply method for the WindowFunctions
  for (auto w = 0; w < numFuncs; w++) {
    windowFunctions_[w]->apply(
        peerStartBuffer_,
        peerEndBuffer_,
        frameStartBuffers_[w],
        frameEndBuffers_[w],
        resultOffset,
        result[w]);
  }

  numPartitionProcessedRows_ += numRows;
  numProcessedRows_ += numRows;
  if (endRow == numPartitionRows_) {
    auto currentPartition = rowStore_->nextPartition();
    // The WindowRowStore returns -1 if no more partitions
    if (currentPartition != -1) {
      callResetPartition(currentPartition);
    }
  }
}

void Window::callApplyLoop(
    vector_size_t numOutputRows,
    const std::vector<VectorPtr>& windowOutputs) {
  // Compute outputs by traversing as many partitions as possible. This
  // logic takes care of partial partitions output also.

  vector_size_t resultIndex = 0;
  vector_size_t numOutputRowsLeft = numOutputRows;
  while (numOutputRowsLeft > 0) {
    auto rowsForCurrentPartition =
        numPartitionRows_ - numPartitionProcessedRows_;
    if (rowsForCurrentPartition <= numOutputRowsLeft) {
      // Current partition can fit completely in the output buffer.
      // So output all its rows.
      callApplyForPartitionRows(
          numPartitionProcessedRows_,
          numPartitionRows_,
          windowOutputs,
          resultIndex);
      resultIndex += rowsForCurrentPartition;
      numOutputRowsLeft -= rowsForCurrentPartition;
    } else {
      // Current partition can fit only partially in the output buffer.
      // Call apply for the rows that can fit in the buffer and break from
      // outputting.
      callApplyForPartitionRows(
          numPartitionProcessedRows_,
          numPartitionProcessedRows_ + numOutputRowsLeft,
          windowOutputs,
          resultIndex);
      numOutputRowsLeft = 0;
      break;
    }
  }
}

RowVectorPtr Window::getOutput() {
  if (finished_ || !noMoreInput_) {
    return nullptr;
  }

  auto numRowsLeft = numRows_ - numProcessedRows_;
  auto numOutputRows = std::min(numRowsPerOutput_, numRowsLeft);
  auto result = std::dynamic_pointer_cast<RowVector>(
      BaseVector::create(outputType_, numOutputRows, operatorCtx_->pool()));

  // Get all passthrough input columns.
  rowStore_->getRows(numProcessedRows_, numOutputRows, result);

  // Construct vectors for the window function output columns.
  std::vector<VectorPtr> windowOutputs;
  windowOutputs.reserve(windowFunctions_.size());
  for (int i = numInputColumns_; i < outputType_->size(); i++) {
    auto output = BaseVector::create(
        outputType_->childAt(i), numOutputRows, operatorCtx_->pool());
    windowOutputs.emplace_back(std::move(output));
  }

  // Compute the output values of window functions.
  callApplyLoop(numOutputRows, windowOutputs);

  for (int j = numInputColumns_; j < outputType_->size(); j++) {
    result->childAt(j) = windowOutputs[j - numInputColumns_];
  }

  finished_ = (numProcessedRows_ == numRows_);
  return result;
}

} // namespace facebook::velox::exec
