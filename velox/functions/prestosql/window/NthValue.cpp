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
#include "velox/common/base/Exceptions.h"
#include "velox/exec/WindowFunction.h"
#include "velox/expression/FunctionSignature.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::window {

namespace {

template <typename T>
class NthValueFunction : public exec::WindowFunction {
 public:
  explicit NthValueFunction(
      const std::vector<column_index_t>& argIndices,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool)
      : WindowFunction(resultType, pool), argIndices_(argIndices) {
    // TODO : Put a better estimation of the buffer size here.
    offsetsBuffer_ = AlignedBuffer::allocate<vector_size_t>(1000, pool);
    rawOffsetsBuffer_ = offsetsBuffer_->asMutable<vector_size_t>();
    inputOffsetsVector_ = BaseVector::create(INTEGER(), 0, pool_);
    inputOffsetsFlatVector_ = inputOffsetsVector_->template asFlatVector<int>();
  }

  void resetPartition(const exec::WindowPartition* partition) {
    partition_ = partition;
    partitionOffset_ = 0;
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      int32_t resultOffset,
      const VectorPtr& result) {
    auto numRows = frameStarts->size() / sizeof(vector_size_t);
    auto frameStartsVector = frameStarts->as<vector_size_t>();
    auto frameEndsVector = frameEnds->as<vector_size_t>();

    offsetsBuffer_->setSize(numRows * sizeof(vector_size_t));
    inputOffsetsVector_->setSize(numRows);
    partition_->extractColumn(
        argIndices_[1], numRows, partitionOffset_, inputOffsetsVector_);

    for (int i = 0; i < numRows; i++) {
      auto inputOffset = inputOffsetsFlatVector_->valueAt(partitionOffset_ + i);
      auto frameStart = frameStartsVector[partitionOffset_ + i];
      auto frameEnd = frameEndsVector[partitionOffset_ + i];

      if (frameStart + inputOffset - 1 <= frameEnd) {
        rawOffsetsBuffer_[i] = frameStart + inputOffset - 1;
      } else {
        rawOffsetsBuffer_[i] = -1;
      }
    }

    partition_->extractColumn(
        argIndices_[0], offsetsBuffer_, resultOffset, result);

    partitionOffset_ += numRows;
  }

 private:
  const exec::WindowPartition* partition_;
  // This is the index of the nth_value arguments in the WindowNode
  // input columns list.
  const std::vector<column_index_t> argIndices_;

  // This offset tracks how far along the partition rows have been output.
  // This is used to index into the argument vectors from the WindowPartition
  // while outputting all the rows for the partition.
  vector_size_t partitionOffset_;

  // The NthValue function uses the extractColumnAtOffsets API of the
  // WindowPartition. The offsetsBuffer_ is used as an input to that function.
  BufferPtr offsetsBuffer_;
  // Raw pointer to the above buffer.
  vector_size_t* rawOffsetsBuffer_;

  VectorPtr inputOffsetsVector_;
  FlatVector<int>* inputOffsetsFlatVector_;
};

template <TypeKind kind>
std::unique_ptr<exec::WindowFunction> createNthValueFunction(
    const std::vector<column_index_t>& argIndices,
    const TypePtr& resultType,
    velox::memory::MemoryPool* pool) {
  using T = typename TypeTraits<kind>::NativeType;
  return std::make_unique<NthValueFunction<T>>(argIndices, resultType, pool);
}

} // namespace

void registerNthValue(const std::string& name) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .argumentType("bigint")
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [name](
          const std::vector<TypePtr>& argTypes,
          const std::vector<column_index_t>& argIndices,
          const TypePtr& resultType,
          velox::memory::MemoryPool* pool)
          -> std::unique_ptr<exec::WindowFunction> {
        auto typeKind = argTypes[0]->kind();
        return VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            createNthValueFunction, typeKind, argIndices, resultType, pool);
      });
}
} // namespace facebook::velox::window
