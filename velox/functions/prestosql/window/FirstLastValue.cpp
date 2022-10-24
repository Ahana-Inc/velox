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

#include "velox/functions/prestosql/window/ValueFunctionsUtility.h"

namespace facebook::velox::window::prestosql {

namespace {

enum class FirstLastType {
  kFirst,
  kLast,
};

template <FirstLastType TValue>
class FirstLastValueFunction : public ValueFunction<ValueType::kFirstLast> {
 public:
  explicit FirstLastValueFunction(
      const std::vector<exec::WindowFunctionArg>& args,
      const TypePtr& resultType,
      velox::memory::MemoryPool* pool)
      : ValueFunction(args, resultType, pool) {}

  void resetPartition(const exec::WindowPartition* partition) override {
    partition_ = partition;
  }

  void apply(
      const BufferPtr& /*peerGroupStarts*/,
      const BufferPtr& /*peerGroupEnds*/,
      const BufferPtr& frameStarts,
      const BufferPtr& frameEnds,
      const SelectivityVector& validRows,
      int32_t resultOffset,
      const VectorPtr& result) override {
    auto numRows = frameStarts->size() / sizeof(vector_size_t);
    rowNumbers_.resize(numRows);

    if constexpr (TValue == FirstLastType::kFirst) {
      auto rawFrameStarts = frameStarts->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameStarts[i]; });
    } else {
      auto rawFrameEnds = frameEnds->as<vector_size_t>();
      validRows.applyToSelected(
          [&](auto i) { rowNumbers_[i] = rawFrameEnds[i]; });
    }
    setRowNumbersForEmptyFrames(validRows);

    auto rowNumbersRange = folly::Range(rowNumbers_.data(), numRows);
    partition_->extractColumn(
        valueIndex_, rowNumbersRange, resultOffset, result);
  }
};
} // namespace

template <FirstLastType TValue>
void registerFirstLastInternal(const std::string& name) {
  // T -> T
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("T")
          .build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [](const std::vector<exec::WindowFunctionArg>& args,
         const TypePtr& resultType,
         velox::memory::MemoryPool* pool,
         HashStringAllocator* /*stringAllocator*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<FirstLastValueFunction<TValue>>(
            args, resultType, pool);
      });
}

void registerFirstValue(const std::string& name) {
  registerFirstLastInternal<FirstLastType::kFirst>(name);
}
void registerLastValue(const std::string& name) {
  registerFirstLastInternal<FirstLastType::kLast>(name);
}
} // namespace facebook::velox::window::prestosql
