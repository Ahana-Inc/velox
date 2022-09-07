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

class CumeDistFunction : public exec::WindowFunction {
 public:
  explicit CumeDistFunction() : WindowFunction(DOUBLE(), nullptr) {}

  void resetPartition(const exec::WindowPartition* /*partition*/) {
    runningTotal_ = 0;
    prevRoundGroupStart_ = 0;
  }

  void apply(
      const BufferPtr& peerGroupStarts,
      const BufferPtr& peerGroupEnds,
      const BufferPtr& /*frameStarts*/,
      const BufferPtr& /*frameEnds*/,
      vector_size_t resultOffset,
      const VectorPtr& result) {
    int numRows = peerGroupStarts->size() / sizeof(vector_size_t);
    auto* rawValues = result->asFlatVector<double>()->mutableRawValues();
    auto* peerGroupStartsValue = peerGroupStarts->as<vector_size_t>();
    auto* peerGroupEndsValue = peerGroupEnds->as<vector_size_t>();
    int totalElements = 0;

    for (int i = 0; i < numRows; i += totalElements) {
      auto groupStart = peerGroupStartsValue[i];
      auto groupEnd = peerGroupEndsValue[i];
      totalElements = groupEnd - groupStart + 1;
      runningTotal_ += totalElements;
      prevRoundTotal_ = runningTotal_;

      for (int j = groupStart; j <= groupEnd; j++) {
        rawValues[resultOffset + j] = (double)(runningTotal_ / numRows);
      }
    }
  }

 private:
  double runningTotal_ = 0;
  int64_t prevRoundTotal_ = 0;
  int64_t prevRoundGroupStart_ = 0;
};

} // namespace

void registerCumeDist(const std::string& name) {
  std::vector<exec::FunctionSignaturePtr> signatures{
      exec::FunctionSignatureBuilder().returnType("double").build(),
  };

  exec::registerWindowFunction(
      name,
      std::move(signatures),
      [name](
          const std::vector<TypePtr>& /*argTypes*/,
          const std::vector<column_index_t>& /*argIndices*/,
          const TypePtr& /*resultType*/,
          velox::memory::MemoryPool* /*pool*/)
          -> std::unique_ptr<exec::WindowFunction> {
        return std::make_unique<CumeDistFunction>();
      });
}
} // namespace facebook::velox::window
