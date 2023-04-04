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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include <memory>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/window/tests/WindowTestBase.h"
#include "velox/vector/FlatVector.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class WindowBenchmark : public functions::test::FunctionBenchmarkBase,
                        public window::test::WindowTestBase {
 public:
  explicit WindowBenchmark() : FunctionBenchmarkBase() {
    window::prestosql::registerAllWindowFunctions();
  }

  void run(size_t times, const std::string& functionName) {
    folly::BenchmarkSuspender suspender;
    auto data = makeSimpleVector(50);
    suspender.dismiss();

    doRun(functionName, {data}, times);
  }

  // We inherit from FunctionBaseTest so that we can get access to the helpers
  // it defines, but since it is supposed to be a test fixture TestBody() is
  // declared pure virtual.  We must provide an implementation here.
  void TestBody() override {}

 private:
  void doRun(
      const std::string& functionName,
      const std::vector<RowVectorPtr>& input,
      size_t times) {
    int cnt = 0;
    for (auto i = 0; i < times; i++) {
      for (const auto& overClause : window::test::kOverClauses) {
        cnt += WindowTestBase::testWindowFunction(
            input, functionName, {overClause}, window::test::kFrameClauses);
      }
    }
    folly::doNotOptimizeAway(cnt);
  }
};
} // namespace

size_t static constexpr kIterationCount = 8;

BENCHMARK(NthValueConstant) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "nth_value(c0, 1)");
}

BENCHMARK(NthValueColumn) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "nth_value(c0, c2)");
}

BENCHMARK(FirstValueC0) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "first_value(c0)");
}

BENCHMARK(LastValueC0) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "last_value(c0)");
}

BENCHMARK(FirstValueC1) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "first_value(c1)");
}

BENCHMARK(LastValueC1) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "last_value(c1)");
}

BENCHMARK(FirstValueC2) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "first_value(c2)");
}

BENCHMARK(LastValueC2) {
  WindowBenchmark benchmark;
  benchmark.run(kIterationCount, "last_value(c2)");
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
