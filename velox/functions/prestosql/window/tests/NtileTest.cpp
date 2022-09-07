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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::window::test {

namespace {

class NtileTest : public OperatorTestBase {
 protected:
  void SetUp() {
    velox::window::registerWindowFunctions();
  }

  std::vector<RowVectorPtr> makeVectors(
      const std::shared_ptr<const RowType>& rowType,
      vector_size_t size,
      int numVectors) {
    std::vector<RowVectorPtr> vectors;
    VectorFuzzer::Options options;
    options.vectorSize = size;
    VectorFuzzer fuzzer(options, pool_.get(), 0);
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector =
          std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzRow(rowType));
      vectors.push_back(vector);
    }
    return vectors;
  }

  std::shared_ptr<const RowType> rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;
};

TEST_F(NtileTest, basic) {
  auto testWindowSql = [&](const RowVectorPtr& input,
                           const std::string& windowSql) -> void {
    VELOX_CHECK_GE(input->size(), 2);

    auto op = PlanBuilder()
                  .values({input})
                  .project({"c0 as c0", "c1 as c1"})
                  .window({windowSql})
                  .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
                  .planNode();
    assertQuery(
        op, "SELECT c0, c1, " + windowSql + " FROM tmp ORDER BY c0, c1");
  };

  auto basicTests = [&](const RowVectorPtr& vectors) -> void {
    testWindowSql(
        vectors,
        "ntile() over (partition by c0 order by c1) as rank_partition");
    testWindowSql(
        vectors,
        "ntile() over (partition by c1 order by c0) as rank_partition");
    testWindowSql(
        vectors,
        "ntile() over (partition by c0 order by c1 desc) as rank_partition");
    testWindowSql(
        vectors,
        "ntile() over (partition by c1 order by c0 desc) as rank_partition");

    // No partition clause
    testWindowSql(vectors, "ntile() over (order by c0, c1) as rank_partition");
    testWindowSql(vectors, "ntile() over (order by c1, c0) as rank_partition");
    testWindowSql(
        vectors, "ntile() over (order by c0 asc, c1 desc) as rank_partition");
    testWindowSql(
        vectors, "ntile() over (order by c1 asc, c0 desc) as rank_partition");

    // No order by clause
    testWindowSql(
        vectors, "ntile() over (partition by c0, c1) as rank_partition");
  };

  auto valueAtC0 = [](auto row) -> int32_t { return row % 5; };
  auto valueAtC1 = [](auto row) -> int32_t { return row % 7; };

  vector_size_t size = 100;
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAtC0),
      makeFlatVector<int32_t>(size, valueAtC1),
  });
  createDuckDbTable({vectors});
  basicTests(vectors);

  // Test all input in a single partition. This data size would
  // need multiple input blocks.
  size = 1000;
  vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAtC0),
      makeFlatVector<int32_t>(size, valueAtC1),
  });
  createDuckDbTable({vectors});
  basicTests(vectors);
}

TEST_F(NtileTest, randomGen) {
  auto vectors = makeVectors(rowType_, 10, 1);
  createDuckDbTable(vectors);

  auto testWindowSql = [&](std::vector<RowVectorPtr>& input,
                           const std::string& windowSql) -> void {
    auto op = PlanBuilder()
                  .values(input)
                  .project({"c0 as c0", "c1 as c1", "c2 as c2", "c3 as c3"})
                  .window({windowSql})
                  .orderBy(
                      {"c0 asc nulls last",
                       "c1 asc nulls last",
                       "c2 asc nulls last",
                       "c3 asc nulls last"},
                      false)
                  .planNode();
    assertQuery(
        op,
        "SELECT c0, c1, c2, c3, " + windowSql +
            " FROM tmp ORDER BY c0, c1, c2, c3");
  };

  testWindowSql(
      vectors,
      "ntile() over (partition by c0 order by c1, c2, c3) as rank_partition");
  testWindowSql(
      vectors,
      "ntile() over (partition by c1 order by c0, c2, c3) as rank_partition");
  testWindowSql(
      vectors,
      "ntile() over (partition by c0 order by c1 desc, c2, c3) as rank_partition");
  testWindowSql(
      vectors,
      "ntile() over (partition by c1 order by c0 desc, c2, c3) as rank_partition");

  testWindowSql(
      vectors, "ntile() over (order by c0, c1, c2, c3) as rank_partition");

  testWindowSql(
      vectors,
      "ntile() over (partition by c0, c1, c2, c3) as row_number_partition");
}

}; // namespace
}; // namespace facebook::velox::window::test
