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

class NthValueTest : public OperatorTestBase {
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

TEST_F(NthValueTest, basicNthValue) {
  auto vectors = makeVectors(rowType_, 10, 1);
  createDuckDbTable(vectors);

  auto op =
      PlanBuilder()
          .values(vectors)
          .project({"c0 as c0", "c1 as c1", "1 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op,
      "SELECT c0, c1, 1 as c2, nth_value(c0, 1) over (partition by c0 order by c1) as nth_value_partition FROM tmp order by c0, c1");
}

TEST_F(NthValueTest, basicNthValue2) {
  vector_size_t size = 10;
  auto valueAtC0 = [](auto row) -> int32_t { return row % 5; };
  auto valueAtC1 = [](auto row) -> int32_t { return row % 5; };

  auto vectors = makeRowVector({
      makeFlatVector<int32_t>(size, valueAtC0),
      makeFlatVector<int32_t>(size, valueAtC1),
  });

  createDuckDbTable({vectors});

  auto op =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "1 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op,
      "SELECT c0, c1, 1 as c2, nth_value(c0, 1) over (partition by c0 order by c1) as nth_value_partition FROM tmp order by c0, c1");
}

TEST_F(NthValueTest, unboundedPrecedingFrame) {
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
      makeFlatVector<int32_t>({3, 2, 5, 6, 9, 4, 1, 8, 7, 11}),
  });
  createDuckDbTable({vectors});

  auto op1 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "1 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 range between unbounded preceding and current row) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op1,
      "SELECT c0, c1, 1 as c2, nth_value(c0, 1) over (partition by c0 order by c1 range between unbounded preceding and current row) as nth_value_partition FROM tmp order by c0, c1");

  auto op2 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "3 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 range between unbounded preceding and current row) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op2,
      "SELECT c0, c1, 3 as c2, nth_value(c0, 3) over (partition by c0 order by c1 range between unbounded preceding and current row) as nth_value_partition FROM tmp order by c0, c1");

  //  auto op3 =
  //      PlanBuilder()
  //          .values({vectors})
  //          .project({"c0 as c0", "c1 as c1", "5 as c2"})
  //          .window(
  //              {"nth_value(c0, c2) over (partition by c0 order by c1 range
  //              between unbounded preceding and current row) as
  //              nth_value_partition"})
  //          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
  //          .planNode();
  //  assertQuery(
  //      op3,
  //      "SELECT c0, c1, 5 as c2, nth_value(c0, 5) over (partition by c0 order
  //      by c1 range between unbounded preceding and current row) as
  //      nth_value_partition FROM tmp order by c0, c1");
}

TEST_F(NthValueTest, unboundedFollowingFrame) {
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
      makeFlatVector<int32_t>({3, 2, 5, 6, 9, 4, 1, 8, 7, 11}),
  });
  createDuckDbTable({vectors});

  auto op1 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "1 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op1,
      "SELECT c0, c1, 1 as c2, nth_value(c0, 1) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition FROM tmp order by c0, c1");

  auto op2 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "3 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op2,
      "SELECT c0, c1, 3 as c2, nth_value(c0, 3) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition FROM tmp order by c0, c1");

  auto op3 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "5 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op3,
      "SELECT c0, c1, 5 as c2, nth_value(c0, 5) over (partition by c0 order by c1 range between current row and unbounded following) as nth_value_partition FROM tmp order by c0, c1");
}

TEST_F(NthValueTest, kBoundedPrecedingFrame) {
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
      makeFlatVector<int32_t>({3, 2, 5, 6, 9, 4, 1, 8, 7, 11}),
  });
  createDuckDbTable({vectors});

  auto op1 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "2 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 rows between c2 preceding and current row) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op1,
      "SELECT c0, c1, 2 as c2, nth_value(c0, 2) over (partition by c0 order by c1 rows between c2 preceding and current row) as nth_value_partition FROM tmp order by c0, c1");

  auto op2 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "3 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 rows between c2 preceding and current row) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op2,
      "SELECT c0, c1, 3 as c2, nth_value(c0, 3) over (partition by c0 order by c1 rows between c2 preceding and current row) as nth_value_partition FROM tmp order by c0, c1");
}

TEST_F(NthValueTest, kBoundedFollowingFrame) {
  auto vectors = makeRowVector({
      makeFlatVector<int32_t>({1, 1, 1, 2, 2, 2, 3, 3, 3, 3}),
      makeFlatVector<int32_t>({3, 2, 5, 6, 9, 4, 1, 8, 7, 11}),
  });
  createDuckDbTable({vectors});

  auto op1 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "2 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 rows between current row and c2 following) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op1,
      "SELECT c0, c1, 2 as c2, nth_value(c0, 2) over (partition by c0 order by c1 rows between current row and c2 following) as nth_value_partition FROM tmp order by c0, c1");

  auto op2 =
      PlanBuilder()
          .values({vectors})
          .project({"c0 as c0", "c1 as c1", "3 as c2"})
          .window(
              {"nth_value(c0, c2) over (partition by c0 order by c1 rows between current row and c2 following) as nth_value_partition"})
          .orderBy({"c0 asc nulls last", "c1 asc nulls last"}, false)
          .planNode();
  assertQuery(
      op2,
      "SELECT c0, c1, 3 as c2, nth_value(c0, 3) over (partition by c0 order by c1 rows between current row and c2 following) as nth_value_partition FROM tmp order by c0, c1");
}

}; // namespace
}; // namespace facebook::velox::window::test
