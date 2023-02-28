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
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/tests/AggregationTestBase.h"

using namespace facebook::velox::exec::test;

namespace facebook::velox::aggregate::test {

namespace {

// Replace the given query's placeholders '{0}' with the given aggregation name.
std::string genAggrQuery(const char* query, const char* aggrName) {
  return fmt::format(fmt::runtime(query), aggrName);
}

// Helper generates aggregation over column string.
std::string genAggr(const char* aggrName, const char* colName) {
  return fmt::format("{}({})", aggrName, colName);
}

// Macro to make it even shorter (assumes we have 'aggrName' var on the stack).
#define GEN_AGG(_colName_) genAggr(aggrName, _colName_)

// The test class.
class VarianceAggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    allowInputShuffle();
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), INTEGER(), BIGINT(), REAL(), DOUBLE()})};

  // We test these aggregations in this class.
  // It is possible to temporarily alter this array to only focus on one, when
  // debugging tests.
  const std::vector<const char*> aggrNames_{
      "stddev",
      "stddev_pop",
      "stddev_samp",
      "variance",
      "var_pop",
      "var_samp"};
};

TEST_F(VarianceAggregationTest, varianceConst) {
  // Have two row vectors at least as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeFlatVector<int64_t>(
              10, [](vector_size_t row) { return row / 3; }),
          makeConstant(5, 10),
          makeConstant(6.0, 10),
      }),
      makeRowVector({
          makeFlatVector<int64_t>(
              10, [](vector_size_t row) { return row / 3; }),
          makeConstant(5, 10),
          makeConstant(6.0, 10),
      }),
  };

  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    auto sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    testAggregations(vectors, {}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);

    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp GROUP BY 1", aggrName);
    testAggregations(vectors, {"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);

    sql = genAggrQuery("SELECT {0}(c0) FROM tmp", aggrName);
    testAggregations(vectors, {}, {GEN_AGG("c0")}, sql);

    sql = genAggrQuery("SELECT c0 % 2, {0}(c0) FROM tmp GROUP BY 1", aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors).project({"c0 % 2 AS c0_mod_2", "c0"});
        },
        {"c0_mod_2"},
        {GEN_AGG("c0")},
        sql);
  }
}

TEST_F(VarianceAggregationTest, varianceConstNull) {
  // Have two row vectors at least as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullConstant(TypeKind::BIGINT, 10),
          makeNullConstant(TypeKind::DOUBLE, 10),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, 1, 2, 0, 1, 2, 0, 1, 2, 0}),
          makeNullConstant(TypeKind::BIGINT, 10),
          makeNullConstant(TypeKind::DOUBLE, 10),
      }),
  };

  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    auto sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    testAggregations(vectors, {}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);

    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp group by c0", aggrName);
    testAggregations(vectors, {"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);
  }
}

TEST_F(VarianceAggregationTest, varianceNulls) {
  // Have two row vectors at least as it triggers different code paths.
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
          makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
          makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
      }),
      makeRowVector({
          makeNullableFlatVector<int64_t>({0, std::nullopt, 2, 0, 1}),
          makeNullableFlatVector<int64_t>({0, 1, std::nullopt, 3, 4}),
          makeNullableFlatVector<double>({0.1, 1.2, 2.3, std::nullopt, 4.4}),
      }),
  };

  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    auto sql = genAggrQuery("SELECT {0}(c1), {0}(c2) FROM tmp", aggrName);
    testAggregations(vectors, {}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);

    sql = genAggrQuery(
        "SELECT c0, {0}(c1), {0}(c2) FROM tmp group by c0", aggrName);
    testAggregations(vectors, {"c0"}, {GEN_AGG("c1"), GEN_AGG("c2")}, sql);
  }
}

// TODO Variance functions are not sensitive to the order of inputs except
// when inputs are very large integers (> 15 digits long). Unfortunately
// makeVectors() generates data that contains a lot of very large integers.
// Replace makeVectors() with a dataset that doesn't contain very large
// integers and enable more testing by calling allowInputShuffle() from
// Setup().
TEST_F(VarianceAggregationTest, varianceWithGlobalAggregation) {
  auto vectors = makeVectors(rowType_, 10, 20);
  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    // Global aggregation
    // We need to use sorting keys to fix the precision difference between Velox
    // and DuckDB.
    auto sql = genAggrQuery(
        "SELECT 1, {0}(c1), {0}(c2), {0}(c4), {0}(c5) FROM tmp", aggrName);
    testAggregations(
        vectors,
        {},
        {GEN_AGG("c1"), GEN_AGG("c2"), GEN_AGG("c4"), GEN_AGG("c5")},
        {"1", "a0"},
        sql,
        {0});

    // Global aggregation; no input
    sql = genAggrQuery("SELECT {0}(c0) FROM tmp WHERE c0 % 2 = 5", aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors).filter("c0 % 2 = 5");
        },
        {},
        {GEN_AGG("c0")},
        sql);
  }
}

TEST_F(VarianceAggregationTest, varianceWithGlobalAggregationAndFilter) {
  auto vectors = makeVectors(rowType_, 10, 20);
  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    // Global aggregation over filter
    const auto sql =
        genAggrQuery("SELECT {0}(c0) FROM tmp WHERE c0 % 5 = 3", aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors).filter("c0 % 5 = 3");
        },
        {},
        {GEN_AGG("c0")},
        sql);
  }
}

TEST_F(VarianceAggregationTest, varianceWithGroupBy) {
  auto vectors = makeVectors(rowType_, 10, 20);
  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    // Group by.
    auto sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1), {0}(c2), {0}(c3::DOUBLE), {0}(c4), {0}(c5) "
        "FROM tmp GROUP BY 1",
        aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors).project(
              {"c0 % 10", "c1", "c2", "c3", "c4", "c5"});
        },
        {"p0"},
        {GEN_AGG("c1"),
         GEN_AGG("c2"),
         GEN_AGG("c3"),
         GEN_AGG("c4"),
         GEN_AGG("c5")},
        sql);

    // Group by; no input
    sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1) FROM tmp WHERE c0 % 2 = 5 GROUP BY 1",
        aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors)
              .filter("c0 % 2 = 5")
              .project({"c0 % 10", "c1"});
        },
        {"p0"},
        {GEN_AGG("c1")},
        sql);
  }
}

TEST_F(VarianceAggregationTest, varianceWithGroupByAndFilter) {
  auto vectors = makeVectors(rowType_, 10, 20);
  createDuckDbTable(vectors);

  for (const auto& aggrName : aggrNames_) {
    // Group by over filter.
    const auto sql = genAggrQuery(
        "SELECT c0 % 10, {0}(c1) FROM tmp WHERE c2 % 5 = 3 GROUP BY 1",
        aggrName);
    testAggregations(
        [&](PlanBuilder& builder) {
          builder.values(vectors)
              .filter("c2 % 5 = 3")
              .project({"c0 % 10", "c1"});
        },
        {"p0"},
        {GEN_AGG("c1")},
        sql);
  }
}

TEST_F(VarianceAggregationTest, variancePrecision) {
  auto vectors = {
      makeRowVector(
          {"c0", "c1"},
          {
              makeNullableFlatVector<int64_t>({-7, 8, -6, std::nullopt, -5}),
              makeNullableFlatVector<int64_t>(
                  {-5, -8, -3, 2, -3833098290310622212}),
          }),
      makeRowVector(
          {"c0", "c1"},
          {
              makeNullableFlatVector<int64_t>({-7, 8, -6, std::nullopt, -5}),
              makeNullableFlatVector<int64_t>(
                  {-5, -8, -3, 2, -3833098290310622212}),
          }),
      makeRowVector(
          {"c0", "c1"},
          {
              makeNullableFlatVector<int64_t>({-7, 8, -6, std::nullopt, -5}),
              makeNullableFlatVector<int64_t>(
                  {-5, -8, -3, 2, -3833098290310622212}),
          }),
      makeRowVector(
          {"c0", "c1"},
          {
              makeNullableFlatVector<int64_t>({-7, 8, -6, std::nullopt, -5}),
              makeNullableFlatVector<int64_t>(
                  {-5, -8, -3, 2, -3833098290310622212}),
          }),
  };

  createDuckDbTable(vectors);

  testAggregations(
      vectors,
      {"c0"},
      {"stddev(c1)"},
      "SELECT c0, stddev(c1) FROM tmp GROUP BY 1");
}

TEST_F(VarianceAggregationTest, varianceFlaky) {
  auto rowType = ROW({"c0"}, {DOUBLE()});
  auto aggrNames = {
//      "stddev",
//      "stddev_pop",
//      "stddev_samp",
      "variance",
//      "var_pop",
//      "var_samp",
  };

//  auto vectors = makeVectors(rowType, 10, 10);
  auto vectors = {
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
      makeRowVector({
          makeNullableFlatVector<double>({
              0.9648885340421604,
              0.9705927790435588,
              0.10986175084420642,
              0.8002804733808399,
              0.9157355237082728,
              0.8784306453988439,
              0.8350085899945795,
              0.6323592500344464,
              0.27849821852222567,
              NULL,
          })
      }),
  };

  createDuckDbTable(vectors);

  auto res0 = duckDbQueryRunner_.execute("select * from tmp");
  std::cout << res0->ToString() << std::endl;

  for (const auto& aggrName : aggrNames) {
    // Global aggregation
    auto sql = genAggrQuery(
        "SELECT 1, {0}(c0) FROM tmp", aggrName);

    auto res = duckDbQueryRunner_.execute(sql);
    std::cout << res->ToString() << std::endl;

    testAggregations(
        vectors,
        {},
        {GEN_AGG("c0")},
        {"1", "a0"},
        sql,
        {0});
  }
}

} // namespace
} // namespace facebook::velox::aggregate::test
