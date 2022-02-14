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

#include <optional>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;

namespace {
class ArraysOverlapTest : public FunctionBaseTest {
 protected:
  void testExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    auto result =
        evaluate<SimpleVector<bool>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
  }
  template <typename T>
  void testInt() {
    auto array1 = makeNullableArrayVector<T>({
        {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
        {1, 2, -2, 1},
        {3, 8, std::nullopt},
        {1, 1, -2, -2, -2, 4, 8},
    });
    auto array2 = makeNullableArrayVector<T>({
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
        {1, -2, 4},
    });
    auto expected = makeNullableFlatVector<bool>({true, true, false, true});
    testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
    testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
  }

  template <typename T>
  void testFloatingPoint() {
    auto array1 = makeNullableArrayVector<T>({
        {1.0001, -2.0, 3.03, std::nullopt, 4.00004},
        {std::numeric_limits<T>::min(), 2.02, -2.001, 1},
        {std::numeric_limits<T>::max(), 8.0001, std::nullopt},
        {9.0009,
         std::numeric_limits<T>::infinity(),
         std::numeric_limits<T>::max()},
        {std::numeric_limits<T>::quiet_NaN(), 9.0009},
    });
    auto array2 = makeNullableArrayVector<T>({
        {1.0, -2.0, 4.0},
        {std::numeric_limits<T>::min(), 2.0199, -2.001, 1.000001},
        {1.0001, -2.02, std::numeric_limits<T>::max(), 8.00099},
        {9.0009, std::numeric_limits<T>::infinity()},
        {9.0009, std::numeric_limits<T>::quiet_NaN()},
    });
    auto expected =
        makeNullableFlatVector<bool>({true, true, true, true, true});

    testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
    testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
  }
}; // class ArraysOverlap
} // namespace

TEST_F(ArraysOverlapTest, intArrays) {
  testInt<int8_t>();
  testInt<int16_t>();
  testInt<int32_t>();
  testInt<int64_t>();
}

TEST_F(ArraysOverlapTest, floatArrays) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}

TEST_F(ArraysOverlapTest, boolArrays) {
  auto array1 = makeNullableArrayVector<bool>(
      {{true, false},
       {true, true},
       {false, false},
       {},
       {true, false, true, std::nullopt},
       {std::nullopt, true, false, true},
       {false, true, false},
       {true, false, true},
       {std::nullopt}});

  auto array2 = makeNullableArrayVector<bool>(
      {{true},
       {true, true},
       {false, false},
       {},
       {true, std::nullopt},
       {std::nullopt, false},
       {false, true, false},
       {true, false, true},
       {std::nullopt}});

  auto expected = makeNullableFlatVector<bool>(
      {true, true, true, false, true, true, true, true, false});

  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

//// Test inline strings.
TEST_F(ArraysOverlapTest, strArrays) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {S("a"), std::nullopt, S("b")},
      {S("a"), S("b"), S("a"), S("a")},
      {std::nullopt, S("b"), std::nullopt},
      {S("abc")},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("a")},
      {S("a"), S("b"), S("b")},
      {std::nullopt, std::nullopt, std::nullopt},
      {S("abc"), S("a"), S("b")},
  });
  auto expected = makeNullableFlatVector<bool>({true, true, false, true});
  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

// Test non-inline (> 12 length) strings.
TEST_F(ArraysOverlapTest, longStrArrays) {
  using S = StringView;

  auto array1 = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead"), S("blue clear sky above")},
      {std::nullopt,
       S("blue clear sky above"),
       S("yellow rose flowers"),
       S("orange beautiful sunset")},
      {},
      {S("red shiny car ahead"),
       S("purple is an elegant color"),
       S("green plants make us happy")},
  });
  auto array2 = makeNullableArrayVector<StringView>({
      {S("red shiny car ahead")},
      {std::nullopt},
      {},
      {S("red shiny car ahead"), S("green plants make us happy")},
  });
  auto expected =
      makeNullableFlatVector<bool>({true, false, false, true, true});
  testExpr(expected, "arrays_overlap(C0, C1)", {array1, array2});
  testExpr(expected, "arrays_overlap(C1, C0)", {array1, array2});
}

//// When one of the arrays is constant.
// TEST_F(ArraysOverlapTest, constant) {
//   auto array1 = makeNullableArrayVector<int32_t>({
//       {1, -2, 3, std::nullopt, 4, 5, 6, std::nullopt},
//       {1, 2, -2, 1},
//       {3, 8, std::nullopt},
//       {1, 1, -2, -2, -2, 4, 8},
//   });
//   auto expected = makeNullableArrayVector<int32_t>({
//       {1, -2, 4},
//       {1, -2},
//       {},
//       {1, -2, 4},
//   });
//   testExpr(expected, "arrays_overlap(C0, ARRAY[1,-2,4])", {array1});
//   testExpr(expected, "arrays_overlap(ARRAY[1,-2,4], C0)", {array1});
//   testExpr(
//       expected, "arrays_overlap(ARRAY[1,1,-2,1,-2,4,1,4,4], C0)", {array1});
//
//   // Array containing NULLs.
//   expected = makeNullableArrayVector<int32_t>({
//       {1, std::nullopt, 4},
//       {1},
//       {std::nullopt},
//       {1, 4},
//   });
//   testExpr(expected, "arrays_overlap(C0, ARRAY[1,NULL,4])", {array1});
//   testExpr(expected, "arrays_overlap(ARRAY[1,NULL,4], C0)", {array1});
// }
//
TEST_F(ArraysOverlapTest, wrongTypes) {
  auto expected = makeNullableFlatVector<bool>({true});
  auto array1 = makeNullableArrayVector<int32_t>({{1}});

  EXPECT_THROW(
      testExpr(expected, "arrays_overlap(1, 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "arrays_overlap(C0, 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "arrays_overlap(ARRAY[1], 1)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "arrays_overlap(C0)", {array1}),
      std::invalid_argument);
  EXPECT_THROW(
      testExpr(expected, "arrays_overlap(C0, C0, C0)", {array1}),
      std::invalid_argument);

  EXPECT_NO_THROW(testExpr(expected, "arrays_overlap(C0, C0)", {array1}));
}
