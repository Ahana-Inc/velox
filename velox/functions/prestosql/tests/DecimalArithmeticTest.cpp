//
// Created by Karteek Murthy Samba Murthy on 6/30/22.
//

#include <optional>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {
class DecimalArithmeticTest : public FunctionBaseTest {
 protected:
  template <typename T>
  void testDecimalExpr(
      const VectorPtr& expected,
      const std::string& expression,
      const std::vector<VectorPtr>& input) {
    VectorPtr result =
        evaluate<SimpleVector<T>>(expression, makeRowVector(input));
    assertEqualVectors(expected, result);
    ASSERT_EQ(result->type()->toString(), expected->type()->toString());
  }

  void testDecimalAdd() {
    auto leftVec = std::vector<std::optional<ShortDecimal>>{
        ShortDecimal(99999999999999999), ShortDecimal(111111111111111111)};
    auto left = makeDecimalFlatVector<ShortDecimal>(leftVec, 17, 3);
    auto rightVec = std::vector<std::optional<ShortDecimal>>{
        ShortDecimal(999999999), ShortDecimal(111111111)};
    auto right = makeDecimalFlatVector<ShortDecimal>(rightVec, 9, 5);
    auto resultLongVector = std::vector<std::optional<LongDecimal>>{
        LongDecimal(buildInt128(0, 0x8AC72304C582C99B)),
        LongDecimal(buildInt128(0, 0x9A3298AFBC4BDD83))};
    auto resultLongFlatVector =
        makeDecimalFlatVector<LongDecimal>(resultLongVector, 20, 5);

    // Add short and short, returning long.
    testDecimalExpr<LongDecimal>(
        resultLongFlatVector, "decimal_add(c0, c1)", {left, right});

    // Add short and short, result is short.
    leftVec[0] = ShortDecimal(9999);
    leftVec[1] = ShortDecimal(-100025);
    left = makeDecimalFlatVector<ShortDecimal>(leftVec, 6, 3);
    auto resultShortVector = std::vector<std::optional<ShortDecimal>>{
        ShortDecimal(1000999899), ShortDecimal(101108611)};
    auto resultShortFlatVector =
        makeDecimalFlatVector<ShortDecimal>(resultShortVector, 10, 5);
    testDecimalExpr<ShortDecimal>(
        resultShortFlatVector, "decimal_add(c0, c1)", {left, right});
  }
};
} // namespace

TEST_F(DecimalArithmeticTest, decimalAddTest) {
  testDecimalAdd();
}
