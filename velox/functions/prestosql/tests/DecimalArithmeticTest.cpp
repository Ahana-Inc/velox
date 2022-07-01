//
// Created by Karteek Murthy Samba Murthy on 6/30/22.
//

#include <optional>
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

static auto kResultLongVector = std::vector<std::optional<LongDecimal>>{
    LongDecimal(buildInt128(0, 0x8AC72304C582C99B)),
    LongDecimal(buildInt128(0, 0x9A3298AFBC4BDD83)),
    LongDecimal(buildInt128(0x0A, 0xAAAAAAF846AED32A)),
    LongDecimal(buildInt128(0x0D, 0xDDDDDDF7BC89EB5D)),
    LongDecimal(buildInt128(0x15, 0x5555555555555554)),
    LongDecimal(buildInt128(0x1B, 0xBBBBBBBBBBBBBBBA)),
};

static auto kResultShortVector = std::vector<std::optional<ShortDecimal>>{
    ShortDecimal(1000999899),
    ShortDecimal(101108611)};
static auto kShortDecimalVector = std::vector<std::optional<ShortDecimal>>{
    ShortDecimal(99999999999999999),
    ShortDecimal(111111111111111111),
    ShortDecimal(999999999),
    ShortDecimal(111111111),
    ShortDecimal(9999),
    ShortDecimal(-100025)};

static auto kLongDecimalVector = std::vector<std::optional<LongDecimal>>{
    LongDecimal(buildInt128(0x0A, 0xAAAAAAAAAAAAAAAA)),
    LongDecimal(buildInt128(0x0D, 0xDDDDDDDDDDDDDDDD))};

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
    leftShortDecimalVector_.push_back(kShortDecimalVector[0]);
    leftShortDecimalVector_.push_back(kShortDecimalVector[1]);
    auto leftShortFlatVector =
        makeDecimalFlatVector<ShortDecimal>(leftShortDecimalVector_, 17, 3);
    rightShortDecimalVector_.push_back(kShortDecimalVector[2]);
    rightShortDecimalVector_.push_back(kShortDecimalVector[3]);
    auto right =
        makeDecimalFlatVector<ShortDecimal>(rightShortDecimalVector_, 9, 5);
    resultLongVector_.push_back(kResultLongVector[0]);
    resultLongVector_.push_back(kResultLongVector[1]);
    auto resultLongFlatVector =
        makeDecimalFlatVector<LongDecimal>(resultLongVector_, 20, 5);

    // Add short and short, returning long.
    testDecimalExpr<LongDecimal>(
        resultLongFlatVector,
        "decimal_add(c0, c1)",
        {leftShortFlatVector, right});

    // Add short and short, result is short.
    leftShortDecimalVector_[0] = kShortDecimalVector[4];
    leftShortDecimalVector_[1] = kShortDecimalVector[5];
    leftShortFlatVector =
        makeDecimalFlatVector<ShortDecimal>(leftShortDecimalVector_, 6, 3);
    resultShortVector_.push_back(kResultShortVector[0]);
    resultShortVector_.push_back(kResultShortVector[1]);
    auto resultShortFlatVector =
        makeDecimalFlatVector<ShortDecimal>(resultShortVector_, 10, 5);
    testDecimalExpr<ShortDecimal>(
        resultShortFlatVector,
        "decimal_add(c0, c1)",
        {leftShortFlatVector, right});

    // Add short and long, result is long.
    leftShortDecimalVector_[0] = ShortDecimal(33333);
    leftShortDecimalVector_[1] = ShortDecimal(11111);
    leftShortFlatVector =
        makeDecimalFlatVector<ShortDecimal>(leftShortDecimalVector_, 6, 3);

    rightLongDecimalVector.push_back(kLongDecimalVector[0]);
    rightLongDecimalVector.push_back(kLongDecimalVector[1]);
    auto rightLongFlatVector =
        makeDecimalFlatVector<LongDecimal>(rightLongDecimalVector, 37, 10);

    resultLongVector_[0] = kResultLongVector[2];
    resultLongVector_[1] = kResultLongVector[3];
    resultLongFlatVector =
        makeDecimalFlatVector<LongDecimal>(resultLongVector_, 38, 10);
    testDecimalExpr<LongDecimal>(
        resultLongFlatVector,
        "decimal_add(c0, c1)",
        {leftShortFlatVector, rightLongFlatVector});

    // Add long and short, result is long.
    testDecimalExpr<LongDecimal>(
        resultLongFlatVector,
        "decimal_add(c0, c1)",
        {rightLongFlatVector, leftShortFlatVector});

    // Add long and long, result is long.
    resultLongVector_[0] = kResultLongVector[4];
    resultLongVector_[1] = kResultLongVector[5];
    resultLongFlatVector =
        makeDecimalFlatVector<LongDecimal>(resultLongVector_, 38, 10);
    testDecimalExpr<LongDecimal>(
        resultLongFlatVector,
        "decimal_add(c0, c1)",
        {rightLongFlatVector, rightLongFlatVector});
  }

 private:
  std::vector<std::optional<LongDecimal>> resultLongVector_;
  std::vector<std::optional<LongDecimal>> leftLongDecimalVector;
  std::vector<std::optional<LongDecimal>> rightLongDecimalVector;
  std::vector<std::optional<ShortDecimal>> resultShortVector_;
  std::vector<std::optional<ShortDecimal>> leftShortDecimalVector_;
  std::vector<std::optional<ShortDecimal>> rightShortDecimalVector_;
};
} // namespace

TEST_F(DecimalArithmeticTest, decimalAddTest) {
  testDecimalAdd();
}
