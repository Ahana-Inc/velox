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

#pragma once

#include <string>
#include "velox/common/base/CheckedArithmetic.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"
#include "velox/type/UnscaledLongDecimal.h"
#include "velox/type/UnscaledShortDecimal.h"

namespace facebook::velox {

static int128_t kSignInt128Mask = ((int128_t)1 << 127);
static __uint128_t kOverflowMultiplier = ((__uint128_t)1 << 127);

/// A static class that holds helper functions for DECIMAL type.
class DecimalUtil {
 public:
  static const int128_t kPowersOfTen[LongDecimalType::kMaxPrecision + 1];

  /// Helper function to convert a decimal value to string.
  template <typename T>
  static std::string toString(const T& value, const TypePtr& type);

  template <typename TInput, typename TOutput>
  inline static std::optional<TOutput> rescaleWithRoundUp(
      const TInput inputValue,
      const int fromPrecision,
      const int fromScale,
      const int toPrecision,
      const int toScale,
      const bool nullOnFailure) {
    int128_t rescaledValue = inputValue.unscaledValue();
    auto scaleDifference = toScale - fromScale;
    bool isOverflow = false;
    if (scaleDifference >= 0) {
      isOverflow = __builtin_mul_overflow(
          rescaledValue,
          DecimalUtil::kPowersOfTen[scaleDifference],
          &rescaledValue);
    } else {
      scaleDifference = -scaleDifference;
      const auto scalingFactor = DecimalUtil::kPowersOfTen[scaleDifference];
      rescaledValue /= scalingFactor;
      int128_t remainder = inputValue.unscaledValue() % scalingFactor;
      if (inputValue.unscaledValue() >= 0 && remainder >= scalingFactor / 2) {
        ++rescaledValue;
      } else if (remainder <= -scalingFactor / 2) {
        --rescaledValue;
      }
    }
    // Check overflow.
    if (rescaledValue < -DecimalUtil::kPowersOfTen[toPrecision] ||
        rescaledValue > DecimalUtil::kPowersOfTen[toPrecision] || isOverflow) {
      if (nullOnFailure) {
        return std::nullopt;
      }
      VELOX_USER_FAIL(
          "Cannot cast DECIMAL '{}' to DECIMAL({},{})",
          DecimalUtil::toString<TInput>(
              inputValue, DECIMAL(fromPrecision, fromScale)),
          toPrecision,
          toScale);
    }
    if constexpr (std::is_same_v<TOutput, UnscaledShortDecimal>) {
      return UnscaledShortDecimal(static_cast<int64_t>(rescaledValue));
    } else {
      return UnscaledLongDecimal(rescaledValue);
    }
  }

  template <typename R, typename A, typename B>
  inline static void divideWithRoundUp(
      R& r,
      const A& a,
      const B& b,
      uint8_t aRescale,
      uint8_t /*bRescale*/) {
    VELOX_CHECK_NE(b, 0, "Division by zero");
    int resultSign = 1;
    R unsignedDividendRescaled(a);
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
    }
    R unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
    }
    unsignedDividendRescaled = checkedMultiply<R>(
        unsignedDividendRescaled, R(DecimalUtil::kPowersOfTen[aRescale]));
    R quotient = unsignedDividendRescaled / unsignedDivisor;
    R remainder = unsignedDividendRescaled % unsignedDivisor;
    if (remainder * 2 >= unsignedDivisor) {
      ++quotient;
    }
    r = quotient * resultSign;
  }

  template <typename R, typename A, typename B>
  inline static void divideInt128(R& r, const A& a, const B& b, B& remainder) {
    VELOX_CHECK_NE(b, 0, "Division by zero");
    int resultSign = 1;
    R unsignedDividendRescaled(a);
    if (a < 0) {
      resultSign = -1;
      unsignedDividendRescaled *= -1;
    }
    R unsignedDivisor(b);
    if (b < 0) {
      resultSign *= -1;
      unsignedDivisor *= -1;
    }
    R quotient = unsignedDividendRescaled / unsignedDivisor;
    remainder = unsignedDividendRescaled % unsignedDivisor;
    r = quotient * resultSign;
  }

  inline static int compareAbsolute(int128_t lhs, int128_t rhs) {
    auto lhsAbs = lhs < 0 ? -lhs : lhs;
    auto rhsAbs = rhs < 0 ? -rhs : rhs;

    if (lhsAbs < rhsAbs)
      return -1;
    if (lhsAbs > rhsAbs)
      return 1;
    return 0;
  }
  /*
   * sum up and return overflow/underflow.
   */
  inline static int64_t addUnsignedValues(
      int128_t& sum,
      const int128_t& lhs,
      const int128_t& rhs,
      bool isResultNegative) {
    __uint128_t unsignedSum = (__uint128_t)lhs + (__uint128_t)rhs;
    // Ignore overflow value.
    sum = (int128_t)unsignedSum & ~(kSignInt128Mask);
    sum = isResultNegative ? -sum : sum;
    return (unsignedSum >> 127);
  }

  /*
   * Subtracts rhs from lhs ignoring the signs.
   */
  inline static void subtractUnsigned(
      int128_t& result,
      const int128_t& lhs,
      const int128_t& rhs,
      bool isResultNegative) {
    __uint128_t finalResult = (__uint128_t)lhs - (__uint128_t)rhs;
    result = isResultNegative ? -finalResult : finalResult;
  }

  inline static int64_t
  addWithOverflow(int128_t& result, const int128_t& lhs, const int128_t& rhs) {
    bool islhsNegative = lhs < 0;
    bool isrhsNegative = rhs < 0;
    int64_t overflow = 0;
    if (islhsNegative == isrhsNegative) {
      // Both inputs of same time.
      if (islhsNegative) {
        // Both negative, ignore signs and add.
        overflow = addUnsignedValues(result, -lhs, -rhs, true);
        overflow = -overflow;
      } else {
        overflow = addUnsignedValues(result, lhs, rhs, false);
      }
    } else {
      // If one of them is negative, use addition.
      result = lhs + rhs;
    }
    return overflow;
  }

  /*
   * Computes average. If there is an overflow value uses the following
   * expression to compute the average.
   *                       ---                                         ---
   *                      |    overflow_multiplier          sum          |
   * average = overflow * |     -----------------  +  ---------------    |
   *                      |         count              count * overflow  |
   *                       ---                                         ---
   */
  inline static void computeAverage(
      int128_t& avg,
      const int128_t& sum,
      const int64_t count,
      const int64_t overflow) {
    if (overflow == 0) {
      divideWithRoundUp<int128_t, int128_t, int64_t>(avg, sum, count, 0, 0);
    } else {
      int64_t remainderA{0};
      __uint128_t sumA{0};
      DecimalUtil::divideInt128<__uint128_t, __uint128_t, int64_t>(
          sumA, kOverflowMultiplier, count, remainderA);
      double totalRemainder = (double)remainderA / count;
      __uint128_t sumB{0};
      int64_t remainderB{0};
      DecimalUtil::divideInt128<__uint128_t, __int128_t, int64_t>(
          sumB, sum, count * overflow, remainderB);
      totalRemainder += (double)remainderB / (count * overflow);
      DecimalUtil::addWithOverflow(avg, sumA, sumB);
      avg = avg * overflow + (int)(totalRemainder * overflow);
    }
  }
}; // DecimalUtil
} // namespace facebook::velox
