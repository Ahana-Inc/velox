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

#include <folly/dynamic.h>
#include <iomanip>
#include <sstream>
#include <string>
#include <type_traits>
#include "folly/hash/Hash.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/StringView.h"
namespace facebook::velox {

#define INT128_UPPER(X) ((int64_t)(X >> 64))
#define INT128_LOWER(X) ((uint64_t)X)
#define MERGE_INT128(UPPER, LOWER) ((int128_t)UPPER << 64) | (LOWER)
#define PRECISION(typmod) ((uint8_t)(typmod >> 8))
#define SCALE(typmod) ((uint8_t)(typmod))
#define TYPMOD(PRECISION, SCALE) (PRECISION << 8 | SCALE)

using int128_t = __int128_t;
static constexpr uint8_t kMaxPrecisionInt128 = 38;
static constexpr uint8_t kDefaultScale = 0;
static constexpr uint8_t kDefaultPrecision = kMaxPrecisionInt128;
static constexpr uint8_t kNumBitsInt128 = sizeof(int128_t) * 8;
static constexpr int64_t kUint64Mask = 0xFFFFFFFFFFFFFFFF;

/**
 * A wrapper struct over int128_t type.
 * Refer https://gcc.gnu.org/onlinedocs/gcc/Integer-Overflow-Builtins.html
 * for supported arithmetic operations extensions.
 */
struct Int128 {
  int128_t value = 0;
  Int128() = default;

  Int128(const Int128& copy) {
    this->value = copy.value;
  }

  Int128(const int128_t value) : value(value) {}

  static Int128 min() {
    return Int128(MERGE_INT128(0x8000000000000000, 0));
  }

  static Int128 max() {
    return Int128(MERGE_INT128(0x7FFFFFFFFFFFFFFF, 0xFFFFFFFFFFFFFFFF));
  }

  FOLLY_ALWAYS_INLINE void operator=(const Int128& rhs) {
    this->value = rhs.value;
  }

  FOLLY_ALWAYS_INLINE Int128 operator+(const Int128& rhs) {
    Int128 sum;
    VELOX_CHECK(!__builtin_add_overflow(this->value, rhs.value, &sum.value));
    return sum;
  }

  FOLLY_ALWAYS_INLINE Int128 operator*(const Int128& rhs) {
    Int128 product;
    VELOX_CHECK(
        !__builtin_mul_overflow(this->value, rhs.value, &product.value));
    return product;
  }

  FOLLY_ALWAYS_INLINE Int128 operator-(const Int128& rhs) {
    Int128 diff;
    VELOX_CHECK(!__builtin_sub_overflow(this->value, rhs.value, &diff.value));
    return diff;
  }

  /*
   * Arithmetic division operation. This algorithm is similar to DuckDbs
   * implementation. This is adopted from Knuth's Art of Programming book,
   * volume 2, Chapter 4.3.1.
   *
   * @lhs Int128 Dividend.
   * @rhs Int128 Divisor.
   * @remainder Int128 Remainder.
   *
   * @return Int128 quotient.
   */
  static Int128 divisionMod(Int128 lhs, Int128 rhs, Int128& remainder) {
    VELOX_CHECK_NE(rhs.value, 0, "Divide by zero error");
    remainder = 0;
    bool isLhsNegative = false;
    if (lhs.value < 0) {
      isLhsNegative = true;
      lhs = lhs.complement();
    }

    bool isRhsNegative = false;
    if (rhs.value < 0) {
      isRhsNegative = true;
      rhs = rhs.complement();
    }

    Int128 quotient(0);
    uint8_t leftMostBit = leftMostBitSet(lhs);
    for (int i = leftMostBit; i >= 0; --i) {
      quotient = quotient << 1;
      remainder = remainder << 1;
      if (lhs.isBitSet(i)) {
        remainder = remainder + 1;
      }
      if (remainder >= rhs) {
        remainder = remainder - rhs;
        quotient = quotient + 1;
      }
    }

    remainder = (isLhsNegative) ? remainder.complement() : remainder;

    return (isLhsNegative ^ isRhsNegative) ? quotient.complement() : quotient;
  }

  FOLLY_ALWAYS_INLINE Int128 operator/(Int128& rhs) {
    Int128 remainder;
    return divisionMod(*this, rhs, remainder);
  }

  FOLLY_ALWAYS_INLINE bool operator>=(const Int128& rhs) {
    return (this->value >= rhs.value);
  }

  FOLLY_ALWAYS_INLINE bool operator==(const Int128& other) const {
    return this->value == other.value;
  }

  FOLLY_ALWAYS_INLINE Int128 operator<<(const Int128 shift) {
    const uint8_t shiftVal = shift.value;
    if (shiftVal == 0) {
      return *this;
    }
    if (shiftVal >= kNumBitsInt128) {
      return Int128(0);
    }
    int64_t upper = INT128_UPPER(this->value);
    uint64_t lower = INT128_LOWER(this->value);
    if (shiftVal < 64) {
      // If shiftVal is less than 64.
      // A part of lower half will get added to upper half.
      upper = (upper << shiftVal) + (lower >> (64 - shiftVal));
      lower = lower << shiftVal;
    } else {
      // shiftVal is >=64
      upper = (lower << (shiftVal - 64)) & kUint64Mask;
      lower = 0;
    }
    return Int128(MERGE_INT128(upper, lower));
  }

  FOLLY_ALWAYS_INLINE Int128 operator>>(const Int128 shift) {
    const uint8_t shiftVal = shift.value;
    if (shiftVal == 0) {
      return *this;
    }
    if (shiftVal >= kNumBitsInt128) {
      return (INT128_UPPER(this->value) < 0) ? Int128(-1) : Int128(0);
    }
    int64_t upper = INT128_UPPER(this->value);
    uint64_t lower = INT128_LOWER(this->value);
    if (shiftVal < 64) {
      lower = (lower >> shiftVal) | (upper << (64 - shiftVal));
      upper = upper >> shiftVal;
    } else {
      lower = (upper >> (shiftVal - 64)) & kUint64Mask;
      upper = (upper < 0) ? -1 : 0;
    }
    return Int128(MERGE_INT128(upper, lower));
  }

  FOLLY_ALWAYS_INLINE Int128 operator~() {
    return ~this->value;
  }

  FOLLY_ALWAYS_INLINE Int128 operator&(const Int128& rhs) const {
    return this->value & rhs.value;
  }

  FOLLY_ALWAYS_INLINE bool isBitSet(const uint8_t i) const {
    return (*this & (Int128(1) << i)).value != 0;
  }
  /*
   * Returns the left most bit set in 128-bit integer value.
   * The position is 0-indexed.
   */
  static uint8_t leftMostBitSet(const Int128& input) {
    // do an and with 2^127
    Int128 mask(MERGE_INT128(0x8000000000000000, 0));
    uint8_t count = 0;
    Int128 value(input.value);
    Int128 shift(1);
    while ((value.value & mask.value) == 0) {
      value = value << 1;
      count++;
    }
    return kNumBitsInt128 - count - 1;
  }

  FOLLY_ALWAYS_INLINE Int128 complement() const {
    return (~this->value + 1);
  }

  FOLLY_ALWAYS_INLINE bool operator<(const Int128 rhs) const {
    return this->value < rhs.value;
  }

  std::string toString() {
    std::string valueStr;
    Int128 number = *this;
    bool isNegative = false;
    if (number == 0) {
      valueStr += '0';
      return valueStr;
    }
    if (number < 0) {
      isNegative = true;
      number = number.complement();
    }
    // 120
    Int128 digit;
    while (number.value != 0) {
      number = divisionMod(number, 10, digit);
      digit = digit < 0 ? digit * -1 : digit;
      valueStr += digit.value + '0';
    }
    if (isNegative) {
      valueStr.append("-");
    }
    std::reverse(valueStr.begin(), valueStr.end());
    return valueStr;
  }
};

/*
 * This class defines the Velox DECIMAL type support to store
 * fixed-point rational numbers.
 */
class Decimal {
 public:
  inline const uint8_t getPrecision() const {
    return precision_;
  }

  inline const uint8_t getScale() const {
    return scale_;
  }

  inline int128_t getUnscaledValue() const {
    return unscaledValue_;
  }

  inline void setUnscaledValue(const int128_t& value) {
    unscaledValue_ = value;
  }

  // Needed for serialization of FlatVector<Decimal>
  // operator StringView() const {VELOX_NYI()}

  static std::string toString();

  operator std::string() const {
    return toString();
  }

  bool operator==(const Decimal& other) const {
    // return this->unscaledValue_ == other.unscaledValue_;
    return (
        this->unscaledValue_ == other.getUnscaledValue() &&
        this->precision_ == other.getPrecision() &&
        this->scale_ == other.getScale());
  }

  bool operator!=(const Decimal& other) const {
    return !(*this == other);
  }

  bool operator<(const Decimal& other) const {
    // VELOX_NYI();
    return this->unscaledValue_ < other.getUnscaledValue();
  }

  bool operator<=(const Decimal& other) const {
    VELOX_NYI();
  }

  bool operator>(const Decimal& other) const {
    VELOX_NYI();
  }

  Decimal(const int128_t& value, int typmod)
      : unscaledValue_(value),
        precision_(PRECISION(typmod)),
        scale_(SCALE(typmod)) {}

  Decimal(const int128_t value) : unscaledValue_(value) {}
  Decimal() = default;

 private:
  int128_t unscaledValue_; // The actual unscaled value with
                           // max precision 38.
  uint8_t precision_ = kDefaultPrecision; // The number of digits in unscaled
                                          // decimal value
  uint8_t scale_ = kDefaultScale; // The number of digits on the right
                                  // of radix point.
};

static const Int128 POWERS_OF_TEN[] = {
    Int128(1),
    Int128(10),
    Int128(100),
    Int128(1000),
    Int128(10000),
    Int128(100000),
    Int128(1000000),
    Int128(10000000),
    Int128(100000000),
    Int128(1000000000),
    Int128(10000000000),
    Int128(100000000000),
    Int128(1000000000000),
    Int128(10000000000000),
    Int128(100000000000000),
    Int128(1000000000000000),
    Int128(10000000000000000),
    Int128(100000000000000000),
    Int128(100000000000000000) * Int128(10),
    Int128(100000000000000000) * Int128(100),
    Int128(100000000000000000) * Int128(1000),
    Int128(100000000000000000) * Int128(10000),
    Int128(100000000000000000) * Int128(100000),
    Int128(100000000000000000) * Int128(1000000),
    Int128(100000000000000000) * Int128(10000000),
    Int128(100000000000000000) * Int128(100000000),
    Int128(100000000000000000) * Int128(1000000000),
    Int128(100000000000000000) * Int128(10000000000),
    Int128(100000000000000000) * Int128(100000000000),
    Int128(100000000000000000) * Int128(1000000000000),
    Int128(100000000000000000) * Int128(10000000000000),
    Int128(100000000000000000) * Int128(100000000000000),
    Int128(100000000000000000) * Int128(1000000000000000),
    Int128(100000000000000000) * Int128(10000000000000000),
    Int128(100000000000000000) * Int128(100000000000000000),
    Int128(100000000000000000) * Int128(100000000000000000) * Int128(10),
    Int128(100000000000000000) * Int128(100000000000000000) * Int128(100),
    Int128(100000000000000000) * Int128(100000000000000000) * Int128(1000),
    Int128(100000000000000000) * Int128(100000000000000000) *
        Int128(10000)}; // 10^38

class DecimalCasts {
 public:
  static Decimal parseStringToDecimal(const std::string& value) {
    // throws overflow exception if length is > 38
    // VELOX_CHECK_GT(
    //    value.length(), 0, "Decimal string must have at least 1 char")
    if (value.length() == 0)
      return Decimal(0);
    int128_t unscaledValue;
    uint8_t precision;
    uint8_t scale;
    try {
      parseToInt128(value, unscaledValue, precision, scale);
    } catch (VeloxRuntimeError const& e) {
      VELOX_USER_CHECK(false, "Decimal overflow");
    }
    return Decimal(unscaledValue);
  }

  /**
   */
  static void parseToInt128(
      std::string value,
      int128_t& result,
      uint8_t& precision,
      uint8_t& scale) {
    result = 0;
    precision = 0;
    scale = 0;
    uint8_t pos = 0;
    bool isNegative = false;
    // Remove leading zeroes.
    if (!isdigit(value[pos])) {
      // Presto allows string literals that start with +123.45
      VELOX_USER_CHECK(
          value[pos] == '-' || value[pos] == '+',
          "Illegal decimal value {}",
          value);
      isNegative = value[pos] == '-';
      value = value.erase(0, 1);
    }
    value = value.erase(0, value.find_first_not_of('0'));
    precision = 0;
    scale = 0;
    bool hasScale = false;
    int128_t digit;
    int128_t exponent((int128_t)10);
    while (pos < value.length()) {
      if (value[pos] == '.') {
        hasScale = true;
        pos++;
        continue;
      }
      VELOX_USER_CHECK(std::isdigit(value[pos]), "Invalid decimal string");
      digit = value[pos] - '0';
      if (isNegative) {
        result = result * exponent - digit;
      } else {
        result = result * exponent + digit;
      }
      if (hasScale) {
        scale++;
      }
      precision++;
      pos++;
    }
  }
};

void parseTo(folly::StringPiece in, Decimal& out);

template <typename T>
void toAppend(const ::facebook::velox::Decimal& value, T* result) {}
} // namespace facebook::velox

namespace std {
template <>
struct hash<::facebook::velox::Decimal> {
  size_t operator()(const ::facebook::velox::Decimal& value) const {
    return folly::hasher<signed __int128>()(value.getUnscaledValue());
  }
};

std::string to_string(const ::facebook::velox::Decimal& ts);
} // namespace std

namespace folly {
template <>
struct hasher<::facebook::velox::Decimal> {
  size_t operator()(const ::facebook::velox::Decimal& value) const {
    return folly::hasher<signed __int128>()(value.getUnscaledValue());
  }
};
} // namespace folly
