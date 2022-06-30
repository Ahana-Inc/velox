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

#include "DecimalUtils.h"

namespace facebook::velox {

uint8_t computeRescaleFactor(
    const uint8_t fromScale,
    const uint8_t toScale,
    bool& isLeft) {
  uint8_t rescaleleft = std::max(0, toScale - fromScale);
  isLeft = true;
  if (rescaleleft == 0) {
    isLeft = false;
    return std::max(0, fromScale - toScale);
  }
  return rescaleleft;
}

void getPrecisionScale(
    const TypePtr& type,
    uint8_t& precision,
    uint8_t& scale) {
  if (type->kind() == TypeKind::SHORT_DECIMAL) {
    auto shortDecimalType = type->asShortDecimal();
    precision = shortDecimalType.precision();
    scale = shortDecimalType.scale();
  } else {
    auto longDecimalType = type->asLongDecimal();
    precision = longDecimalType.precision();
    scale = longDecimalType.scale();
  }
}

uint8_t computeResultPrecision(
    const uint8_t aPrecision,
    const uint8_t aScale,
    const uint8_t bPrecision,
    const uint8_t bScale) {
  return std::min(
      38,
      std::max(aPrecision - aScale, bPrecision - bScale) +
          std::max(aScale, bScale) + 1);
}
} // namespace facebook::velox
