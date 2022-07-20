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

#include "velox/expression/DecodedArgs.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/DecimalUtils.h"

namespace facebook::velox::functions {
namespace {

template <
    typename R /* Result Type */,
    typename A /* Argument1 */,
    typename B /* Argument2 */,
    typename Operation /* Arithmetic operation */>
class DecimalBaseFunction : public exec::VectorFunction {
 public:
  DecimalBaseFunction(uint8_t aRescale, uint8_t bRescale)
      : aRescale_(aRescale), bRescale_(bRescale) {}
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& resultType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, const).
      auto constant = args[0]->asUnchecked<SimpleVector<A>>()->valueAt(0);
      auto flatValues = args[1]->asUnchecked<FlatVector<B>>();
      auto rawValues = flatValues->mutableRawValues();
      auto rawResults =
          getRawResults(rows, args[1], resultType, context, result);
      context->applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], constant, rawValues[row], aRescale_, bRescale_);
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      auto flatValues = args[0]->asUnchecked<FlatVector<A>>();
      auto constant = args[1]->asUnchecked<SimpleVector<B>>()->valueAt(0);
      auto rawValues = flatValues->mutableRawValues();
      auto rawResults =
          getRawResults(rows, args[1], resultType, context, result);
      context->applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], rawValues[row], constant, aRescale_, bRescale_);
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      auto flatA = args[0]->asUnchecked<FlatVector<A>>();
      auto rawA = flatA->mutableRawValues();
      auto flatB = args[1]->asUnchecked<FlatVector<B>>();
      auto rawB = flatB->mutableRawValues();

      R* rawResults = nullptr;
      if (!(*result)) {
        // Try to reuse one of the flat vectors to store result.
        if (resultType->kind() == flatA->typeKind() &&
            BaseVector::isReusableFlatVector(args[0])) {
          rawResults = reinterpret_cast<R*>(rawA);
          *result = std::move(args[0]);
        } else if (
            resultType->kind() == flatB->typeKind() &&
            BaseVector::isReusableFlatVector(args[1])) {
          rawResults = reinterpret_cast<R*>(rawB);
          *result = std::move(args[1]);
        }
      }
      if (!rawResults) {
        rawResults = prepareResults(rows, resultType, context, result);
      }
      context->applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row], rawA[row], rawB[row], aRescale_, bRescale_);
      });
    } else {
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto a = decodedArgs.at(0);
      auto b = decodedArgs.at(1);
      BaseVector::ensureWritable(rows, resultType, context->pool(), result);
      auto rawResults =
          (*result)->asUnchecked<FlatVector<R>>()->mutableRawValues();
      context->applyToSelectedNoThrow(rows, [&](auto row) {
        Operation::template apply<R, A, B>(
            rawResults[row],
            a->valueAt<A>(row),
            b->valueAt<B>(row),
            aRescale_,
            bRescale_);
      });
    }
  }

 private:
  R* prepareResults(
      const SelectivityVector& rows,
      const TypePtr& resultType,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    BaseVector::ensureWritable(rows, resultType, context->pool(), result);
    (*result)->clearNulls(rows);
    return (*result)->asUnchecked<FlatVector<R>>()->mutableRawValues();
  }

  R* getRawResults(
      const SelectivityVector& rows,
      VectorPtr& flat,
      const TypePtr& resultType,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    // Check if input can be reused for results.
    R* rawResults;
    if (!(*result) && flat->typeKind() == resultType->kind() &&
        BaseVector::isReusableFlatVector(flat)) {
      auto flatValues = flat->asUnchecked<FlatVector<R>>();
      rawResults = flatValues->mutableRawValues();
      *result = std::move(flat);
    } else {
      rawResults = prepareResults(rows, resultType, context, result);
    }
    return rawResults;
  }
  uint8_t aRescale_;
  uint8_t bRescale_;
};

class Addition {
 public:
  template <typename R, typename A, typename B>
  inline static void
  apply(R& r, const A& a, const B& b, uint8_t aRescale, uint8_t bRescale) {
    r.setUnscaledValue(
        a.unscaledValue() * kPowersOfTen[aRescale] +
        b.unscaledValue() * kPowersOfTen[bRescale]);
  }

  inline static uint8_t
  computeRescaleFactor(uint8_t fromScale, uint8_t toScale, uint8_t rScale = 0) {
    return std::max(0, toScale - fromScale);
  }

  inline static void computeResultPrecisionScale(
      uint8_t aPrecision,
      uint8_t aScale,
      uint8_t bPrecision,
      uint8_t bScale,
      uint8_t& rPrecision,
      uint8_t& rScale) {
    rPrecision = std::min(
        38,
        std::max(aPrecision - aScale, bPrecision - bScale) +
            std::max(aScale, bScale) + 1);
    rScale = std::max(aScale, bScale);
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalAddSubSignature() {
  return {
      exec::FunctionSignatureBuilder()
          .returnType("DECIMAL(r_precision, r_scale)")
          .argumentType("DECIMAL(a_precision, a_scale)")
          .argumentType("DECIMAL(b_precision, b_scale)")
          .variableConstraint(
              "r_precision",
              "min(38, max(a_precision - a_scale, b_precision - b_scale) + max(a_scale, b_scale) + 1)")
          .variableConstraint("r_scale", "max(a_scale, b_scale)")
          .build()};
}

template <typename Operation>
std::shared_ptr<exec::VectorFunction> createDecimalFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  uint8_t aScale;
  uint8_t bScale;
  uint8_t aPrecision;
  uint8_t bPrecision;
  uint8_t rScale;
  uint8_t rPrecision;
  getDecimalPrecisionScale(aType, aPrecision, aScale);
  getDecimalPrecisionScale(bType, bPrecision, bScale);
  Operation::computeResultPrecisionScale(
      aPrecision, aScale, bPrecision, bScale, rPrecision, rScale);
  uint8_t aRescale = Operation::computeRescaleFactor(aScale, bScale, rScale);
  uint8_t bRescale = Operation::computeRescaleFactor(bScale, aScale, rScale);
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      if (rPrecision > DecimalType<TypeKind::SHORT_DECIMAL>::kMaxPrecision) {
        // Arguments are short decimals and result is a long decimal.
        return std::make_shared<DecimalBaseFunction<
            LongDecimal /*result*/,
            ShortDecimal,
            ShortDecimal,
            Operation>>(aRescale, bRescale);
      } else {
        // Arguments are short decimals and result is a short decimal.
        return std::make_shared<DecimalBaseFunction<
            ShortDecimal /*result*/,
            ShortDecimal,
            ShortDecimal,
            Operation>>(aRescale, bRescale);
      }
    } else {
      // LHS is short decimal and rhs is a long decimal, result is long decimal.
      return std::make_shared<DecimalBaseFunction<
          LongDecimal /*result*/,
          ShortDecimal,
          LongDecimal,
          Operation>>(aRescale, bRescale);
    }
  } else {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      // LHS is long decimal and rhs is short decimal, result is a long decimal.
      return std::make_shared<DecimalBaseFunction<
          LongDecimal /*result*/,
          LongDecimal,
          ShortDecimal,
          Operation>>(aRescale, bRescale);
    } else {
      // Arguments and result are all long decimals.
      return std::make_shared<DecimalBaseFunction<
          LongDecimal /*result*/,
          LongDecimal,
          LongDecimal,
          Operation>>(aRescale, bRescale);
    }
  }
  VELOX_UNSUPPORTED();
}
}; // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add,
    decimalAddSubSignature(),
    createDecimalFunction<Addition>);
}; // namespace facebook::velox::functions
