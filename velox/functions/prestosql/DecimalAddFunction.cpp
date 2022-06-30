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

#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/prestosql/CheckedArithmeticImpl.h"
#include "velox/type/DecimalUtils.cpp"

namespace facebook::velox::functions {
namespace {

template <typename R, typename LHS, typename RHS>
void call(R& result, LHS& a, RHS& b, uint8_t rescaleFactor, bool rescaleLeft) {
  if (rescaleLeft) {
    result = a * POWERS_OF_TEN[rescaleFactor] + b;
  } else {
    result = a + b * POWERS_OF_TEN[rescaleFactor];
  }
}

template <TypeKind R, TypeKind LHS, TypeKind RHS>
class DecimalAddFunction : public exec::VectorFunction {
 public:
  DecimalAddFunction(uint8_t rescale, bool rescaleLeft)
      : rescaleFactor_(rescale), rescaleLeft_(rescaleLeft) {}

  using ResNativeType = typename TypeTraits<R>::NativeType;
  using LHSNativeType = typename TypeTraits<LHS>::NativeType;
  using RHSNativeType = typename TypeTraits<RHS>::NativeType;

  using ResDeepCopiedType = typename TypeTraits<R>::DeepCopiedType;
  using LDeepCopiedType = typename TypeTraits<LHS>::DeepCopiedType;
  using RDeepCopiedType = typename TypeTraits<RHS>::DeepCopiedType;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    memory::MemoryPool* pool = context->pool();
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();

    auto aFlatVector = args[0]->asFlatVector<LHSNativeType>();
    auto bFlatVector = args[1]->asFlatVector<RHSNativeType>();
    // Initialize flat results vector.
    BaseVector::ensureWritable(rows, outputType, context->pool(), result);
    BufferPtr resultValues =
        (*result)->as<FlatVector<ResNativeType>>()->mutableValues(rows.size());
    auto rawValues = resultValues->asMutable<ResNativeType>();

    rows.applyToSelected([&](vector_size_t row) {
      LDeepCopiedType a = aFlatVector->valueAt(row).unscaledValue();
      RDeepCopiedType b = bFlatVector->valueAt(row).unscaledValue();
      ResDeepCopiedType result;
      call<ResDeepCopiedType, LDeepCopiedType, RDeepCopiedType>(
          result, a, b, rescaleFactor_, rescaleLeft_);
      rawValues[row] = ResNativeType(result);
    });
  }

 private:
  const uint8_t rescaleFactor_;
  const bool rescaleLeft_;
};

std::vector<std::shared_ptr<exec::FunctionSignature>> decimalAddSignature() {
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

std::shared_ptr<exec::VectorFunction> createDecimalAddFunction(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  auto aType = inputArgs[0].type;
  auto bType = inputArgs[1].type;
  uint8_t aScale, bScale, aPrecision, bPrecision;
  getPrecisionScale(aType, aPrecision, aScale);
  getPrecisionScale(bType, bPrecision, bScale);
  bool rescaleLeft = true;
  uint8_t rescale = computeRescaleFactor(aScale, bScale, rescaleLeft);
  uint8_t resultPrescision =
      computeResultPrecision(aPrecision, aScale, bPrecision, bScale);
  if (aType->kind() == TypeKind::SHORT_DECIMAL) {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      if (resultPrescision > ShortDecimalType::kMaxPrecision) {
        // Add two short decimals and result is a long decimal.
        return std::make_shared<DecimalAddFunction<
            TypeKind::LONG_DECIMAL /*result*/,
            TypeKind::SHORT_DECIMAL,
            TypeKind::SHORT_DECIMAL>>(rescale, rescaleLeft);

      } else {
        // Add two short decimals and result is a short decimal.
        return std::make_shared<DecimalAddFunction<
            TypeKind::SHORT_DECIMAL /*result*/,
            TypeKind::SHORT_DECIMAL,
            TypeKind::SHORT_DECIMAL>>(rescale, rescaleLeft);
      }
    } else {
      // Add a short decimal and long decimal, result is long decimal.
      return std::make_shared<DecimalAddFunction<
          TypeKind::LONG_DECIMAL /*result*/,
          TypeKind::SHORT_DECIMAL,
          TypeKind::LONG_DECIMAL>>(rescale, rescaleLeft);
    }
  } else {
    if (bType->kind() == TypeKind::SHORT_DECIMAL) {
      // Add long decimal and short decimals, result is a long decimal.
      return std::make_shared<DecimalAddFunction<
          TypeKind::LONG_DECIMAL /*result*/,
          TypeKind::LONG_DECIMAL,
          TypeKind::SHORT_DECIMAL>>(rescale, rescaleLeft);
    } else {
      return std::make_shared<DecimalAddFunction<
          TypeKind::LONG_DECIMAL /*result*/,
          TypeKind::LONG_DECIMAL,
          TypeKind::LONG_DECIMAL>>(rescale, rescaleLeft);
    }
  }
  VELOX_UNSUPPORTED();
}
}; // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_decimal_add,
    decimalAddSignature(),
    createDecimalAddFunction);
}; // namespace facebook::velox::functions
