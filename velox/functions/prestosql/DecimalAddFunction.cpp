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
void call(R& result, LHS& a, RHS& b, uint8_t aRescale, uint8_t bRescale) {
  result = a * POWERS_OF_TEN[aRescale] + b * POWERS_OF_TEN[bRescale];
}

template <TypeKind R, TypeKind LHS, TypeKind RHS>
class DecimalAddFunction : public exec::VectorFunction {
 public:
  DecimalAddFunction(uint8_t aRescale, uint8_t bRescale)
      : aRescale_(aRescale), bRescale_(bRescale) {}

  using ResType = typename TypeTraits<R>::NativeType;
  using LType = typename TypeTraits<LHS>::NativeType;
  using RType = typename TypeTraits<RHS>::NativeType;

  using ResInternalType = typename TypeTraits<R>::DeepCopiedType;
  using LInternalType = typename TypeTraits<LHS>::DeepCopiedType;
  using RInternalType = typename TypeTraits<RHS>::DeepCopiedType;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    memory::MemoryPool* pool = context->pool();
    BaseVector* left = args[0].get();
    BaseVector* right = args[1].get();

    auto aFlatVector = args[0]->asFlatVector<LType>();
    auto bFlatVector = args[1]->asFlatVector<RType>();
    // Initialize flat results vector.
    BaseVector::ensureWritable(rows, outputType, context->pool(), result);
    BufferPtr resultValues =
        (*result)->as<FlatVector<ResType>>()->mutableValues(rows.size());
    auto rawValues = resultValues->asMutable<ResType>();

    rows.applyToSelected([&](vector_size_t row) {
      LInternalType a = aFlatVector->valueAt(row).unscaledValue();
      RInternalType b = bFlatVector->valueAt(row).unscaledValue();
      ResInternalType result;
      call<ResInternalType, LInternalType, RInternalType>(
          result, a, b, aRescale_, bRescale_);
      rawValues[row] = ResType(result);
    });
  }

 private:
  void rescale(
      FlatVector<ShortDecimal>* aFlatVector,
      FlatVector<ShortDecimal>* bFlatVector,
      const vector_size_t i,
      std::vector<int128_t>& rescaled) const {
    rescaled.push_back(
        aFlatVector->isNullAt(i)
            ? 0
            : ((int128_t)aFlatVector->valueAt(i).unscaledValue()) *
                (int128_t)pow(10, aRescale_));
    rescaled.push_back(
        bFlatVector->isNullAt(i)
            ? 0
            : ((int128_t)bFlatVector->valueAt(i).unscaledValue()) *
                (int128_t)pow(10, bRescale_));
  }
  const uint8_t aRescale_;
  const uint8_t bRescale_;
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
  ShortDecimalTypePtr aType =
      std::dynamic_pointer_cast<const ShortDecimalType>(inputArgs[0].type);
  ShortDecimalTypePtr bType =
      std::dynamic_pointer_cast<const ShortDecimalType>(inputArgs[1].type);
  uint8_t aRescale = computeRescaleFactor(aType->scale(), bType->scale());
  uint8_t bRescale = computeRescaleFactor(bType->scale(), aType->scale());
  return std::make_shared<DecimalAddFunction<
      TypeKind::LONG_DECIMAL,
      TypeKind::SHORT_DECIMAL,
      TypeKind::SHORT_DECIMAL>>(aRescale, bRescale);
}
}; // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_add_short_short,
    decimalAddSignature(),
    createDecimalAddFunction);

}; // namespace facebook::velox::functions
