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
#include "velox/expression//EvalCtx.h"
#include "velox/expression/VectorFunction.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {

// Returns the total number of nested elements for the specified top-levels rows
// in array or map vector. T is either ArrayVector or MapVector.
template <typename T>
vector_size_t countElements(
    const SelectivityVector& rows,
    DecodedVector& decodedVector) {
  auto indices = decodedVector.indices();
  auto rawSizes = decodedVector.base()->as<T>()->rawSizes();

  vector_size_t count = 0;
  rows.applyToSelected([&](vector_size_t row) {
    if (decodedVector.isNullAt(row)) {
      return;
    }
    count += rawSizes[indices[row]];
  });
  return count;
}

// Returns SelectivityVector for the nested vector with all rows corresponding
// to specified top-level rows selected. The optional topLevelRowMapping is
// used to pass the dictionary indices if the topLevelVector is dictionary
// encoded.
template <typename T>
SelectivityVector toElementRows(
    vector_size_t size,
    const SelectivityVector& topLevelRows,
    const T* topLevelVector,
    const vector_size_t* topLevelRowMapping = nullptr) {
  auto rawNulls = topLevelVector->rawNulls();
  auto rawSizes = topLevelVector->rawSizes();
  auto rawOffsets = topLevelVector->rawOffsets();

  SelectivityVector elementRows(size, false);
  topLevelRows.applyToSelected([&](vector_size_t row) {
    auto index = topLevelRowMapping ? topLevelRowMapping[row] : row;
    if (rawNulls && bits::isBitNull(rawNulls, index)) {
      return;
    }
    auto size = rawSizes[index];
    auto offset = rawOffsets[index];
    elementRows.setValidRange(offset, offset + size, true);
  });
  elementRows.updateBounds();
  return elementRows;
}

// Returns an array of indices that allows aligning captures with the nested
// elements of an array or vector. For each top-level row, the index equal to
// the row number is repeated for each of the nested rows.
template <typename T>
BufferPtr toWrapCapture(
    vector_size_t size,
    const Callable* callable,
    const SelectivityVector& topLevelRows,
    const std::shared_ptr<T>& topLevelVector) {
  if (!callable->hasCapture()) {
    return nullptr;
  }

  auto rawNulls = topLevelVector->rawNulls();
  auto rawSizes = topLevelVector->rawSizes();
  auto rawOffsets = topLevelVector->rawOffsets();

  BufferPtr wrapCapture = allocateIndices(size, topLevelVector->pool());
  auto rawWrapCapture = wrapCapture->asMutable<vector_size_t>();
  topLevelRows.applyToSelected([&](vector_size_t row) {
    if (rawNulls && bits::isBitNull(rawNulls, row)) {
      return;
    }
    auto size = rawSizes[row];
    auto offset = rawOffsets[row];
    for (auto i = 0; i < size; ++i) {
      rawWrapCapture[offset + i] = row;
    }
  });
  return wrapCapture;
}

// Given possibly wrapped array vector, flattens the wrappings and returns a
// flat array vector. Returns the original vector unmodified if the vector is
// not wrapped. Flattening is shallow, e.g. elements vector may still be
// wrapped.
ArrayVectorPtr flattenArray(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector);

// Given possibly wrapped map vector, flattens the wrappings and returns a flat
// map vector. Returns the original vector unmodified if the vector is not
// wrapped. Flattening is shallow, e.g. keys and values vectors may still be
// wrapped.
MapVectorPtr flattenMap(
    const SelectivityVector& rows,
    const VectorPtr& vector,
    DecodedVector& decodedVector);

template <typename T>
struct SetWithNull {
  SetWithNull(vector_size_t initialSetSize = kInitialSetSize) {
    set.reserve(initialSetSize);
  }

  void reset() {
    set.clear();
    hasNull = false;
  }

  std::unordered_set<T> set;
  bool hasNull{false};
  static constexpr vector_size_t kInitialSetSize{128};
};

// Generates a set based on the elements of an ArrayVector. Note that we take
// rightSet as a parameter (instead of returning a new one) to reuse the
// allocated memory.
template <typename T, typename TVector>
void generateSet(
    const ArrayVector* arrayVector,
    const TVector* arrayElements,
    vector_size_t idx,
    SetWithNull<T>& rightSet) {
  auto size = arrayVector->sizeAt(idx);
  auto offset = arrayVector->offsetAt(idx);
  rightSet.reset();

  for (vector_size_t i = offset; i < (offset + size); ++i) {
    if (arrayElements->isNullAt(i)) {
      rightSet.hasNull = true;
    } else {
      // Function can be called with either FlatVector or DecodedVector, but
      // their APIs are slightly different.
      if constexpr (std::is_same_v<TVector, DecodedVector>) {
        rightSet.set.insert(arrayElements->template valueAt<T>(i));
      } else {
        rightSet.set.insert(arrayElements->valueAt(i));
      }
    }
  }
}

// Returns pointer to a decoded elements vector for a given BaseVector.
// Here is how the nesting of classes looks like:
//        ArrayVector {
//          OFFSETS:[]
//          SIZES:[]
//          ElementsVector --> returns this
//        }
DecodedVector* getDecodedElementsFromArrayVector(
    exec::LocalDecodedVector& decoder,
    exec::LocalDecodedVector& elementsDecoder,
    const SelectivityVector& rows);

// Validates a vector of VectorFunctionArg satisfies PrestoDb array functions
// that accept 2 parameters like array_intersect, array_except, arrays_overlap.
void validateType(
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const std::string name,
    const vector_size_t expectedArgCount);
} // namespace facebook::velox::functions
