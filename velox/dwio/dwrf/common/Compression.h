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

#include "velox/common/compression/Compression.h"
#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/common/compression/Compression.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/Config.h"
#include "velox/dwio/dwrf/common/Decryption.h"
#include "velox/dwio/dwrf/common/Encryption.h"

namespace facebook::velox::dwrf {

using namespace dwio::common::compression;

constexpr uint8_t PAGE_HEADER_SIZE = 3;

static const CompressionOptions getDwrfOrcCompressionOptions(
    velox::common::CompressionKind kind,
    uint32_t compressionThreshold,
    int32_t zlibCompressionLevel,
    int32_t zstdCompressionLevel) {
  CompressionOptions options;
  options.compressionThreshold = compressionThreshold;

  if (kind == velox::common::CompressionKind_ZLIB) {
    options.format.zlib.windowBits = Compressor::DWRF_ORC_ZLIB_WINDOW_BITS;
    options.format.zlib.compressionLevel = zlibCompressionLevel;
  } else if (kind == velox::common::CompressionKind_ZSTD) {
    options.format.zstd.compressionLevel = zstdCompressionLevel;
  }
  return options;
}

/**
 * Create a compressor for the given compression kind.
 * @param kind The compression type to implement
 * @param bufferPool Pool for compression buffer
 * @param bufferHolder Buffer holder that handles buffer allocation and
 * collection
 * @param config The compression options to use
 */
static std::unique_ptr<dwio::common::BufferedOutputStream> createCompressor(
    common::CompressionKind kind,
    CompressionBufferPool& bufferPool,
    dwio::common::DataBufferHolder& bufferHolder,
    const Config& config,
    const dwio::common::encryption::Encrypter* encrypter = nullptr) {
  CompressionOptions dwrfOrcCompressionOptions = getDwrfOrcCompressionOptions(
      kind,
      config.get(Config::COMPRESSION_THRESHOLD),
      config.get(Config::ZLIB_COMPRESSION_LEVEL),
      config.get(Config::ZSTD_COMPRESSION_LEVEL));

  return createCompressor(
      kind,
      bufferPool,
      bufferHolder,
      PAGE_HEADER_SIZE,
      dwrfOrcCompressionOptions,
      encrypter);
}

static const CompressionOptions getDwrfOrcDecompressionOptions() {
  CompressionOptions options;
  options.format.zlib.windowBits = Compressor::DWRF_ORC_ZLIB_WINDOW_BITS;
  return options;
}

/**
 * Create a decompressor for the given compression kind.
 * @param kind The compression type to implement
 * @param input The input stream that is the underlying source
 * @param bufferSize The maximum size of the buffer
 * @param pool The memory pool
 */
static std::unique_ptr<dwio::common::SeekableInputStream> createDecompressor(
    facebook::velox::common::CompressionKind kind,
    std::unique_ptr<dwio::common::SeekableInputStream> input,
    uint64_t bufferSize,
    memory::MemoryPool& pool,
    const std::string& streamDebugInfo,
    const dwio::common::encryption::Decrypter* decryptr = nullptr) {
  const CompressionOptions& options = getDwrfOrcDecompressionOptions();
  return createDecompressor(
      kind,
      std::move(input),
      bufferSize,
      pool,
      options,
      streamDebugInfo,
      decryptr);
}

} // namespace facebook::velox::dwrf
