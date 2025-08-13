// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "storage/parser_utils.h"

#include "base/vlog.h"
#include "compression/compression.h"
#include "model/compression.h"
#include "model/record.h"
#include "model/record_utils.h"

#include <seastar/core/byteorder.hh>
#include <seastar/core/coroutine.hh>

namespace storage::internal {

ss::future<model::record_batch> decompress_batch(model::record_batch&& b) {
    return ss::futurize_invoke(decompress_batch_sync, std::move(b));
}

ss::future<model::record_batch> decompress_batch(const model::record_batch& b) {
    return ss::futurize_invoke(maybe_decompress_batch_sync, b);
}

model::record_batch decompress_batch_sync(model::record_batch&& b) {
    if (!b.compressed()) {
        return std::move(b);
    }

    return maybe_decompress_batch_sync(b);
}

model::record_batch maybe_decompress_batch_sync(const model::record_batch& b) {
    if (unlikely(!b.compressed())) {
        throw std::runtime_error(fmt_with_ctx(
          fmt::format,
          "Asked to decompressed a non-compressed batch:{}",
          b.header()));
    }
    iobuf body_buf = compression::compressor::uncompress(
      b.data(), b.header().attrs.compression());
    // must remove compression first!
    auto h = b.header();
    h.attrs.remove_compression();
    reset_size_checksum_metadata(h, body_buf);
    auto batch = model::record_batch(
      h, std::move(body_buf), model::record_batch::tag_ctor_ng{});
    return batch;
}

ss::future<model::record_batch>
compress_batch(model::compression c, model::record_batch b) {
    if (c == model::compression::none) {
        vassert(
          b.header().attrs.compression() == model::compression::none,
          "Asked to compress a batch with `none` compression, but header "
          "metadata is incorrect: {}",
          b.header());
        co_return b;
    }
    auto h = b.header();
    auto payload = co_await compression::stream_compressor::compress(
      std::move(b).release_data(), c);
    // compression bit must be set first!
    h.attrs |= c;
    reset_size_checksum_metadata(h, payload);
    auto batch = model::record_batch(
      h, std::move(payload), model::record_batch::tag_ctor_ng{});
    co_return batch;
}

/// \brief resets the size, header crc and payload crc
void reset_size_checksum_metadata(
  model::record_batch_header& hdr, const iobuf& records) {
    hdr.size_bytes = model::packed_record_batch_header_size
                     + records.size_bytes();
    hdr.crc = model::crc_record_batch(hdr, records);
    hdr.header_crc = model::internal_header_only_crc(hdr);
}

} // namespace storage::internal
