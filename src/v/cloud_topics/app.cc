/*
 * Copyright 2024 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
#include "cloud_topics/app.h"

namespace experimental::cloud_topics {

constexpr ss::shard_id control_plane_shard = 0;

app::app(
  ss::shared_ptr<data_plane_api> dp, std::unique_ptr<l1::control_plane> l1_cp)
  : _data_plane(std::move(dp))
  , _control_plane(std::move(l1_cp)) {}

seastar::future<> app::start() {
    if (ss::this_shard_id() == control_plane_shard) {
        co_await _control_plane->start();
    }
    co_await _data_plane->start();
}

seastar::future<> app::stop() {
    if (ss::this_shard_id() == control_plane_shard) {
        co_await _control_plane->stop();
    }
    co_await _data_plane->stop();
}

ss::shared_ptr<data_plane_api> app::get_data_plane_api() { return _data_plane; }

} // namespace experimental::cloud_topics
