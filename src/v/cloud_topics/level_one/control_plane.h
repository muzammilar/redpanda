/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "base/seastarx.h"

#include <seastar/core/future.hh>

namespace cluster {
class controller;
}

namespace experimental::cloud_topics::l1 {

// The control plane of cloud topics is responsible for creating
// domains.
//
// There is only a single control plane per node and it runs on shard 0.
class control_plane {
    class impl;

public:
    explicit control_plane(cluster::controller*);
    control_plane(const control_plane&) = delete;
    control_plane(control_plane&&) = delete;
    control_plane& operator=(const control_plane&) = delete;
    control_plane& operator=(control_plane&&) = delete;
    ~control_plane();

    ss::future<> start();
    ss::future<> stop();

private:
    std::unique_ptr<impl> _impl;
};

} // namespace experimental::cloud_topics::l1
