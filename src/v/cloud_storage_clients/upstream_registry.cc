/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream_registry.h"

#include "cloud_storage_clients/detail/registry_def.h" // IWYU pragma: keep
#include "cloud_storage_clients/logger.h"

#include <seastar/core/sharded.hh>

namespace cloud_storage_clients {

template class detail::
  basic_registry<upstream, upstream_key, upstream_registry>;

upstream_registry::upstream_registry(client_configuration config)
  : detail::basic_registry<upstream, upstream_key, upstream_registry>(
      pool_log, no_entry_limit)
  , _config(std::move(config)) {}

ss::future<>
upstream_registry::start_svc(ss::sharded<upstream>& svc, const upstream_key&) {
    return svc.start(_config);
}

} // namespace cloud_storage_clients
