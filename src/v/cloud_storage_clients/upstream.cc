/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#include "cloud_storage_clients/upstream.h"

#include "cloud_storage_clients/configuration.h"

namespace cloud_storage_clients {

upstream::upstream(client_configuration config)
  : _config(std::move(config)) {}

} // namespace cloud_storage_clients
