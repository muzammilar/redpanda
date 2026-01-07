/*
 * Copyright 2026 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "cloud_storage_clients/configuration.h"

namespace cloud_storage_clients {

/// A cloud storage upstream/client factory.
class upstream {
public:
    explicit upstream(client_configuration config);

private:
    client_configuration _config;
};

}; // namespace cloud_storage_clients
