/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */

#pragma once

#include "http/probe.h"
#include "metrics/metrics.h"
#include "net/types.h"

namespace iceberg::rest_client {

class client_probe : public http::client_probe {
public:
    client_probe(
      net::public_metrics_disabled public_disable, ss::metrics::label_instance);

private:
    void setup_public_metrics(
      net::public_metrics_disabled disable, ss::metrics::label_instance);

    metrics::internal_metric_groups _public_metrics;
};

} // namespace iceberg::rest_client
