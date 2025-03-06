/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "iceberg/rest_client/client_probe.h"

#include "metrics/metrics.h"
#include "metrics/prometheus_sanitize.h"

namespace iceberg::rest_client {

namespace {
const auto group_name = prometheus_sanitize::metrics_name(
  "iceberg:rest_client");
} // namespace

client_probe::client_probe(
  net::public_metrics_disabled public_disable,
  ss::metrics::label_instance label)
  : http::client_probe() {
    setup_public_metrics(public_disable, std::move(label));
}

void client_probe::setup_public_metrics(
  net::public_metrics_disabled disable, ss::metrics::label_instance label) {
    namespace sm = ss::metrics;
    if (disable) {
        return;
    }

    std::vector<sm::label_instance> labels;
    labels.emplace_back(std::move(label));
    _public_metrics.add_group(
      group_name,
      {
        sm::make_counter(
          "total_puts",
          [this] { return get_total_put_requests(); },
          sm::description("Number of completed PUT requests"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_gets",
          [this] { return get_total_get_requests(); },
          sm::description("Number of completed GET requests"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_requests",
          [this] { return get_total_requests(); },
          sm::description(
            "Number of completed HTTP requests (includes PUT and GET)"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_puts",
          [this] { return get_active_put_requests(); },
          sm::description("Number of active PUT requests at the moment"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_gets",
          [this] { return get_active_get_requests(); },
          sm::description("Number of active GET requests at the moment"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_gauge(
          "active_requests",
          [this] { return get_active_requests(); },
          sm::description("Number of active HTTP requests at the moment "
                          "(includes PUT and GET)"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_inbound_bytes",
          [this] { return get_inbound_bytes(); },
          sm::description(
            "Total number of bytes received from the Iceberg REST catalog"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "total_outbound_bytes",
          [this] { return get_outbound_bytes(); },
          sm::description(
            "Total number of bytes sent to the Iceberg REST catalog"),
          labels)
          .aggregate({sm::shard_label}),
        sm::make_counter(
          "num_transport_errors",
          [this] { return get_transport_errors(); },
          sm::description("Total number of transport errors (TCP and TLS)"),
          labels)
          .aggregate({sm::shard_label}),
      });
}

} // namespace iceberg::rest_client
