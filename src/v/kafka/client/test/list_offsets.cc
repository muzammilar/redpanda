// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/client/client.h"
#include "kafka/client/test/fixture.h"
#include "kafka/protocol/errors.h"
#include "model/fundamental.h"

#include <boost/test/tools/old/interface.hpp>

inline const model::topic_partition unknown_tp{
  model::topic{"unknown"}, model::partition_id{0}};

class list_offsets_fixture : public kafka_client_fixture {};

FIXTURE_TEST(test_unknown_topic, list_offsets_fixture) {
    auto client = make_connected_client();
    auto stop_client = ss::defer([&client]() { client.stop().get(); });

    client.config().retry_base_backoff.set_value(10ms);
    client.config().retries.set_value(size_t(5));

    BOOST_REQUIRE_EXCEPTION(
      client.list_offsets(unknown_tp).get(),
      kafka::exception_base,
      [](kafka::exception_base ex) {
          return ex.error == kafka::error_code::unknown_topic_or_partition;
      });
}
