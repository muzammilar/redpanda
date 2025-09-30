/*
 * Copyright 2025 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cloud_topics/level_zero/stm/placeholder.h"

namespace cloud_topics {

dl_placeholder parse_placeholder_batch(model::record_batch batch) {
    iobuf payload = std::move(batch).release_data();
    iobuf_parser parser(std::move(payload));
    auto record = model::parse_one_record_from_buffer(parser);
    iobuf value = std::move(record).release_value();
    auto placeholder = serde::from_iobuf<cloud_topics::dl_placeholder>(
      std::move(value));
    return placeholder;
}

} // namespace cloud_topics
