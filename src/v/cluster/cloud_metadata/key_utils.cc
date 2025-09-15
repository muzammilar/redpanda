/*
 * Copyright 2023 Redpanda Data, Inc.
 *
 * Licensed as a Redpanda Enterprise file under the Redpanda Community
 * License (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * https://github.com/redpanda-data/redpanda/blob/master/licenses/rcl.md
 */
#include "cluster/cloud_metadata/key_utils.h"

#include "cluster/cloud_metadata/types.h"
#include "model/fundamental.h"

namespace cluster::cloud_metadata {

constexpr std::string_view prefix = "cluster_metadata";

ss::sstring cluster_uuid_prefix(const model::cluster_uuid& cluster_uuid) {
    return fmt::format("{}/{}", prefix, ss::sstring(cluster_uuid()));
}

ss::sstring cluster_manifests_prefix(const model::cluster_uuid& cluster_uuid) {
    return fmt::format("{}/manifests", cluster_uuid_prefix(cluster_uuid));
}

cloud_storage::remote_manifest_path cluster_manifest_key(
  const model::cluster_uuid& cluster_uuid, const cluster_metadata_id& meta_id) {
    return cloud_storage::remote_manifest_path(
      fmt::format(
        "{}/{}/cluster_manifest.json",
        cluster_manifests_prefix(cluster_uuid),
        meta_id()));
}

cloud_storage::remote_segment_path controller_snapshot_key(
  const model::cluster_uuid& cluster_uuid, const model::offset& offset) {
    return cloud_storage::remote_segment_path(
      fmt::format(
        "{}/{}/controller.snapshot",
        cluster_uuid_prefix(cluster_uuid),
        offset()));
}

ss::sstring cluster_metadata_prefix(
  const model::cluster_uuid& cluster_uuid, const cluster_metadata_id& meta_id) {
    return fmt::format("{}/{}", cluster_uuid_prefix(cluster_uuid), meta_id());
}

cloud_storage_clients::object_key offsets_snapshot_key(
  const model::cluster_uuid& cluster_uuid,
  const cluster_metadata_id& meta_id,
  const model::partition_id& pid,
  size_t snapshot_idx) {
    return cloud_storage_clients::object_key{fmt::format(
      "{}/{}/offsets/{}/{}.snapshot",
      cluster_uuid_prefix(cluster_uuid),
      meta_id(),
      pid(),
      snapshot_idx)};
}

cloud_storage_clients::object_key cluster_name_ref_for_uuid_key(
  const ss::sstring& name, const model::cluster_uuid& cluster_uuid) {
    return cloud_storage_clients::object_key{fmt::format(
      "cluster_name/{}/uuid/{}", name, ss::sstring(cluster_uuid()))};
}

} // namespace cluster::cloud_metadata
