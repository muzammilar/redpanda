# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest


class HostMetricsTest(RedpandaTest):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, num_brokers=1, **kwargs)

    @cluster(num_nodes=1)
    def test_basic_hoststats(self):
        """
        Test that we export the host stats

        """

        metrics = list(self.redpanda.metrics(self.redpanda.nodes[0]))

        disk_count = 0
        netstat_count = 0
        snmp_count = 0
        for family in metrics:
            if family.name == "vectorized_host_diskstats_reads":
                for sample in family.samples:
                    disk_count += int(sample.value)
            if family.name == "vectorized_host_netstat_bytes_received":
                for sample in family.samples:
                    netstat_count += int(sample.value)
            if family.name == "vectorized_host_snmp_packets_received":
                for sample in family.samples:
                    snmp_count += int(sample.value)

        assert disk_count > 0, "Expected some disk reads"
        assert netstat_count > 0, "Expected some received bytes"
        assert snmp_count > 0, "Expected some received packets"

    @cluster(num_nodes=1)
    def test_info_metric(self):
        """
        Test that the host_metrics info metric exists with exactly one sample
        per node and has the expected label values.
        """
        node = self.redpanda.nodes[0]
        metrics = list(self.redpanda.metrics(node))

        info_samples = []
        for family in metrics:
            if family.name == "vectorized_host_metrics_info":
                info_samples.extend(family.samples)

        assert len(info_samples) == 1, (
            f"Expected exactly 1 info metric sample, got {len(info_samples)}"
        )

        labels = info_samples[0].labels
        assert int(info_samples[0].value) == 1, "Info metric should be 1"

        def check(label, expected):
            actual = labels[label]
            assert actual == expected, f"Expected {label}={expected}, got {actual}"

        if self.redpanda.dedicated_nodes:
            # On dedicated nodes (EC2 etc.) device resolution should succeed.
            # Data and cache default to the same directory so they share a
            # partition.
            check("data_resolved", "1")
            check("cache_resolved", "1")
            check("same_partition", "1")
        else:
            # In docker the data directory is on an overlay filesystem whose
            # device (major 0) has no /sys/dev/block entry, so resolution
            # fails.
            check("data_resolved", "0")
            check("cache_resolved", "0")
            check("same_partition", "0")
            check("data_has_io_queue", "0")
