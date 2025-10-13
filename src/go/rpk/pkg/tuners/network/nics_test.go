// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package network

import (
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/stretchr/testify/require"
)

const PUCount = 8

type fakeMasks struct {
	irq.CPUMasks
}

func (f *fakeMasks) GetAllCpusMask() (string, error) { return "0xff", nil }

func TestCanDefaultToDedicatedMode(t *testing.T) {
	tests := []struct {
		name       string
		cliCpuset  string
		additional []string
		tuners     config.RpkNodeTuners
		want       bool
	}{
		{
			name:       "additional flags has cpuset",
			cliCpuset:  "0xff",
			additional: []string{"--cpuset", "0x1"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "tuner cli cpuset",
			cliCpuset:  "0x1",
			additional: []string{},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "smp unacceptable",
			cliCpuset:  "0xff",
			additional: []string{"--smp", "2"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       false,
		},
		{
			name:       "slightly lowered smp",
			cliCpuset:  "0xff",
			additional: []string{"--smp", "6"},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(4), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       true,
		},
		{
			name:       "all checks pass",
			cliCpuset:  "0xff",
			additional: []string{},
			tuners:     config.RpkNodeTuners{CoresPerDedicatedInterruptCore: intPtr(PUCount), AllowDedicatedInterruptMode: boolPtr(true)},
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fm := &fakeMasks{}
			rnc := config.RpkNodeConfig{AdditionalStartFlags: tt.additional, Tuners: tt.tuners}
			got, err := canDefaultToDedicatedMode(PUCount, tt.cliCpuset, fm, rnc)
			require.NoError(t, err)
			require.Equal(t, tt.want, got)
		})
	}
}

func intPtr(i int) *int    { return &i }
func boolPtr(b bool) *bool { return &b }
