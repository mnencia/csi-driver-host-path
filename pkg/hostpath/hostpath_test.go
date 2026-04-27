/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package hostpath

import (
	"os"
	"testing"

	"github.com/kubernetes-csi/csi-driver-host-path/pkg/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func newTestDriver(t *testing.T, nodeDeployment bool) *hostPath {
	t.Helper()
	stateDir, err := os.MkdirTemp(t.TempDir(), "csi-data-dir")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		StateDir:       stateDir,
		Endpoint:       "unix://tmp/csi.sock",
		DriverName:     "hostpath.csi.k8s.io",
		NodeID:         "fakeNode",
		MaxVolumeSize:  1024 * 1024 * 1024,
		EnableTopology: true,
		NodeDeployment: nodeDeployment,
	}
	hp, err := NewHostPathDriver(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return hp
}

func TestLoadFromMissingSourceErrorCode(t *testing.T) {
	cases := []struct {
		name           string
		nodeDeployment bool
		wantCode       codes.Code
	}{
		{"centralized returns NotFound", false, codes.NotFound},
		{"node-deployment returns ResourceExhausted", true, codes.ResourceExhausted},
	}
	for _, tc := range cases {
		t.Run("snapshot/"+tc.name, func(t *testing.T) {
			hp := newTestDriver(t, tc.nodeDeployment)
			err := hp.loadFromSnapshot(1024, "missing-snapshot", "/tmp/unused", state.MountAccess)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("loadFromSnapshot: got code %v, want %v (err=%v)", got, tc.wantCode, err)
			}
		})
		t.Run("volume/"+tc.name, func(t *testing.T) {
			hp := newTestDriver(t, tc.nodeDeployment)
			err := hp.loadFromVolume(1024, "missing-volume", "/tmp/unused", state.MountAccess)
			if got := status.Code(err); got != tc.wantCode {
				t.Fatalf("loadFromVolume: got code %v, want %v (err=%v)", got, tc.wantCode, err)
			}
		})
	}
}
