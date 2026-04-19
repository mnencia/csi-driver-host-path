/*
Copyright 2026 The Kubernetes Authors.

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

package coordinator

import (
	"context"
	"fmt"

	snapshotclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
)

const (
	// SnapshotManagedByLabel is set by snapshot-controller running with
	// --enable-distributed-snapshotting and records the node owning the
	// snapshot data.
	SnapshotManagedByLabel = "snapshot.storage.kubernetes.io/managed-by"

	// CSITopologyKeyNode is the per-node topology key used by the hostpath
	// driver and matches what its CreateVolume response carries.
	CSITopologyKeyNode = "topology.hostpath.csi/node"
)

// Resolver looks up the node that owns the data source of a PVC.
type Resolver struct {
	Kube       kubernetes.Interface
	Snap       snapshotclient.Interface
	DriverName string
	// TopologyKey is the CSI topology key under which our driver advertises
	// the owning node in PV nodeAffinity.
	TopologyKey string
}

// SnapshotSourceNode looks up the VolumeSnapshotContent whose
// status.snapshotHandle matches snapshotHandle and returns the node recorded
// in the SnapshotManagedByLabel. Empty string means unknown.
func (r *Resolver) SnapshotSourceNode(ctx context.Context, namespace, snapshotName string) (string, error) {
	vs, err := r.Snap.SnapshotV1().VolumeSnapshots(namespace).Get(ctx, snapshotName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting VolumeSnapshot %s/%s: %w", namespace, snapshotName, err)
	}
	if vs.Status == nil || vs.Status.BoundVolumeSnapshotContentName == nil {
		return "", nil
	}
	contentName := *vs.Status.BoundVolumeSnapshotContentName
	vsc, err := r.Snap.SnapshotV1().VolumeSnapshotContents().Get(ctx, contentName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting VolumeSnapshotContent %s: %w", contentName, err)
	}
	if vsc.Spec.Driver != r.DriverName {
		return "", nil
	}
	node := vsc.Labels[SnapshotManagedByLabel]
	if node == "" {
		klog.V(2).Infof("VolumeSnapshotContent %q has no %q label; snapshot-controller needs --enable-distributed-snapshotting",
			vsc.Name, SnapshotManagedByLabel)
	}
	return node, nil
}

// CloneSourceNode finds the PV whose spec.csi.volumeHandle matches the claim's
// bound PV and returns the node encoded in its nodeAffinity. Only PVs owned by
// this driver are considered.
func (r *Resolver) CloneSourceNode(ctx context.Context, namespace, pvcName string) (string, error) {
	pvc, err := r.Kube.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting source PVC %s/%s: %w", namespace, pvcName, err)
	}
	if pvc.Spec.VolumeName == "" {
		// Source claim not bound yet, nothing to do.
		return "", nil
	}
	pv, err := r.Kube.CoreV1().PersistentVolumes().Get(ctx, pvc.Spec.VolumeName, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("getting source PV %s: %w", pvc.Spec.VolumeName, err)
	}
	if pv.Spec.CSI == nil || pv.Spec.CSI.Driver != r.DriverName {
		return "", nil
	}
	return nodeFromPVAffinity(pv, r.TopologyKey), nil
}

// nodeFromPVAffinity extracts the single-node value from PV.spec.nodeAffinity
// produced by this driver. Returns "" when no matching expression exists.
func nodeFromPVAffinity(pv *corev1.PersistentVolume, topologyKey string) string {
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return ""
	}
	for _, term := range pv.Spec.NodeAffinity.Required.NodeSelectorTerms {
		for _, expr := range term.MatchExpressions {
			if expr.Key != topologyKey {
				continue
			}
			if expr.Operator != corev1.NodeSelectorOpIn {
				continue
			}
			if len(expr.Values) == 0 {
				continue
			}
			return expr.Values[0]
		}
	}
	return ""
}
