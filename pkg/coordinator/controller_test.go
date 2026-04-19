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
	"testing"
	"time"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	snapfake "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned/fake"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
)

const (
	driverName    = "hostpath.csi.k8s.io"
	otherDriver   = "other.csi.k8s.io"
	remoteNode    = "node-b"
	pvcNamespace  = "default"
	targetPVCName = "target-pvc"
	ourSC         = "csi-hostpath-sc"
	otherSC       = "other-sc"
	snapshotName  = "snap-1"
	snapshotHdl   = "snap-handle-1"
)

func sc(name, provisioner string, mode storagev1.VolumeBindingMode) *storagev1.StorageClass {
	return &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: name},
		Provisioner:       provisioner,
		VolumeBindingMode: &mode,
	}
}

func snapshotDataSource(name string) *corev1.TypedLocalObjectReference {
	grp := "snapshot.storage.k8s.io"
	return &corev1.TypedLocalObjectReference{APIGroup: &grp, Kind: "VolumeSnapshot", Name: name}
}

func pvcDataSource(name string) *corev1.TypedLocalObjectReference {
	grp := ""
	return &corev1.TypedLocalObjectReference{APIGroup: &grp, Kind: "PersistentVolumeClaim", Name: name}
}

func targetPVC(scName string, src *corev1.TypedLocalObjectReference) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: targetPVCName, Namespace: pvcNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			StorageClassName: &scName,
			DataSource:       src,
		},
	}
}

func snapshot(node string) (*snapv1.VolumeSnapshot, *snapv1.VolumeSnapshotContent) {
	contentName := "snapcontent-" + snapshotName
	handle := snapshotHdl
	driver := driverName
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: snapshotName, Namespace: pvcNamespace},
		Status: &snapv1.VolumeSnapshotStatus{
			BoundVolumeSnapshotContentName: &contentName,
		},
	}
	vsc := &snapv1.VolumeSnapshotContent{
		ObjectMeta: metav1.ObjectMeta{
			Name:   contentName,
			Labels: map[string]string{SnapshotManagedByLabel: node},
		},
		Spec: snapv1.VolumeSnapshotContentSpec{Driver: driver},
		Status: &snapv1.VolumeSnapshotContentStatus{
			SnapshotHandle: &handle,
		},
	}
	if node == "" {
		delete(vsc.Labels, SnapshotManagedByLabel)
	}
	return vs, vsc
}

func clonePVCAndPV(sourcePVCName, node, driver string) (*corev1.PersistentVolumeClaim, *corev1.PersistentVolume) {
	pvName := "pv-" + sourcePVCName
	srcPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: sourcePVCName, Namespace: pvcNamespace},
		Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: pvName},
	}
	pv := &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: pvName},
		Spec: corev1.PersistentVolumeSpec{
			PersistentVolumeSource: corev1.PersistentVolumeSource{
				CSI: &corev1.CSIPersistentVolumeSource{
					Driver:       driver,
					VolumeHandle: "handle-" + sourcePVCName,
				},
			},
			NodeAffinity: &corev1.VolumeNodeAffinity{
				Required: &corev1.NodeSelector{
					NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{
							Key:      CSITopologyKeyNode,
							Operator: corev1.NodeSelectorOpIn,
							Values:   []string{node},
						}},
					}},
				},
			},
		},
	}
	return srcPVC, pv
}

// runController spins up the controller, waits until its reconciler has
// processed the target PVC once, then stops it. The reconciler signals via a
// hook passed into newTest (not available on the production New constructor,
// so tests use newTest directly).
func runController(t *testing.T, kube kubernetes.Interface, snap *snapfake.Clientset) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)

	factory := informers.NewSharedInformerFactory(kube, 0)
	resolver := &Resolver{
		Kube:        kube,
		Snap:        snap,
		DriverName:  driverName,
		TopologyKey: CSITopologyKeyNode,
	}
	ctrl := New(kube, factory, resolver)

	processed := make(chan struct{}, 16)
	ctrl.onReconciled = func(key string) {
		if key == pvcNamespace+"/"+targetPVCName {
			select {
			case processed <- struct{}{}:
			default:
			}
		}
	}

	factory.Start(ctx.Done())
	go func() { _ = ctrl.Run(ctx, 1) }()

	// Wait for the first reconcile of the target PVC.
	select {
	case <-processed:
	case <-ctx.Done():
		t.Fatalf("timed out waiting for reconcile of %s/%s", pvcNamespace, targetPVCName)
	}
}

func TestReconcile_SnapshotSourceOnRemoteNode_PatchesPVC(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, err := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	require.NoError(t, err)
	assert.Equal(t, remoteNode, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_AlreadyCorrectAnnotation_NoChange(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	pvc.Annotations = map[string]string{SelectedNodeAnnotation: remoteNode}
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Equal(t, remoteNode, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_MissingManagedByLabel_NoPatch(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot("") // no label

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_ForeignStorageClass_Skipped(t *testing.T) {
	pvc := targetPVC(otherSC, snapshotDataSource(snapshotName))
	storageClass := sc(otherSC, otherDriver, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_ImmediateBinding_Skipped(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingImmediate)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_BoundPVC_Skipped(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	pvc.Spec.VolumeName = "already-bound-pv"
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_NoDataSource_Skipped(t *testing.T) {
	pvc := targetPVC(ourSC, nil)
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset()

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_CloneCrossNode_Patches(t *testing.T) {
	srcPVC, srcPV := clonePVCAndPV("src-pvc", remoteNode, driverName)
	pvc := targetPVC(ourSC, pvcDataSource("src-pvc"))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)

	kube := fake.NewClientset(pvc, storageClass, srcPVC, srcPV)
	snap := snapfake.NewSimpleClientset()

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Equal(t, remoteNode, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_CloneSourcePVFromOtherDriver_NotPatched(t *testing.T) {
	srcPVC, srcPV := clonePVCAndPV("src-pvc", remoteNode, otherDriver)
	pvc := targetPVC(ourSC, pvcDataSource("src-pvc"))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)

	kube := fake.NewClientset(pvc, storageClass, srcPVC, srcPV)
	snap := snapfake.NewSimpleClientset()

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_SnapshotContentFromOtherDriver_NotPatched(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)
	vsc.Spec.Driver = otherDriver

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Empty(t, out.Annotations[SelectedNodeAnnotation])
}

func TestReconcile_PreservesOtherAnnotations(t *testing.T) {
	pvc := targetPVC(ourSC, snapshotDataSource(snapshotName))
	pvc.Annotations = map[string]string{
		"volume.beta.kubernetes.io/storage-provisioner": driverName,
		"cnpg.io/backup": "daily",
	}
	storageClass := sc(ourSC, driverName, storagev1.VolumeBindingWaitForFirstConsumer)
	vs, vsc := snapshot(remoteNode)

	kube := fake.NewClientset(pvc, storageClass)
	snap := snapfake.NewSimpleClientset(vs, vsc)

	runController(t, kube, snap)

	out, _ := kube.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(context.Background(), targetPVCName, metav1.GetOptions{})
	assert.Equal(t, remoteNode, out.Annotations[SelectedNodeAnnotation])
	assert.Equal(t, driverName, out.Annotations["volume.beta.kubernetes.io/storage-provisioner"])
	assert.Equal(t, "daily", out.Annotations["cnpg.io/backup"])
}
