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

// Package coordinator implements a small Kubernetes controller that pre-sets
// the volume.kubernetes.io/selected-node annotation on PVCs whose data source
// lives on a specific node (snapshot restore or cross-node clone for a
// per-node CSI driver). Pre-setting this annotation lets kube-scheduler pin
// the consuming pod directly to the source node via the volume-binding
// plugin's fast-path.
package coordinator

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

// SelectedNodeAnnotation is the annotation kube-scheduler (or our coordinator)
// sets on a PVC to pin provisioning to a specific node.
const SelectedNodeAnnotation = "volume.kubernetes.io/selected-node"

// Controller watches PVCs and patches their selected-node annotation when the
// data source points at a specific node that differs from the current value.
type Controller struct {
	kube       kubernetes.Interface
	resolver   *Resolver
	driverName string

	pvcLister corelisters.PersistentVolumeClaimLister
	pvcSynced cache.InformerSynced
	scLister  storagelisters.StorageClassLister
	scSynced  cache.InformerSynced

	queue workqueue.TypedRateLimitingInterface[string]

	// onReconciled fires after every reconcile attempt. Used by tests to
	// deterministically wait for processing instead of polling. nil in
	// production builds.
	onReconciled func(key string)
}

// New returns a Controller wired to informers from factory. The factory must
// be started by the caller.
func New(kube kubernetes.Interface, factory informers.SharedInformerFactory, resolver *Resolver) *Controller {
	pvcInformer := factory.Core().V1().PersistentVolumeClaims()
	scInformer := factory.Storage().V1().StorageClasses()

	c := &Controller{
		kube:       kube,
		resolver:   resolver,
		driverName: resolver.DriverName,
		pvcLister:  pvcInformer.Lister(),
		pvcSynced:  pvcInformer.Informer().HasSynced,
		scLister:   scInformer.Lister(),
		scSynced:   scInformer.Informer().HasSynced,
		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: "csi-topology-coordinator"},
		),
	}

	pvcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { c.enqueue(obj) },
		UpdateFunc: func(_, obj any) { c.enqueue(obj) },
	})
	return c
}

// Run starts worker goroutines until ctx is cancelled. Expect the informer
// factory to have been Start()ed and caches populated; Run blocks waiting for
// them before processing.
func (c *Controller) Run(ctx context.Context, workers int) error {
	defer c.queue.ShutDown()

	klog.Info("coordinator: waiting for informer caches to sync")
	if !cache.WaitForCacheSync(ctx.Done(), c.pvcSynced, c.scSynced) {
		return fmt.Errorf("failed to sync caches")
	}
	klog.Info("coordinator: caches synced, starting workers")

	for i := 0; i < workers; i++ {
		go c.runWorker(ctx)
	}
	<-ctx.Done()
	klog.Info("coordinator: shutting down")
	return nil
}

func (c *Controller) enqueue(obj any) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		klog.Errorf("could not derive key: %v", err)
		return
	}
	c.queue.Add(key)
}

func (c *Controller) runWorker(ctx context.Context) {
	defer utilruntime.HandleCrash()
	for c.processNext(ctx) {
	}
}

func (c *Controller) processNext(ctx context.Context) bool {
	key, shutdown := c.queue.Get()
	if shutdown {
		return false
	}
	defer c.queue.Done(key)

	err := c.reconcile(ctx, key)
	if c.onReconciled != nil {
		c.onReconciled(key)
	}
	if err == nil {
		c.queue.Forget(key)
		return true
	}
	// Retry on error. The informer will re-enqueue on next update, so we
	// use a bounded retry via the rate limiter.
	klog.Warningf("reconcile %q failed (will retry): %v", key, err)
	c.queue.AddRateLimited(key)
	return true
}

// reconcile decides whether the PVC keyed by namespace/name needs its
// selected-node annotation set, and patches it if so.
func (c *Controller) reconcile(ctx context.Context, key string) error {
	ns, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil
	}
	pvc, err := c.pvcLister.PersistentVolumeClaims(ns).Get(name)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	ok, err := c.shouldConsider(pvc)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}

	sourceNode, err := c.sourceNode(ctx, pvc)
	if err != nil {
		return err
	}
	if sourceNode == "" {
		return nil
	}

	if pvc.Annotations[SelectedNodeAnnotation] == sourceNode {
		return nil
	}

	klog.V(2).Infof("coordinator: pinning PVC %s/%s to node %q (current: %q)",
		pvc.Namespace, pvc.Name, sourceNode, pvc.Annotations[SelectedNodeAnnotation])

	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				SelectedNodeAnnotation: sourceNode,
			},
		},
	}
	raw, err := json.Marshal(patch)
	if err != nil {
		return err
	}
	_, err = c.kube.CoreV1().PersistentVolumeClaims(pvc.Namespace).Patch(
		ctx, pvc.Name, types.MergePatchType, raw, metav1.PatchOptions{},
	)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("patching PVC %s/%s selected-node: %w", pvc.Namespace, pvc.Name, err)
	}
	return nil
}

// shouldConsider reports whether the coordinator needs to act on pvc.
//
// A non-nil error means the information required to decide is not available
// yet (e.g. StorageClass still syncing on startup). Callers must retry via
// the workqueue rather than dropping the PVC.
func (c *Controller) shouldConsider(pvc *corev1.PersistentVolumeClaim) (bool, error) {
	if pvc.Spec.VolumeName != "" {
		return false, nil
	}
	if !hasDataSource(pvc) {
		return false, nil
	}
	if pvc.Spec.StorageClassName == nil || *pvc.Spec.StorageClassName == "" {
		return false, nil
	}
	sc, err := c.scLister.Get(*pvc.Spec.StorageClassName)
	if err != nil {
		return false, fmt.Errorf("looking up StorageClass %q for PVC %s/%s: %w",
			*pvc.Spec.StorageClassName, pvc.Namespace, pvc.Name, err)
	}
	if sc.Provisioner != c.driverName {
		return false, nil
	}
	if sc.VolumeBindingMode == nil || *sc.VolumeBindingMode != storagev1.VolumeBindingWaitForFirstConsumer {
		return false, nil
	}
	return true, nil
}

// sourceNode resolves the node that owns the PVC's data source. Returns "" if
// unresolvable (e.g. missing managed-by label, unbound source PVC).
func (c *Controller) sourceNode(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (string, error) {
	src := pvc.Spec.DataSource
	if pvc.Spec.DataSourceRef != nil {
		src = &corev1.TypedLocalObjectReference{
			APIGroup: pvc.Spec.DataSourceRef.APIGroup,
			Kind:     pvc.Spec.DataSourceRef.Kind,
			Name:     pvc.Spec.DataSourceRef.Name,
		}
	}
	if src == nil || src.Name == "" {
		return "", nil
	}

	switch {
	case src.Kind == "VolumeSnapshot" && src.APIGroup != nil && *src.APIGroup == "snapshot.storage.k8s.io":
		return c.resolver.SnapshotSourceNode(ctx, pvc.Namespace, src.Name)
	case src.Kind == "PersistentVolumeClaim" && (src.APIGroup == nil || *src.APIGroup == ""):
		return c.resolver.CloneSourceNode(ctx, pvc.Namespace, src.Name)
	default:
		return "", nil
	}
}

func hasDataSource(pvc *corev1.PersistentVolumeClaim) bool {
	if pvc.Spec.DataSource != nil && pvc.Spec.DataSource.Name != "" {
		return true
	}
	if pvc.Spec.DataSourceRef != nil && pvc.Spec.DataSourceRef.Name != "" {
		return true
	}
	return false
}

// ResyncPeriod is the SharedInformerFactory resync interval; exposed for the
// binary to reuse.
const ResyncPeriod = 5 * time.Minute
