This deployment is meant for Kubernetes clusters with
CSIStorageCapacity enabled. It deploys the hostpath driver on each
node, using distributed provisioning, and configures it so that it has
10Gi of "fast" storage and 100Gi of "slow" storage.

The "kind" storage class parameter can selected between the two. If
not set, an arbitrary kind with enough capacity is picked.

## Prerequisites

Snapshot support in this deployment uses per-node `csi-snapshotter`
sidecars (`--node-deployment=true`). For that to work, the cluster
must run an external `snapshot-controller` started with
`--enable-distributed-snapshotting=true`, plus the matching
`VolumeSnapshot*` CRDs. The `deploy.sh` script in this directory does
not install snapshot-controller; install it separately before
deploying the driver. See the
[external-snapshotter documentation](https://github.com/kubernetes-csi/external-snapshotter#distributed-snapshotting)
for the install steps.

## Cross-node snapshot restore

The distributed deployment stores snapshot data on the node that
created it. Without extra machinery, restoring such a snapshot fails
whenever kube-scheduler picks a different node for the consuming pod,
because `CreateVolume` on the wrong node cannot see the `.snap` file.

A dedicated controller, `csi-topology-coordinator`, closes the gap at
the Kubernetes API level without giving any permissions to the CSI
driver itself. It watches PVCs that use this driver's StorageClass
under `WaitForFirstConsumer` and have a snapshot or clone data
source. For each one it resolves the owning node (by reading the
`snapshot.storage.kubernetes.io/managed-by` label on the
VolumeSnapshotContent, or the source PV's `spec.nodeAffinity`) and
patches the PVC's `volume.kubernetes.io/selected-node` annotation
to that node.

kube-scheduler's volume-binding plugin honours the pre-set annotation
during `FindPodVolumes` for unbound WaitForFirstConsumer PVCs, so the
consuming pod is placed directly on the source node, the
external-provisioner on that node provisions the volume locally, and
no reschedule round-trip is needed.

### Permission profile

The CSI driver DaemonSet needs no cluster-API access. The coordinator
runs as a single-replica Deployment with its own ServiceAccount whose
permissions are:

* `persistentvolumeclaims`: get, list, watch, patch
* `storageclasses`: get, list, watch
* `persistentvolumes`: get
* `volumesnapshots`, `volumesnapshotcontents`: get
* `leases` in the coordinator's namespace, scoped to the coordinator's
  own Lease name (`csi-topology-coordinator`): create, get, update,
  patch (for leader election)

### Known caveat

The volume-binder honours the pre-set annotation only when it is set
before a pod consuming the PVC is scheduled. If a pod is already
pending and the coordinator has not yet patched the PVC, the regular
scheduler path can still pick the wrong node. In practice the
coordinator reacts on PVC creation (well before a pod is usually
created), so this is not observed in CI.
