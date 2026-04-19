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

// csi-topology-coordinator pre-sets volume.kubernetes.io/selected-node on
// PVCs whose data source (snapshot or clone) lives on a specific node.
// kube-scheduler then pins the consuming pod directly to that node via the
// volume-binding plugin's fast-path, avoiding a reschedule round-trip.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	snapshotclient "github.com/kubernetes-csi/external-snapshotter/client/v8/clientset/versioned"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog/v2"

	"github.com/kubernetes-csi/csi-driver-host-path/pkg/coordinator"
)

var version = ""

func main() {
	var (
		kubeconfig  string
		driverName  string
		topologyKey string
		leaderNS    string
		leaderName  string
		leaseDur    time.Duration
		renewDur    time.Duration
		retryDur    time.Duration
		workers     int
		showVersion bool
	)

	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to kubeconfig; empty = in-cluster config")
	flag.StringVar(&driverName, "driver-name", "hostpath.csi.k8s.io", "CSI driver this coordinator manages")
	flag.StringVar(&topologyKey, "topology-key", coordinator.CSITopologyKeyNode, "PV nodeAffinity key this driver advertises")
	flag.StringVar(&leaderNS, "leader-namespace", "", "namespace for the leader-election Lease; defaults to POD_NAMESPACE env or 'default'")
	flag.StringVar(&leaderName, "leader-name", "csi-topology-coordinator", "Lease name for leader election")
	flag.DurationVar(&leaseDur, "leader-lease-duration", 15*time.Second, "Lease duration")
	flag.DurationVar(&renewDur, "leader-renew-deadline", 10*time.Second, "Lease renew deadline")
	flag.DurationVar(&retryDur, "leader-retry-period", 2*time.Second, "Lease retry period")
	flag.IntVar(&workers, "workers", 2, "number of reconcile workers")
	flag.BoolVar(&showVersion, "version", false, "show version and exit")

	klog.InitFlags(nil)
	flag.Parse()

	if showVersion {
		fmt.Println(path.Base(os.Args[0]), version)
		return
	}

	cfg, err := loadRESTConfig(kubeconfig)
	if err != nil {
		klog.Fatalf("kubernetes config: %v", err)
	}

	kube, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("kubernetes client: %v", err)
	}
	snap, err := snapshotclient.NewForConfig(cfg)
	if err != nil {
		klog.Fatalf("snapshot client: %v", err)
	}

	if leaderNS == "" {
		leaderNS = os.Getenv("POD_NAMESPACE")
	}
	if leaderNS == "" {
		leaderNS = "default"
	}
	identity := os.Getenv("POD_NAME")
	if identity == "" {
		hostname, _ := os.Hostname()
		identity = hostname
	}

	ctx, cancel := signalContext()
	defer cancel()

	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{Name: leaderName, Namespace: leaderNS},
		Client:    kube.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: identity,
		},
	}

	run := func(ctx context.Context) {
		factory := informers.NewSharedInformerFactory(kube, coordinator.ResyncPeriod)
		resolver := &coordinator.Resolver{
			Kube:        kube,
			Snap:        snap,
			DriverName:  driverName,
			TopologyKey: topologyKey,
		}
		ctrl := coordinator.New(kube, factory, resolver)

		factory.Start(ctx.Done())
		if err := ctrl.Run(ctx, workers); err != nil {
			klog.Errorf("controller exited: %v", err)
		}
	}

	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: leaseDur,
		RenewDeadline: renewDur,
		RetryPeriod:   retryDur,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: run,
			OnStoppedLeading: func() {
				// Surface the reason explicitly so a restart is traceable in pod events
				// instead of a silent Exit 0.
				klog.Fatalf("lost leadership (%s)", identity)
			},
			OnNewLeader: func(id string) {
				if id != identity {
					klog.Infof("leader is %s", id)
				}
			},
		},
		ReleaseOnCancel: true,
		Name:            leaderName,
	})
}

func loadRESTConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	}
	return rest.InClusterConfig()
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGTERM, syscall.SIGINT, syscall.SIGHUP, syscall.SIGQUIT)
	go func() {
		<-sigc
		cancel()
	}()
	return ctx, cancel
}
