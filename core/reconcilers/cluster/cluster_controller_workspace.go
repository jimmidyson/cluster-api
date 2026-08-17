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

package cluster

import (
	"context"
	"time"

	pkgerrors "github.com/pkg/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"

	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"

	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/controllers/clustercache"
	"sigs.k8s.io/cluster-api/controllers/external"
	"sigs.k8s.io/cluster-api/feature"
	capicontrollerutil "sigs.k8s.io/cluster-api/util/controller"
	"sigs.k8s.io/cluster-api/util/predicates"
)

// SetupWithMulticlusterManager sets up the reconciler as one controller serving
// every cluster the manager's provider offers, rather than one controller per
// cluster.
//
// It is the same wiring as SetupWithManager: the same For, the same watches in
// the same order, the same map functions, the same predicates and the same
// feature gate. Only the builder differs, so that the queue is keyed on a
// request that carries the cluster.
//
// # What the caller has to supply
//
// Client, APIReader and ClusterCache must be scoped by the cluster in the
// context. Every reconcile-path use of all three takes a context — the
// reconciler is handed one carrying the cluster by the builder — so a
// context-scoped implementation serves the whole fleet through the one field,
// and no reconcile code changes. ClusterCache.GetClusterSource is the sole
// exception, which is why the source arrives as a parameter instead; see
// clustercache.MulticlusterClusterSourceFunc.
//
// # What it does not solve
//
// The event recorder is the local manager's, so events this controller records
// land in the management cluster rather than in the cluster the object came
// from. record.EventRecorder takes no context, so this cannot be fixed the way
// the clients are; a recorder that routes on the object instead would have to
// come from the caller. Nothing in the reconcile path depends on where the event
// lands, so this is a fidelity gap rather than a correctness one, and it is
// stated rather than hidden.
func (r *Reconciler) SetupWithMulticlusterManager(
	ctx context.Context,
	mgr mcmanager.Manager,
	options controller.TypedOptions[mcreconcile.Request],
	clusterSource clustercache.MulticlusterClusterSourceFunc,
	opts ...capicontrollerutil.MulticlusterOption,
) error {
	if r.Client == nil || r.APIReader == nil || r.ClusterCache == nil || r.RemoteConnectionGracePeriod == time.Duration(0) {
		return pkgerrors.New("Client, APIReader and ClusterCache must not be nil and RemoteConnectionGracePeriod must not be 0")
	}
	if clusterSource == nil {
		return pkgerrors.New("clusterSource must not be nil")
	}

	localMgr := mgr.GetLocalManager()
	predicateLog := ctrl.LoggerFrom(ctx).WithValues("controller", "cluster")

	b := capicontrollerutil.NewMulticlusterControllerManagedBy(mgr, predicateLog).
		Apply(opts...).
		For(&clusterv1.Cluster{}).
		WatchesRawSource(clusterSource("cluster", func(_ context.Context, o client.Object) []ctrl.Request {
			return []ctrl.Request{{NamespacedName: client.ObjectKeyFromObject(o)}}
		}, clustercache.WatchForProbeFailure(r.RemoteConnectionGracePeriod))).
		Watches(
			&clusterv1.Machine{},
			handler.EnqueueRequestsFromMapFunc(r.controlPlaneMachineToCluster),
		).
		Watches(
			&clusterv1.MachineDeployment{},
			handler.EnqueueRequestsFromMapFunc(r.machineDeploymentToCluster),
		)
	if feature.Gates.Enabled(feature.MachinePool) {
		b = b.Watches(
			&clusterv1.MachinePool{},
			handler.EnqueueRequestsFromMapFunc(r.machinePoolToCluster),
		)
	}

	c, err := b.
		WithOptions(options).
		WithEventFilter(predicates.ResourceHasFilterLabel(localMgr.GetScheme(), predicateLog, r.WatchFilterValue)).
		Build(ctx, r)

	if err != nil {
		return pkgerrors.Wrap(err, "failed setting up with a multicluster manager")
	}

	r.recorder = localMgr.GetEventRecorderFor("cluster-controller")
	// No Cache: a fleet-wide watch resolves each cluster's cache when that
	// cluster engages, so there is no one cache to bind the tracker to.
	r.externalTracker = external.ObjectTracker{
		MultiClusterController: c,
		Scheme:                 localMgr.GetScheme(),
		PredicateLogger:        &predicateLog,
	}
	return nil
}
