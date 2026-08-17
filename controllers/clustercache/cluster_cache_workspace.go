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

package clustercache

import (
	"context"

	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/source"

	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
)

// MulticlusterClusterSourceFunc is GetClusterSource for a controller that serves
// every cluster rather than one.
//
// # Why this is a function the caller supplies, rather than a method here
//
// ClusterCache keys its accessors and its last-event times by namespace and
// name, with no cluster (clusterAccessors and lastEventSentTimeByCluster,
// both map[client.ObjectKey]). Running one ClusterCache across many logical
// clusters would let two clusters' identically named Clusters share an accessor,
// so it stays per-cluster instead — which is the cheap containment, not a
// limitation being worked around.
//
// That leaves a fleet-wide controller with no single ClusterCache to ask for a
// source, and no way to ask each of them either, because clusters engage and
// disengage long after the controller is built.
//
// Whoever owns the per-cluster ClusterCaches — the provider integration, not
// Cluster API — can do what neither this package nor the builder can: fan those
// channels into one source, and label each event with the cluster whose
// ClusterCache produced it. It knows which that is without inspecting the
// object, because it created that ClusterCache for that cluster.
//
// # Why the signature is GetClusterSource's
//
// Only the request type differs. A setup function therefore passes the same map
// function, the same controller name and the same options on both paths, so the
// two cannot quietly diverge in what they ask for — for instance in the
// probe-failure durations, which decide when a Cluster is reconciled after its
// remote connection goes away.
type MulticlusterClusterSourceFunc func(
	controllerName string,
	mapFunc func(ctx context.Context, cluster client.Object) []ctrl.Request,
	opts ...GetClusterSourceOption,
) source.TypedSource[mcreconcile.Request]
