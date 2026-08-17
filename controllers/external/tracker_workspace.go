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

package external

import (
	"fmt"
	"sync"

	"github.com/go-logr/logr"
	pkgerrors "github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	mchandler "sigs.k8s.io/multicluster-runtime/pkg/handler"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	mcsource "sigs.k8s.io/multicluster-runtime/pkg/source"

	"sigs.k8s.io/cluster-api/util/predicates"
)

// MulticlusterObjectTracker is ObjectTracker for a controller that serves every
// cluster rather than one.
//
// # Why the reconcilers need it
//
// Both core reconcilers resolve contract-versioned references —
// spec.infrastructureRef, spec.controlPlaneRef, spec.bootstrap.configRef — to
// types that are not known until an object naming them is seen. They add those
// watches at runtime through an ObjectTracker rather than declaring them in
// SetupWithManager.
//
// ObjectTracker holds a controller.Controller, which is keyed on
// reconcile.Request, and passes a plain handler.EventHandler straight through.
// A fleet-wide controller's queue is keyed on mcreconcile.Request, so requests
// enqueued by the untouched tracker would carry no cluster and two clusters'
// identically named objects would collide. This lifts them, exactly as the
// multicluster builder lifts the statically declared watches.
//
// # Why this is a separate type rather than ObjectTracker made generic
//
// The distinction drawn elsewhere in this change: behavioural machinery is
// genericised so the two paths cannot drift, and *declarative registration* is
// duplicated so nothing existing has to be modified.
//
// This is registration. Its whole body is a deduplicating LoadOrStore around a
// single Watch call, with no behaviour to keep in step — whereas making
// ObjectTracker generic would need a lift function that cannot be defaulted,
// because every reconciler constructs the tracker as a struct literal with no
// constructor to supply one. The single-cluster ObjectTracker is therefore
// untouched.
//
// # Wiring
//
// MulticlusterBuilder.Build returns a MulticlusterController, which satisfies
// MultiClusterWatcher — so the controller a reconciler's setup function builds
// is what goes in the Controller field here.
type MulticlusterObjectTracker struct {
	m sync.Map

	// Controller is anything that can register a fleet-wide watch — in practice
	// what MulticlusterBuilder.Build returns.
	//
	// Narrowed to the single method this uses rather than taking the whole
	// controller interface: it keeps the dependency honest about what a tracker
	// actually needs, and it avoids this package having to name a type from the
	// builder package it does not otherwise depend on.
	Controller MultiClusterWatcher

	// No Cache field, unlike ObjectTracker. A multicluster source resolves each
	// cluster's cache itself when the controller engages that cluster, so there
	// is no single cache to hand it — and passing one would silently bind every
	// cluster's watch to one cluster's informers.
	Scheme          *runtime.Scheme
	PredicateLogger *logr.Logger
}

// MultiClusterWatcher registers a source with every cluster a controller
// serves, including clusters that engage after registration.
//
// controller-runtime's Watch cannot express that: it binds a source to one
// cache. A watch added at runtime — which is what this tracker exists for —
// must reach clusters that were not engaged when it was added.
type MultiClusterWatcher interface {
	MultiClusterWatch(src mcsource.TypedSource[client.Object, mcreconcile.Request]) error
}

// Watch adds a watch for an external object, if one is not already registered
// for its GroupKind.
//
// The handler is an ordinary single-cluster one — the reconcilers construct
// handler.EnqueueRequestForOwner and pass it in unchanged — and is lifted here
// so the requests it enqueues carry the cluster the event came from.
func (o *MulticlusterObjectTracker) Watch(log logr.Logger, obj client.Object, h handler.EventHandler, p ...predicate.Predicate) error {
	if o.Controller == nil || o.Scheme == nil || o.PredicateLogger == nil {
		return pkgerrors.New("all of Controller, Scheme and PredicateLogger must be set for object tracker")
	}

	gvk := obj.GetObjectKind().GroupVersionKind()
	key := gvk.GroupKind().String()
	if _, loaded := o.m.LoadOrStore(key, struct{}{}); loaded {
		return nil
	}

	log.Info(fmt.Sprintf("Adding watch on external object %q", gvk.String()))

	// mchandler.Lift produces a handler *factory* keyed by cluster rather than a
	// handler, because a multicluster source builds one handler per engaged
	// cluster. That is why this cannot go through controller-runtime's
	// source.Kind: there is no single cluster to build the handler for at
	// registration time.
	err := o.Controller.MultiClusterWatch(mcsource.TypedKind(
		obj.DeepCopyObject().(client.Object),
		mchandler.Lift(h),
		append(p, predicates.ResourceNotPaused(o.Scheme, *o.PredicateLogger))...,
	))
	if err != nil {
		o.m.Delete(key)
		return pkgerrors.Wrapf(err, "failed to add watch on external object %q", gvk.String())
	}
	return nil
}
