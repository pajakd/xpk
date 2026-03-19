/*
Copyright The Kubernetes Authors.

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

package controller

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/google/go-cmp/cmp"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/admissioncheck"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/workload"

	"tpu-slice-controller/api/v1beta1"
	"tpu-slice-controller/internal/core"
	"tpu-slice-controller/internal/features"
	"tpu-slice-controller/internal/topology"
	"tpu-slice-controller/internal/util/api"
	"tpu-slice-controller/internal/util/node"
	utilpod "tpu-slice-controller/internal/util/pod"
	utilslice "tpu-slice-controller/internal/util/slice"
	utilworkload "tpu-slice-controller/internal/util/workload"
)

const (
	SliceControllerName = "accelerator.gke.io/slice"

	SlicesCreatedEventType         = "SlicesCreated"
	FailedCreateSliceEventType     = "FailedCreateSlice"
	AdmissionCheckUpdatedEventType = "AdmissionCheckUpdated"
)

const (
	updatesBatchPeriod       = time.Second
	cleanupRetryAfter        = 5 * time.Second
	initializationRetryAfter = 5 * time.Second
)

var (
	realClock          = clock.RealClock{}
	errWorkloadEvicted = errors.New("workload evicted")
)

// WorkloadReconciler reconciles a Workload object
type WorkloadReconciler struct {
	client                   client.Client
	record                   record.EventRecorder
	clock                    clock.Clock
	activationTimeout        time.Duration
	retryDelayOnSliceFailure time.Duration
}

var _ reconcile.Reconciler = (*WorkloadReconciler)(nil)

func NewWorkloadReconciler(cl client.Client, record record.EventRecorder, activationTimeout time.Duration, retryDelayOnSliceFailure time.Duration) *WorkloadReconciler {
	return &WorkloadReconciler{
		client:                   cl,
		record:                   record,
		clock:                    realClock,
		activationTimeout:        activationTimeout,
		retryDelayOnSliceFailure: retryDelayOnSliceFailure,
	}
}

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads,verbs=get;list;watch;create;update;patch
// +kubebuilder:rbac:groups=kueue.x-k8s.io,resources=workloads/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=accelerator.gke.io,resources=slices/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=events,verbs=create;watch;update;patch
// +kubebuilder:rbac:groups=jobset.x-k8s.io,resources=jobsets,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch

func (r *WorkloadReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	wl := &kueue.Workload{}
	err := r.client.Get(ctx, req.NamespacedName, wl)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	log := ctrl.LoggerFrom(ctx)
	log.V(3).Info("Reconcile Workload")

	// If the workload has been deleted, evicted, has finished, or is no longer active,
	// finalize the Workload by removing its slices and then the finalizer.
	if finalize, reason := utilworkload.ShouldFinalize(wl); finalize {
		if controllerutil.ContainsFinalizer(wl, SliceControllerName) {
			log.V(3).Info("Cleaning up the Slices and finalizing the Workload", "reason", reason)
			cleanedUp, err := r.cleanupSlices(ctx, wl)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !cleanedUp {
				return ctrl.Result{RequeueAfter: cleanupRetryAfter}, nil
			}
			err = r.finalizeWorkload(ctx, wl)
			if apierrors.IsConflict(err) {
				log.V(3).Info("Failed to remove the finalizer", "error", err)
				return ctrl.Result{RequeueAfter: 5 * time.Millisecond}, nil
			}
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		return ctrl.Result{}, nil
	}

	nodes, err := node.GetNodes(ctx, r.client)
	if err != nil {
		return ctrl.Result{}, err
	}

	if err = utilworkload.ValidateRelevant(wl, nodes); err != nil {
		log.V(3).Info("Skipping workload", "reason", err.Error())
		return ctrl.Result{}, nil
	}

	ac, err := r.sliceAC(ctx, wl)
	if err != nil {
		return reconcile.Result{}, err
	}
	if ac == nil {
		log.V(3).Info("Admission check not found - skipping reconciliation")
		return reconcile.Result{}, nil
	}

	log = log.WithValues("admissionCheck", ac.Name)
	ctrl.LoggerInto(ctx, log)

	// Finalizer is needed because we need to cleanup Slice objects
	// before the workload is deleted.
	if controllerutil.AddFinalizer(wl, SliceControllerName) {
		if err = r.client.Update(ctx, wl); err != nil {
			if apierrors.IsConflict(err) {
				log.Info("Failed to add finalizer", "error", err)
			} else if !apierrors.IsNotFound(err) {
				log.Error(err, "Failed to add finalizer")
			}
			return ctrl.Result{}, client.IgnoreNotFound(err)
		}
		log.V(3).Info("Added finalizer")
		return ctrl.Result{}, nil
	}

	slices, err := r.findWorkloadSlices(ctx, wl)
	if err != nil {
		log.Error(err, "Failed to list Slices")
		return ctrl.Result{}, err
	}

	slicesByState := utilslice.GroupSlicesByState(slices, r.activationTimeout)

	desiredSlicesCount := utilworkload.TotalDesiredSlices(wl, nodes)
	if ac.State == kueue.CheckStateReady && (len(slicesByState[core.SliceStateDeleted]) > 0 || len(slices) != desiredSlicesCount) {
		log.V(3).Info("Slice has been deleted for a running workload, evicting the workload")
		if err := r.evictWorkload(ctx, wl, ac, "Slice has been deleted"); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.deleteAllNonDeletedSlices(ctx, slices)
	}

	if len(slicesByState[core.SliceStateDeleted]) > 0 {
		log.V(3).Info(
			"Waiting for deleted Slices to be cleaned up; skipping reconciliation for now",
			"deletedSlices", klog.KObjSlice(slicesByState[core.SliceStateDeleted]),
		)
		return ctrl.Result{}, nil
	}

	// Create any missing Slices based on the Workload's PodSet assignments.
	updatedSlices, changed, err := r.syncSlices(ctx, wl, ac, slices, nodes)
	if err != nil {
		if errors.Is(err, errWorkloadEvicted) {
			return ctrl.Result{RequeueAfter: initializationRetryAfter}, nil
		}
		return ctrl.Result{}, err
	}
	if changed {
		slices = updatedSlices
		slicesByState = utilslice.GroupSlicesByState(slices, r.activationTimeout)
	}

	// Update the Workload's AdmissionCheck status based on the current state of the Slices.
	err = r.syncAdmissionCheckStatus(ctx, wl, ac, slices, desiredSlicesCount)
	if err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if ac.State == kueue.CheckStateRetry {
		return ctrl.Result{}, r.deleteAllNonDeletedSlices(ctx, slices)
	}

	// Delete any Slices that are in a failed or stale state.
	toDelete := append(slicesByState[core.SliceStateFailed], slicesByState[core.SliceStateStale]...)
	if len(toDelete) > 0 {
		log.V(2).Info(
			"Deleting Slices",
			"slices", klog.KObjSlice(toDelete),
		)
		err = r.deleteSlices(ctx, toDelete)
		if err != nil {
			return ctrl.Result{}, err
		}
	}

	// If there are Slices that are still being created or activated, requeue Reconcile.
	// This is to delete and re-create slices that get stuck during initialization.
	initializing := append(slicesByState[core.SliceStateCreated], slicesByState[core.SliceStateActivating]...)
	if len(initializing) > 0 {
		log.V(3).Info(
			"Waiting for Slices to be initialized",
			"slices", klog.KObjSlice(initializing),
		)
		return ctrl.Result{RequeueAfter: initializationRetryAfter}, nil
	}
	return ctrl.Result{}, nil
}

func (r *WorkloadReconciler) cleanupSlices(ctx context.Context, wl *kueue.Workload) (bool, error) {
	log := ctrl.LoggerFrom(ctx)

	slices, err := r.findWorkloadSlices(ctx, wl)
	if err != nil {
		log.Error(err, "Failed to fetch Slices")
		return false, err
	}

	slicesByState := utilslice.GroupSlicesByState(slices, r.activationTimeout)

	if len(slicesByState[core.SliceStateDeleted]) == len(slices) {
		log.V(3).Info("All slices already deleted; finishing cleanup")
		return true, nil
	}

	terminated, err := r.ownerPodsFinished(ctx, wl)
	if err != nil || !terminated {
		return false, err
	}

	err = r.deleteAllNonDeletedSlices(ctx, slices)
	return err == nil, err
}

func (r *WorkloadReconciler) findWorkloadSlices(ctx context.Context, wl *kueue.Workload) ([]v1beta1.Slice, error) {
	slices := &v1beta1.SliceList{}
	opts := []client.ListOption{
		client.MatchingFields{
			WorkloadNamespaceIndex: wl.Namespace,
			WorkloadNameIndex:      wl.Name,
		},
	}
	if err := r.client.List(ctx, slices, opts...); err != nil {
		return nil, err
	}
	return slices.Items, nil
}

func (r *WorkloadReconciler) findAllSlices(ctx context.Context) ([]v1beta1.Slice, error) {
	slices := &v1beta1.SliceList{}
	if err := r.client.List(ctx, slices); err != nil {
		return nil, err
	}
	return slices.Items, nil
}

func (r *WorkloadReconciler) deleteAllNonDeletedSlices(ctx context.Context, slices []v1beta1.Slice) error {
	var toDelete []*v1beta1.Slice
	for i := range slices {
		if core.GetSliceState(slices[i], r.activationTimeout) != core.SliceStateDeleted {
			toDelete = append(toDelete, &slices[i])
		}
	}
	if len(toDelete) == 0 {
		return nil
	}
	ctrl.LoggerFrom(ctx).V(3).Info("Deleting all remaining non-deleted Slices", "slices", klog.KObjSlice(toDelete))
	return r.deleteSlices(ctx, toDelete)
}

func (r *WorkloadReconciler) deleteSlices(ctx context.Context, slices []*v1beta1.Slice) error {
	log := ctrl.LoggerFrom(ctx)
	for _, slice := range slices {
		log = log.WithValues("slice", klog.KObj(slice))
		log.V(3).Info("Deleting the Slice")
		err := r.client.Delete(ctx, slice)
		if client.IgnoreNotFound(err) != nil {
			log.Error(err, "Failed to delete the Slice")
			return err
		}
	}
	return nil
}

func (r *WorkloadReconciler) ownerPodsFinished(ctx context.Context, wl *kueue.Workload) (bool, error) {
	owner := metav1.GetControllerOf(wl)
	if owner == nil {
		return true, nil
	}

	var ownerObj client.Object
	var podLabelKey string

	if utilworkload.IsJobSetOwner(wl) {
		ownerObj = &jobset.JobSet{}
		podLabelKey = jobset.JobSetNameKey
	} else if utilworkload.IsJobOwner(wl) {
		ownerObj = &batchv1.Job{}
		podLabelKey = "batch.kubernetes.io/job-name"
	} else {
		// Finalize Workloads that have unsupported owner types.
		return true, nil
	}

	log := ctrl.LoggerFrom(ctx).WithValues(owner.Kind, klog.KRef(wl.Namespace, owner.Name))
	ownerKey := types.NamespacedName{Name: owner.Name, Namespace: wl.Namespace}
	if err := r.client.Get(ctx, ownerKey, ownerObj); err != nil {
		if apierrors.IsNotFound(err) {
			log.V(3).Info(fmt.Sprintf("%s already deleted", owner.Kind))
			// That means the owner has already been deleted, along with all associated Pods
			// we should delete Slice and cleanup Workload.
			return true, nil
		} else {
			log.Error(err, fmt.Sprintf("Failed to get %s", owner.Kind))
			return false, err
		}
	}

	pods := &corev1.PodList{}
	opts := []client.ListOption{
		client.InNamespace(wl.Namespace),
		client.MatchingLabels{podLabelKey: owner.Name},
	}
	if err := r.client.List(ctx, pods, opts...); err != nil {
		log.Error(err, "Failed to get Pods")
		return false, err
	}

	for _, pod := range pods.Items {
		if !utilpod.IsTerminated(&pod) {
			log.V(3).Info("Pods are still running – skipping finalization for now")
			return false, nil
		}
	}

	log.V(3).Info(fmt.Sprintf("All Pods in the %s have finished", owner.Kind))

	return true, nil
}

func (r *WorkloadReconciler) finalizeWorkload(ctx context.Context, wl *kueue.Workload) error {
	log := ctrl.LoggerFrom(ctx)

	controllerutil.RemoveFinalizer(wl, SliceControllerName)
	if err := r.client.Update(ctx, wl); err != nil {
		return fmt.Errorf("failed to remove finalizer: %w", err)
	}

	log.V(3).Info("Removed finalizer")

	return nil
}

func (r *WorkloadReconciler) sliceAC(ctx context.Context, wl *kueue.Workload) (*kueue.AdmissionCheckState, error) {
	relevantChecks, err := admissioncheck.FilterForController(ctx, r.client, wl.Status.AdmissionChecks, SliceControllerName)
	if err != nil {
		return nil, err
	}
	if len(relevantChecks) == 0 {
		return nil, nil
	}
	if len(relevantChecks) > 1 {
		ctrl.LoggerFrom(ctx).V(2).Info(
			"WARNING: More than one AdmissionCheck found. Using the first one",
			"selected", relevantChecks[0],
		)
	}
	return admissioncheck.FindAdmissionCheck(wl.Status.AdmissionChecks, relevantChecks[0]), nil
}

// syncSlices creates missing Slices and deletes existing Slices with incorrect partition IDs.
// It returns the updated list of Slices, a boolean indicating if changes were made, and any error encountered.
func (r *WorkloadReconciler) syncSlices(
	ctx context.Context,
	wl *kueue.Workload,
	ac *kueue.AdmissionCheckState,
	slices []v1beta1.Slice,
	nodes map[string]corev1.Node,
) ([]v1beta1.Slice, bool, error) {
	// this is to prevent from creating slices when AC is Retry
	// and the workload still has the old Admission
	if ac.State == kueue.CheckStateRetry || ac.State == kueue.CheckStateRejected {
		return slices, false, nil
	}
	existingSlicesByName := make(map[string]*v1beta1.Slice, len(slices))
	for i := range slices {
		existingSlicesByName[slices[i].Name] = &slices[i]
	}

	allCreatedSlices, allDeletedSliceNames, err := r.syncAllPodSetAssignments(ctx, wl, ac, nodes, existingSlicesByName)
	if err != nil {
		return nil, false, err
	}

	updatedSlices, changed := core.ApplySliceChanges(slices, allCreatedSlices, allDeletedSliceNames)

	if len(allCreatedSlices) > 0 {
		msg := utilslice.BuildCreationEventMessage(allCreatedSlices)
		ctrl.LoggerFrom(ctx).V(3).Info(msg)
		r.record.Event(wl, corev1.EventTypeNormal, SlicesCreatedEventType, api.TruncateEventMessage(msg))
	}

	return updatedSlices, changed, nil
}

func (r *WorkloadReconciler) syncAllPodSetAssignments(
	ctx context.Context,
	wl *kueue.Workload,
	ac *kueue.AdmissionCheckState,
	nodes map[string]corev1.Node,
	existingSlicesByName map[string]*v1beta1.Slice,
) ([]v1beta1.Slice, []string, error) {
	var allDeletedSliceNames []string
	allCreatedSlices := make([]v1beta1.Slice, 0, len(wl.Status.Admission.PodSetAssignments))

	for _, psa := range wl.Status.Admission.PodSetAssignments {
		if !utilworkload.ShouldCreateSlicesForPodSetAssignment(wl, psa, nodes) {
			continue
		}
		ps := podset.FindPodSetByName(wl.Spec.PodSets, psa.Name)
		desiredNumberOfSlices := ptr.Deref(ps.TopologyRequest.SubGroupCount, 1)

		createdSlices, deletedSlices, err := r.syncSlicesForAssignment(ctx, wl, ac, &psa, nodes, existingSlicesByName, desiredNumberOfSlices)
		if err != nil {
			return nil, nil, err
		}
		allCreatedSlices = append(allCreatedSlices, createdSlices...)
		allDeletedSliceNames = append(allDeletedSliceNames, deletedSlices...)
	}

	return allCreatedSlices, allDeletedSliceNames, nil
}

func (r *WorkloadReconciler) syncSlicesForAssignment(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, psa *kueue.PodSetAssignment, nodes map[string]corev1.Node, existingSlicesByName map[string]*v1beta1.Slice, desiredNumberOfSlices int32) ([]v1beta1.Slice, []string, error) {
	slicesToCreate, slicesToDelete := r.prepareSliceActions(wl, psa, nodes, existingSlicesByName, desiredNumberOfSlices)

	var deletedSliceNames []string
	for _, s := range slicesToDelete {
		log := ctrl.LoggerFrom(ctx).WithValues("slice", klog.KObj(s))
		log.V(2).Info("Existing Slice has wrong partition IDs, deleting it")
		if err := r.client.Delete(ctx, s); client.IgnoreNotFound(err) != nil {
			return nil, nil, err
		}
		deletedSliceNames = append(deletedSliceNames, s.Name)
	}

	if err := errors.Join(
		r.validatePartitionConflicts(ctx, slicesToCreate),
		r.validatePartitionCount(ctx, slicesToCreate)); err != nil {
		log := ctrl.LoggerFrom(ctx)
		log.V(2).Info("Slice validation failed, not creating Slices, evicting the workload", "error", err)
		msg := err.Error()
		if patchErr := r.evictWorkload(ctx, wl, ac, msg); patchErr != nil {
			return nil, nil, errors.Join(err, patchErr)
		}
		return nil, nil, errWorkloadEvicted
	}

	createdSlices, err := r.createSlices(ctx, wl, ac, slicesToCreate)
	if err != nil {
		return nil, nil, err
	}

	return createdSlices, deletedSliceNames, nil
}

// prepareSliceActions determines which slices to create and which to delete for a given pod set assignment.
func (r *WorkloadReconciler) prepareSliceActions(wl *kueue.Workload, psa *kueue.PodSetAssignment, nodes map[string]corev1.Node, existingSlicesByName map[string]*v1beta1.Slice, desiredNumberOfSlices int32) ([]*v1beta1.Slice, []*v1beta1.Slice) {
	ps := podset.FindPodSetByName(wl.Spec.PodSets, psa.Name)
	label := topology.GetPartitionIDLabel(ps.Template)
	parsedAssignment := topology.ParseAssignment(psa.TopologyAssignment, nodes, label)
	chunkSize := 0
	if desiredNumberOfSlices > 0 {
		chunkSize = len(parsedAssignment.PartitionIDs) / int(desiredNumberOfSlices)
	}

	var slicesToCreate []*v1beta1.Slice
	var slicesToDelete []*v1beta1.Slice

	for i := int32(0); i < desiredNumberOfSlices; i++ {
		sliceName := core.SliceName(wl.Namespace, wl.Name, psa.Name, i)
		start := int(i) * chunkSize
		end := start + chunkSize
		var expectedPartitionIDs []string
		if len(parsedAssignment.PartitionIDs) > 0 {
			expectedPartitionIDs = parsedAssignment.PartitionIDs[start:end]
		}

		if existingSlice, exist := existingSlicesByName[sliceName]; exist {
			if !slices.Equal(existingSlice.Spec.PartitionIds, expectedPartitionIDs) {
				if existingSlice.DeletionTimestamp.IsZero() {
					slicesToDelete = append(slicesToDelete, existingSlice)
				}
			}
			// Slice already exists, nothing to do.
			continue
		}

		slice := core.ConstructSliceForWorkload(wl, psa.Name, i, ps.Template, expectedPartitionIDs)
		slicesToCreate = append(slicesToCreate, slice)
	}
	return slicesToCreate, slicesToDelete
}

func (r *WorkloadReconciler) createSlices(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, slicesToCreate []*v1beta1.Slice) ([]v1beta1.Slice, error) {
	var createdSlices []v1beta1.Slice
	for _, slice := range slicesToCreate {
		log := ctrl.LoggerFrom(ctx).WithValues("slice", klog.KObj(slice))
		log.V(2).Info("Creating Slice")

		if err := r.client.Create(ctx, slice); err != nil {
			msg := fmt.Sprintf("Error creating Slice %q: %v", slice.Name, err)
			log.Error(err, msg)
			r.record.Event(wl, corev1.EventTypeWarning, FailedCreateSliceEventType, api.TruncateEventMessage(msg))
			log.V(2).Info(fmt.Sprintf("Admission check %q updated state from %q to %q", ac.Name, ac.State, kueue.CheckStatePending), "reason", msg)
			ac.State = kueue.CheckStatePending
			ac.Message = api.TruncateConditionMessage(msg)
			if patchErr := r.updateWorkloadAdmissionCheckStatus(ctx, wl, ac); patchErr != nil {
				return nil, errors.Join(err, patchErr)
			}
			return nil, err
		}
		createdSlices = append(createdSlices, *slice)
	}
	return createdSlices, nil
}

func (r *WorkloadReconciler) validatePartitionConflicts(
	ctx context.Context,
	slicesToCreate []*v1beta1.Slice,
) error {
	// if we use the retry mechanism, it is going to retry on partition conflicts
	if features.Enabled(features.UseRetryMechanismForSliceCreation) {
		return nil
	}
	log := ctrl.LoggerFrom(ctx)

	allSlices, err := r.findAllSlices(ctx)
	if err != nil {
		return err
	}

	usedPartitionIDs := make(map[string]string)
	for _, s := range allSlices {
		for _, id := range s.Spec.PartitionIds {
			usedPartitionIDs[id] = s.Name
		}
	}

	var conflictingIDs []string
	for _, slice := range slicesToCreate {
		for _, id := range slice.Spec.PartitionIds {
			if oldSliceName, ok := usedPartitionIDs[id]; ok {
				log.V(3).Info("Partition ID collision detected", "partitionID", id, "oldSlice", oldSliceName, "newSlice", slice.Name)
				conflictingIDs = append(conflictingIDs, fmt.Sprintf("%v (used by %v)", id, oldSliceName))
			}
		}
	}
	if len(conflictingIDs) > 0 {
		return fmt.Errorf("partition IDs %q are already used by existing Slices", conflictingIDs)
	}
	return nil
}

func (r *WorkloadReconciler) validatePartitionCount(
	ctx context.Context,
	slicesToCreate []*v1beta1.Slice,
) error {
	log := ctrl.LoggerFrom(ctx)
	var incorrectSlices []string
	for _, slice := range slicesToCreate {
		parsed, err := topology.ParseTopologyV7(slice.Spec.Topology)
		if err != nil {
			return err
		}
		desiredNumberOfPartitions := parsed.DesiredNumberOfPartitions()
		if len(slice.Spec.PartitionIds) != int(desiredNumberOfPartitions) {
			incorrectSlices = append(incorrectSlices, slice.Name)
			log.V(3).Info("The number of partition IDs in topology assignment does not match the topology",
				"slice", slice.Name,
				"topology", slice.Spec.Topology,
				"expectedPartitions", int(desiredNumberOfPartitions),
				"actualPartitions", len(slice.Spec.PartitionIds),
			)
		}
	}
	if len(incorrectSlices) > 0 {
		return fmt.Errorf("incorrect number of partitions for slices: %v", incorrectSlices)
	}
	return nil
}

func (r *WorkloadReconciler) evictWorkload(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, message string) error {
	ac.State = kueue.CheckStateRetry
	ac.RequeueAfterSeconds = ptr.To(int32(r.retryDelayOnSliceFailure.Round(time.Second).Seconds()))
	ac.Message = api.TruncateConditionMessage(message)
	return r.updateWorkloadAdmissionCheckStatus(ctx, wl, ac)
}

func (r *WorkloadReconciler) updateWorkloadAdmissionCheckStatus(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState) error {
	wlPatch := workload.BaseSSAWorkload(wl, true)
	workload.SetAdmissionCheckState(&wlPatch.Status.AdmissionChecks, *ac, r.clock)
	err := r.client.Status().Patch(ctx, wlPatch, client.Apply, client.FieldOwner(SliceControllerName), client.ForceOwnership)
	if err != nil && !apierrors.IsNotFound(err) {
		ctrl.LoggerFrom(ctx).Error(err, "Failed to patch the Workload's admission status")
	}
	return err
}

// syncAdmissionCheckStatus syncs the admission check status with the state of the Slices.
func (r *WorkloadReconciler) syncAdmissionCheckStatus(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, slices []v1beta1.Slice, desiredSlicesCount int) error {
	originalState := ac.State
	originalMessage := ac.Message

	r.prepareAdmissionCheckStatus(ctx, wl, ac, slices, desiredSlicesCount)

	// No changes.
	if originalState == ac.State && ac.Message == originalMessage {
		return nil
	}

	if err := r.updateWorkloadAdmissionCheckStatus(ctx, wl, ac); err != nil {
		return err
	}

	log := ctrl.LoggerFrom(ctx)

	if originalState != ac.State {
		message := fmt.Sprintf("Admission check %q updated state from %q to %q", ac.Name, originalState, ac.State)
		log.V(2).Info(message)
		r.record.Event(wl, corev1.EventTypeNormal, AdmissionCheckUpdatedEventType, message)
	}

	if ac.Message != originalMessage {
		// Logging error messages if exists
		for _, slice := range slices {
			cond := meta.FindStatusCondition(slice.Status.Conditions, v1beta1.SliceStateConditionType)
			if cond != nil && cond.Status == metav1.ConditionFalse && cond.Reason == string(core.MMIGHealthStatusFailed) {
				log.V(2).Info(
					"WARNING: The Slice is not operational due to an error",
					"slice", klog.KObj(&slice),
					"error", cond.Message,
				)
			}
		}
	}

	return nil
}

func (r *WorkloadReconciler) prepareAdmissionCheckStatus(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, slices []v1beta1.Slice, desiredSlicesCount int) {
	// wait for Kueue to reset check to Pending after eviction
	if ac.State == kueue.CheckStateRetry {
		return
	}
	slicesByState := utilslice.GroupSlicesByState(slices, r.activationTimeout)

	r.applyAdmissionCheckState(ctx, wl, ac, slicesByState, desiredSlicesCount)
	ac.Message = api.TruncateConditionMessage(utilslice.BuildAdmissionCheckMessage(slicesByState))
}

func (r *WorkloadReconciler) applyAdmissionCheckState(ctx context.Context, wl *kueue.Workload, ac *kueue.AdmissionCheckState, slicesByState map[core.SliceState][]*v1beta1.Slice, desiredSlicesCount int) {
	log := ctrl.LoggerFrom(ctx)
	switch {
	case desiredSlicesCount == len(slicesByState[core.SliceStateActive])+len(slicesByState[core.SliceStateActiveDegraded]):
		ac.State = kueue.CheckStateReady
		ac.PodSetUpdates = utilworkload.BuildPodSetUpdates(wl)
	case len(slicesByState[core.SliceStateFailed]) > 0:
		ac.State = kueue.CheckStateRetry
		ac.RequeueAfterSeconds = ptr.To(int32(r.retryDelayOnSliceFailure.Round(time.Second).Seconds()))
	case (features.Enabled(features.UseRetryMechanismForSliceCreation) && len(slicesByState[core.SliceStateStale]) > 0):
		var staleSliceNames []string
		for _, s := range slicesByState[core.SliceStateStale] {
			staleSliceNames = append(staleSliceNames, s.Name)
		}
		log.V(2).Info("Setting AdmissionCheck to Retry due to Slices that failed to initialize despite retry mechanism",
			"staleSlices", staleSliceNames)
		ac.State = kueue.CheckStateRetry
		ac.RequeueAfterSeconds = ptr.To(int32(r.retryDelayOnSliceFailure.Round(time.Second).Seconds()))
	default:
		ac.State = kueue.CheckStatePending
	}
}

// SetupWithManager sets up the controller with the Manager.
func (r *WorkloadReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&kueue.Workload{}).
		Named("workload_controller").
		Watches(&v1beta1.Slice{}, &sliceHandler{client: r.client}).
		Complete(r)
}

var _ handler.EventHandler = (*sliceHandler)(nil)

type sliceHandler struct {
	client client.Client
}

func (h *sliceHandler) Generic(context.Context, event.GenericEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
}

func (h *sliceHandler) Create(context.Context, event.CreateEvent, workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// No need to handle create event. We should wait for at least Forming state.
}

func (h *sliceHandler) Delete(ctx context.Context, e event.DeleteEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	if slice, ok := e.Object.(*v1beta1.Slice); ok {
		ctrl.LoggerFrom(ctx).V(2).Info("Slice deleted", "slice", slice.Name)
	}
	h.handleEvent(ctx, e.Object, q)
}

func (h *sliceHandler) Update(ctx context.Context, e event.UpdateEvent, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	if oldSlice, ok := e.ObjectOld.(*v1beta1.Slice); ok {
		if newSlice, ok := e.ObjectNew.(*v1beta1.Slice); ok {
			condOld := meta.FindStatusCondition(oldSlice.Status.Conditions, v1beta1.SliceStateConditionType)
			condNew := meta.FindStatusCondition(newSlice.Status.Conditions, v1beta1.SliceStateConditionType)
			if diff := cmp.Diff(condOld, condNew); diff != "" {
				ctrl.LoggerFrom(ctx).V(2).Info("Slice state updated", "diff", diff)
			}
		}
	}
	h.handleEvent(ctx, e.ObjectNew, q)
}

func (h *sliceHandler) handleEvent(ctx context.Context, obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	slice, isSlice := obj.(*v1beta1.Slice)
	// Only Slice should be handled.
	if !isSlice {
		return
	}

	log := ctrl.LoggerFrom(ctx)

	workloadNamespace, nsFound := slice.Annotations[core.OwnerWorkloadNamespaceAnnotation]
	workloadName, nameFound := slice.Annotations[core.OwnerWorkloadNameAnnotation]

	if !nsFound || !nameFound {
		log.V(3).Info("Slice is missing workload owner annotations, skipping event handling", "slice", klog.KObj(slice))
		return
	}

	log.V(3).Info("Handle Slice event", "workload", klog.KRef(workloadNamespace, workloadName))

	req := reconcile.Request{
		NamespacedName: types.NamespacedName{
			Name:      workloadName,
			Namespace: workloadNamespace,
		},
	}

	q.AddAfter(req, updatesBatchPeriod)
}
