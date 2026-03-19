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

package workload

import (
	"errors"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	jobset "sigs.k8s.io/jobset/api/jobset/v1alpha2"
	kueue "sigs.k8s.io/kueue/apis/kueue/v1beta2"
	"sigs.k8s.io/kueue/pkg/util/podset"
	"sigs.k8s.io/kueue/pkg/workload"

	"tpu-slice-controller/internal/core"
	"tpu-slice-controller/internal/topology"
)

func ShouldFinalize(wl *kueue.Workload) (bool, string) {
	if !wl.DeletionTimestamp.IsZero() {
		return true, "it has been deleted"
	}
	if workload.IsFinished(wl) {
		return true, "it has finished"
	}
	if workload.IsEvicted(wl) {
		return true, "it was evicted"
	}
	if !workload.IsActive(wl) {
		return true, "it is no longer active"
	}
	if !controllerutil.HasControllerReference(wl) {
		return true, "it doesn't have owner"
	}
	if !HasSupportedOwner(wl) {
		return true, "it has an unsupported owner"
	}
	return false, ""
}

func HasSupportedOwner(wl *kueue.Workload) bool {
	return IsJobSetOwner(wl) || IsJobOwner(wl)
}

func IsJobSetOwner(wl *kueue.Workload) bool {
	if owner := metav1.GetControllerOf(wl); owner != nil {
		return owner.APIVersion == jobset.SchemeGroupVersion.String() && owner.Kind == "JobSet"
	}
	return false
}

func IsJobOwner(wl *kueue.Workload) bool {
	if owner := metav1.GetControllerOf(wl); owner != nil {
		return owner.APIVersion == batchv1.SchemeGroupVersion.String() && owner.Kind == "Job"
	}
	return false
}

func ValidateRelevant(wl *kueue.Workload, nodes map[string]corev1.Node) error {
	if !HasSupportedOwner(wl) {
		return errors.New("does not have a supported owner")
	}
	if !core.HasRelevantPodSet(wl.Spec.PodSets) {
		return errors.New("does not have a relevant podset")
	}
	if !workload.HasQuotaReservation(wl) {
		return errors.New("does not have a quota reservation")
	}
	if wl.Status.Admission == nil {
		return errors.New("has no admission")
	}
	if !topology.AnyAssignment(wl.Status.Admission) {
		return errors.New("has no topology assignment")
	}
	if !topology.AllAssignmentsValid(wl, nodes) {
		return errors.New("has invalid topology assignments")
	}
	return nil
}

func ShouldCreateSlicesForPodSetAssignment(wl *kueue.Workload, psa kueue.PodSetAssignment, nodes map[string]corev1.Node) bool {
	if podSet := podset.FindPodSetByName(wl.Spec.PodSets, psa.Name); podSet != nil {
		label := topology.GetPartitionIDLabel(podSet.Template)
		return core.IsRelevantPodTemplateSpec(podSet.Template) &&
			topology.IsAssignmentValid(psa, nodes, label) &&
			podSet.TopologyRequest != nil
	}
	return false
}

func TotalDesiredSlices(wl *kueue.Workload, nodes map[string]corev1.Node) int {
	if wl.Status.Admission == nil {
		return 0
	}
	count := 0
	for _, psa := range wl.Status.Admission.PodSetAssignments {
		if !ShouldCreateSlicesForPodSetAssignment(wl, psa, nodes) {
			continue
		}
		ps := podset.FindPodSetByName(wl.Spec.PodSets, psa.Name)
		count += int(ptr.Deref(ps.TopologyRequest.SubGroupCount, 1))
	}
	return count
}

func BuildPodSetUpdates(wl *kueue.Workload) []kueue.PodSetUpdate {
	var podSetUpdates []kueue.PodSetUpdate
	for _, ps := range wl.Spec.PodSets {
		if topology := core.GetTPUTopology(ps.Template); topology != "" {
			podSetUpdates = append(podSetUpdates, kueue.PodSetUpdate{
				Name: ps.Name,
				NodeSelector: map[string]string{
					core.TPUTopologyAnnotation: topology,
				},
			})
		}
	}
	return podSetUpdates
}
