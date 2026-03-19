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
package slice

import (
	"fmt"
	"sort"
	"strings"
	"time"
	"tpu-slice-controller/api/v1beta1"
	"tpu-slice-controller/internal/core"

	"k8s.io/apimachinery/pkg/api/meta"
)

// GroupSlicesByState groups a list of Slices by their current state.
func GroupSlicesByState(slices []v1beta1.Slice, timeout time.Duration) map[core.SliceState][]*v1beta1.Slice {
	slicesByState := make(map[core.SliceState][]*v1beta1.Slice)
	for i := range slices {
		state := core.GetSliceState(slices[i], timeout)
		slicesByState[state] = append(slicesByState[state], &slices[i])
	}
	return slicesByState
}

func BuildCreationEventMessage(slices []v1beta1.Slice) string {
	sliceNames := make([]string, len(slices))
	for index, slice := range slices {
		sliceNames[index] = fmt.Sprintf("%q", slice.Name)
	}
	sort.Strings(sliceNames)
	return fmt.Sprintf("The Slices %s have been created", strings.Join(sliceNames, ", "))
}

func BuildAdmissionCheckMessage(slicesByState map[core.SliceState][]*v1beta1.Slice) string {
	var stateMessages []string
	for _, state := range core.SliceStates {
		if count := len(slicesByState[state]); count > 0 {
			stateMessages = append(stateMessages, fmt.Sprintf("%d %s", count, state))
		}
	}

	var message string
	if len(stateMessages) > 0 {
		message = fmt.Sprintf("Slices are in states: %s", strings.Join(stateMessages, ", "))
	} else {
		message = "Waiting for Slices to be created"
	}

	if len(slicesByState[core.SliceStateFailed]) > 0 {
		var errMessages []string
		for _, slice := range slicesByState[core.SliceStateFailed] {
			cond := meta.FindStatusCondition(slice.Status.Conditions, v1beta1.SliceStateConditionType)
			if cond != nil {
				errMessages = append(errMessages, cond.Message)
			}
		}
		message += ". Errors: " + strings.Join(errMessages, "; ")
	}
	return message
}
