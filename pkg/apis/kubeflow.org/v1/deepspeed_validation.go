// Copyright 2021 The Kubeflow Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	"fmt"
)

func ValidateV1DeepspeedJobSpec(c *DeepspeedJobSpec) error {
	if c.DeepspeedReplicaSpecs == nil {
		return fmt.Errorf("MPIReplicaSpecs is not valid")
	}
	launcherExists := false
	for rType, value := range c.DeepspeedReplicaSpecs {
		if value == nil || len(value.Template.Spec.Containers) == 0 {
			return fmt.Errorf("MPIReplicaSpecs is not valid: containers definition expected in %v", rType)
		}
		// Make sure the replica type is valid.
		validReplicaTypes := []ReplicaType{DeepspeedJobReplicaTypeLauncher, DeepspeedJobReplicaTypeWorker}

		isValidReplicaType := false
		for _, t := range validReplicaTypes {
			if t == rType {
				isValidReplicaType = true
				break
			}
		}
		if !isValidReplicaType {
			return fmt.Errorf("DeepspeedReplicaType is %v but must be one of %v", rType, validReplicaTypes)
		}

		for _, container := range value.Template.Spec.Containers {
			if container.Image == "" {
				msg := fmt.Sprintf("DeepspeedReplicaSpec is not valid: Image is undefined in the container of %v", rType)
				return fmt.Errorf(msg)
			}

			if container.Name == "" {
				msg := fmt.Sprintf("DeepspeedReplicaSpec is not valid: ImageName is undefined in the container of %v", rType)
				return fmt.Errorf(msg)
			}
		}
		if rType == DeepspeedJobReplicaTypeLauncher {
			launcherExists = true
			if value.Replicas != nil && int(*value.Replicas) != 1 {
				return fmt.Errorf("DeepspeedReplicaSpec is not valid: There must be only 1 launcher replica")
			}
		}

	}

	if !launcherExists {
		return fmt.Errorf("DeepspeedReplicaSpec is not valid: Master ReplicaSpec must be present")
	}
	return nil

}
