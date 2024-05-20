// Copyright 2019 The Kubeflow Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package v1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	// DeepspeedJobDefaultPortName is name of the port used to communicate between Master and Workers.
	DeepspeedJobDefaultPortName = "Deepspeed-port"
	// DeepspeedJobDefaultPort is default value of the port.
	DeepspeedJobDefaultPort = 9999
	// DeepspeedJobDefaultContainerName is the name of the DeepspeedJob container.
	DeepspeedJobDefaultContainerName = "Deepspeed"
	// DeepspeedJobDefaultRestartPolicy is default RestartPolicy for ReplicaSpec.
	DeepspeedJobDefaultRestartPolicy = RestartPolicyNever
	DeepspeedJobKind                 = "DeepspeedJob"
	// DeepspeedJobPlural is the DeepspeedJobPlural for TFJob.
	DeepspeedJobPlural = "Deepspeedjobs"
	// DeepspeedJobSingular is the singular for TFJob.
	DeepspeedJobSingular = "Deepspeedjob"
	// DeepspeedJobFrameworkName is the name of the ML Framework
	DeepspeedJobFrameworkName = "Deepspeed"
	// DeepspeedJobReplicaTypeLauncher is the type for launcher replica.
	DeepspeedJobReplicaTypeLauncher ReplicaType = "Launcher"
	// DeepspeedJobReplicaTypeWorker is the type for worker replicas.
	DeepspeedJobReplicaTypeWorker ReplicaType = "Worker"
)

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=Deepspeedjob
// +kubebuilder:object:root=true
// +kubebuilder:printcolumn:JSONPath=`.metadata.creationTimestamp`,name="Age",type=date
// +kubebuilder:printcolumn:JSONPath=`.status.conditions[-1:].type`,name="State",type=string
// +kubebuilder:subresource:status

type DeepspeedJob struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              DeepspeedJobSpec `json:"spec,omitempty"`
	Status            JobStatus        `json:"status,omitempty"`
}

type DeepspeedJobSpec struct {

	// Specifies the number of slots per worker used in hostfile.
	// Defaults to 1.
	// +optional
	SlotsPerWorker *int32 `json:"slotsPerWorker,omitempty"`

	// CleanPodPolicy defines the policy that whether to kill pods after the job completes.
	// Defaults to None.
	CleanPodPolicy *CleanPodPolicy `json:"cleanPodPolicy,omitempty"`

	// `DeepspeedReplicaSpecs` contains maps from `DeepspeedReplicaType` to `ReplicaSpec` that
	// specify the Deepspeed replicas to run.
	DeepspeedReplicaSpecs map[ReplicaType]*ReplicaSpec `json:"DeepspeedReplicaSpecs"`

	// MainContainer specifies name of the main container which
	// executes the Deepspeed code.
	MainContainer string `json:"mainContainer,omitempty"`

	// `RunPolicy` encapsulates various runtime policies of the distributed training
	// job, for example how to clean up resources and how long the job can stay
	// active.
	RunPolicy RunPolicy `json:"runPolicy,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +resource:path=Deepspeedjobs
// +kubebuilder:object:root=true

type DeepspeedJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DeepspeedJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&DeepspeedJob{}, &DeepspeedJobList{})
	SchemeBuilder.SchemeBuilder.Register(addDeepspeedJobDefaultingFuncs)
}
