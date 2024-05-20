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

package deepspeed

import (
	"strings"

	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
)

const (
	configSuffix               = "-config"
	configVolumeName           = "deepspeed-job-config"
	configMountPath            = "/etc/deepspeed"
	kubexecScriptName          = "kubexec.sh"
	hostfileName               = "hostfile"
	discoverHostsScriptName    = "discover_hosts.sh"
	kubectlDeliveryName        = "kubectl-delivery"
	kubectlTargetDirEnv        = "TARGET_DIR"
	kubectlVolumeName          = "deepspeed-job-kubectl"
	kubectlMountPath           = "/opt/kube"
	launcher                   = "launcher"
	worker                     = "worker"
	launcherSuffix             = "-launcher"
	workerSuffix               = "-worker"
	gpuResourceNameSuffix      = ".com/gpu"
	gpuResourceNamePattern     = "gpu"
	initContainerCpu           = "100m"
	initContainerEphStorage    = "5Gi"
	initContainerMem           = "512Mi"
	iDeepspeedDefaultBootstrap = "rsh"
)

const (
	// ErrResourceExists is used as part of the Event 'reason' when an DeepspeedJob
	// fails to sync due to dependent resources of the same name already
	// existing.
	ErrResourceExists = "ErrResourceExists"

	// MessageResourceExists is the message used for Events when a resource
	// fails to sync due to dependent resources already existing.
	MessageResourceExists = "Resource %q of DeepspeedJobKind %q already exists and is not managed by DeepspeedJob"

	// ErrResourceDoesNotExist is used as part of the Event 'reason' when some
	// resource is missing in yaml
	ErrResourceDoesNotExist = "ErrResourceDoesNotExist"

	// MessageResourceDoesNotExist is used for Events when some
	// resource is missing in yaml
	MessageResourceDoesNotExist = "Resource %q is missing in yaml"

	// podTemplateRestartPolicyReason is the warning reason when the restart
	// policy is set in pod template.
	podTemplateRestartPolicyReason = "SettedPodTemplateRestartPolicy"

	// podTemplateSchedulerNameReason is the warning reason when other scheduler name is set
	// in pod templates with gang-scheduling enabled
	podTemplateSchedulerNameReason = "SettedPodTemplateSchedulerName"

	// DeepspeedJobEvict
	DeepspeedJobEvict = "DeepspeedJobEvicted"
)

// initializeDeepspeedJobStatuses initializes the ReplicaStatuses for DeepspeedJob.
func initializeDeepspeedJobStatuses(DeepspeedJob *kubeflowv1.DeepspeedJob, rType kubeflowv1.ReplicaType) {
	if DeepspeedJob.Status.ReplicaStatuses == nil {
		DeepspeedJob.Status.ReplicaStatuses = make(map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaStatus)
	}

	DeepspeedJob.Status.ReplicaStatuses[rType] = &kubeflowv1.ReplicaStatus{}
}

// updateDeepspeedJobConditions updates the conditions of the given DeepspeedJob.
func updateDeepspeedJobConditions(DeepspeedJob *kubeflowv1.DeepspeedJob, conditionType kubeflowv1.JobConditionType, reason, message string) error {
	condition := newCondition(conditionType, reason, message)
	setCondition(&DeepspeedJob.Status, condition)
	return nil
}

// newCondition creates a new DeepspeedJob condition.
func newCondition(conditionType kubeflowv1.JobConditionType, reason, message string) kubeflowv1.JobCondition {
	return kubeflowv1.JobCondition{
		Type:               conditionType,
		Status:             corev1.ConditionTrue,
		LastUpdateTime:     metav1.Now(),
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	}
}

// getCondition returns the condition with the provided type.
func getCondition(status kubeflowv1.JobStatus, condType kubeflowv1.JobConditionType) *kubeflowv1.JobCondition {
	for _, condition := range status.Conditions {
		if condition.Type == condType {
			return &condition
		}
	}
	return nil
}

func isEvicted(status kubeflowv1.JobStatus) bool {
	for _, condition := range status.Conditions {
		if condition.Type == kubeflowv1.JobFailed &&
			condition.Status == corev1.ConditionTrue &&
			condition.Reason == DeepspeedJobEvict {
			return true
		}
	}
	return false
}

// setCondition updates the DeepspeedJob to include the provided condition.
// If the condition that we are about to add already exists
// and has the same status and reason then we are not going to update.
func setCondition(status *kubeflowv1.JobStatus, condition kubeflowv1.JobCondition) {

	currentCond := getCondition(*status, condition.Type)

	// Do nothing if condition doesn't change
	if currentCond != nil && currentCond.Status == condition.Status && currentCond.Reason == condition.Reason {
		return
	}

	// Do not update lastTransitionTime if the status of the condition doesn't change.
	if currentCond != nil && currentCond.Status == condition.Status {
		condition.LastTransitionTime = currentCond.LastTransitionTime
	}

	// Append the updated condition
	newConditions := filterOutCondition(status.Conditions, condition.Type)
	status.Conditions = append(newConditions, condition)
}

// filterOutCondition returns a new slice of DeepspeedJob conditions without conditions with the provided type.
func filterOutCondition(conditions []kubeflowv1.JobCondition, condType kubeflowv1.JobConditionType) []kubeflowv1.JobCondition {
	var newConditions []kubeflowv1.JobCondition
	for _, c := range conditions {
		if condType == kubeflowv1.JobRestarting && c.Type == kubeflowv1.JobRunning {
			continue
		}
		if condType == kubeflowv1.JobRunning && c.Type == kubeflowv1.JobRestarting {
			continue
		}

		if c.Type == condType {
			continue
		}

		// Set the running condition status to be false when current condition failed or succeeded
		if (condType == kubeflowv1.JobFailed || condType == kubeflowv1.JobSucceeded) && (c.Type == kubeflowv1.JobRunning || c.Type == kubeflowv1.JobFailed) {
			c.Status = corev1.ConditionFalse
		}

		newConditions = append(newConditions, c)
	}
	return newConditions
}

func isPodFinished(j *corev1.Pod) bool {
	return isPodSucceeded(j) || isPodFailed(j)
}

func isPodFailed(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodFailed
}

func isPodSucceeded(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodSucceeded
}

func isPodRunning(p *corev1.Pod) bool {
	return p.Status.Phase == corev1.PodRunning
}

// isGPULauncher checks whether the launcher needs GPU.
func isGPULauncher(DeepspeedJob *kubeflowv1.DeepspeedJob) bool {
	for _, container := range DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.Containers {
		for key := range container.Resources.Limits {
			if strings.HasSuffix(string(key), gpuResourceNameSuffix) {
				return true
			}
			if strings.Contains(string(key), gpuResourceNamePattern) {
				return true
			}
		}
	}
	return false
}

// hasIntelDeepspeedBootstrapValues returns the existence of I_Deepspeed_HYDRA_BOOTSTRAP
// and I_Deepspeed_HYDRA_BOOTSTRAP_EXEC values.
// There are also _EXEC_EXTRA_ARGS and _AUTOFORK under the I_Deepspeed_HYDRA_BOOTSTRAP
// prefix but those are not checked on purpose.
func hasIntelDeepspeedBootstrapValues(envs []corev1.EnvVar) (bootstrap, exec bool) {
	for _, env := range envs {
		if env.Name == "I_Deepspeed_HYDRA_BOOTSTRAP" {
			bootstrap = true
		} else if env.Name == "I_Deepspeed_HYDRA_BOOTSTRAP_EXEC" {
			exec = true
		}

		if bootstrap && exec {
			break
		}
	}

	return bootstrap, exec
}

func defaultReplicaLabels(genericLabels map[string]string, roleLabelVal string) map[string]string {
	replicaLabels := map[string]string{}
	for k, v := range genericLabels {
		replicaLabels[k] = v
	}

	replicaLabels[kubeflowv1.ReplicaTypeLabel] = roleLabelVal
	return replicaLabels
}

func defaultWorkerLabels(genericLabels map[string]string) map[string]string {
	return defaultReplicaLabels(genericLabels, worker)
}

func defaultLauncherLabels(genericLabels map[string]string) map[string]string {
	return defaultReplicaLabels(genericLabels, launcher)
}

func workerSelector(genericLabels map[string]string) (labels.Selector, error) {
	labels := defaultWorkerLabels(genericLabels)

	labelSelector := metav1.LabelSelector{
		MatchLabels: labels,
	}

	selector, err := metav1.LabelSelectorAsSelector(&labelSelector)
	if err != nil {
		return nil, err
	}

	return selector, nil
}

// initializeReplicaStatuses initializes the ReplicaStatuses for replica.
// originally from pkg/controller.v1/tensorflow/status.go (deleted)
func initializeReplicaStatuses(jobStatus *kubeflowv1.JobStatus, rtype kubeflowv1.ReplicaType) {
	if jobStatus.ReplicaStatuses == nil {
		jobStatus.ReplicaStatuses = make(map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaStatus)
	}

	jobStatus.ReplicaStatuses[rtype] = &kubeflowv1.ReplicaStatus{}
}
