package v1

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

func expectedDeepspeedJob(cleanPodPolicy CleanPodPolicy, restartPolicy RestartPolicy) *DeepspeedJob {
	return &DeepspeedJob{
		Spec: DeepspeedJobSpec{
			CleanPodPolicy: &cleanPodPolicy,
			RunPolicy: RunPolicy{
				CleanPodPolicy: &cleanPodPolicy,
			},
			DeepspeedReplicaSpecs: map[ReplicaType]*ReplicaSpec{
				DeepspeedJobReplicaTypeLauncher: {
					Replicas:      ptr.To[int32](1),
					RestartPolicy: restartPolicy,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  DeepspeedJobDefaultContainerName,
									Image: testImage,
								},
							},
						},
					},
				},
				DeepspeedJobReplicaTypeWorker: {
					Replicas:      ptr.To[int32](0),
					RestartPolicy: restartPolicy,
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  DeepspeedJobDefaultContainerName,
									Image: testImage,
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestSetDefaults_DeepspeedJob(t *testing.T) {
	customRestartPolicy := RestartPolicyAlways

	testCases := map[string]struct {
		original *DeepspeedJob
		expected *DeepspeedJob
	}{
		"set default replicas": {
			original: &DeepspeedJob{
				Spec: DeepspeedJobSpec{
					CleanPodPolicy: CleanPodPolicyPointer(CleanPodPolicyRunning),
					RunPolicy: RunPolicy{
						CleanPodPolicy: CleanPodPolicyPointer(CleanPodPolicyRunning),
					},
					DeepspeedReplicaSpecs: map[ReplicaType]*ReplicaSpec{
						DeepspeedJobReplicaTypeLauncher: {
							RestartPolicy: customRestartPolicy,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
						DeepspeedJobReplicaTypeWorker: {
							RestartPolicy: customRestartPolicy,
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
					},
				},
			},
			expected: expectedDeepspeedJob(CleanPodPolicyRunning, customRestartPolicy),
		},
		"set default clean pod policy": {
			original: &DeepspeedJob{
				Spec: DeepspeedJobSpec{
					DeepspeedReplicaSpecs: map[ReplicaType]*ReplicaSpec{
						DeepspeedJobReplicaTypeLauncher: {
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
						DeepspeedJobReplicaTypeWorker: {
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
					},
				},
			},
			expected: expectedDeepspeedJob(CleanPodPolicyNone, DeepspeedJobDefaultRestartPolicy),
		},
		"set default restart policy": {
			original: &DeepspeedJob{
				Spec: DeepspeedJobSpec{
					DeepspeedReplicaSpecs: map[ReplicaType]*ReplicaSpec{
						DeepspeedJobReplicaTypeLauncher: {
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
						DeepspeedJobReplicaTypeWorker: {
							Template: corev1.PodTemplateSpec{
								Spec: corev1.PodSpec{
									Containers: []corev1.Container{
										{
											Name:  DeepspeedJobDefaultContainerName,
											Image: testImage,
										},
									},
								},
							},
						},
					},
				},
			},
			expected: expectedDeepspeedJob(CleanPodPolicyNone, DeepspeedJobDefaultRestartPolicy),
		},
	}
	for name, tc := range testCases {
		SetDefaults_DeepspeedJob(tc.original)
		if !reflect.DeepEqual(tc.original, tc.expected) {
			t.Errorf("%s: Want\n%v; Got\n %v", name, tc.expected, tc.original)
		}
	}
}
