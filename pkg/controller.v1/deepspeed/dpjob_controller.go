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

package deepspeed

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	kubeclientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"
	schedulerpluginsv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"

	kubeflowv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	trainingoperatorcommon "github.com/kubeflow/training-operator/pkg/common"
	"github.com/kubeflow/training-operator/pkg/common/util"
	ctlrconfig "github.com/kubeflow/training-operator/pkg/config"
	"github.com/kubeflow/training-operator/pkg/controller.v1/common"
	"github.com/kubeflow/training-operator/pkg/controller.v1/control"
	"github.com/kubeflow/training-operator/pkg/controller.v1/expectation"
	commonutil "github.com/kubeflow/training-operator/pkg/util"
)

const (
	FailedDeleteJobReason     = "FailedDeleteJob"
	SuccessfulDeleteJobReason = "SuccessfulDeleteJob"

	controllerName        = "Deepspeedjob-controller"
	labelDeepspeedJobName = "Deepspeed-job-name"
)

func NewReconciler(mgr manager.Manager, gangSchedulingSetupFunc common.GangSchedulingSetupFunc) *DeepspeedJobReconciler {
	r := &DeepspeedJobReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		recorder:  mgr.GetEventRecorderFor(controllerName),
		apiReader: mgr.GetAPIReader(),
		Log:       log.Log,
	}

	cfg := mgr.GetConfig()
	kubeClientSet := kubeclientset.NewForConfigOrDie(cfg)
	sharedInformers := informers.NewSharedInformerFactory(kubeClientSet, 0)
	priorityClassInformer := sharedInformers.Scheduling().V1().PriorityClasses()

	r.JobController = common.JobController{
		Controller:                  r,
		Expectations:                expectation.NewControllerExpectations(),
		WorkQueue:                   &util.FakeWorkQueue{},
		Recorder:                    r.recorder,
		KubeClientSet:               kubeClientSet,
		PriorityClassLister:         priorityClassInformer.Lister(),
		PriorityClassInformerSynced: priorityClassInformer.Informer().HasSynced,
		PodControl:                  control.RealPodControl{KubeClient: kubeClientSet, Recorder: r.recorder},
		ServiceControl:              control.RealServiceControl{KubeClient: kubeClientSet, Recorder: r.recorder},
	}

	gangSchedulingSetupFunc(&r.JobController)

	return r
}

// DeepspeedJobReconciler reconciles a DeepspeedJob object
type DeepspeedJobReconciler struct {
	common.JobController
	client.Client
	Scheme    *runtime.Scheme
	recorder  record.EventRecorder
	apiReader client.Reader
	Log       logr.Logger
}

//+kubebuilder:rbac:groups=kubeflow.org,resources=Deepspeedjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=kubeflow.org,resources=Deepspeedjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=kubeflow.org,resources=Deepspeedjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=get;list;watch;create
//+kubebuilder:rbac:groups="",resources=configmaps,verbs=list;watch;create;update
//+kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=roles,verbs=list;watch;create;update
//+kubebuilder:rbac:groups="rbac.authorization.k8s.io",resources=rolebindings,verbs=list;watch;create;update
//+kubebuilder:rbac:groups="",resources=pods/exec,verbs=create
//+kubebuilder:rbac:groups=scheduling.volcano.sh,resources=podgroups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=scheduling.x-k8s.io,resources=podgroups,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=events,verbs=get;list;watch;create;update;patch;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
func (jc *DeepspeedJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)
	logger := jc.Log.WithValues(kubeflowv1.DeepspeedJobSingular, req.NamespacedName)

	Deepspeedjob := &kubeflowv1.DeepspeedJob{}
	err := jc.Get(ctx, req.NamespacedName, Deepspeedjob)
	if err != nil {
		logger.Info("delete dp svc")
		jc.deleteWorkerHeadlessSVC(req.Name, Deepspeedjob.Namespace)
		logger.Info(err.Error(), "unable to fetch DeepspeedJob", req.NamespacedName.String())
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if err = kubeflowv1.ValidateV1DeepspeedJobSpec(&Deepspeedjob.Spec); err != nil {
		logger.Error(err, "DeepspeedJob failed validation")
		jc.Recorder.Eventf(Deepspeedjob, corev1.EventTypeWarning, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedValidationReason),
			"DeepspeedJob failed validation because %s", err)
		return ctrl.Result{}, err
	}

	// skip for DeepspeedJob that is being deleted
	if Deepspeedjob.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	// Set default priorities to DeepspeedJob
	jc.Scheme.Default(Deepspeedjob)

	// 1) validation rules out CleanPolicy with contradicting value
	// 2) if both fields leave empty, Default function fills with None
	// 3) if only one field set, sync value
	cleanPolicyDefined := Deepspeedjob.Spec.CleanPodPolicy
	if Deepspeedjob.Spec.RunPolicy.CleanPodPolicy != nil {
		cleanPolicyDefined = Deepspeedjob.Spec.RunPolicy.CleanPodPolicy
	}
	Deepspeedjob.Spec.CleanPodPolicy = cleanPolicyDefined
	Deepspeedjob.Spec.RunPolicy.CleanPodPolicy = cleanPolicyDefined

	// Use common to reconcile the job related pod and service
	// DeepspeedJob needs not service
	err = jc.ReconcileJobs(Deepspeedjob, Deepspeedjob.Spec.DeepspeedReplicaSpecs, Deepspeedjob.Status, &Deepspeedjob.Spec.RunPolicy)
	if err != nil {
		logrus.Warnf("Reconcile DeepspeedJob error %v", err)
		return ctrl.Result{}, err
	}

	t, err := util.DurationUntilExpireTime(&Deepspeedjob.Spec.RunPolicy, Deepspeedjob.Status)
	if err != nil {
		logrus.Warnf("Reconcile DeepspeedJob Job error %v", err)
		return ctrl.Result{}, err
	}
	if t >= 0 {
		return ctrl.Result{Requeue: true, RequeueAfter: t}, nil
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (jc *DeepspeedJobReconciler) SetupWithManager(mgr ctrl.Manager, controllerThreads int) error {
	c, err := controller.New(jc.ControllerName(), mgr, controller.Options{
		Reconciler:              jc,
		MaxConcurrentReconciles: controllerThreads,
	})
	if err != nil {
		return err
	}

	// using onOwnerCreateFunc is easier to set defaults
	if err = c.Watch(source.Kind(mgr.GetCache(), &kubeflowv1.DeepspeedJob{}), &handler.EnqueueRequestForObject{},
		predicate.Funcs{CreateFunc: jc.onOwnerCreateFunc()},
	); err != nil {
		return err
	}

	// eventHandler for owned objects
	eventHandler := handler.EnqueueRequestForOwner(mgr.GetScheme(), mgr.GetRESTMapper(), &kubeflowv1.DeepspeedJob{}, handler.OnlyControllerOwner())
	predicates := predicate.Funcs{
		CreateFunc: util.OnDependentCreateFunc(jc.Expectations),
		UpdateFunc: util.OnDependentUpdateFunc(&jc.JobController),
		DeleteFunc: util.OnDependentDeleteFunc(jc.Expectations),
	}
	// Create generic predicates
	genericPredicates := predicate.Funcs{
		CreateFunc: util.OnDependentCreateFuncGeneric(jc.Expectations),
		UpdateFunc: util.OnDependentUpdateFuncGeneric(&jc.JobController),
		DeleteFunc: util.OnDependentDeleteFuncGeneric(jc.Expectations),
	}
	// inject watching for job related pod
	if err = c.Watch(source.Kind(mgr.GetCache(), &corev1.Pod{}), eventHandler, predicates); err != nil {
		return err
	}
	// inject watching for job related ConfigMap
	if err = c.Watch(source.Kind(mgr.GetCache(), &corev1.ConfigMap{}), eventHandler, genericPredicates); err != nil {
		return err
	}
	// inject watching for job related Role
	if err = c.Watch(source.Kind(mgr.GetCache(), &rbacv1.Role{}), eventHandler, genericPredicates); err != nil {
		return err
	}
	// inject watching for job related RoleBinding
	if err = c.Watch(source.Kind(mgr.GetCache(), &rbacv1.RoleBinding{}), eventHandler, genericPredicates); err != nil {
		return err
	}
	// inject watching for job related ServiceAccount
	if err = c.Watch(source.Kind(mgr.GetCache(), &corev1.ServiceAccount{}), eventHandler, genericPredicates); err != nil {
		return err
	}
	// skip watching volcano PodGroup if volcano PodGroup is not installed
	if _, err = mgr.GetRESTMapper().RESTMapping(schema.GroupKind{Group: v1beta1.GroupName, Kind: "PodGroup"},
		v1beta1.SchemeGroupVersion.Version,
	); err == nil {
		// inject watching for job related volcano PodGroup
		if err = c.Watch(source.Kind(mgr.GetCache(), &v1beta1.PodGroup{}), eventHandler, genericPredicates); err != nil {
			return err
		}
	}
	// skip watching scheduler-plugins PodGroup if scheduler-plugins PodGroup is not installed
	if _, err = mgr.GetRESTMapper().RESTMapping(
		schema.GroupKind{Group: schedulerpluginsv1alpha1.SchemeGroupVersion.Group, Kind: "PodGroup"},
		schedulerpluginsv1alpha1.SchemeGroupVersion.Version,
	); err == nil {
		// inject watching for job related scheduler-plugins PodGroup
		if err = c.Watch(source.Kind(mgr.GetCache(), &schedulerpluginsv1alpha1.PodGroup{}), eventHandler, genericPredicates); err != nil {
			return err
		}
	}

	return nil
}

// ReconcileServices is overridden because Deepspeed-reconciler.v1 does not need to reconcile services
func (jc *DeepspeedJobReconciler) ReconcileServices(
	job metav1.Object,
	services []*corev1.Service,
	rtype kubeflowv1.ReplicaType,
	spec *kubeflowv1.ReplicaSpec) error {
	return nil
}

func (jc *DeepspeedJobReconciler) ControllerName() string {
	return controllerName
}

func (jc *DeepspeedJobReconciler) GetAPIGroupVersionKind() schema.GroupVersionKind {
	return kubeflowv1.GroupVersion.WithKind(kubeflowv1.DeepspeedJobKind)
}

func (jc *DeepspeedJobReconciler) GetAPIGroupVersion() schema.GroupVersion {
	return kubeflowv1.GroupVersion
}

func (jc *DeepspeedJobReconciler) GetGroupNameLabelValue() string {
	return kubeflowv1.GroupVersion.Group
}

func (jc *DeepspeedJobReconciler) GetFrameworkName() string {
	return kubeflowv1.DeepspeedJobFrameworkName
}

// SetClusterSpec is overridden because no cluster spec is needed for DeepspeedJob
func (jc *DeepspeedJobReconciler) SetClusterSpec(job interface{}, podTemplate *corev1.PodTemplateSpec, rtype, index string) error {
	return nil
}

func (jc *DeepspeedJobReconciler) GetDefaultContainerName() string {
	return kubeflowv1.DeepspeedJobDefaultContainerName
}

func (jc *DeepspeedJobReconciler) GetDefaultContainerPortName() string {
	return kubeflowv1.DeepspeedJobDefaultPortName
}

func (jc *DeepspeedJobReconciler) IsMasterRole(replicas map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaSpec,
	rtype kubeflowv1.ReplicaType, index int) bool {
	return string(rtype) == string(kubeflowv1.DeepspeedJobReplicaTypeLauncher)
}

func (jc *DeepspeedJobReconciler) GetJobFromInformerCache(namespace, name string) (metav1.Object, error) {
	Deepspeedjob := &kubeflowv1.DeepspeedJob{}
	err := jc.Get(context.Background(), types.NamespacedName{
		Namespace: namespace, Name: name,
	}, Deepspeedjob)
	return Deepspeedjob, err
}

// onOwnerCreateFunc modify creation condition.
func (jc *DeepspeedJobReconciler) onOwnerCreateFunc() func(event.CreateEvent) bool {
	return func(e event.CreateEvent) bool {
		DeepspeedJob, ok := e.Object.(*kubeflowv1.DeepspeedJob)
		if !ok {
			return true
		}

		jc.Scheme.Default(DeepspeedJob)
		msg := fmt.Sprintf("DeepspeedJob %s is created.", e.Object.GetName())
		logrus.Info(msg)
		trainingoperatorcommon.CreatedJobsCounterInc(DeepspeedJob.Namespace, jc.GetFrameworkName())
		commonutil.UpdateJobConditions(&DeepspeedJob.Status, kubeflowv1.JobCreated, corev1.ConditionTrue, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobCreatedReason), msg)
		return true
	}
}

func (jc *DeepspeedJobReconciler) ReconcilePods(
	job interface{},
	jobStatus *kubeflowv1.JobStatus,
	pods []*corev1.Pod,
	rtype kubeflowv1.ReplicaType,
	spec *kubeflowv1.ReplicaSpec,
	replicas map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaSpec,
) error {

	DeepspeedJob, ok := job.(*kubeflowv1.DeepspeedJob)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepspeedJob", DeepspeedJob)
	}

	// first set StartTime.
	if jobStatus.StartTime == nil {
		now := metav1.Now()
		jobStatus.StartTime = &now
	}

	initializeReplicaStatuses(jobStatus, rtype)

	// Get the launcher Job for this DeepspeedJob.
	launcher, err := jc.getLauncherJob(DeepspeedJob)
	if err != nil {
		return err
	}

	var worker []*corev1.Pod
	// We're done if the launcher either succeeded or failed.
	done := launcher != nil && isPodFinished(launcher)

	if !done {
		workerSpec := DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeWorker]
		workerReplicas := int32(0)
		if workerSpec != nil && workerSpec.Replicas != nil {
			workerReplicas = *workerSpec.Replicas
		}
		isGPULauncher := isGPULauncher(DeepspeedJob)

		// Get the launcher ServiceAccount for this DeepspeedJob.
		if sa, err := jc.getOrCreateLauncherServiceAccount(DeepspeedJob); sa == nil || err != nil {
			return err
		}

		// Get the ConfigMap for this DeepspeedJob.
		if config, err := jc.getOrCreateConfigMap(DeepspeedJob, workerReplicas, isGPULauncher); config == nil || err != nil {
			return err
		}

		// Get the launcher Role for this DeepspeedJob.
		if r, err := jc.getOrCreateLauncherRole(DeepspeedJob, workerReplicas); r == nil || err != nil {
			return err
		}

		// Get the launcher RoleBinding for this DeepspeedJob.
		if rb, err := jc.getLauncherRoleBinding(DeepspeedJob); rb == nil || err != nil {
			return err
		}

		worker, err = jc.getOrCreateWorker(DeepspeedJob)
		if err != nil {
			return err
		}

		if launcher == nil {
			launcher, err = jc.KubeClientSet.CoreV1().Pods(DeepspeedJob.Namespace).Create(context.Background(), jc.newLauncher(DeepspeedJob, ctlrconfig.Config.DeepspeedKubectlDeliveryImage, isGPULauncher), metav1.CreateOptions{})
			if err != nil {
				jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeWarning, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedReason), "launcher pod created failed: %v", err)
				return err
			} else {
				jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRunningReason), "launcher pod created success: %v", launcher.Name)
			}
		}
	}

	// Finally, we update the status block of the DeepspeedJob resource to reflect the
	// current state of the world.
	err = jc.updateDeepspeedJobStatus(DeepspeedJob, launcher, worker)
	if err != nil {
		return err
	}
	return nil
}

func (jc *DeepspeedJobReconciler) updateDeepspeedJobStatus(DeepspeedJob *kubeflowv1.DeepspeedJob, launcher *corev1.Pod, worker []*corev1.Pod) error {
	if launcher != nil {
		initializeDeepspeedJobStatuses(DeepspeedJob, kubeflowv1.DeepspeedJobReplicaTypeLauncher)
		if isPodSucceeded(launcher) {
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Succeeded = 1
			msg := fmt.Sprintf("DeepspeedJob %s/%s successfully completed.", DeepspeedJob.Namespace, DeepspeedJob.Name)
			jc.Recorder.Event(DeepspeedJob, corev1.EventTypeNormal, commonutil.NewReason(kubeflowv1.DeepspeedJobPlural, commonutil.JobSucceededReason), msg)
			if DeepspeedJob.Status.CompletionTime == nil {
				now := metav1.Now()
				DeepspeedJob.Status.CompletionTime = &now
			}
			logrus.Info("job finish delete svc")
			jc.deleteWorkerHeadlessSVC(DeepspeedJob.Name, DeepspeedJob.Namespace)
			err := updateDeepspeedJobConditions(DeepspeedJob, kubeflowv1.JobSucceeded, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobSucceededReason), msg)
			if err != nil {
				return err
			}
		} else if isPodFailed(launcher) {
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Failed = 1
			msg := fmt.Sprintf("DeepspeedJob %s/%s has failed", DeepspeedJob.Namespace, DeepspeedJob.Name)
			logrus.Info("job failed delete svc")
			jc.deleteWorkerHeadlessSVC(DeepspeedJob.Name, DeepspeedJob.Namespace)
			reason := launcher.Status.Reason
			if reason == "" {
				reason = commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedReason)
			}
			jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, reason, msg)
			if reason == "Evicted" {
				reason = DeepspeedJobEvict
			} else if !isEvicted(DeepspeedJob.Status) && DeepspeedJob.Status.CompletionTime == nil {
				now := metav1.Now()
				DeepspeedJob.Status.CompletionTime = &now
			}
			err := updateDeepspeedJobConditions(DeepspeedJob, kubeflowv1.JobFailed, reason, msg)
			if err != nil {
				klog.Errorf("Append DeepspeedJob(%s/%s) condition error: %v", DeepspeedJob.Namespace, DeepspeedJob.Name, err)
				return err
			}

		} else if isPodRunning(launcher) {
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Active = 1
		}
	}

	var (
		running = 0
		evict   = 0
	)

	initializeDeepspeedJobStatuses(DeepspeedJob, kubeflowv1.DeepspeedJobReplicaTypeWorker)
	for i := 0; i < len(worker); i++ {
		switch worker[i].Status.Phase {
		case corev1.PodFailed:
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeWorker].Failed += 1
			if worker[i].Status.Reason == "Evicted" {
				evict += 1
			}
		case corev1.PodSucceeded:
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeWorker].Succeeded += 1
		case corev1.PodRunning:
			running += 1
			DeepspeedJob.Status.ReplicaStatuses[kubeflowv1.DeepspeedJobReplicaTypeWorker].Active += 1
		}
	}
	if evict > 0 {
		msg := fmt.Sprintf("%d/%d workers are evicted", evict, len(worker))
		if err := updateDeepspeedJobConditions(DeepspeedJob, kubeflowv1.JobFailed, DeepspeedJobEvict, msg); err != nil {
			return err
		}
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, DeepspeedJobEvict, msg)
	}

	if launcher != nil && launcher.Status.Phase == corev1.PodRunning && running == len(worker) {
		msg := fmt.Sprintf("DeepspeedJob %s/%s is running.", DeepspeedJob.Namespace, DeepspeedJob.Name)
		err := updateDeepspeedJobConditions(DeepspeedJob, kubeflowv1.JobRunning, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRunningReason), msg)
		if err != nil {
			return err
		}
		jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRunningReason), "DeepspeedJob %s/%s is running", DeepspeedJob.Namespace, DeepspeedJob.Name)
	}
	return nil
}

func (jc *DeepspeedJobReconciler) GetJobFromAPIClient(namespace, name string) (metav1.Object, error) {
	job := &kubeflowv1.DeepspeedJob{}

	err := jc.apiReader.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, job)
	if err != nil {
		if errors.IsNotFound(err) {
			logrus.Error(err, "DeepspeedJob not found", "namespace", namespace, "name", name)
		} else {
			logrus.Error(err, "failed to get job from api-server", "namespace", namespace, "name", name)
		}
		return nil, err
	}
	return job, nil
}

// GetPodsForJob returns the set of pods that this job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned Pods are pointers into the cache.
func (jc *DeepspeedJobReconciler) GetPodsForJob(jobObject interface{}) ([]*corev1.Pod, error) {
	job, ok := jobObject.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("job is not of type metav1.Object")
	}

	// Create selector.
	selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
		MatchLabels: jc.GenLabels(job.GetName()),
	})

	if err != nil {
		return nil, fmt.Errorf("couldn't convert Job selector: %v", err)
	}
	// List all pods to include those that don't match the selector anymore
	// but have a ControllerRef pointing to this controller.
	podlist := &corev1.PodList{}
	err = jc.List(context.Background(), podlist,
		client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(job.GetNamespace()))
	if err != nil {
		return nil, err
	}

	return util.JobControlledPodList(podlist.Items, job), nil
}

func (jc *DeepspeedJobReconciler) DeleteJob(job interface{}) error {
	DeepspeedJob, ok := job.(*kubeflowv1.DeepspeedJob)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepspeedJob", DeepspeedJob)
	}

	log := commonutil.LoggerForJob(DeepspeedJob)
	if err := jc.Delete(context.Background(), DeepspeedJob); err != nil {
		jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeWarning, FailedDeleteJobReason, "Error deleting: %v", err)
		log.Errorf("failed to delete job %s/%s, %v", DeepspeedJob.Namespace, DeepspeedJob.Name, err)
		return err
	}
	jc.deleteWorkerHeadlessSVC(DeepspeedJob.Name, DeepspeedJob.Namespace)
	jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, SuccessfulDeleteJobReason, "Deleted job: %v", DeepspeedJob.Name)
	log.Infof("job %s/%s has been deleted", DeepspeedJob.Namespace, DeepspeedJob.Name)
	trainingoperatorcommon.DeletedJobsCounterInc(DeepspeedJob.Namespace, jc.GetFrameworkName())
	return nil
}

// GetServicesForJob returns the set of services that this job should manage.
// It also reconciles ControllerRef by adopting/orphaning.
// Note that the returned services are pointers into the cache.
func (jc *DeepspeedJobReconciler) GetServicesForJob(jobObject interface{}) ([]*corev1.Service, error) {
	return nil, nil
}

func (jc *DeepspeedJobReconciler) UpdateJobStatus(job interface{}, replicas map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaSpec, jobStatus *kubeflowv1.JobStatus) error {
	DeepspeedJob, ok := job.(*kubeflowv1.DeepspeedJob)
	if !ok {
		return fmt.Errorf("%+v is not a type of DeepspeedJob", job)
	}

	for rtype, spec := range replicas {
		fmt.Println(rtype)
		fmt.Printf("job status %v \n", jobStatus.ReplicaStatuses)
		fmt.Println(jobStatus.ReplicaStatuses[rtype])
		status := jobStatus.ReplicaStatuses[rtype]

		succeeded := status.Succeeded
		expected := *(spec.Replicas) - succeeded
		running := status.Active
		failed := status.Failed

		logrus.Infof("DeepspeedJob=%s, ReplicaType=%s expected=%d, running=%d, succeeded=%d , failed=%d",
			DeepspeedJob.Name, rtype, expected, running, succeeded, failed)

		if rtype == kubeflowv1.DeepspeedJobReplicaTypeLauncher {
			if running > 0 {
				msg := fmt.Sprintf("DeepspeedJob %s is running.", DeepspeedJob.Name)
				commonutil.UpdateJobConditions(jobStatus, kubeflowv1.JobRunning, corev1.ConditionTrue, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRunningReason), msg)
			}
			// when launcher is succeed, the job is finished.
			if expected == 0 {
				msg := fmt.Sprintf("DeepspeedJob %s is successfully completed.", DeepspeedJob.Name)
				logrus.Info(msg)
				jc.Recorder.Event(DeepspeedJob, corev1.EventTypeNormal, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobSucceededReason), msg)
				if jobStatus.CompletionTime == nil {
					now := metav1.Now()
					jobStatus.CompletionTime = &now
				}
				commonutil.UpdateJobConditions(jobStatus, kubeflowv1.JobSucceeded, corev1.ConditionTrue, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobSucceededReason), msg)
				trainingoperatorcommon.SuccessfulJobsCounterInc(DeepspeedJob.Namespace, jc.GetFrameworkName())
				return nil
			}
		}
		if failed > 0 {
			if spec.RestartPolicy == kubeflowv1.RestartPolicyExitCode {
				msg := fmt.Sprintf("DeepspeedJob %s is restarting because %d %s replica(s) failed.", DeepspeedJob.Name, failed, rtype)
				jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRestartingReason), msg)
				commonutil.UpdateJobConditions(jobStatus, kubeflowv1.JobRestarting, corev1.ConditionTrue, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobRestartingReason), msg)
				trainingoperatorcommon.RestartedJobsCounterInc(DeepspeedJob.Namespace, jc.GetFrameworkName())
			} else {
				msg := fmt.Sprintf("DeepspeedJob %s is failed because %d %s replica(s) failed.", DeepspeedJob.Name, failed, rtype)
				jc.Recorder.Event(DeepspeedJob, corev1.EventTypeNormal, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedReason), msg)
				if jobStatus.CompletionTime == nil {
					now := metav1.Now()
					jobStatus.CompletionTime = &now
				}
				commonutil.UpdateJobConditions(jobStatus, kubeflowv1.JobFailed, corev1.ConditionTrue, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedReason)), msg)
				trainingoperatorcommon.FailedJobsCounterInc(DeepspeedJob.Namespace, jc.GetFrameworkName())
			}
		}
	}
	DeepspeedJob.Status = *jobStatus.DeepCopy()
	return nil
}

func (jc *DeepspeedJobReconciler) UpdateJobStatusInApiServer(job interface{}, jobStatus *kubeflowv1.JobStatus) error {
	if jobStatus.ReplicaStatuses == nil {
		jobStatus.ReplicaStatuses = map[kubeflowv1.ReplicaType]*kubeflowv1.ReplicaStatus{}
	}

	DeepspeedJob, ok := job.(*kubeflowv1.DeepspeedJob)
	trainingoperatorcommon.ClearGeneratedFields(&DeepspeedJob.ObjectMeta)
	if !ok {
		return fmt.Errorf("%v is not a type of DeepspeedJob", DeepspeedJob)
	}

	startTime := time.Now()
	logger := commonutil.LoggerForJob(DeepspeedJob)
	defer func() {
		logger.Infof("Finished updating DeepspeedJobs Status %q (%v)",
			DeepspeedJob.Name, time.Since(startTime))
	}()

	DeepspeedJob = DeepspeedJob.DeepCopy()
	DeepspeedJob.Status = *jobStatus.DeepCopy()

	result := jc.Status().Update(context.Background(), DeepspeedJob)

	if result != nil {
		jc.Log.WithValues("Deepspeedjob", types.NamespacedName{
			Namespace: DeepspeedJob.GetNamespace(),
			Name:      DeepspeedJob.GetName(),
		})
		return result
	}

	return nil
}

// getLauncherJob gets the launcher Job controlled by this DeepspeedJob.
func (jc *DeepspeedJobReconciler) getLauncherJob(DeepspeedJob *kubeflowv1.DeepspeedJob) (*corev1.Pod, error) {
	launcher := &corev1.Pod{}
	NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: DeepspeedJob.Name + launcherSuffix}
	err := jc.Get(context.Background(), NamespacedName, launcher)
	if errors.IsNotFound(err) {
		return nil, nil
	}
	if err != nil {
		// If an error occurs during Get, we'll requeue the item so we can
		// attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		return nil, err
	}

	// If the launcher is not controlled by this DeepspeedJob resource, we should log
	// a warning to the event recorder and return.
	if !metav1.IsControlledBy(launcher, DeepspeedJob) {
		msg := fmt.Sprintf(MessageResourceExists, launcher.Name, launcher.Kind)
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return launcher, fmt.Errorf(msg)
	}
	return launcher, nil
}

// getOrCreateConfigMap gets the ConfigMap controlled by this DeepspeedJob, or creates
// one if it doesn't exist.
func (jc *DeepspeedJobReconciler) getOrCreateConfigMap(DeepspeedJob *kubeflowv1.DeepspeedJob, workerReplicas int32, isGPULauncher bool) (*corev1.ConfigMap, error) {
	newCM := newConfigMap(DeepspeedJob, workerReplicas, isGPULauncher)
	podList, err := jc.getRunningWorkerPods(DeepspeedJob)
	if err != nil {
		return nil, err
	}
	updateDiscoverHostsInConfigMap(newCM, DeepspeedJob, podList, isGPULauncher)
	jc.createWorkerHeadlessSVC(podList, DeepspeedJob.Name)
	cm := &corev1.ConfigMap{}
	NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: DeepspeedJob.Name + configSuffix}
	err = jc.Get(context.Background(), NamespacedName, cm)

	// If the ConfigMap doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		cm, err = jc.KubeClientSet.CoreV1().ConfigMaps(DeepspeedJob.Namespace).Create(context.Background(), newCM, metav1.CreateOptions{})
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}

	// If the ConfigMap is not controlled by this DeepspeedJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(cm, DeepspeedJob) {
		msg := fmt.Sprintf(MessageResourceExists, cm.Name, cm.Kind)
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	// If the ConfigMap is changed, update it
	if !reflect.DeepEqual(cm.Data, newCM.Data) {
		cm, err = jc.KubeClientSet.CoreV1().ConfigMaps(DeepspeedJob.Namespace).Update(context.Background(), newCM, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return cm, nil
}

// getOrCreateLauncherServiceAccount gets the launcher ServiceAccount controlled
// by this DeepspeedJob, or creates one if it doesn't exist.
func (jc *DeepspeedJobReconciler) getOrCreateLauncherServiceAccount(DeepspeedJob *kubeflowv1.DeepspeedJob) (*corev1.ServiceAccount, error) {
	saName := DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName

	if len(saName) == 0 {
		saName = DeepspeedJob.Name + launcherSuffix
	}

	sa := &corev1.ServiceAccount{}
	NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: saName}
	err := jc.Get(context.Background(), NamespacedName, sa)

	if err == nil {
		jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, "ServiceAccount is exist", "ServiceAccount: %v", sa.Name)
	}

	if errors.IsNotFound(err) {
		sa, err = jc.KubeClientSet.CoreV1().ServiceAccounts(DeepspeedJob.Namespace).Create(context.Background(), newLauncherServiceAccount(DeepspeedJob), metav1.CreateOptions{})
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}

	return sa, nil
}

// getOrCreateLauncherRole gets the launcher Role controlled by this DeepspeedJob.
func (jc *DeepspeedJobReconciler) getOrCreateLauncherRole(DeepspeedJob *kubeflowv1.DeepspeedJob, workerReplicas int32) (*rbacv1.Role, error) {
	role := &rbacv1.Role{}
	NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: DeepspeedJob.Name + launcherSuffix}
	err := jc.Get(context.Background(), NamespacedName, role)

	if err == nil {
		jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, "LauncherRole is exist", "LauncherRole: %v", role.Name)
	}

	launcherRole := newLauncherRole(DeepspeedJob, workerReplicas)
	// If the Role doesn't exist, we'll create it.
	if errors.IsNotFound(err) {
		role, err = jc.KubeClientSet.RbacV1().Roles(DeepspeedJob.Namespace).Create(context.Background(), launcherRole, metav1.CreateOptions{})
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}
	// If the launcher Role is not controlled by this DeepspeedJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(role, DeepspeedJob) {
		msg := fmt.Sprintf(MessageResourceExists, role.Name, role.Kind)
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	if !reflect.DeepEqual(role.Rules, launcherRole.Rules) {
		role, err = jc.KubeClientSet.RbacV1().Roles(DeepspeedJob.Namespace).Update(context.Background(), launcherRole, metav1.UpdateOptions{})
		if err != nil {
			return nil, err
		}
	}

	return role, nil
}

// getLauncherRoleBinding gets the launcher RoleBinding controlled by this
// DeepspeedJob, or creates one if it doesn't exist.
func (jc *DeepspeedJobReconciler) getLauncherRoleBinding(DeepspeedJob *kubeflowv1.DeepspeedJob) (*rbacv1.RoleBinding, error) {
	rb := &rbacv1.RoleBinding{}
	NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: DeepspeedJob.Name + launcherSuffix}
	err := jc.Get(context.Background(), NamespacedName, rb)
	// If the RoleBinding doesn't exist, we'll create it.

	if err == nil {
		jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, "RoleBinding is exist", "RoleBinding: %v", rb.Name)
	}

	if errors.IsNotFound(err) {
		rb, err = jc.KubeClientSet.RbacV1().RoleBindings(DeepspeedJob.Namespace).Create(context.Background(), newLauncherRoleBinding(DeepspeedJob), metav1.CreateOptions{})
	}
	// If an error occurs during Get/Create, we'll requeue the item so we
	// can attempt processing again later. This could have been caused by a
	// temporary network failure, or any other transient reason.
	if err != nil {
		return nil, err
	}
	// If the launcher RoleBinding is not controlled by this DeepspeedJob resource, we
	// should log a warning to the event recorder and return.
	if !metav1.IsControlledBy(rb, DeepspeedJob) {
		msg := fmt.Sprintf(MessageResourceExists, rb.Name, rb.Kind)
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceExists, msg)
		return nil, fmt.Errorf(msg)
	}

	return rb, nil
}

// getOrCreateWorker gets the worker Pod controlled by this
// DeepspeedJob, or creates one if it doesn't exist.
func (jc *DeepspeedJobReconciler) getOrCreateWorker(DeepspeedJob *kubeflowv1.DeepspeedJob) ([]*corev1.Pod, error) {
	var (
		workerPrefix   string        = DeepspeedJob.Name + workerSuffix
		workerPods     []*corev1.Pod = []*corev1.Pod{}
		i              int32         = 0
		workerReplicas *int32
	)
	if worker, ok := DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeWorker]; ok && worker != nil {
		workerReplicas = worker.Replicas
	} else {
		return workerPods, nil
	}

	// Remove Pods when replicas are scaled down
	genericLabels := jc.GenLabels(DeepspeedJob.GetName())
	selector, err := workerSelector(genericLabels)
	if err != nil {
		return nil, err
	}

	podlist := &corev1.PodList{}
	err = jc.List(context.Background(), podlist, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(DeepspeedJob.GetNamespace()))
	if err != nil {
		return nil, err
	}
	if len(podlist.Items) > int(*workerReplicas) {
		for _, pod := range podlist.Items {
			indexStr, ok := pod.Labels[kubeflowv1.ReplicaIndexLabel]
			if !ok {
				return nil, err
			}
			index, err := strconv.Atoi(indexStr)
			if err == nil {
				if index >= int(*workerReplicas) {
					err = jc.KubeClientSet.CoreV1().Pods(pod.Namespace).Delete(context.Background(), pod.Name, metav1.DeleteOptions{})
					if err != nil {
						return nil, err
					}
				}
			}
		}
	}

	for ; i < *workerReplicas; i++ {
		name := fmt.Sprintf("%s-%d", workerPrefix, i)

		pod := &corev1.Pod{}
		NamespacedName := types.NamespacedName{Namespace: DeepspeedJob.Namespace, Name: name}
		err := jc.Get(context.Background(), NamespacedName, pod)

		// If the worker Pod doesn't exist, we'll create it.
		if errors.IsNotFound(err) {
			worker := jc.newWorker(DeepspeedJob, name)
			if worker == nil {
				msg := fmt.Sprintf(MessageResourceDoesNotExist, "Worker")
				jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceDoesNotExist, msg)
				err = fmt.Errorf(msg)
				return nil, err
			}
			// Insert ReplicaIndexLabel
			worker.Labels[kubeflowv1.ReplicaIndexLabel] = strconv.Itoa(int(i))
			worker.Labels["dp-worker"] = name
			pod, err = jc.KubeClientSet.CoreV1().Pods(DeepspeedJob.Namespace).Create(context.Background(), worker, metav1.CreateOptions{})
			if err == nil {
				jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeNormal, "SuccessfulCreatePod", "Created worker pod: %v", pod.Name)
			} else {
				jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeWarning, "FailedCreatePod", "Created worker pod: %v", pod.Name)
			}
		}

		// If an error occurs during Get/Create, we'll requeue the item so we
		// can attempt processing again later. This could have been caused by a
		// temporary network failure, or any other transient reason.
		if err != nil && !errors.IsNotFound(err) {
			jc.Recorder.Eventf(DeepspeedJob, corev1.EventTypeWarning, commonutil.NewReason(kubeflowv1.DeepspeedJobKind, commonutil.JobFailedReason),
				"worker pod created failed: %v", err)
			return nil, err
		}
		// If the worker is not controlled by this DeepspeedJob resource, we should log
		// a warning to the event recorder and return.
		if pod != nil && !metav1.IsControlledBy(pod, DeepspeedJob) {
			msg := fmt.Sprintf(MessageResourceExists, pod.Name, pod.Kind)
			jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceExists, msg)
			return nil, fmt.Errorf(msg)
		}
		workerPods = append(workerPods, pod)
	}

	return workerPods, nil
}

// newWorker creates a new worker Pod for an DeepspeedJob resource. It also
// sets the appropriate OwnerReferences on the resource so handleObject can
// discover the DeepspeedJob resource that 'owns' it.
func (jc *DeepspeedJobReconciler) newWorker(DeepspeedJob *kubeflowv1.DeepspeedJob, name string) *corev1.Pod {
	genericLabels := jc.GenLabels(DeepspeedJob.GetName())
	labels := defaultWorkerLabels(genericLabels)

	podSpec := DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeWorker].Template.DeepCopy()

	// keep the labels which are set in PodTemplate
	if len(podSpec.Labels) == 0 {
		podSpec.Labels = make(map[string]string)
	}

	for key, value := range labels {
		podSpec.Labels[key] = value
	}
	setRestartPolicy(podSpec, DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeWorker])
	logger := commonutil.LoggerForReplica(DeepspeedJob, strings.ToLower(string(kubeflowv1.DeepspeedJobReplicaTypeLauncher)))
	if len(podSpec.Spec.Containers) == 0 {
		klog.Errorln("Worker pod does not have any containers in its spec")
		return nil
	}
	container := podSpec.Spec.Containers[0]
	if len(container.Command) == 0 {
		container.Command = []string{"sleep"}
		container.Args = []string{"365d"}
	}

	// We need the kubexec.sh script here because Open Deepspeed checks for the path
	// in every rank.
	container.VolumeMounts = append(container.VolumeMounts, corev1.VolumeMount{
		Name:      configVolumeName,
		MountPath: configMountPath,
	})
	podSpec.Spec.Containers[0] = container

	scriptMode := int32(0555)
	podSpec.Spec.Volumes = append(podSpec.Spec.Volumes, corev1.Volume{
		Name: configVolumeName,
		VolumeSource: corev1.VolumeSource{
			ConfigMap: &corev1.ConfigMapVolumeSource{
				LocalObjectReference: corev1.LocalObjectReference{
					Name: DeepspeedJob.Name + configSuffix,
				},
				Items: []corev1.KeyToPath{
					{
						Key:  kubexecScriptName,
						Path: kubexecScriptName,
						Mode: &scriptMode,
					},
				},
			},
		},
	})

	// if gang-scheduling is enabled:
	// 1. if user has specified other scheduler, we report a warning without overriding any fields.
	// 2. if no SchedulerName is set for pods, then we set the SchedulerName to "volcano".
	if jc.Config.EnableGangScheduling() {
		if !util.IsGangSchedulerSet(DeepspeedJob.Spec.DeepspeedReplicaSpecs, jc.PodGroupControl.GetSchedulerName()) {
			errMsg := "Another scheduler is specified when gang-scheduling is enabled and it will not be overwritten"
			logger.Warning(errMsg)
			jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, podTemplateSchedulerNameReason, errMsg)
		}

		rtWorker := strings.ToLower(string(kubeflowv1.DeepspeedJobReplicaTypeWorker))
		jc.PodGroupControl.DecoratePodTemplateSpec(podSpec, DeepspeedJob, rtWorker)
	}

	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        name,
			Namespace:   DeepspeedJob.Namespace,
			Labels:      podSpec.Labels,
			Annotations: podSpec.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
		Spec: podSpec.Spec,
	}
}

// newLauncher creates a new launcher Job for an DeepspeedJob resource. It also sets
// the appropriate OwnerReferences on the resource so handleObject can discover
// the DeepspeedJob resource that 'owns' it.
func (jc *DeepspeedJobReconciler) newLauncher(DeepspeedJob *kubeflowv1.DeepspeedJob, kubectlDeliveryImage string, isGPULauncher bool) *corev1.Pod {
	launcherName := DeepspeedJob.Name + launcherSuffix

	genericLabels := jc.GenLabels(DeepspeedJob.GetName())
	labels := defaultLauncherLabels(genericLabels)

	masterRole := jc.IsMasterRole(DeepspeedJob.Spec.DeepspeedReplicaSpecs, kubeflowv1.DeepspeedJobReplicaTypeLauncher, 0)
	if masterRole {
		labels[kubeflowv1.JobRoleLabel] = "master"
	}
	podSpec := DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.DeepCopy()
	// copy the labels and annotations to pod from PodTemplate
	if len(podSpec.Labels) == 0 {
		podSpec.Labels = make(map[string]string)
	}
	for key, value := range labels {
		podSpec.Labels[key] = value
	}

	logger := commonutil.LoggerForReplica(DeepspeedJob, strings.ToLower(string(kubeflowv1.DeepspeedJobReplicaTypeLauncher)))
	// add SchedulerName to podSpec
	if jc.Config.EnableGangScheduling() {
		if !util.IsGangSchedulerSet(DeepspeedJob.Spec.DeepspeedReplicaSpecs, jc.PodGroupControl.GetSchedulerName()) {
			errMsg := "Another scheduler is specified when gang-scheduling is enabled and it will not be overwritten"
			logger.Warning(errMsg)
			jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, podTemplateSchedulerNameReason, errMsg)
		}

		rt := strings.ToLower(string(kubeflowv1.DeepspeedJobReplicaTypeLauncher))
		jc.PodGroupControl.DecoratePodTemplateSpec(podSpec, DeepspeedJob, rt)
	}

	if len(DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName) == 0 {
		podSpec.Spec.ServiceAccountName = launcherName
	}

	podSpec.Spec.InitContainers = append(podSpec.Spec.InitContainers, corev1.Container{
		Name:            kubectlDeliveryName,
		Image:           kubectlDeliveryImage,
		ImagePullPolicy: corev1.PullIfNotPresent,
		Env: []corev1.EnvVar{
			{
				Name:  kubectlTargetDirEnv,
				Value: kubectlMountPath,
			},
			{
				Name:  "NAMESPACE",
				Value: DeepspeedJob.Namespace,
			},
		},
		VolumeMounts: []corev1.VolumeMount{
			{
				Name:      kubectlVolumeName,
				MountPath: kubectlMountPath,
			},
			{
				Name:      configVolumeName,
				MountPath: configMountPath,
			},
		},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
				corev1.ResourceMemory:           resource.MustParse(initContainerMem),
				corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:              resource.MustParse(initContainerCpu),
				corev1.ResourceMemory:           resource.MustParse(initContainerMem),
				corev1.ResourceEphemeralStorage: resource.MustParse(initContainerEphStorage),
			},
		},
		Command: []string{"/etc/deepspeed/update_hosts.sh"},
	})
	if len(podSpec.Spec.Containers) == 0 {
		klog.Errorln("Launcher pod does not have any containers in its spec")
		msg := fmt.Sprintf(MessageResourceDoesNotExist, "Launcher")
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, ErrResourceDoesNotExist, msg)
		return nil
	}
	container := podSpec.Spec.Containers[0]
	container.Env = append(container.Env,
		corev1.EnvVar{
			Name:  "ODeepspeed_MCA_plm_rsh_agent",
			Value: fmt.Sprintf("%s/%s", configMountPath, kubexecScriptName),
		},
		corev1.EnvVar{
			Name:  "ODeepspeed_MCA_orte_default_hostfile",
			Value: fmt.Sprintf("%s/%s", configMountPath, hostfileName),
		},
	)

	if !isGPULauncher {
		container.Env = append(container.Env,
			// We overwrite these environment variables so that users will not
			// be mistakenly using GPU resources for launcher due to potential
			// issues with scheduler/container technologies.
			corev1.EnvVar{
				Name:  "NVIDIA_VISIBLE_DEVICES",
				Value: "",
			},
			corev1.EnvVar{
				Name:  "NVIDIA_DRIVER_CAPABILITIES",
				Value: "",
			})
	}

	// Add default Intel Deepspeed bootstrap variables if not provided by the user.
	bootstrap, exec := hasIntelDeepspeedBootstrapValues(container.Env)
	if !bootstrap {
		container.Env = append(container.Env,
			corev1.EnvVar{
				Name:  "I_Deepspeed_HYDRA_BOOTSTRAP",
				Value: iDeepspeedDefaultBootstrap,
			},
		)
	}
	if !exec {
		container.Env = append(container.Env,
			corev1.EnvVar{
				Name:  "I_Deepspeed_HYDRA_BOOTSTRAP_EXEC",
				Value: fmt.Sprintf("%s/%s", configMountPath, kubexecScriptName),
			},
		)
	}

	container.VolumeMounts = append(container.VolumeMounts,
		corev1.VolumeMount{
			Name:      kubectlVolumeName,
			MountPath: kubectlMountPath,
		},
		corev1.VolumeMount{
			Name:      configVolumeName,
			MountPath: configMountPath,
		})
	podSpec.Spec.Containers[0] = container

	// Submit a warning event if the user specifies restart policy for
	// the pod template. We recommend to set it from the replica level.
	if podSpec.Spec.RestartPolicy != corev1.RestartPolicy("") {
		errMsg := "Restart policy in pod template will be overwritten by restart policy in replica spec"
		klog.Warning(errMsg)
		jc.Recorder.Event(DeepspeedJob, corev1.EventTypeWarning, podTemplateRestartPolicyReason, errMsg)
	}
	setRestartPolicy(podSpec, DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher])

	scriptsMode := int32(0555)
	hostfileMode := int32(0444)
	podSpec.Spec.Volumes = append(podSpec.Spec.Volumes,
		corev1.Volume{
			Name: kubectlVolumeName,
			VolumeSource: corev1.VolumeSource{
				EmptyDir: &corev1.EmptyDirVolumeSource{},
			},
		},
		corev1.Volume{
			Name: configVolumeName,
			VolumeSource: corev1.VolumeSource{
				ConfigMap: &corev1.ConfigMapVolumeSource{
					LocalObjectReference: corev1.LocalObjectReference{
						Name: DeepspeedJob.Name + configSuffix,
					},
					Items: []corev1.KeyToPath{
						{
							Key:  kubexecScriptName,
							Path: kubexecScriptName,
							Mode: &scriptsMode,
						},
						{
							Key:  hostfileName,
							Path: hostfileName,
							Mode: &hostfileMode,
						},
						{
							Key:  discoverHostsScriptName,
							Path: discoverHostsScriptName,
							Mode: &scriptsMode,
						},
						{
							Key:  "update_hosts.sh",
							Path: "update_hosts.sh",
							Mode: &scriptsMode,
						},
					},
				},
			},
		})
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        launcherName,
			Namespace:   DeepspeedJob.Namespace,
			Labels:      podSpec.Labels,
			Annotations: podSpec.Annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
		Spec: podSpec.Spec,
	}
}

// getRunningWorkerPods get all worker Pods with Running phase controlled by this DeepspeedJob.
func (jc *DeepspeedJobReconciler) getRunningWorkerPods(DeepspeedJob *kubeflowv1.DeepspeedJob) ([]*corev1.Pod, error) {
	genericLabels := jc.GenLabels(DeepspeedJob.GetName())
	selector, err := workerSelector(genericLabels)
	if err != nil {
		return nil, err
	}

	podFullList := &corev1.PodList{}
	err = jc.List(context.Background(), podFullList, client.MatchingLabelsSelector{Selector: selector}, client.InNamespace(DeepspeedJob.GetNamespace()))
	//podFullList, err := r.PodLister.List(selector)
	if err != nil {
		return nil, err
	}
	// Only running Pods should be included within the `discover_hosts.sh` script.
	var podList []corev1.Pod
	for idx, pod := range podFullList.Items {
		if pod.Status.Phase == corev1.PodRunning {
			podList = append(podList, podFullList.Items[idx])
		}
	}
	return util.JobControlledPodList(podList, DeepspeedJob), nil
}

// newConfigMap creates a new ConfigMap containing configurations for an DeepspeedJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the DeepspeedJob resource that 'owns' it.
func newConfigMap(DeepspeedJob *kubeflowv1.DeepspeedJob, workerReplicas int32, isGPULauncher bool) *corev1.ConfigMap {
	kubexec := fmt.Sprintf(`#!/bin/sh
set -x
POD_NAME=$1
shift
%s/kubectl exec ${POD_NAME}`, kubectlMountPath)
	if len(DeepspeedJob.Spec.MainContainer) > 0 {
		kubexec = fmt.Sprintf("%s --container %s", kubexec, DeepspeedJob.Spec.MainContainer)
	}
	kubexec = fmt.Sprintf("%s -- /bin/sh -c \"$*\"", kubexec)

	// If no processing unit is specified, default to 1 slot.
	slots := 1
	if DeepspeedJob.Spec.SlotsPerWorker != nil {
		slots = int(*DeepspeedJob.Spec.SlotsPerWorker)
	}
	var buffer bytes.Buffer
	if isGPULauncher {
		buffer.WriteString(fmt.Sprintf("%s%s slots=%d\n", DeepspeedJob.Name, launcherSuffix, slots))
	}
	for i := 0; i < int(workerReplicas); i++ {
		buffer.WriteString(fmt.Sprintf("%s%s-%d slots=%d\n", DeepspeedJob.Name, workerSuffix, i, slots))
	}
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DeepspeedJob.Name + configSuffix,
			Namespace: DeepspeedJob.Namespace,
			Labels: map[string]string{
				"app": DeepspeedJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
		Data: map[string]string{
			hostfileName:      buffer.String(),
			kubexecScriptName: kubexec,
		},
	}
}

// updateDiscoverHostsInConfigMap updates the ConfigMap if the content of `discover_hosts.sh` changes.
func updateDiscoverHostsInConfigMap(configMap *corev1.ConfigMap, DeepspeedJob *kubeflowv1.DeepspeedJob, runningPods []*corev1.Pod, isGPULauncher bool) {
	slots := 1
	if DeepspeedJob.Spec.SlotsPerWorker != nil {
		slots = int(*DeepspeedJob.Spec.SlotsPerWorker)
	}

	// Sort the slice of Pods to make sure the order of entries in `discover_hosts.sh` is maintained.
	sort.Slice(runningPods, func(i, j int) bool {
		return runningPods[i].Name < runningPods[j].Name
	})

	discoverHosts := "#!/bin/sh"
	if isGPULauncher {
		discoverHosts = fmt.Sprintf("%s\necho %s%s:%d\n", discoverHosts, DeepspeedJob.Name, launcherSuffix, slots)
	}
	for _, p := range runningPods {
		discoverHosts = fmt.Sprintf("%s\necho %s:%d", discoverHosts, p.Name, slots)
	}

	oldDiscoverHosts, exist := configMap.Data[discoverHostsScriptName]
	if exist {
		if oldDiscoverHosts == discoverHosts {
			return
		}
	}
	configMap.Data[discoverHostsScriptName] = discoverHosts
}

// newLauncherServiceAccount creates a new launcher ServiceAccount for an DeepspeedJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the DeepspeedJob resource that 'owns' it.
func newLauncherServiceAccount(DeepspeedJob *kubeflowv1.DeepspeedJob) *corev1.ServiceAccount {
	launcherName := DeepspeedJob.Name + launcherSuffix

	if len(DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName) > 0 {
		launcherName = DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName
	}

	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      launcherName,
			Namespace: DeepspeedJob.Namespace,
			Labels: map[string]string{
				"app": DeepspeedJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
	}
}

// newLauncherRole creates a new launcher Role for an DeepspeedJob resource. It also
// sets the appropriate OwnerReferences on the resource so handleObject can
// discover the DeepspeedJob resource that 'owns' it.
func newLauncherRole(DeepspeedJob *kubeflowv1.DeepspeedJob, workerReplicas int32) *rbacv1.Role {
	var podNames []string
	for i := 0; i < int(workerReplicas); i++ {
		podNames = append(podNames, fmt.Sprintf("%s%s-%d", DeepspeedJob.Name, workerSuffix, i))
	}
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      DeepspeedJob.Name + launcherSuffix,
			Namespace: DeepspeedJob.Namespace,
			Labels: map[string]string{
				"app": DeepspeedJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
		Rules: []rbacv1.PolicyRule{
			{
				Verbs:     []string{"get", "list", "watch"},
				APIGroups: []string{""},
				Resources: []string{"pods"},
			},
			{
				Verbs:         []string{"create"},
				APIGroups:     []string{""},
				Resources:     []string{"pods/exec"},
				ResourceNames: podNames,
			},
		},
	}
}

// newLauncherRoleBinding creates a new launcher RoleBinding for an DeepspeedJob
// resource. It also sets the appropriate OwnerReferences on the resource so
// handleObject can discover the DeepspeedJob resource that 'owns' it.
func newLauncherRoleBinding(DeepspeedJob *kubeflowv1.DeepspeedJob) *rbacv1.RoleBinding {
	launcherName := DeepspeedJob.Name + launcherSuffix
	saName := launcherName

	if len(DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName) > 0 {
		saName = DeepspeedJob.Spec.DeepspeedReplicaSpecs[kubeflowv1.DeepspeedJobReplicaTypeLauncher].Template.Spec.ServiceAccountName
	}

	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      launcherName,
			Namespace: DeepspeedJob.Namespace,
			Labels: map[string]string{
				"app": DeepspeedJob.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(DeepspeedJob, kubeflowv1.DeepspeedJobSchemeGroupVersionKind),
			},
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      rbacv1.ServiceAccountKind,
				Name:      saName,
				Namespace: DeepspeedJob.Namespace,
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     launcherName,
		},
	}
}

func setRestartPolicy(podTemplateSpec *corev1.PodTemplateSpec, spec *kubeflowv1.ReplicaSpec) {
	if spec.RestartPolicy == kubeflowv1.RestartPolicyExitCode {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicyNever
	} else {
		podTemplateSpec.Spec.RestartPolicy = corev1.RestartPolicy(spec.RestartPolicy)
	}
}

func (jc *DeepspeedJobReconciler) createWorkerHeadlessSVC(podList []*corev1.Pod, jobName string) {
	for _, p := range podList {
		headlessService := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:   p.Name,
				Labels: map[string]string{"dpjob": jobName},
			},
			Spec: corev1.ServiceSpec{
				ClusterIP: corev1.ClusterIPNone, // 设置为 None 创建 Headless Service
				Selector: map[string]string{
					"dp-worker": p.Name, // 使用标签选择器匹配 Pod
				},
			},
		}
		_, err := jc.KubeClientSet.CoreV1().Services(p.Namespace).Create(context.Background(), headlessService, metav1.CreateOptions{})
		if err != nil {
			logrus.Error(err.Error())
		}
	}

}

func (jc *DeepspeedJobReconciler) deleteWorkerHeadlessSVC(jobName string, namespace string) {

	serviceList, err := jc.KubeClientSet.CoreV1().Services(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("dpjob=%s", jobName),
	})
	logrus.Info(fmt.Sprintf("dpjob=%s", jobName))
	if err != nil {
		logrus.Error(err.Error())
	}
	for _, svc := range serviceList.Items {
		err = jc.KubeClientSet.CoreV1().Services(svc.Namespace).Delete(context.Background(), svc.Name, metav1.DeleteOptions{})
		if err != nil {
			logrus.Error(err.Error())
		}
		logrus.Infof("Deleted Service %s in Namespace %s\n", svc.Name, svc.Namespace)
	}
	logrus.Info("delete svc finish")
}
