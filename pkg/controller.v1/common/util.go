// Copyright 2019 The Kubeflow Authors
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

package common

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"sort"
	"strings"

	apiv1 "github.com/kubeflow/training-operator/pkg/apis/kubeflow.org/v1"
	log "github.com/sirupsen/logrus"
	"golang.org/x/crypto/ssh"
	v1 "k8s.io/api/core/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ReplicasPriority is a slice of ReplicaPriority.
type ReplicasPriority []ReplicaPriority

type ReplicaPriority struct {
	priority int32

	apiv1.ReplicaSpec
}

func (p ReplicasPriority) Len() int {
	return len(p)
}

func (p ReplicasPriority) Less(i, j int) bool {
	return p[i].priority > p[j].priority
}

func (p ReplicasPriority) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func GenGeneralName(jobName string, rtype string, index string) string {
	if rtype == "launcher" {
		n := jobName + "-" + strings.ToLower(rtype)
		return strings.Replace(n, "/", "-", -1)
	}
	n := jobName + "-" + strings.ToLower(rtype) + "-" + index
	return strings.Replace(n, "/", "-", -1)
}

// RecheckDeletionTimestamp returns a CanAdopt() function to recheck deletion.
//
// The CanAdopt() function calls getObject() to fetch the latest value,
// and denies adoption attempts if that object has a non-nil DeletionTimestamp.
func RecheckDeletionTimestamp(getObject func() (metav1.Object, error)) func() error {
	return func() error {
		obj, err := getObject()
		if err != nil {
			return fmt.Errorf("can't recheck DeletionTimestamp: %v", err)
		}
		if obj.GetDeletionTimestamp() != nil {
			return fmt.Errorf("%v/%v has just been deleted at %v", obj.GetNamespace(), obj.GetName(), obj.GetDeletionTimestamp())
		}
		return nil
	}
}

func MaxInt(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func AddResourceList(list, req, limit v1.ResourceList) {
	for name, quantity := range req {

		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}

	if req != nil {
		return
	}

	// If Requests is omitted for a container,
	// it defaults to Limits if that is explicitly specified.
	for name, quantity := range limit {
		if value, ok := list[name]; !ok {
			list[name] = quantity.DeepCopy()
		} else {
			value.Add(quantity)
			list[name] = value
		}
	}
}

type PriorityClassGetFunc func(string) (*schedulingv1.PriorityClass, error)

func CalcPGMinResources(minMember int32, replicas map[apiv1.ReplicaType]*apiv1.ReplicaSpec, pcGetFunc PriorityClassGetFunc) *v1.ResourceList {
	var replicasPriority ReplicasPriority
	for t, replica := range replicas {
		rp := ReplicaPriority{0, *replica}
		pc := replica.Template.Spec.PriorityClassName

		priorityClass, err := pcGetFunc(pc)
		if err != nil || priorityClass == nil {
			log.Warnf("Ignore task %s priority class %s: %v", t, pc, err)
		} else {
			rp.priority = priorityClass.Value
		}

		replicasPriority = append(replicasPriority, rp)
	}

	sort.Sort(replicasPriority)

	minAvailableTasksRes := v1.ResourceList{}
	podCnt := int32(0)
	for _, task := range replicasPriority {
		if task.Replicas == nil {
			continue
		}

		for i := int32(0); i < *task.Replicas; i++ {
			if podCnt >= minMember {
				break
			}
			podCnt++
			for _, c := range task.Template.Spec.Containers {
				AddResourceList(minAvailableTasksRes, c.Resources.Requests, c.Resources.Limits)
			}
		}
	}

	return &minAvailableTasksRes
}

func GenSshKeyPair() (string, string) {

	// 生成RSA密钥对，长度为2048位
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法生成私钥: %s\n", err)
		os.Exit(1)
	}

	// 将私钥编码为PEM格式
	privDER := x509.MarshalPKCS1PrivateKey(privateKey)
	privBlock := &pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: privDER,
	}
	privPEM := pem.EncodeToMemory(privBlock)

	// 将PEM格式的私钥写入文件（可选）
	// err = os.WriteFile("id_rsa", privPEM, 0600)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "无法写入私钥文件: %s\n", err)
	// 	os.Exit(1)
	// }

	// 生成公钥（通常SSH公钥是公钥的SSH格式，而不是PEM或DER）
	publicKey := &privateKey.PublicKey
	sshKey, err := ssh.NewPublicKey(publicKey)
	if err != nil {
		fmt.Fprintf(os.Stderr, "无法创建SSH公钥: %s\n", err)
		os.Exit(1)
	}

	// 将公钥转换为SSH格式并写入文件（可选）
	sshPub := ssh.MarshalAuthorizedKey(sshKey)
	// err = os.WriteFile("id_rsa.pub", sshPub, 0644)
	// if err != nil {
	// 	fmt.Fprintf(os.Stderr, "无法写入公钥文件: %s\n", err)
	// 	os.Exit(1)
	// }

	fmt.Println("密钥对生成成功！")
	return string(sshPub), string(privPEM)

}
