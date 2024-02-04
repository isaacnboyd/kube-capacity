// Copyright 2019 Kube Capacity Authors
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

package capacity

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/robscott/kube-capacity/pkg/kube"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	k8taints "k8s.io/kubernetes/pkg/util/taints"
	v1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

// FetchAndPrint gathers cluster resource data and outputs it
func FetchAndPrint(opts Options) {
	clientset, err := kube.NewClientSet(opts.KubeConfig, opts.KubeContext, opts.ImpersonateUser, opts.ImpersonateGroup)
	if err != nil {
		fmt.Printf("Error connecting to Kubernetes: %v\n", err)
		os.Exit(1)
	}

	podList, nodeList := getPodsAndNodes(clientset, opts.ExcludeTainted, opts.PodLabels, opts.NodeLabels, opts.NodeTaints, opts.NamespaceLabels, opts.Namespace)
	var pmList *v1beta1.PodMetricsList
	var nmList *v1beta1.NodeMetricsList

	if opts.ShowUtil {
		mClientset, err := kube.NewMetricsClientSet(opts.KubeContext, opts.KubeConfig)
		if err != nil {
			fmt.Printf("Error connecting to Metrics API: %v\n", err)
			os.Exit(4)
		}

		pmList = getPodMetrics(mClientset, opts.Namespace)
		if opts.Namespace == "" && opts.NamespaceLabels == "" {
			nmList = getNodeMetrics(mClientset, nodeList, opts.NodeLabels)
		}
	}

	cm := buildClusterMetric(podList, pmList, nodeList, nmList)
	showNamespace := opts.Namespace == ""

	printList(&cm, opts.ShowContainers, opts.ShowPods, opts.ShowUtil, opts.ShowPodCount, showNamespace, opts.OutputFormat, opts.SortBy, opts.AvailableFormat)
}

func getPodsAndNodes(clientset kubernetes.Interface, excludeTainted bool, podLabels, nodeLabels, nodeTaints, namespaceLabels, namespace string) (*corev1.PodList, *corev1.NodeList) {
	nodeList, err := clientset.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{
		LabelSelector: nodeLabels,
	})
	if err != nil {
		fmt.Printf("Error listing Nodes: %v\n", err)
		os.Exit(2)
	}
	if excludeTainted {
		filteredNodeList := []corev1.Node{}
		for _, node := range nodeList.Items {
			if len(node.Spec.Taints) == 0 {
				filteredNodeList = append(filteredNodeList, node)
			}
		}
		nodeList.Items = filteredNodeList
	}

	if nodeTaints != "" {
		taints := strings.Split(nodeTaints, ",")
		taintsToAdd, taintsToRemove, error := k8taints.ParseTaints(taints)
		if error != nil {
			fmt.Printf("Error parsing taint parameter: %v\n", error)
			os.Exit(3)
		}

		onlyAdd := false
		onlyRemove := false
		addAndRemove := false
		if len(taintsToAdd) > 0 && len(taintsToRemove) == 0 {
			println("onlyAdd 93")
			onlyAdd = true
		}
		if len(taintsToAdd) == 0 && len(taintsToRemove) > 0 {
			println("onlyRemove 97")
			onlyRemove = true
		}
		if len(taintsToAdd) > 0 && len(taintsToRemove) > 0 {
			println("addAndRemove 101")
			addAndRemove = true
		}
		nodeIsTainted := false
		dontAddNode := false
		var tempNodeList corev1.NodeList
		for _, node := range nodeList.Items {
			for _, nodeTaint := range node.Spec.Taints {
				if onlyAdd || addAndRemove {
					for _, paramTaint := range taintsToAdd {
						if nodeTaint.Key == paramTaint.Key && nodeTaint.Effect == paramTaint.Effect {
							nodeIsTainted = true
							tempNodeList.Items = append(tempNodeList.Items, node)
							println("Added node " + node.ObjectMeta.Name + " 114")
						}
					}
				}
				if onlyRemove || addAndRemove {
					for _, paramTaint := range taintsToRemove {
						if nodeTaint.Key == paramTaint.Key && nodeTaint.Effect == paramTaint.Effect {
							print("debug " + node.ObjectMeta.Name + " ")
							fmt.Sprintf("%t", nodeIsTainted)
							if addAndRemove && nodeIsTainted == true {
								tempNodeList.Items = tempNodeList.Items[:len(tempNodeList.Items)-1]
								println("Remove node " + node.ObjectMeta.Name + " 123")
							}
							if onlyRemove {
								dontAddNode = true
							}
						}
					}
				}
			}
			if onlyRemove == true && dontAddNode == false {
				println("Added node " + node.ObjectMeta.Name + " 133")
				tempNodeList.Items = append(tempNodeList.Items, node)
			}
			dontAddNode = false
			nodeIsTainted = false
		}
		*nodeList = tempNodeList
	}

	podList, err := clientset.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: podLabels,
	})
	if err != nil {
		fmt.Printf("Error listing Pods: %v\n", err)
		os.Exit(3)
	}

	newPodItems := []corev1.Pod{}

	nodes := map[string]bool{}
	for _, node := range nodeList.Items {
		nodes[node.GetName()] = true
	}

	for _, pod := range podList.Items {
		if !nodes[pod.Spec.NodeName] {
			continue
		}

		newPodItems = append(newPodItems, pod)
	}

	podList.Items = newPodItems

	if namespace == "" && namespaceLabels != "" {
		namespaceList, err := clientset.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{
			LabelSelector: namespaceLabels,
		})
		if err != nil {
			fmt.Printf("Error listing Namespaces: %v\n", err)
			os.Exit(3)
		}

		namespaces := map[string]bool{}
		for _, ns := range namespaceList.Items {
			namespaces[ns.GetName()] = true
		}

		newPodItems := []corev1.Pod{}

		for _, pod := range podList.Items {
			if !namespaces[pod.GetNamespace()] {
				continue
			}

			newPodItems = append(newPodItems, pod)
		}

		podList.Items = newPodItems
	}

	return podList, nodeList
}

func getPodMetrics(mClientset *metrics.Clientset, namespace string) *v1beta1.PodMetricsList {
	pmList, err := mClientset.MetricsV1beta1().PodMetricses(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Error getting Pod Metrics: %v\n", err)
		fmt.Println("For this to work, metrics-server needs to be running in your cluster")
		os.Exit(6)
	}

	return pmList
}

func getNodeMetrics(mClientset *metrics.Clientset, nodeList *corev1.NodeList, nodeLabels string) *v1beta1.NodeMetricsList {
	nmList, err := mClientset.MetricsV1beta1().NodeMetricses().List(context.TODO(), metav1.ListOptions{
		LabelSelector: nodeLabels,
	})

	if err != nil {
		fmt.Printf("Error getting Node Metrics: %v\n", err)
		fmt.Println("For this to work, metrics-server needs to be running in your cluster")
		os.Exit(7)
	}

	return nmList
}
