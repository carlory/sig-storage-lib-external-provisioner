/*
Copyright 2019 The Kubernetes Authors.

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
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
)

// PVCPopulator iterates through PVCs and checks if for bound PVCs
// their size doesn't match with Persistent Volume size
type PVCPopulator interface {
	Run(stopCh <-chan struct{})
}

type pvcPopulator struct {
	loopPeriod time.Duration
	resizeMap  VolumeResizeMap
	pvcLister  corelisters.PersistentVolumeClaimLister
	pvLister   corelisters.PersistentVolumeLister
	kubeClient kubernetes.Interface
	recorder   record.EventRecorder
}

// NewPVCPopulator returns PVCPopulator
func NewPVCPopulator(
	loopPeriod time.Duration,
	resizeMap VolumeResizeMap,
	pvcLister corelisters.PersistentVolumeClaimLister,
	pvLister corelisters.PersistentVolumeLister,
	kubeClient kubernetes.Interface) PVCPopulator {
	populator := &pvcPopulator{
		loopPeriod: loopPeriod,
		resizeMap:  resizeMap,
		pvcLister:  pvcLister,
		pvLister:   pvLister,
		kubeClient: kubeClient,
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: kubeClient.CoreV1().Events("")})
	populator.recorder = eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: "external_volume_expand"})
	return populator
}

func (populator *pvcPopulator) Run(stopCh <-chan struct{}) {
	wait.Until(populator.Sync, populator.loopPeriod, stopCh)
}

func (populator *pvcPopulator) Sync() {
	pvcs, err := populator.pvcLister.List(labels.Everything())
	if err != nil {
		klog.Errorf("Listing PVCs failed in populator: %v", err)
		return
	}

	for _, pvc := range pvcs {
		pv, err := getPersistentVolume(pvc, populator.pvLister)
		if err != nil {
			klog.Errorf("Error getting persistent volume for PVC %q: %v", pvc.UID, err)
			continue
		}

		// We are only going to add PVCs which are
		//  - bound
		//  - pvc.Spec.Size > pvc.Status.Size
		// these 2 checks are alreay perform in AddPVCUpdate function before
		// add pvc for reszie and hence we do not repeat those checks here.
		populator.resizeMap.AddPVCUpdate(pvc, pv)
	}
}

func getPersistentVolume(pvc *v1.PersistentVolumeClaim, pvLister corelisters.PersistentVolumeLister) (*v1.PersistentVolume, error) {
	volumeName := pvc.Spec.VolumeName
	pv, err := pvLister.Get(volumeName)

	if err != nil {
		return nil, fmt.Errorf("failed to find PV %q in PV informer cache with error: %v", volumeName, err)
	}
	return pv, nil
}
