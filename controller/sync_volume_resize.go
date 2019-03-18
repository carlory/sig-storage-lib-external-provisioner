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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog"
	kevents "k8s.io/kubernetes/pkg/kubelet/events"
	"k8s.io/kubernetes/pkg/volume"
	"k8s.io/kubernetes/pkg/volume/util"
)

// SyncVolumeResize iterates through PVCRs and sync volume size.
type SyncVolumeResize interface {
	Run(stopCh <-chan struct{})
}

type syncResize struct {
	loopPeriod  time.Duration
	resizeMap   VolumeResizeMap
	kubeClient  kubernetes.Interface
	provisioner ExpandableProvisioner
	recorder    record.EventRecorder
}

// NewSyncVolumeResize returns SyncVolumeResize
func NewSyncVolumeResize(
	loopReriod time.Duration,
	resizeMap VolumeResizeMap,
	kubeClient kubernetes.Interface,
	provisioner ExpandableProvisioner,
	recorder record.EventRecorder) SyncVolumeResize {
	rc := &syncResize{
		loopPeriod:  loopReriod,
		resizeMap:   resizeMap,
		kubeClient:  kubeClient,
		provisioner: provisioner,
		recorder:    recorder,
	}
	return rc
}

func (rc *syncResize) Run(stopCh <-chan struct{}) {
	wait.Until(rc.Sync, rc.loopPeriod, stopCh)
}

func (rc *syncResize) Sync() {
	// Resize PVCs that require resize
	for _, pvcWithResizeRequest := range rc.resizeMap.GetPVCsWithResizeRequest() {
		updatedClaim, err := markPVCResizeInProgress(pvcWithResizeRequest, rc.kubeClient)
		if err != nil {
			klog.V(5).Infof("Error setting PVC %s in progress with error : %v", pvcWithResizeRequest.QualifiedName(), err)
			continue
		}
		if updatedClaim != nil {
			pvcWithResizeRequest.PVC = updatedClaim
		}

		growErr := rc.expandVolume(pvcWithResizeRequest)
		if growErr != nil {
			klog.Errorf("Error growing pvc %s with %v", pvcWithResizeRequest.QualifiedName(), growErr)
		}
	}
}

func (rc *syncResize) expandVolume(pvcr *PVCWithResizeRequest) error {
	if rc.provisioner == nil {
		return nil
	}
	volumeSpec := volume.NewSpecFromPersistentVolume(pvcr.PersistentVolume, false)
	newSize := pvcr.ExpectedSize
	pvSize := pvcr.PersistentVolume.Spec.Capacity[v1.ResourceStorage]
	if pvSize.Cmp(newSize) < 0 {
		updatedSize, expandErr := rc.provisioner.ExpandVolumeDevice(volumeSpec, pvcr.ExpectedSize, pvcr.CurrentSize)
		if expandErr != nil {
			rc.recorder.Eventf(pvcr.PVC, v1.EventTypeWarning, kevents.VolumeResizeFailed, expandErr.Error())
			return fmt.Errorf("Error expanding volume %q of provisioner: %v", pvcr.QualifiedName(), expandErr)
		}
		klog.Infof("ExpandVolume succeeded for volume %s", pvcr.QualifiedName())
		newSize = updatedSize

		// k8s doesn't have transactions, we can't guarantee that after updating PV - updating PVC will be
		// successful, that is why all PVCs for which pvc.Spec.Size > pvc.Status.Size must be reprocessed
		// until they reflect user requested size in pvc.Status.Size
		updateErr := rc.resizeMap.UpdatePVSize(pvcr, newSize)
		if updateErr != nil {
			rc.recorder.Eventf(pvcr.PVC, v1.EventTypeWarning, kevents.VolumeResizeFailed, updateErr.Error())
			return fmt.Errorf("Error updating PV spec capacity for volume %q with : %v", pvcr.QualifiedName(), updateErr)
		}
		klog.Infof("ExpandVolume.UpdatePV succeeded for volume %s", pvcr.QualifiedName())
	}

	if !rc.provisioner.RequiresFSResize() {
		klog.V(4).Infof("Controller resizing done for PVC %s", pvcr.QualifiedName())
		err := rc.resizeMap.MarkAsResized(pvcr, newSize)
		if err != nil {
			rc.recorder.Eventf(pvcr.PVC, v1.EventTypeWarning, kevents.VolumeResizeFailed, err.Error())
			return fmt.Errorf("Error marking pvc %s as resized : %v", pvcr.QualifiedName(), err)
		}
		successMsg := fmt.Sprintf("ExpandVolume succeeded for volume %s", pvcr.QualifiedName())
		rc.recorder.Eventf(pvcr.PVC, v1.EventTypeNormal, kevents.VolumeResizeSuccess, successMsg)
	}

	return nil
}

func markPVCResizeInProgress(pvcWithResizeRequest *PVCWithResizeRequest, kubeClient kubernetes.Interface) (*v1.PersistentVolumeClaim, error) {
	// Mark PVC as Resize Started
	progressCondition := v1.PersistentVolumeClaimCondition{
		Type:               v1.PersistentVolumeClaimResizing,
		Status:             v1.ConditionTrue,
		LastTransitionTime: metav1.Now(),
	}
	conditions := []v1.PersistentVolumeClaimCondition{progressCondition}
	newPVC := pvcWithResizeRequest.PVC.DeepCopy()
	newPVC = util.MergeResizeConditionOnPVC(newPVC, conditions)

	return util.PatchPVCStatus(pvcWithResizeRequest.PVC /*oldPVC*/, newPVC, kubeClient)
}
