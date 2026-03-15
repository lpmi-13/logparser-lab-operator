/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// LogParserLabSpec defines the desired state of LogParserLab.
type LogParserLabSpec struct {
	// Activity selects a fixed activity ID or "random".
	// Empty values default to "random".
	// +optional
	Activity string `json:"activity,omitempty"`
	// LogsDir overrides the host-side directory where the operator writes the single active log file.
	// Relative paths are resolved from the operator process working directory.
	// If empty, the operator uses ./logs.
	// +optional
	LogsDir string `json:"logsDir,omitempty"`
	// AnswerFile overrides the host-side answer file path.
	// If empty, the operator uses /tmp/logparser-labs/<lab>/answer.txt.
	// +optional
	AnswerFile string `json:"answerFile,omitempty"`
}

// LogParserLabStatus defines the observed state of LogParserLab.
type LogParserLabStatus struct {
	// +optional
	CurrentActivityID string `json:"currentActivityID,omitempty"`
	// +optional
	CurrentTitle string `json:"currentTitle,omitempty"`
	// +optional
	CurrentDataset string `json:"currentDataset,omitempty"`
	// +optional
	Question string `json:"question,omitempty"`
	// +optional
	OutputFormat string `json:"outputFormat,omitempty"`
	// +optional
	SuggestedTools []string `json:"suggestedTools,omitempty"`
	// +optional
	State string `json:"state,omitempty"`
	// +optional
	Message string `json:"message,omitempty"`
	// +optional
	LogsDir string `json:"logsDir,omitempty"`
	// +optional
	CurrentLogPath string `json:"currentLogPath,omitempty"`
	// +optional
	AnswerFile string `json:"answerFile,omitempty"`
	// +optional
	CompletedActivities []string `json:"completedActivities,omitempty"`
	// +optional
	Round int32 `json:"round,omitempty"`
	// +optional
	RoundSeed int64 `json:"roundSeed,omitempty"`
}

const (
	StateInitialized = "Initialized"
	StateActive      = "Active"
	StateCompleted   = "Completed"
	StateError       = "Error"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// LogParserLab is the Schema for the logparserlabs API.
type LogParserLab struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   LogParserLabSpec   `json:"spec,omitempty"`
	Status LogParserLabStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// LogParserLabList contains a list of LogParserLab.
type LogParserLabList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []LogParserLab `json:"items"`
}

func init() {
	SchemeBuilder.Register(&LogParserLab{}, &LogParserLabList{})
}
