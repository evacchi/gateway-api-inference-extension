/*
Copyright 2025 The Kubernetes Authors.

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

package datastore

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"
)

func TestConfigMapUpdateOrAddIfNotExist(t *testing.T) {
	tests := []struct {
		name                    string
		existingConfigMaps      []*corev1.ConfigMap // ConfigMaps to add before the test operation
		configMapToAdd          *corev1.ConfigMap   // ConfigMap to add/update
		wantErr                 bool
		wantLoraAdapterMappings map[string]string // Expected loraAdapterToBaseModel mappings
		wantBaseModels          map[string]int    // Expected baseModels count (grouped by ConfigMap)
	}{
		{
			name:               "Add new ConfigMap with single adapter",
			existingConfigMaps: []*corev1.ConfigMap{},
			configMapToAdd:     makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			wantErr:            false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b",
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1,
			},
		},
		{
			name:               "Add new ConfigMap with multiple adapters",
			existingConfigMaps: []*corev1.ConfigMap{},
			configMapToAdd:     makeConfigMap("cm2", "gpt-4", []string{"adapter1", "adapter2", "adapter3"}),
			wantErr:            false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "gpt-4",
				"adapter2": "gpt-4",
				"adapter3": "gpt-4",
			},
			wantBaseModels: map[string]int{
				"gpt-4": 1,
			},
		},
		{
			name: "Add second ConfigMap with same base model",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			configMapToAdd: makeConfigMap("cm2", "llama-2-7b", []string{"adapter2", "adapter3"}),
			wantErr:        false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b",
				"adapter2": "llama-2-7b",
				"adapter3": "llama-2-7b",
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 2, // Two ConfigMaps with same base model
			},
		},
		{
			name: "Update existing ConfigMap - add new adapters",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "adapter2"}),
			},
			configMapToAdd: makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "adapter2", "adapter3"}),
			wantErr:        false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b",
				"adapter2": "llama-2-7b",
				"adapter3": "llama-2-7b",
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1, // Still only one ConfigMap
			},
		},
		{
			name: "Update existing ConfigMap - remove adapters",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "adapter2", "adapter3"}),
			},
			configMapToAdd: makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			wantErr:        false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b",
				// adapter2 and adapter3 removed
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1,
			},
		},
		{
			name:                    "ConfigMap with empty adapters list",
			existingConfigMaps:      []*corev1.ConfigMap{},
			configMapToAdd:          makeConfigMap("cm1", "llama-2-7b", nil),
			wantErr:                 false,
			wantLoraAdapterMappings: map[string]string{
				// No adapters
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1, // Base model still tracked
			},
		},
		{
			name:                    "ConfigMap with empty baseModel field",
			existingConfigMaps:      []*corev1.ConfigMap{makeConfigMap("cm1", "", []string{"adapter1"})},
			wantErr:                 true,
			wantLoraAdapterMappings: map[string]string{},
			wantBaseModels:          map[string]int{},
		},
		{
			name:               "ConfigMap with adapters containing whitespace",
			existingConfigMaps: []*corev1.ConfigMap{},
			configMapToAdd:     makeConfigMap("cm1", "llama-2-7b", []string{" adapter1 ", "adapter2"}),
			wantErr:            false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b", // Whitespace trimmed
				"adapter2": "llama-2-7b",
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1,
			},
		},
		{
			name:               "ConfigMap with adapters containing empty strings",
			existingConfigMaps: []*corev1.ConfigMap{},
			configMapToAdd:     makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "", "adapter2"}),
			wantErr:            false,
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b",
				"adapter2": "llama-2-7b",
				// Empty string skipped
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDatastore()

			// Add existing ConfigMaps
			for _, cm := range tt.existingConfigMaps {
				err := ds.ConfigMapUpdateOrAddIfNotExist(cm)
				require.NoError(t, err, "Failed to add existing ConfigMap")
			}

			// Execute the operation
			err := ds.ConfigMapUpdateOrAddIfNotExist(tt.configMapToAdd)

			// Check error
			if tt.wantErr {
				require.Error(t, err)
			} else {
				verifyDatastoreState(t, ds, tt.wantLoraAdapterMappings, tt.wantBaseModels)
			}
		})
	}
}

func TestConfigMapDelete(t *testing.T) {
	tests := []struct {
		name                    string
		existingConfigMaps      []*corev1.ConfigMap
		configMapToDelete       *corev1.ConfigMap
		wantLoraAdapterMappings map[string]string
		wantBaseModels          map[string]int
	}{
		{
			name: "Delete existing ConfigMap",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "adapter2"}),
			},
			configMapToDelete:       makeConfigMap("cm1", "llama-2-7b", []string{"adapter1", "adapter2"}),
			wantLoraAdapterMappings: map[string]string{},
			wantBaseModels:          map[string]int{},
		},
		{
			name: "Delete non-existent ConfigMap",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			configMapToDelete: makeConfigMap("cm2", "gpt-4", []string{"adapter2"}),
			wantLoraAdapterMappings: map[string]string{
				"adapter1": "llama-2-7b", // Original data unchanged
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1,
			},
		},
		{
			name: "Delete one ConfigMap when multiple exist with same base model",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
				makeConfigMap("cm2", "llama-2-7b", []string{"adapter2", "adapter3"}),
			},
			configMapToDelete: makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			wantLoraAdapterMappings: map[string]string{
				// adapter1 removed, adapter2 and adapter3 remain
				"adapter2": "llama-2-7b",
				"adapter3": "llama-2-7b",
			},
			wantBaseModels: map[string]int{
				"llama-2-7b": 1, // Count decreased to 1
			},
		},
		{
			name: "Delete last ConfigMap for a base model",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			configMapToDelete:       makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			wantLoraAdapterMappings: map[string]string{},
			wantBaseModels:          map[string]int{}, // Base model removed
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDatastore()

			// Add existing ConfigMaps
			for _, cm := range tt.existingConfigMaps {
				err := ds.ConfigMapUpdateOrAddIfNotExist(cm)
				require.NoError(t, err, "Failed to add existing ConfigMap")
			}

			// Execute delete
			ds.ConfigMapDelete(tt.configMapToDelete)

			// Verify internal state matches expectations
			verifyDatastoreState(t, ds, tt.wantLoraAdapterMappings, tt.wantBaseModels)
		})
	}
}

func TestGetBaseModel(t *testing.T) {
	tests := []struct {
		name               string
		existingConfigMaps []*corev1.ConfigMap
		modelNameToLookup  string
		want               string
	}{
		{
			name: "Lookup LoRA adapter returns base model",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"my-adapter"}),
			},
			modelNameToLookup: "my-adapter",
			want:              "llama-2-7b",
		},
		{
			name: "Lookup base model returns itself",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			modelNameToLookup: "llama-2-7b",
			want:              "llama-2-7b",
		},
		{
			name: "Lookup unknown model returns empty string",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			modelNameToLookup: "unknown-model",
			want:              "",
		},
		{
			name: "Lookup with leading/trailing whitespace",
			existingConfigMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"my-adapter"}),
			},
			modelNameToLookup: "  my-adapter  ",
			want:              "llama-2-7b", // GetBaseModel trims the input
		},
		{
			name:               "Lookup in empty datastore",
			existingConfigMaps: []*corev1.ConfigMap{},
			modelNameToLookup:  "any-model",
			want:               "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := NewDatastore()

			// Add existing ConfigMaps
			for _, cm := range tt.existingConfigMaps {
				err := ds.ConfigMapUpdateOrAddIfNotExist(cm)
				require.NoError(t, err, "Failed to add existing ConfigMap")
			}

			// Execute lookup
			got := ds.GetBaseModel(tt.modelNameToLookup)

			// Compare result
			require.Equal(t, tt.want, got)
		})
	}
}

func makeConfigMap(name, baseModel string, adapters []string) *corev1.ConfigMap {
	const ns = "default"
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Data: map[string]string{
			baseModelKey: baseModel,
		},
	}

	// Marshal adapters to YAML if provided
	if len(adapters) > 0 {
		adaptersYAML, err := yaml.Marshal(adapters)
		if err != nil {
			panic(err) // Should never happen in tests with valid data
		}
		cm.Data[adaptersKey] = string(adaptersYAML)
	}

	return cm
}

// Helper function to verify internal datastore state
func verifyDatastoreState(t *testing.T, ds Datastore, wantAdapterMappings map[string]string, wantBaseModels map[string]int) {
	t.Helper()

	// Cast to concrete type to access private fields
	concrete, ok := ds.(*datastore)
	if !ok {
		t.Fatal("Failed to cast datastore to concrete type")
	}

	// Verify adapter to base model mappings
	require.Empty(t, cmp.Diff(wantAdapterMappings, concrete.loraAdapterToBaseModel),
		"loraAdapterToBaseModel mismatch")

	// Verify base model counts
	require.Empty(t, cmp.Diff(wantBaseModels, concrete.baseModels),
		"baseModels mismatch")
}
