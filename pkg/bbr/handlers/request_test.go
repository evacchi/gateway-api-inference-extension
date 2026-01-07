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

package handlers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	basepb "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extProcPb "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/google/go-cmp/cmp"
	"google.golang.org/protobuf/testing/protocmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metricsutils "k8s.io/component-base/metrics/testutil"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/gateway-api-inference-extension/pkg/bbr/datastore"
	"sigs.k8s.io/gateway-api-inference-extension/pkg/bbr/metrics"
	logutil "sigs.k8s.io/gateway-api-inference-extension/pkg/epp/util/logging"
)

func TestHandleRequestBody(t *testing.T) {
	metrics.Register()
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	tests := []struct {
		name      string
		body      map[string]any
		streaming bool
		want      []*extProcPb.ProcessingResponse
		wantErr   bool
	}{
		{
			name: "model not found",
			body: map[string]any{
				"prompt": "Tell me a joke",
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{},
					},
				},
			},
		},
		{
			name: "model not found with streaming",
			body: map[string]any{
				"prompt": "Tell me a joke",
			},
			streaming: true,
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestHeaders{
						RequestHeaders: &extProcPb.HeadersResponse{},
					},
				},
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								BodyMutation: &extProcPb.BodyMutation{
									Mutation: &extProcPb.BodyMutation_StreamedResponse{
										StreamedResponse: &extProcPb.StreamedBodyResponse{
											Body: mapToBytes(t, map[string]any{
												"prompt": "Tell me a joke",
											}),
											EndOfStream: true,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "model is not string",
			body: map[string]any{
				"model":  1,
				"prompt": "Tell me a joke",
			},
			wantErr: true,
		},
		{
			name: "success",
			body: map[string]any{
				"model":  "foo",
				"prompt": "Tell me a joke",
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								// Necessary so that the new headers are used in the routing decision.
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("foo"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte(""),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "success-with-streaming",
			body: map[string]any{
				"model":  "foo",
				"prompt": "Tell me a joke",
			},
			streaming: true,
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestHeaders{
						RequestHeaders: &extProcPb.HeadersResponse{
							Response: &extProcPb.CommonResponse{
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("foo"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte(""),
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								BodyMutation: &extProcPb.BodyMutation{
									Mutation: &extProcPb.BodyMutation_StreamedResponse{
										StreamedResponse: &extProcPb.StreamedBodyResponse{
											Body: mapToBytes(t, map[string]any{
												"model":  "foo",
												"prompt": "Tell me a joke",
											}),
											EndOfStream: true,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "success-with-streaming-large-body",
			body: func() map[string]any {
				return map[string]any{
					"model":  "foo",
					"prompt": strings.Repeat("a", 70000),
				}
			}(),
			streaming: true,
			want: func() []*extProcPb.ProcessingResponse {
				m := map[string]any{
					"model":  "foo",
					"prompt": strings.Repeat("a", 70000),
				}
				b, _ := json.Marshal(m)
				limit := 62000
				return []*extProcPb.ProcessingResponse{
					{
						Response: &extProcPb.ProcessingResponse_RequestHeaders{
							RequestHeaders: &extProcPb.HeadersResponse{
								Response: &extProcPb.CommonResponse{
									ClearRouteCache: true,
									HeaderMutation: &extProcPb.HeaderMutation{
										SetHeaders: []*basepb.HeaderValueOption{
											{
												Header: &basepb.HeaderValue{
													Key:      modelHeader,
													RawValue: []byte("foo"),
												},
											},
											{
												Header: &basepb.HeaderValue{
													Key:      baseModelHeader,
													RawValue: []byte(""),
												},
											},
										},
									},
								},
							},
						},
					},
					{
						Response: &extProcPb.ProcessingResponse_RequestBody{
							RequestBody: &extProcPb.BodyResponse{
								Response: &extProcPb.CommonResponse{
									BodyMutation: &extProcPb.BodyMutation{
										Mutation: &extProcPb.BodyMutation_StreamedResponse{
											StreamedResponse: &extProcPb.StreamedBodyResponse{
												Body:        b[:limit],
												EndOfStream: false,
											},
										},
									},
								},
							},
						},
					},
					{
						Response: &extProcPb.ProcessingResponse_RequestBody{
							RequestBody: &extProcPb.BodyResponse{
								Response: &extProcPb.CommonResponse{
									BodyMutation: &extProcPb.BodyMutation{
										Mutation: &extProcPb.BodyMutation_StreamedResponse{
											StreamedResponse: &extProcPb.StreamedBodyResponse{
												Body:        b[limit:],
												EndOfStream: true,
											},
										},
									},
								},
							},
						},
					},
				}
			}(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := NewServer(test.streaming, &fakeDatastore{})
			bodyBytes, _ := json.Marshal(test.body)
			resp, err := server.HandleRequestBody(ctx, bodyBytes)
			if err != nil {
				if !test.wantErr {
					t.Fatalf("HandleRequestBody returned unexpected error: %v, want %v", err, test.wantErr)
				}
				return
			}

			if diff := cmp.Diff(test.want, resp, protocmp.Transform()); diff != "" {
				t.Errorf("HandleRequestBody returned unexpected response, diff(-want, +got): %v", diff)
			}
		})
	}

	wantMetrics := `
	# HELP bbr_model_not_in_body_total [ALPHA] Count of times the model was not present in the request body.
	# TYPE bbr_model_not_in_body_total counter
	bbr_model_not_in_body_total{} 1
	# HELP bbr_model_not_parsed_total [ALPHA] Count of times the model was in the request body but we could not parse it.
	# TYPE bbr_model_not_parsed_total counter
	bbr_model_not_parsed_total{} 1
	# HELP bbr_success_total [ALPHA] Count of successes pulling model name from body and injecting it in the request headers.
	# TYPE bbr_success_total counter
	bbr_success_total{} 1
	`

	if err := metricsutils.GatherAndCompare(crmetrics.Registry, strings.NewReader(wantMetrics), "inference_objective_request_total"); err != nil {
		t.Error(err)
	}
}

// TestHandleRequestBodyWithDatastore tests the integration between the handler and datastore
// to verify that LoRA adapter to base model mappings result in correct headers
func TestHandleRequestBodyWithDatastore(t *testing.T) {
	metrics.Register()
	ctx := logutil.NewTestLoggerIntoContext(context.Background())

	tests := []struct {
		name       string
		body       map[string]any
		streaming  bool
		configMaps []*corev1.ConfigMap // ConfigMaps to populate datastore
		want       []*extProcPb.ProcessingResponse
	}{
		{
			name: "LoRA adapter request (non-streaming)",
			body: map[string]any{
				"model":  "my-lora-adapter",
				"prompt": "Tell me a joke",
			},
			streaming: false,
			configMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"my-lora-adapter"}),
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("my-lora-adapter"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte("llama-2-7b"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "LoRA adapter request (streaming)",
			body: map[string]any{
				"model":  "my-lora-adapter",
				"prompt": "Tell me a joke",
			},
			streaming: true,
			configMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"my-lora-adapter"}),
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestHeaders{
						RequestHeaders: &extProcPb.HeadersResponse{
							Response: &extProcPb.CommonResponse{
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("my-lora-adapter"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte("llama-2-7b"),
											},
										},
									},
								},
							},
						},
					},
				},
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								BodyMutation: &extProcPb.BodyMutation{
									Mutation: &extProcPb.BodyMutation_StreamedResponse{
										StreamedResponse: &extProcPb.StreamedBodyResponse{
											Body: mapToBytes(t, map[string]any{
												"model":  "my-lora-adapter",
												"prompt": "Tell me a joke",
											}),
											EndOfStream: true,
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "Base model request returns itself",
			body: map[string]any{
				"model":  "llama-2-7b",
				"prompt": "Tell me a joke",
			},
			streaming: false,
			configMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("llama-2-7b"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte("llama-2-7b"),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			name: "unknown-model returns empty base model header",
			body: map[string]any{
				"model":  "unknown-model",
				"prompt": "Tell me a joke",
			},
			streaming: false,
			configMaps: []*corev1.ConfigMap{
				makeConfigMap("cm1", "llama-2-7b", []string{"adapter1"}),
			},
			want: []*extProcPb.ProcessingResponse{
				{
					Response: &extProcPb.ProcessingResponse_RequestBody{
						RequestBody: &extProcPb.BodyResponse{
							Response: &extProcPb.CommonResponse{
								ClearRouteCache: true,
								HeaderMutation: &extProcPb.HeaderMutation{
									SetHeaders: []*basepb.HeaderValueOption{
										{
											Header: &basepb.HeaderValue{
												Key:      modelHeader,
												RawValue: []byte("unknown-model"),
											},
										},
										{
											Header: &basepb.HeaderValue{
												Key:      baseModelHeader,
												RawValue: []byte(""),
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ds := datastore.NewDatastore()
			for _, cm := range tt.configMaps {
				if err := ds.ConfigMapUpdateOrAddIfNotExist(cm); err != nil {
					t.Fatalf("ConfigMap update returned unexpected error: %v", err)
				}
			}

			server := NewServer(tt.streaming, ds)

			bodyBytes, _ := json.Marshal(tt.body)
			resp, err := server.HandleRequestBody(ctx, bodyBytes)
			if err != nil {
				t.Fatalf("HandleRequestBody returned unexpected error: %v", err)
			}

			if diff := cmp.Diff(tt.want, resp, protocmp.Transform()); diff != "" {
				t.Errorf("HandleRequestBody returned unexpected response, diff(-want, +got): %v", diff)
			}
		})
	}
}

// mapToBytes converts a map to a JSON byte slice
func mapToBytes(t *testing.T, m map[string]any) []byte {
	bytes, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("Marshal(): %v", err)
	}
	return bytes
}

// makeConfigMap creates a ConfigMap for testing with base model and adapters
func makeConfigMap(name, baseModel string, adapters []string) *corev1.ConfigMap {
	const (
		ns           = "default"
		baseModelKey = "baseModel"
		adaptersKey  = "adapters"
	)

	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
		},
		Data: map[string]string{
			baseModelKey: baseModel,
		},
	}

	if len(adapters) > 0 {
		adaptersYAML, err := yaml.Marshal(adapters)
		if err != nil {
			panic(err) // Should never happen in tests
		}
		cm.Data[adaptersKey] = string(adaptersYAML)
	}

	return cm
}
