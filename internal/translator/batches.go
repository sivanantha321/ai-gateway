// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"fmt"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
)

// BatchesRequest is the OpenAI-shaped request context passed to a Batch translator
// as the reused generic Translator's ReqT.
type BatchesRequest struct {
	// NativeID is the backend-native batch id, already substituted by the processor
	// (retrieve/cancel); "" otherwise.
	NativeID string
	// Path is the request :path, already native-rewritten by the processor.
	Path string
	// Method is the request :method.
	Method string
	// Query is the list query with the native "after" cursor already substituted; "" otherwise.
	Query string
}

// BatchesTranslator maps the OpenAI Batch request/response shapes to a provider's batch API.
// SpanT is `any` because batches have no tracing span, so instantiating
// the generic here introduces NO dependency on internal/tracing.
type BatchesTranslator = Translator[BatchesRequest, any]

// NewBatchCreateTranslator selects the POST /v1/batches translator.
func NewBatchCreateTranslator(schema filterapi.VersionedAPISchema) (BatchesTranslator, error) {
	switch schema.Name {
	case filterapi.APISchemaOpenAI:
		return &openAIBatchCreateTranslator{}, nil
	default:
		return nil, errUnsupportedBatchesSchema(schema)
	}
}

// NewBatchRetrieveTranslator selects the GET /v1/batches/{id} translator.
func NewBatchRetrieveTranslator(schema filterapi.VersionedAPISchema) (BatchesTranslator, error) {
	switch schema.Name {
	case filterapi.APISchemaOpenAI:
		return &openAIBatchRetrieveTranslator{}, nil
	default:
		return nil, errUnsupportedBatchesSchema(schema)
	}
}

// NewBatchCancelTranslator selects the POST /v1/batches/{id}/cancel translator.
func NewBatchCancelTranslator(schema filterapi.VersionedAPISchema) (BatchesTranslator, error) {
	switch schema.Name {
	case filterapi.APISchemaOpenAI:
		return &openAIBatchCancelTranslator{}, nil
	default:
		return nil, errUnsupportedBatchesSchema(schema)
	}
}

// NewBatchListTranslator selects the GET /v1/batches translator.
func NewBatchListTranslator(schema filterapi.VersionedAPISchema) (BatchesTranslator, error) {
	switch schema.Name {
	case filterapi.APISchemaOpenAI:
		return &openAIBatchListTranslator{}, nil
	default:
		return nil, errUnsupportedBatchesSchema(schema)
	}
}

// errUnsupportedBatchesSchema is the selection error the processor maps to HTTP 501 when
// a Batch route points at a backend whose batch translation does not yet exist.
func errUnsupportedBatchesSchema(schema filterapi.VersionedAPISchema) error {
	return fmt.Errorf("unsupported API schema for batches: backend=%s", schema)
}
