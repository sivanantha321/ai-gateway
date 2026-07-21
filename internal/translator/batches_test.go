// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
)

func TestOpenAIBatchesTranslators_Identity(t *testing.T) {
	// Every per-op type is its own concrete identity; assert each one's RequestBody/ResponseHeaders/
	// ResponseError are no-ops (ResponseBody is asserted separately because retrieve extracts usage).
	translators := map[string]BatchesTranslator{
		"create":   &openAIBatchCreateTranslator{},
		"retrieve": &openAIBatchRetrieveTranslator{},
		"cancel":   &openAIBatchCancelTranslator{},
		"list":     &openAIBatchListTranslator{},
	}

	for op, tr := range translators {
		t.Run(op+"/RequestBody is a no-op", func(t *testing.T) {
			in := &BatchesRequest{NativeID: "batch-native", Path: "/v1/batches/batch-native", Method: "GET"}
			hdrs, body, err := tr.RequestBody([]byte("ignored"), in, false)
			require.NoError(t, err)
			require.Nil(t, hdrs)
			require.Nil(t, body)
		})

		t.Run(op+"/ResponseHeaders is a no-op", func(t *testing.T) {
			hdrs, err := tr.ResponseHeaders(map[string]string{"content-type": "application/json"})
			require.NoError(t, err)
			require.Nil(t, hdrs)
		})

		t.Run(op+"/ResponseError is a no-op", func(t *testing.T) {
			hdrs, body, err := tr.ResponseError(map[string]string{}, bytes.NewReader([]byte(`{"error":{}}`)))
			require.NoError(t, err)
			require.Nil(t, hdrs)
			require.Nil(t, body)
		})
	}
}

func TestOpenAIBatchesTranslators_ResponseBody(t *testing.T) {
	// create/cancel/list pass through with zero usage regardless of the body.
	for op, tr := range map[string]BatchesTranslator{
		"create": &openAIBatchCreateTranslator{},
		"cancel": &openAIBatchCancelTranslator{},
		"list":   &openAIBatchListTranslator{},
	} {
		t.Run(op+"/passes through with zero usage", func(t *testing.T) {
			hdrs, body, usage, model, err := tr.ResponseBody(
				map[string]string{}, bytes.NewReader([]byte(`{"id":"batch-native","usage":{"total_tokens":99}}`)), true, nil)
			require.NoError(t, err)
			require.Nil(t, hdrs)
			require.Nil(t, body)
			require.Equal(t, metrics.TokenUsage{}, usage)
			require.Empty(t, model)
		})
	}

	retrieve := &openAIBatchRetrieveTranslator{}

	t.Run("retrieve extracts usage and model, passes body through", func(t *testing.T) {
		raw := []byte(`{
			"id":"batch-native",
			"model":"gpt-5-2025-08-07",
			"usage":{
				"input_tokens":100,
				"output_tokens":40,
				"total_tokens":140,
				"input_tokens_details":{"cached_tokens":25},
				"output_tokens_details":{"reasoning_tokens":10}
			}
		}`)
		hdrs, body, usage, model, err := retrieve.ResponseBody(map[string]string{}, bytes.NewReader(raw), true, nil)
		require.NoError(t, err)
		require.Nil(t, hdrs)
		require.Nil(t, body) // body passes through unchanged; processor re-encodes ids.
		require.Equal(t, "gpt-5-2025-08-07", string(model))

		in, ok := usage.InputTokens()
		require.True(t, ok)
		require.Equal(t, uint32(100), in)
		out, ok := usage.OutputTokens()
		require.True(t, ok)
		require.Equal(t, uint32(40), out)
		total, ok := usage.TotalTokens()
		require.True(t, ok)
		require.Equal(t, uint32(140), total)
		cached, ok := usage.CachedInputTokens()
		require.True(t, ok)
		require.Equal(t, uint32(25), cached)
		reasoning, ok := usage.ReasoningTokens()
		require.True(t, ok)
		require.Equal(t, uint32(10), reasoning)
	})

	t.Run("retrieve with absent usage records zero usage", func(t *testing.T) {
		// A pending batch (or one created before 2025-09-07) has no usage object.
		raw := []byte(`{"id":"batch-native","status":"in_progress"}`)
		hdrs, body, usage, model, err := retrieve.ResponseBody(map[string]string{}, bytes.NewReader(raw), true, nil)
		require.NoError(t, err)
		require.Nil(t, hdrs)
		require.Nil(t, body)
		require.Empty(t, model)
		in, ok := usage.InputTokens()
		require.True(t, ok) // set to 0 explicitly
		require.Equal(t, uint32(0), in)
		// Optional detail fields are only set when > 0.
		_, ok = usage.CachedInputTokens()
		require.False(t, ok)
		_, ok = usage.ReasoningTokens()
		require.False(t, ok)
	})

	t.Run("retrieve degrades to zero usage on malformed body", func(t *testing.T) {
		hdrs, body, usage, model, err := retrieve.ResponseBody(map[string]string{}, bytes.NewReader([]byte(`not json`)), true, nil)
		require.NoError(t, err)
		require.Nil(t, hdrs)
		require.Nil(t, body)
		require.Empty(t, model)
		require.Equal(t, metrics.TokenUsage{}, usage)
	})
}

func TestNewBatchTranslators(t *testing.T) {
	// Each per-op constructor shares the same schema gate (identity for OpenAI, fail-closed
	// otherwise) but returns its OWN concrete per-op type.
	type opCase struct {
		newTranslator func(filterapi.VersionedAPISchema) (BatchesTranslator, error)
		wantType      BatchesTranslator
	}
	ops := map[string]opCase{
		"create":   {NewBatchCreateTranslator, &openAIBatchCreateTranslator{}},
		"retrieve": {NewBatchRetrieveTranslator, &openAIBatchRetrieveTranslator{}},
		"cancel":   {NewBatchCancelTranslator, &openAIBatchCancelTranslator{}},
		"list":     {NewBatchListTranslator, &openAIBatchListTranslator{}},
	}

	openAISchemas := []filterapi.VersionedAPISchema{
		{Name: filterapi.APISchemaOpenAI},
		{Name: filterapi.APISchemaOpenAI, Prefix: "v1"},
	}
	unsupportedSchemas := []filterapi.VersionedAPISchema{
		{Name: filterapi.APISchemaAWSBedrock},
		{Name: filterapi.APISchemaAzureOpenAI},
		{Name: filterapi.APISchemaGCPVertexAI},
		{Name: filterapi.APISchemaGCPAnthropic},
		{Name: filterapi.APISchemaAnthropic},
		{Name: filterapi.APISchemaAWSAnthropic},
		{Name: filterapi.APISchemaCohere},
	}

	for op, tc := range ops {
		for _, schema := range openAISchemas {
			t.Run(op+"/openai returns its own concrete identity", func(t *testing.T) {
				tr, err := tc.newTranslator(schema)
				require.NoError(t, err)
				require.IsType(t, tc.wantType, tr)
			})
		}
		for _, schema := range unsupportedSchemas {
			t.Run(op+"/"+string(schema.Name)+" fails closed", func(t *testing.T) {
				tr, err := tc.newTranslator(schema)
				require.Error(t, err)
				require.Nil(t, tr)
				require.Contains(t, err.Error(), "unsupported API schema for batches")
			})
		}
	}
}
