// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package translator

import (
	"encoding/json"
	"io"

	"github.com/envoyproxy/ai-gateway/internal/apischema/openai"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
)

// Every method here is a no-op: the batches processor (internal/extproc) keeps its own canonical
// native-id-substituted path/body on the way upstream and re-encodes ids on the raw response on the
// way back, so the translators only focus on the OpenAI to/from provider API mapping.
//
// The one exception is retrieve's ResponseBody, which extracts the batch-level token usage from the
// OpenAI-shaped body so the processor can record it for future rate limiting.
type (
	openAIBatchCreateTranslator   struct{}
	openAIBatchRetrieveTranslator struct{}
	openAIBatchCancelTranslator   struct{}
	openAIBatchListTranslator     struct{}
)

var (
	_ BatchesTranslator = (*openAIBatchCreateTranslator)(nil)
	_ BatchesTranslator = (*openAIBatchRetrieveTranslator)(nil)
	_ BatchesTranslator = (*openAIBatchCancelTranslator)(nil)
	_ BatchesTranslator = (*openAIBatchListTranslator)(nil)
)

// --- create: POST /v1/batches ---

// RequestBody implements [Translator.RequestBody]. Returning a nil body keeps the processor's
// canonical (native-file-id-substituted) request unchanged; returning nil headers leaves :path/:method
// as the processor set them.
func (*openAIBatchCreateTranslator) RequestBody(_ []byte, _ *BatchesRequest, _ bool) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// ResponseHeaders implements [Translator.ResponseHeaders]. The OpenAI response headers need no
// translation.
func (*openAIBatchCreateTranslator) ResponseHeaders(map[string]string) (
	newHeaders []internalapi.Header, err error,
) {
	return nil, nil
}

// ResponseBody implements [Translator.ResponseBody]. The create response is already OpenAI-shaped
// and carries no usable token usage yet, so this returns a nil body (pass through unchanged) with
// zero usage. The processor re-encodes native ids into gateway ids on the raw response itself.
func (*openAIBatchCreateTranslator) ResponseBody(_ map[string]string, _ io.Reader, _ bool, _ any) (
	newHeaders []internalapi.Header,
	mutatedBody []byte,
	tokenUsage metrics.TokenUsage,
	responseModel internalapi.ResponseModel,
	err error,
) {
	return nil, nil, metrics.TokenUsage{}, "", nil
}

// ResponseError implements [Translator.ResponseError]. OpenAI error envelopes pass through unchanged.
func (*openAIBatchCreateTranslator) ResponseError(_ map[string]string, _ io.Reader) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// --- retrieve: GET /v1/batches/{id} ---

// RequestBody implements [Translator.RequestBody]. Returning a nil body keeps the processor's
// canonical (native-id-substituted) request unchanged; returning nil headers leaves :path/:method
// as the processor set them.
func (*openAIBatchRetrieveTranslator) RequestBody(_ []byte, _ *BatchesRequest, _ bool) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// ResponseHeaders implements [Translator.ResponseHeaders]. The OpenAI response headers need no
// translation.
func (*openAIBatchRetrieveTranslator) ResponseHeaders(map[string]string) (
	newHeaders []internalapi.Header, err error,
) {
	return nil, nil
}

// ResponseBody implements [Translator.ResponseBody]. The batch retrieve body carries a batch-level
// "usage" object (present for batches created after 2025-09-07). It is extracted into a
// metrics.TokenUsage so the processor can record it. The body itself passes through unchanged (the
// processor re-encodes native ids into gateway ids on the raw response); on any parse failure this
// degrades to zero usage rather than failing the client's retrieve.
func (*openAIBatchRetrieveTranslator) ResponseBody(_ map[string]string, body io.Reader, _ bool, _ any) (
	newHeaders []internalapi.Header,
	mutatedBody []byte,
	tokenUsage metrics.TokenUsage,
	responseModel internalapi.ResponseModel,
	err error,
) {
	raw, readErr := io.ReadAll(body)
	if readErr != nil {
		return nil, nil, metrics.TokenUsage{}, "", nil
	}
	var batch openai.Batch
	if unmarshalErr := json.Unmarshal(raw, &batch); unmarshalErr != nil {
		return nil, nil, metrics.TokenUsage{}, "", nil
	}
	return nil, nil, batchTokenUsage(&batch), internalapi.ResponseModel(batch.Model), nil
}

// ResponseError implements [Translator.ResponseError]. OpenAI error envelopes pass through unchanged.
func (*openAIBatchRetrieveTranslator) ResponseError(_ map[string]string, _ io.Reader) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// --- cancel: POST /v1/batches/{id}/cancel ---

// RequestBody implements [Translator.RequestBody]. Returning a nil body keeps the processor's
// canonical (native-id-substituted) request unchanged; returning nil headers leaves :path/:method
// as the processor set them.
func (*openAIBatchCancelTranslator) RequestBody(_ []byte, _ *BatchesRequest, _ bool) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// ResponseHeaders implements [Translator.ResponseHeaders]. The OpenAI response headers need no
// translation.
func (*openAIBatchCancelTranslator) ResponseHeaders(map[string]string) (
	newHeaders []internalapi.Header, err error,
) {
	return nil, nil
}

// ResponseBody implements [Translator.ResponseBody]. The cancel response is already OpenAI-shaped,
// so this returns a nil body (pass through unchanged) with zero usage. The processor re-encodes
// native ids into gateway ids on the raw response itself.
func (*openAIBatchCancelTranslator) ResponseBody(_ map[string]string, _ io.Reader, _ bool, _ any) (
	newHeaders []internalapi.Header,
	mutatedBody []byte,
	tokenUsage metrics.TokenUsage,
	responseModel internalapi.ResponseModel,
	err error,
) {
	return nil, nil, metrics.TokenUsage{}, "", nil
}

// ResponseError implements [Translator.ResponseError]. OpenAI error envelopes pass through unchanged.
func (*openAIBatchCancelTranslator) ResponseError(_ map[string]string, _ io.Reader) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// --- list: GET /v1/batches ---

// RequestBody implements [Translator.RequestBody]. Returning a nil body keeps the processor's
// canonical (native-cursor-substituted) request unchanged; returning nil headers leaves :path/:method
// as the processor set them.
func (*openAIBatchListTranslator) RequestBody(_ []byte, _ *BatchesRequest, _ bool) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// ResponseHeaders implements [Translator.ResponseHeaders]. The OpenAI response headers need no
// translation.
func (*openAIBatchListTranslator) ResponseHeaders(map[string]string) (
	newHeaders []internalapi.Header, err error,
) {
	return nil, nil
}

// ResponseBody implements [Translator.ResponseBody]. The body is already an OpenAI-shaped list
// envelope, so this returns a nil body (pass through unchanged) with zero usage. The processor
// re-encodes native ids into gateway ids and stitches the cross-backend walk on the raw response
// itself.
func (*openAIBatchListTranslator) ResponseBody(_ map[string]string, _ io.Reader, _ bool, _ any) (
	newHeaders []internalapi.Header,
	mutatedBody []byte,
	tokenUsage metrics.TokenUsage,
	responseModel internalapi.ResponseModel,
	err error,
) {
	return nil, nil, metrics.TokenUsage{}, "", nil
}

// ResponseError implements [Translator.ResponseError]. OpenAI error envelopes pass through unchanged.
func (*openAIBatchListTranslator) ResponseError(_ map[string]string, _ io.Reader) (
	newHeaders []internalapi.Header, mutatedBody []byte, err error,
) {
	return nil, nil, nil
}

// batchTokenUsage maps a batch's OpenAI-shaped usage object onto a metrics.TokenUsage. A zero-valued
// (absent) usage object yields a zero TokenUsage, which the processor records as no usage.
func batchTokenUsage(batch *openai.Batch) metrics.TokenUsage {
	var usage metrics.TokenUsage
	usage.SetInputTokens(uint32(batch.Usage.InputTokens))   //nolint:gosec // upstream-reported count
	usage.SetOutputTokens(uint32(batch.Usage.OutputTokens)) //nolint:gosec // upstream-reported count
	usage.SetTotalTokens(uint32(batch.Usage.TotalTokens))   //nolint:gosec // upstream-reported count
	if cached := batch.Usage.InputTokensDetails.CachedTokens; cached > 0 {
		usage.SetCachedInputTokens(uint32(cached)) //nolint:gosec // upstream-reported count
	}
	if reasoning := batch.Usage.OutputTokensDetails.ReasoningTokens; reasoning > 0 {
		usage.SetReasoningTokens(uint32(reasoning)) //nolint:gosec // upstream-reported count
	}
	return usage
}
