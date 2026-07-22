// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"testing"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/idcodec"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
	"github.com/envoyproxy/ai-gateway/internal/translator"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// encodeGatewayBatchID mints a valid gateway batch id for testing.
func encodeGatewayBatchID(t *testing.T, codec idcodec.Codec, ns, name, nativeID string) string {
	t.Helper()
	id, err := codec.Encode(idcodec.BackendID{
		Namespace: ns,
		Name:      name,
		Kind:      idcodec.KindBatch,
		NativeID:  nativeID,
	})
	require.NoError(t, err)
	return id
}

// newBatchProcessorForPath creates a router-level batchesProcessor for a given path and method.
func newBatchProcessorForPath(method, path string, config *filterapi.RuntimeConfig) *batchesProcessor {
	return &batchesProcessor{
		codec:          newTestCodec(),
		config:         config,
		requestHeaders: map[string]string{":method": method, ":path": path},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
}

// capturingMetricsFactory is a metrics.Factory that retains the last created mockMetrics so tests
// can inspect what was recorded.
type capturingMetricsFactory struct {
	last *mockMetrics
}

func (f *capturingMetricsFactory) NewMetrics() metrics.Metrics {
	f.last = &mockMetrics{}
	return f.last
}

// batchResponseHeaderValue reads a set-header from the RequestBody response phase of a
// ProcessRequestBody call.
func batchRequestBodyHeaderValue(resp *extprocv3.ProcessingResponse, key string) (string, bool) {
	rb := resp.GetRequestBody()
	if rb == nil || rb.Response == nil || rb.Response.HeaderMutation == nil {
		return "", false
	}
	for _, h := range rb.Response.HeaderMutation.SetHeaders {
		if h.Header.Key == key {
			return string(h.Header.RawValue), true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// classifyBatchesRequest
// ---------------------------------------------------------------------------

func TestClassifyBatchesRequest(t *testing.T) {
	for _, tc := range []struct {
		method  string
		path    string
		wantOp  batchOperation
		wantID  string
		wantErr bool
	}{
		// create
		{http.MethodPost, "/v1/batches", batchOpCreate, "", false},
		{http.MethodPost, "/v1/batches/", batchOpCreate, "", false},
		// list
		{http.MethodGet, "/v1/batches", batchOpList, "", false},
		{http.MethodGet, "/v1/batches?limit=2&model=m", batchOpList, "", false},
		// retrieve
		{http.MethodGet, "/v1/batches/batch-abc123", batchOpRetrieve, "batch-abc123", false},
		{http.MethodGet, "/v1/batches/batch-abc123?foo=bar", batchOpRetrieve, "batch-abc123", false},
		// cancel
		{http.MethodPost, "/v1/batches/batch-abc123/cancel", batchOpCancel, "batch-abc123", false},
		// unsupported methods on collection
		{http.MethodDelete, "/v1/batches", 0, "", true},
		{http.MethodPut, "/v1/batches", 0, "", true},
		// unsupported methods on item
		{http.MethodDelete, "/v1/batches/batch-abc123", 0, "", true},
		{http.MethodGet, "/v1/batches/batch-abc123/cancel", 0, "", true},
		// not a batches path
		{http.MethodGet, "/v1/chat/completions", 0, "", true},
		// prefix preserved
		{http.MethodGet, "/openai/v1/batches/batch-abc123", batchOpRetrieve, "batch-abc123", false},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			op, id, err := classifyBatchesRequest(tc.method, tc.path)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.wantOp, op)
			require.Equal(t, tc.wantID, id)
		})
	}
}

// ---------------------------------------------------------------------------
// ProcessRequestHeaders — router filter
// ---------------------------------------------------------------------------

func TestBatchProcessRequestHeaders_UpstreamFilterPassthrough(t *testing.T) {
	p := &batchesProcessor{isUpstreamFilter: true, logger: slog.Default(), metricsFactory: &mockMetricsFactory{}}
	resp, err := p.ProcessRequestHeaders(context.Background(), &corev3.HeaderMap{})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())
	_, ok := resp.Response.(*extprocv3.ProcessingResponse_RequestHeaders)
	require.True(t, ok)
}

func TestBatchProcessRequestHeaders_UnsupportedPath(t *testing.T) {
	p := newBatchProcessorForPath(http.MethodPatch, "/v1/batches", nil)
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotFound), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestHeaders_Create_DefersToBody(t *testing.T) {
	// Create must defer routing to ProcessRequestBody — only a bare RequestHeaders response.
	p := newBatchProcessorForPath(http.MethodPost, "/v1/batches", nil)
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())
	_, ok := resp.Response.(*extprocv3.ProcessingResponse_RequestHeaders)
	require.True(t, ok)
	require.Equal(t, batchOpCreate, p.op)
}

func TestBatchProcessRequestHeaders_List_MissingModel(t *testing.T) {
	config := runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"})
	p := newBatchProcessorForPath(http.MethodGet, "/v1/batches", config)
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestHeaders_List_WithModel(t *testing.T) {
	config := runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"})
	p := newBatchProcessorForPath(http.MethodGet, "/v1/batches?model=m2", config)
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())
	require.Equal(t, batchOpList, p.op)
	// ?model= stripped from upstream path, x-ai-eg-model set.
	newPath, ok := responseHeaderValue(resp, ":path")
	require.True(t, ok)
	require.NotContains(t, newPath, "model=")
	model, ok := responseHeaderValue(resp, internalapi.ModelNameHeaderKeyDefault)
	require.True(t, ok)
	require.Equal(t, "m2", model)
}

func TestBatchProcessRequestHeaders_Retrieve_InvalidID(t *testing.T) {
	config := runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"})
	p := newBatchProcessorForPath(http.MethodGet, "/v1/batches/not-a-valid-id", config)
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotFound), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestHeaders_Retrieve_WrongKind(t *testing.T) {
	// A gateway file id used as a batch id must be rejected (kind mismatch).
	codec := newTestCodec()
	fileID := encodeGatewayFileID(t, codec, "ns", "apple", "file-native1")
	config := runtimeConfigWithSchema("ns", "apple", "myroute", filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI})
	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodGet, ":path": "/v1/batches/" + fileID},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotFound), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestHeaders_Retrieve_ValidID(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema)
	gwBatchID := encodeGatewayBatchID(t, codec, "ns", "apple", "batch-native1")

	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodGet, ":path": "/v1/batches/" + gwBatchID},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())
	require.Equal(t, batchOpRetrieve, p.op)
	require.True(t, p.backendKnown)
	require.True(t, p.backendFromDecode)
	require.Equal(t, "ns", p.backendNamespace)
	require.Equal(t, "apple", p.backendName)

	// Path rewritten to native id.
	newPath, ok := responseHeaderValue(resp, ":path")
	require.True(t, ok)
	require.Contains(t, newPath, "batch-native1")
	require.NotContains(t, newPath, gwBatchID)

	// Original path preserved for upstream filter.
	orig, ok := responseHeaderValue(resp, originalPathHeader)
	require.True(t, ok)
	require.Contains(t, orig, gwBatchID)

	// Dynamic metadata pins the backend.
	dm := resp.GetDynamicMetadata()
	require.NotNil(t, dm)
	ns := dm.Fields[internalapi.AIGatewayFilterMetadataNamespace]
	require.NotNil(t, ns)
	sv := ns.GetStructValue().Fields[internalapi.AIGatewaySelectedBackendMetadataKey]
	require.Equal(t, "ns.apple", sv.GetStringValue())
}

func TestBatchProcessRequestHeaders_Cancel_ValidID(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema)
	gwBatchID := encodeGatewayBatchID(t, codec, "ns", "apple", "batch-cancel1")

	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodPost, ":path": "/v1/batches/" + gwBatchID + "/cancel"},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())
	require.Equal(t, batchOpCancel, p.op)

	// Path must include /cancel suffix after native id.
	newPath, ok := responseHeaderValue(resp, ":path")
	require.True(t, ok)
	require.Contains(t, newPath, "batch-cancel1/cancel")
}

func TestBatchProcessRequestHeaders_Retrieve_BackendGone(t *testing.T) {
	codec := newTestCodec()
	config := runtimeConfigWithSchema("ns", "banana", "myroute", filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI})
	gwBatchID := encodeGatewayBatchID(t, codec, "ns", "apple", "batch-native1") // apple != banana

	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodGet, ":path": "/v1/batches/" + gwBatchID},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusGone), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestHeaders_Retrieve_NonOpenAISchema(t *testing.T) {
	codec := newTestCodec()
	config := runtimeConfigWithSchema("ns", "apple", "myroute", filterapi.VersionedAPISchema{Name: filterapi.APISchemaAWSBedrock})
	gwBatchID := encodeGatewayBatchID(t, codec, "ns", "apple", "batch-native1")

	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodGet, ":path": "/v1/batches/" + gwBatchID},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
	}
	resp, err := p.ProcessRequestHeaders(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusNotImplemented), int32(resp.GetImmediateResponse().Status.Code))
}

// ---------------------------------------------------------------------------
// rewrittenPath
// ---------------------------------------------------------------------------

func TestBatchRewrittenPath(t *testing.T) {
	p := &batchesProcessor{
		requestHeaders: map[string]string{":path": "/v1/batches/old-gw-id?foo=bar"},
	}
	got := p.rewrittenPath(batchOpRetrieve, "batch-native")
	require.Equal(t, "/v1/batches/batch-native?foo=bar", got)

	got = p.rewrittenPath(batchOpCancel, "batch-native")
	require.Equal(t, "/v1/batches/batch-native/cancel?foo=bar", got)
}

func TestBatchRewrittenPath_WithPrefix(t *testing.T) {
	p := &batchesProcessor{
		requestHeaders: map[string]string{":path": "/openai/v1/batches/gw-id"},
	}
	got := p.rewrittenPath(batchOpRetrieve, "nat1")
	require.Equal(t, "/openai/v1/batches/nat1", got)
}

// ---------------------------------------------------------------------------
// ProcessRequestBody — create (decode input_file_id, rewrite, sticky-pin)
// ---------------------------------------------------------------------------

func TestBatchProcessRequestBody_Create_ValidFileID(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema)
	gwFileID := encodeGatewayFileID(t, codec, "ns", "apple", "file-native1")

	p := &batchesProcessor{
		codec:          codec,
		config:         config,
		requestHeaders: map[string]string{":method": http.MethodPost, ":path": "/v1/batches"},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
		op:             batchOpCreate,
	}
	body := fmt.Sprintf(`{"input_file_id":%q,"endpoint":"/v1/chat/completions","completion_window":"24h"}`, gwFileID)
	resp, err := p.ProcessRequestBody(context.Background(), &extprocv3.HttpBody{Body: []byte(body)})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())

	// Backend decoded from input_file_id.
	require.True(t, p.backendKnown)
	require.True(t, p.backendFromDecode)
	require.Equal(t, "ns", p.backendNamespace)
	require.Equal(t, "apple", p.backendName)

	// input_file_id in forwarded body must be the backend-native id.
	rb := resp.GetRequestBody()
	require.NotNil(t, rb)
	mutatedBody := rb.Response.BodyMutation.GetBody()
	nativeFileID := gjson.GetBytes(mutatedBody, "input_file_id").String()
	require.Equal(t, "file-native1", nativeFileID)

	// Content-length updated.
	cl, ok := batchRequestBodyHeaderValue(resp, "content-length")
	require.True(t, ok)
	require.Equal(t, fmt.Sprintf("%d", len(mutatedBody)), cl)

	// Dynamic metadata pins the backend.
	dm := resp.GetDynamicMetadata()
	require.NotNil(t, dm)
	sv := dm.Fields[internalapi.AIGatewayFilterMetadataNamespace].GetStructValue().
		Fields[internalapi.AIGatewaySelectedBackendMetadataKey]
	require.Equal(t, "ns.apple", sv.GetStringValue())

	// Route cache cleared.
	require.True(t, rb.Response.ClearRouteCache)
}

func TestBatchProcessRequestBody_Create_MissingFileID(t *testing.T) {
	p := &batchesProcessor{
		codec:          newTestCodec(),
		config:         runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"}),
		requestHeaders: map[string]string{":method": http.MethodPost, ":path": "/v1/batches"},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
		op:             batchOpCreate,
	}
	resp, err := p.ProcessRequestBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"endpoint":"/v1/chat/completions"}`)})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestBody_Create_InvalidFileID(t *testing.T) {
	p := &batchesProcessor{
		codec:          newTestCodec(),
		config:         runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"}),
		requestHeaders: map[string]string{":method": http.MethodPost, ":path": "/v1/batches"},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
		op:             batchOpCreate,
	}
	resp, err := p.ProcessRequestBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"input_file_id":"not-a-gateway-id"}`)})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestBody_Create_WrongKind(t *testing.T) {
	// A batch id used where a file id is expected.
	codec := newTestCodec()
	gwBatchID := encodeGatewayBatchID(t, codec, "ns", "apple", "batch-native1")
	p := &batchesProcessor{
		codec:          codec,
		config:         runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"}),
		requestHeaders: map[string]string{":method": http.MethodPost, ":path": "/v1/batches"},
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
		op:             batchOpCreate,
	}
	body := fmt.Sprintf(`{"input_file_id":%q}`, gwBatchID)
	resp, err := p.ProcessRequestBody(context.Background(), &extprocv3.HttpBody{Body: []byte(body)})
	require.NoError(t, err)
	require.Equal(t, int32(http.StatusBadRequest), int32(resp.GetImmediateResponse().Status.Code))
}

func TestBatchProcessRequestBody_NonCreate_PassesThrough(t *testing.T) {
	// Retrieve/cancel/list have no body to process — processor should pass through.
	for _, op := range []batchOperation{batchOpRetrieve, batchOpCancel, batchOpList} {
		p := &batchesProcessor{
			op:             op,
			logger:         slog.Default(),
			metricsFactory: &mockMetricsFactory{},
		}
		resp, err := p.ProcessRequestBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{}`)})
		require.NoError(t, err)
		require.Nil(t, resp.GetImmediateResponse())
		_, ok := resp.Response.(*extprocv3.ProcessingResponse_RequestBody)
		require.True(t, ok)
	}
}

// ---------------------------------------------------------------------------
// ProcessResponseBody — id re-encoding (create/retrieve/cancel)
// ---------------------------------------------------------------------------

func TestBatchProcessResponseBody_ReEncodeResponse(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}

	p := &batchesProcessor{
		codec:            codec,
		config:           runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema),
		requestHeaders:   map[string]string{},
		logger:           slog.Default(),
		metricsFactory:   &mockMetricsFactory{},
		op:               batchOpCreate,
		backendNamespace: "ns",
		backendName:      "apple",
		backendKnown:     true,
		translator:       mustBatchCreateTranslator(t, openAISchema),
		responseHeaders:  map[string]string{},
	}

	raw := []byte(`{
		"id": "batch-native1",
		"object": "batch",
		"input_file_id": "file-input-native",
		"output_file_id": "file-output-native",
		"error_file_id": "file-error-native",
		"status": "completed"
	}`)
	resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: raw})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())

	mutated := resp.GetResponseBody().Response.BodyMutation.GetBody()
	require.NotNil(t, mutated)

	// Batch id must be gateway-encoded (kind batch).
	batchID := gjson.GetBytes(mutated, "id").String()
	require.NotEmpty(t, batchID)
	decoded, err := codec.Decode(batchID)
	require.NoError(t, err)
	require.Equal(t, idcodec.KindBatch, decoded.Kind)
	require.Equal(t, "batch-native1", decoded.NativeID)

	// All three file ids must be gateway-encoded (kind file).
	for _, field := range []string{"input_file_id", "output_file_id", "error_file_id"} {
		gwFileID := gjson.GetBytes(mutated, field).String()
		require.NotEmpty(t, gwFileID, "expected %s to be re-encoded", field)
		fd, decErr := codec.Decode(gwFileID)
		require.NoError(t, decErr)
		require.Equal(t, idcodec.KindFile, fd.Kind)
	}
}

// TestBatchProcessResponseBody_FailClosed asserts that when a batch response cannot be safely
// re-encoded, the processor returns a 502 rather than leaking the backend-native body.
func TestBatchProcessResponseBody_FailClosed(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}

	newProc := func(op batchOperation, backendKnown bool, tr translator.BatchesTranslator) *batchesProcessor {
		return &batchesProcessor{
			codec:            codec,
			config:           runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema),
			requestHeaders:   map[string]string{},
			logger:           slog.Default(),
			metricsFactory:   &mockMetricsFactory{},
			op:               op,
			backendNamespace: "ns",
			backendName:      "apple",
			backendKnown:     backendKnown,
			translator:       tr,
			responseHeaders:  map[string]string{},
		}
	}

	assert502 := func(t *testing.T, resp *extprocv3.ProcessingResponse, err error) {
		t.Helper()
		require.NoError(t, err)
		require.NotNil(t, resp.GetImmediateResponse())
		require.Equal(t, int32(http.StatusBadGateway), int32(resp.GetImmediateResponse().Status.Code))
	}

	t.Run("create: backend not known", func(t *testing.T) {
		p := newProc(batchOpCreate, false, mustBatchCreateTranslator(t, openAISchema))
		resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"id":"batch-native1"}`)})
		assert502(t, resp, err)
	})

	t.Run("create: empty body", func(t *testing.T) {
		p := newProc(batchOpCreate, true, mustBatchCreateTranslator(t, openAISchema))
		resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: nil})
		assert502(t, resp, err)
	})

	t.Run("create: missing id", func(t *testing.T) {
		p := newProc(batchOpCreate, true, mustBatchCreateTranslator(t, openAISchema))
		resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"object":"batch"}`)})
		assert502(t, resp, err)
	})

	t.Run("list: non-array data", func(t *testing.T) {
		p := newProc(batchOpList, true, mustBatchListTranslator(t, openAISchema))
		resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"error":{"message":"boom"}}`)})
		assert502(t, resp, err)
	})

	t.Run("list: backend not known", func(t *testing.T) {
		p := newProc(batchOpList, false, mustBatchListTranslator(t, openAISchema))
		resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: []byte(`{"data":[]}`)})
		assert502(t, resp, err)
	})
}

func TestBatchProcessResponseBody_RetrieveRecordsMetrics(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	mf := &capturingMetricsFactory{}

	p := &batchesProcessor{
		codec:            codec,
		config:           runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema),
		requestHeaders:   map[string]string{},
		logger:           slog.Default(),
		metricsFactory:   mf,
		op:               batchOpRetrieve,
		backendNamespace: "ns",
		backendName:      "apple",
		backendKnown:     true,
		translator:       mustBatchRetrieveTranslator(t, openAISchema),
		responseHeaders:  map[string]string{},
	}

	raw := []byte(`{
		"id": "batch-native1",
		"model": "gpt-5-2025-08-07",
		"status": "completed",
		"input_file_id": "file-input-native",
		"usage": {
			"input_tokens": 100,
			"output_tokens": 40,
			"total_tokens": 140,
			"input_tokens_details": {"cached_tokens": 10},
			"output_tokens_details": {"reasoning_tokens": 5}
		}
	}`)
	resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: raw})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())

	// Metrics must have been recorded.
	require.NotNil(t, mf.last)
	require.Equal(t, 100, mf.last.inputTokenCount)
	require.Equal(t, 40, mf.last.outputTokenCount)
	require.Equal(t, 10, mf.last.cachedInputTokenCount)
	require.Equal(t, 1, mf.last.requestSuccessCount)
	require.Equal(t, "gpt-5-2025-08-07", mf.last.responseModel)
}

func TestBatchProcessResponseBody_NonRetrieveDoesNotRecordMetrics(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	mf := &capturingMetricsFactory{}

	p := &batchesProcessor{
		codec:            codec,
		config:           runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema),
		requestHeaders:   map[string]string{},
		logger:           slog.Default(),
		metricsFactory:   mf,
		op:               batchOpCreate,
		backendNamespace: "ns",
		backendName:      "apple",
		backendKnown:     true,
		translator:       mustBatchCreateTranslator(t, openAISchema),
		responseHeaders:  map[string]string{},
	}
	raw := []byte(`{"id":"batch-native1","status":"validating","input_file_id":"file-input-native"}`)
	_, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: raw})
	require.NoError(t, err)
	// metricsFactory.NewMetrics() must NOT have been called for create.
	require.Nil(t, mf.last)
}

func TestBatchProcessResponseBody_SkipsAbsentFileIDs(t *testing.T) {
	// output_file_id / error_file_id are optional — absent fields must not cause failure.
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}

	p := &batchesProcessor{
		codec:            codec,
		config:           runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema),
		requestHeaders:   map[string]string{},
		logger:           slog.Default(),
		metricsFactory:   &mockMetricsFactory{},
		op:               batchOpRetrieve,
		backendNamespace: "ns",
		backendName:      "apple",
		backendKnown:     true,
		translator:       mustBatchRetrieveTranslator(t, openAISchema),
		responseHeaders:  map[string]string{},
	}
	// Only id and input_file_id; output/error absent.
	raw := []byte(`{"id":"batch-native1","input_file_id":"file-input-native","status":"in_progress"}`)
	resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: raw})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())

	mutated := resp.GetResponseBody().Response.BodyMutation.GetBody()
	// output_file_id / error_file_id must be absent (not set to empty string).
	require.False(t, gjson.GetBytes(mutated, "output_file_id").Exists())
	require.False(t, gjson.GetBytes(mutated, "error_file_id").Exists())
}

// ---------------------------------------------------------------------------
// ProcessResponseBody — list walk
// ---------------------------------------------------------------------------

func TestBatchProcessResponseBody_ListWalk_ReEncodesBatchAndFileIDs(t *testing.T) {
	codec := newTestCodec()
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithBackends(t, [3]string{"ns", "apple", "myroute"})

	p := &batchesProcessor{
		codec:            codec,
		config:           config,
		requestHeaders:   map[string]string{},
		logger:           slog.Default(),
		metricsFactory:   &mockMetricsFactory{},
		op:               batchOpList,
		backendNamespace: "ns",
		backendName:      "apple",
		backendKnown:     true,
		listRouteName:    "myroute",
		translator:       mustBatchListTranslator(t, openAISchema),
		responseHeaders:  map[string]string{},
	}

	raw := []byte(`{
		"object": "list",
		"data": [
			{"id":"batch-n1","input_file_id":"file-in1","output_file_id":"file-out1"},
			{"id":"batch-n2","input_file_id":"file-in2"}
		],
		"first_id": "batch-n1",
		"last_id": "batch-n2",
		"has_more": false
	}`)
	resp, err := p.ProcessResponseBody(context.Background(), &extprocv3.HttpBody{Body: raw})
	require.NoError(t, err)
	require.Nil(t, resp.GetImmediateResponse())

	mutated := resp.GetResponseBody().Response.BodyMutation.GetBody()
	require.NotNil(t, mutated)

	for i := range gjson.GetBytes(mutated, "data").Array() {
		bID := gjson.GetBytes(mutated, fmt.Sprintf("data.%d.id", i)).String()
		bd, decErr := codec.Decode(bID)
		require.NoError(t, decErr)
		require.Equal(t, idcodec.KindBatch, bd.Kind)

		fID := gjson.GetBytes(mutated, fmt.Sprintf("data.%d.input_file_id", i)).String()
		fd, decErr := codec.Decode(fID)
		require.NoError(t, decErr)
		require.Equal(t, idcodec.KindFile, fd.Kind)
	}

	// first_id must be re-encoded as a batch id.
	firstID := gjson.GetBytes(mutated, "first_id").String()
	fd, err := codec.Decode(firstID)
	require.NoError(t, err)
	require.Equal(t, idcodec.KindBatch, fd.Kind)
}

// ---------------------------------------------------------------------------
// SetBackend
// ---------------------------------------------------------------------------

func TestBatchSetBackend_UpdatesRouterState(t *testing.T) {
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema)
	key := internalapi.PerRouteRuleRefBackendName("ns", "apple", "myroute", 0, 0)
	backend := &filterapi.RuntimeBackend{Backend: &filterapi.Backend{Name: key, Schema: openAISchema}}

	router := &batchesProcessor{
		config:         config,
		logger:         slog.Default(),
		metricsFactory: &mockMetricsFactory{},
		op:             batchOpList, // list-first-page: backendFromDecode is false.
	}
	upstream := &batchesProcessor{isUpstreamFilter: true, logger: slog.Default(), metricsFactory: &mockMetricsFactory{}}

	err := upstream.SetBackend(context.Background(), backend, "myroute", router)
	require.NoError(t, err)
	require.True(t, router.backendKnown)
	require.Equal(t, "ns", router.backendNamespace)
	require.Equal(t, "apple", router.backendName)
	require.Equal(t, "myroute", router.listRouteName)
}

func TestBatchSetBackend_DoesNotOverrideDecodedBackend(t *testing.T) {
	openAISchema := filterapi.VersionedAPISchema{Name: filterapi.APISchemaOpenAI}
	config := runtimeConfigWithSchema("ns", "apple", "myroute", openAISchema)
	key := internalapi.PerRouteRuleRefBackendName("ns", "banana", "myroute", 0, 0)
	backend := &filterapi.RuntimeBackend{Backend: &filterapi.Backend{Name: key, Schema: openAISchema}}

	router := &batchesProcessor{
		config:            config,
		logger:            slog.Default(),
		metricsFactory:    &mockMetricsFactory{},
		op:                batchOpRetrieve,
		backendNamespace:  "ns",
		backendName:       "apple", // decoded from id
		backendKnown:      true,
		backendFromDecode: true,
	}
	upstream := &batchesProcessor{isUpstreamFilter: true, logger: slog.Default(), metricsFactory: &mockMetricsFactory{}}

	err := upstream.SetBackend(context.Background(), backend, "myroute", router)
	require.NoError(t, err)
	// Must not be overwritten by the banana backend.
	require.Equal(t, "apple", router.backendName)
}

func TestBatchSetBackend_PanicsOnWrongRouterType(t *testing.T) {
	upstream := &batchesProcessor{isUpstreamFilter: true, logger: slog.Default(), metricsFactory: &mockMetricsFactory{}}
	key := internalapi.PerRouteRuleRefBackendName("ns", "apple", "myroute", 0, 0)
	backend := &filterapi.RuntimeBackend{Backend: &filterapi.Backend{Name: key}}

	require.Panics(t, func() {
		_ = upstream.SetBackend(context.Background(), backend, "myroute", &filesProcessor{})
	})
}

// ---------------------------------------------------------------------------
// Translator helpers
// ---------------------------------------------------------------------------

func mustBatchCreateTranslator(t *testing.T, schema filterapi.VersionedAPISchema) translator.BatchesTranslator {
	t.Helper()
	tr, err := translator.NewBatchCreateTranslator(schema)
	require.NoError(t, err)
	return tr
}

func mustBatchRetrieveTranslator(t *testing.T, schema filterapi.VersionedAPISchema) translator.BatchesTranslator {
	t.Helper()
	tr, err := translator.NewBatchRetrieveTranslator(schema)
	require.NoError(t, err)
	return tr
}

func mustBatchListTranslator(t *testing.T, schema filterapi.VersionedAPISchema) translator.BatchesTranslator {
	t.Helper()
	tr, err := translator.NewBatchListTranslator(schema)
	require.NoError(t, err)
	return tr
}
