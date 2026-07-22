// Copyright Envoy AI Gateway Authors
// SPDX-License-Identifier: Apache-2.0
// The full text of the Apache license is available in the LICENSE file at
// the root of the repo.

package extproc

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/envoyproxy/ai-gateway/internal/filterapi"
	"github.com/envoyproxy/ai-gateway/internal/idcodec"
	"github.com/envoyproxy/ai-gateway/internal/internalapi"
	"github.com/envoyproxy/ai-gateway/internal/metrics"
	"github.com/envoyproxy/ai-gateway/internal/translator"
)

// batchesPathMarker is the canonical Batch API path segment. The registered route prefix may be
// preceded by a configurable root/OpenAI prefix, so the operation is derived from the suffix
// that follows this marker rather than from the full path.
const batchesPathMarker = "/v1/batches"

// batchOperation identifies which Batch API endpoint a request targets.
type batchOperation int

const (
	batchOpCreate   batchOperation = iota // POST /v1/batches
	batchOpList                           // GET  /v1/batches
	batchOpRetrieve                       // GET  /v1/batches/{id}
	batchOpCancel                         // POST /v1/batches/{id}/cancel
)

// NewBatchesProcessorFactory returns a [ProcessorFactory] for the OpenAI Batch API endpoints
// ("/v1/batches", "/v1/batches/{id}", "/v1/batches/{id}/cancel"). It implements backend-sticky,
// id-driven routing using the given backend id codec:
//
//   - Create is routed by decoding the backend from the body's input_file_id (kind file); the
//     response batch id is rewritten to encode the serving backend.
//   - Retrieve/cancel decode the backend from the path batch id, pin the request to that backend
//     via the selected_backend sticky dynamic metadata, and rewrite the path to the backend-native id.
//   - List presents a single cross-backend view by walking the route's backends one page at a time,
//     carrying the walk position in an encrypted pagination cursor (see files_list_walk.go).
//   - All client-visible ids are gateway-encoded; all ids sent upstream are backend-native.
//   - On retrieve, the batch-level token usage from the response body is recorded via metricsFactory.
func NewBatchesProcessorFactory(codec idcodec.Codec, metricsFactory metrics.Factory) ProcessorFactory {
	return func(config *filterapi.RuntimeConfig, requestHeaders map[string]string, logger *slog.Logger, isUpstreamFilter bool, _ bool) (Processor, error) {
		return &batchesProcessor{
			codec:            codec,
			config:           config,
			requestHeaders:   requestHeaders,
			logger:           logger,
			isUpstreamFilter: isUpstreamFilter,
			metricsFactory:   metricsFactory,
		}, nil
	}
}

// batchesProcessor implements [Processor] for the Batches API at both the router and upstream
// filter levels. A single instance serves one filter stream; the router instance holds the
// per-request state and performs request routing and response id rewriting, while the upstream
// instance only captures the load-balancer-selected backend via SetBackend and pushes it into
// the linked router instance.
type batchesProcessor struct {
	codec            idcodec.Codec
	config           *filterapi.RuntimeConfig
	requestHeaders   map[string]string
	logger           *slog.Logger
	isUpstreamFilter bool
	metricsFactory   metrics.Factory

	// Router-side state, established during request processing and consumed during response
	// processing (which is handled at the router filter level).
	op batchOperation
	// backendNamespace/backendName identify the owning backend used to (re-)encode response ids.
	// They are set either by decoding the request id/input_file_id (create/retrieve/cancel) or,
	// for list (which has no id to decode on first page), by SetBackend from the LB-selected backend.
	backendNamespace string
	backendName      string
	backendKnown     bool
	// backendFromDecode is true when the backend was decoded from a request id or input_file_id.
	// In that case SetBackend must not override it. For list-first-page it is false, so SetBackend
	// updates the backend on every call — important on retries/fallback.
	backendFromDecode bool

	// List-walk state (op == batchOpList). Mirrors the Files list walk; see files_list_walk.go.
	listRouteName  string
	listStart      backendKey
	listStartKnown bool

	// translator is the resolved per-operation translator for the backend schema.
	translator translator.BatchesTranslator
	// responseHeaders captures the upstream response headers for ResponseBody translator calls.
	responseHeaders map[string]string
}

var _ Processor = (*batchesProcessor)(nil)

// ProcessRequestHeaders implements [Processor.ProcessRequestHeaders].
func (p *batchesProcessor) ProcessRequestHeaders(_ context.Context, _ *corev3.HeaderMap) (*extprocv3.ProcessingResponse, error) {
	if p.isUpstreamFilter {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{}}, nil
	}

	method := p.requestHeaders[":method"]
	path := p.requestHeaders[":path"]
	op, id, err := classifyBatchesRequest(method, path)
	if err != nil {
		p.logger.Error("unsupported batches request", slog.String("method", method), slog.String("path", path))
		return createUserFacingErrorResponse(http.StatusNotFound, "NotFoundError", "unsupported Batch API request"), nil
	}
	p.op = op

	switch op {
	case batchOpCreate:
		// Routing depends on the JSON body (input_file_id); defer to ProcessRequestBody.
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestHeaders{}}, nil
	case batchOpList:
		return p.handleListRequestHeaders()
	default: // retrieve, cancel
		return p.handleIDBearingRequestHeaders(op, id)
	}
}

// handleListRequestHeaders sets up routing for GET /v1/batches. Mirrors handleListRequestHeaders
// in files_processor.go: a mandatory ?model= on the first page selects the route; cursor pages
// decode the encrypted walk position and pin the request to the appropriate backend.
func (p *batchesProcessor) handleListRequestHeaders() (*extprocv3.ProcessingResponse, error) {
	path := p.requestHeaders[":path"]
	headerMutation := &extprocv3.HeaderMutation{}
	setHeader(headerMutation, originalPathHeader, path)

	modelOverride := queryParam(path, "model")
	upstreamPath := stripQueryParam(path, "model")

	after := queryParam(path, "after")
	if after == "" {
		if modelOverride == "" {
			p.logger.Error("list batches request is missing the required model query parameter", slog.String("path", path))
			return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", "the 'model' query parameter is required"), nil
		}
		setHeader(headerMutation, internalapi.ModelNameHeaderKeyDefault, modelOverride)
		p.requestHeaders[internalapi.ModelNameHeaderKeyDefault] = modelOverride
		setHeader(headerMutation, ":path", upstreamPath)
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{
					Response: &extprocv3.CommonResponse{
						HeaderMutation:  headerMutation,
						ClearRouteCache: true,
					},
				},
			},
		}, nil
	}

	decoded, err := p.codec.Decode(after)
	if err != nil {
		p.logger.Error("rejecting batches list request with undecodable after cursor", slog.String("after", after))
		return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", "invalid after cursor"), nil
	}

	var current backendKey
	var nativeAfter string
	switch decoded.Kind {
	case idcodec.KindListCursor:
		cur, ok := decodeListWalkCursor(decoded)
		if !ok {
			return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", "invalid after cursor"), nil
		}
		current, nativeAfter = cur.current, cur.nativeAfter
		p.listStart, p.listStartKnown = cur.start, true
	case idcodec.KindBatch:
		// Stock SDK pagination passes after=data[-1].id (a gateway batch id). Continue within that
		// batch's backend; the walk then proceeds through the deterministic cycle from there.
		current = backendKey{namespace: decoded.Namespace, name: decoded.Name}
		nativeAfter = decoded.NativeID
		p.listStart, p.listStartKnown = current, true
	default:
		return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", "invalid after cursor"), nil
	}

	p.backendNamespace = current.namespace
	p.backendName = current.name
	p.backendKnown = true
	setHeader(headerMutation, ":path", rewriteAfterParam(upstreamPath, nativeAfter))

	schema, ok := p.schemaForBackend(p.config, current.namespace, current.name)
	if !ok {
		p.logger.Error("backend not found in config for list cursor", slog.String("namespace", current.namespace), slog.String("name", current.name))
		return createUserFacingErrorResponse(http.StatusGone, "NotFoundError", "backend for cursor not found"), nil
	}
	if err = p.resolveTranslator(schema); err != nil {
		p.logger.Error("unsupported schema for batches list operation", slog.String("schema", string(schema.Name)))
		return createUserFacingErrorResponse(http.StatusNotImplemented, "not_implemented", "Batch API not supported for this backend schema"), nil
	}

	_, queryPart := splitQuery(upstreamPath)
	req := &translator.BatchesRequest{
		Path:   rewriteAfterParam(upstreamPath, nativeAfter),
		Method: p.requestHeaders[":method"],
		Query:  queryPart,
	}
	newHeaders, newBody, err := p.translator.RequestBody(nil, req, false)
	if err != nil {
		p.logger.Error("translator RequestBody failed for list cursor", slog.String("error", err.Error()))
		return createUserFacingErrorResponse(http.StatusInternalServerError, "internal_error", "failed to translate request"), nil
	}
	additionalHeaderMutation, bodyMutation := mutationsFromTranslationResult(newHeaders, newBody)
	if additionalHeaderMutation != nil {
		headerMutation.SetHeaders = append(headerMutation.SetHeaders, additionalHeaderMutation.SetHeaders...)
		headerMutation.RemoveHeaders = append(headerMutation.RemoveHeaders, additionalHeaderMutation.RemoveHeaders...)
	}
	if bodyMutation != nil {
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{
					Response: &extprocv3.CommonResponse{
						HeaderMutation:  headerMutation,
						BodyMutation:    bodyMutation,
						ClearRouteCache: true,
					},
				},
			},
			DynamicMetadata: stickyBackendDynamicMetadata(current.namespace, current.name),
		}, nil
	}
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{
					HeaderMutation:  headerMutation,
					ClearRouteCache: true,
				},
			},
		},
		DynamicMetadata: stickyBackendDynamicMetadata(current.namespace, current.name),
	}, nil
}

// handleIDBearingRequestHeaders decodes the backend from a gateway-issued batch id in the path,
// pins the request to that backend via sticky dynamic metadata, and rewrites the path to the
// backend-native id. An undecodable/forged/wrong-kind id yields a 404.
func (p *batchesProcessor) handleIDBearingRequestHeaders(op batchOperation, gatewayID string) (*extprocv3.ProcessingResponse, error) {
	decoded, err := p.codec.Decode(gatewayID)
	if err != nil || decoded.Kind != idcodec.KindBatch {
		p.logger.Error("rejecting batches request with undecodable id", slog.String("id", gatewayID))
		return createUserFacingErrorResponse(http.StatusNotFound, "NotFoundError", fmt.Sprintf("No such Batch object: %s", gatewayID)), nil
	}
	p.backendNamespace = decoded.Namespace
	p.backendName = decoded.Name
	p.backendKnown = true
	p.backendFromDecode = true

	schema, ok := p.schemaForBackend(p.config, decoded.Namespace, decoded.Name)
	if !ok {
		p.logger.Error("backend not found in config for decoded batch id", slog.String("namespace", decoded.Namespace), slog.String("name", decoded.Name))
		return createUserFacingErrorResponse(http.StatusGone, "NotFoundError", fmt.Sprintf("No such Batch object: %s", gatewayID)), nil
	}
	if err = p.resolveTranslator(schema); err != nil {
		p.logger.Error("unsupported schema for batches operation", slog.String("schema", string(schema.Name)), slog.String("op", fmt.Sprintf("%d", op)))
		return createUserFacingErrorResponse(http.StatusNotImplemented, "not_implemented", "Batch API not supported for this backend schema"), nil
	}

	headerMutation := &extprocv3.HeaderMutation{}
	setHeader(headerMutation, ":path", p.rewrittenPath(op, decoded.NativeID))
	setHeader(headerMutation, originalPathHeader, p.requestHeaders[":path"])

	req := &translator.BatchesRequest{
		NativeID: decoded.NativeID,
		Path:     p.rewrittenPath(op, decoded.NativeID),
		Method:   p.requestHeaders[":method"],
	}
	newHeaders, newBody, err := p.translator.RequestBody(nil, req, false)
	if err != nil {
		p.logger.Error("failed to translate request body", slog.String("op", fmt.Sprintf("%d", op)), slog.String("error", err.Error()))
		return createUserFacingErrorResponse(http.StatusInternalServerError, "internal_error", "failed to translate request"), nil
	}
	additionalHeaderMutation, bodyMutation := mutationsFromTranslationResult(newHeaders, newBody)
	if additionalHeaderMutation != nil {
		headerMutation.SetHeaders = append(headerMutation.SetHeaders, additionalHeaderMutation.SetHeaders...)
		headerMutation.RemoveHeaders = append(headerMutation.RemoveHeaders, additionalHeaderMutation.RemoveHeaders...)
	}
	if bodyMutation != nil {
		return &extprocv3.ProcessingResponse{
			Response: &extprocv3.ProcessingResponse_RequestHeaders{
				RequestHeaders: &extprocv3.HeadersResponse{
					Response: &extprocv3.CommonResponse{
						HeaderMutation:  headerMutation,
						BodyMutation:    bodyMutation,
						ClearRouteCache: true,
					},
				},
			},
			DynamicMetadata: stickyBackendDynamicMetadata(decoded.Namespace, decoded.Name),
		}, nil
	}
	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestHeaders{
			RequestHeaders: &extprocv3.HeadersResponse{
				Response: &extprocv3.CommonResponse{
					HeaderMutation:  headerMutation,
					ClearRouteCache: true,
				},
			},
		},
		DynamicMetadata: stickyBackendDynamicMetadata(decoded.Namespace, decoded.Name),
	}, nil
}

// ProcessRequestBody implements [Processor.ProcessRequestBody]. Only create processes a body:
// it decodes the backend from input_file_id, pins the request to that backend, and rewrites
// input_file_id to the backend-native file id before forwarding.
func (p *batchesProcessor) ProcessRequestBody(_ context.Context, rawBody *extprocv3.HttpBody) (*extprocv3.ProcessingResponse, error) {
	if p.isUpstreamFilter || p.op != batchOpCreate {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_RequestBody{}}, nil
	}

	headerMutation := &extprocv3.HeaderMutation{}
	setHeader(headerMutation, originalPathHeader, p.requestHeaders[":path"])

	gatewayFileID := gjson.GetBytes(rawBody.Body, "input_file_id").String()
	if gatewayFileID == "" {
		p.logger.Error("create batch request missing input_file_id")
		return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", "the 'input_file_id' field is required"), nil
	}

	decoded, err := p.codec.Decode(gatewayFileID)
	if err != nil || decoded.Kind != idcodec.KindFile {
		p.logger.Error("create batch request has invalid input_file_id", slog.String("input_file_id", gatewayFileID))
		return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("No such File object: %s", gatewayFileID)), nil
	}

	p.backendNamespace = decoded.Namespace
	p.backendName = decoded.Name
	p.backendKnown = true
	p.backendFromDecode = true

	schema, ok := p.schemaForBackend(p.config, decoded.Namespace, decoded.Name)
	if !ok {
		p.logger.Error("backend not found in config for input_file_id", slog.String("namespace", decoded.Namespace), slog.String("name", decoded.Name))
		return createUserFacingErrorResponse(http.StatusBadRequest, "invalid_request_error", fmt.Sprintf("No such File object: %s", gatewayFileID)), nil
	}
	if err = p.resolveTranslator(schema); err != nil {
		p.logger.Error("unsupported schema for batch create", slog.String("schema", string(schema.Name)))
		return createUserFacingErrorResponse(http.StatusNotImplemented, "not_implemented", "Batch API not supported for this backend schema"), nil
	}

	// Rewrite input_file_id to the backend-native file id before forwarding.
	newBodyBytes, err := sjson.SetBytes(rawBody.Body, "input_file_id", decoded.NativeID)
	if err != nil {
		p.logger.Error("failed to rewrite input_file_id in create batch body", slog.String("error", err.Error()))
		return createUserFacingErrorResponse(http.StatusInternalServerError, "internal_error", "failed to rewrite request body"), nil
	}
	setHeader(headerMutation, "content-length", strconv.Itoa(len(newBodyBytes)))

	return &extprocv3.ProcessingResponse{
		Response: &extprocv3.ProcessingResponse_RequestBody{
			RequestBody: &extprocv3.BodyResponse{
				Response: &extprocv3.CommonResponse{
					HeaderMutation: headerMutation,
					BodyMutation: &extprocv3.BodyMutation{
						Mutation: &extprocv3.BodyMutation_Body{Body: newBodyBytes},
					},
					ClearRouteCache: true,
				},
			},
		},
		DynamicMetadata: stickyBackendDynamicMetadata(decoded.Namespace, decoded.Name),
	}, nil
}

// ProcessResponseHeaders implements [Processor.ProcessResponseHeaders]. Captures upstream response
// headers for use by ResponseBody translator calls in ProcessResponseBody.
func (p *batchesProcessor) ProcessResponseHeaders(_ context.Context, headers *corev3.HeaderMap) (*extprocv3.ProcessingResponse, error) {
	if p.isUpstreamFilter {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{}}, nil
	}
	p.responseHeaders = make(map[string]string, len(headers.GetHeaders()))
	for _, h := range headers.GetHeaders() {
		p.responseHeaders[h.Key] = string(h.GetRawValue())
	}
	return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseHeaders{}}, nil
}

// ProcessResponseBody implements [Processor.ProcessResponseBody]. Response bodies are processed
// at the router filter level, where the owning backend is known, so client-visible ids can be
// (re-)encoded.
func (p *batchesProcessor) ProcessResponseBody(ctx context.Context, body *extprocv3.HttpBody) (*extprocv3.ProcessingResponse, error) {
	if p.isUpstreamFilter {
		return &extprocv3.ProcessingResponse{Response: &extprocv3.ProcessingResponse_ResponseBody{}}, nil
	}
	switch p.op {
	case batchOpList:
		return p.buildListWalkResponse(body.Body), nil
	default: // create, retrieve, cancel
		return p.reEncodeResponse(ctx, body.Body), nil
	}
}

// reEncodeResponse rewrites the batch id + embedded file ids in a JSON response into gateway-
// encoded ids (create/retrieve/cancel). For retrieve, token usage is also recorded. On any failure
// it returns a 502 rather than forwarding the upstream body, which would leak backend-native ids.
func (p *batchesProcessor) reEncodeResponse(ctx context.Context, raw []byte) *extprocv3.ProcessingResponse {
	if !p.backendKnown {
		p.logger.Error("backend unknown; cannot re-encode batches response ids")
		return batchesReEncodeError("backend unknown for upstream batches response")
	}
	if len(raw) == 0 {
		p.logger.Error("empty body; cannot re-encode batches response ids")
		return batchesReEncodeError("empty body in upstream batches response")
	}

	workingBody := raw
	if p.responseHeaders != nil {
		_, mutatedBody, tokenUsage, responseModel, err := p.translator.ResponseBody(p.responseHeaders, bytes.NewReader(raw), true, nil)
		if err != nil {
			p.logger.Error("failed to translate response body", slog.String("error", err.Error()))
			return batchesReEncodeError("failed to process upstream batches response")
		}
		if mutatedBody != nil {
			workingBody = mutatedBody
		}
		// Record token usage for retrieve responses (usage is zero for create/cancel).
		if p.op == batchOpRetrieve {
			m := p.metricsFactory.NewMetrics()
			m.SetResponseModel(responseModel)
			m.RecordTokenUsage(ctx, tokenUsage, p.requestHeaders)
			m.RecordRequestCompletion(ctx, true, p.requestHeaders)
		}
	}

	// Re-encode the top-level batch id (kind batch).
	nativeBatchID := gjson.GetBytes(workingBody, "id").String()
	if nativeBatchID == "" {
		p.logger.Error("missing id field; cannot re-encode batches response")
		return batchesReEncodeError("missing id in upstream batches response")
	}
	gwBatchID, err := p.encodeBatchID(nativeBatchID)
	if err != nil {
		p.logger.Error("failed to re-encode batch response id", slog.String("error", err.Error()))
		return batchesReEncodeError("failed to encode batch id in upstream batches response")
	}
	workingBody, err = sjson.SetBytes(workingBody, "id", gwBatchID)
	if err != nil {
		p.logger.Error("failed to set batch id in response", slog.String("error", err.Error()))
		return batchesReEncodeError("failed to encode batch id in upstream batches response")
	}

	// Re-encode the embedded file ids (kind file). Missing/null fields are skipped.
	for _, field := range []string{"input_file_id", "output_file_id", "error_file_id"} {
		nativeFileID := gjson.GetBytes(workingBody, field).String()
		if nativeFileID == "" {
			continue
		}
		gwFileID, encErr := p.encodeFileID(nativeFileID)
		if encErr != nil {
			p.logger.Error("failed to re-encode file id field", slog.String("field", field), slog.String("error", encErr.Error()))
			return batchesReEncodeError("failed to encode file id in upstream batches response")
		}
		workingBody, err = sjson.SetBytes(workingBody, field, gwFileID)
		if err != nil {
			p.logger.Error("failed to set file id field in response", slog.String("field", field), slog.String("error", err.Error()))
			return batchesReEncodeError("failed to encode file id in upstream batches response")
		}
	}

	return bodyMutationResponse(workingBody)
}

// buildListWalkResponse re-encodes every data[].id + embedded file ids for the serving backend
// and stitches this single-backend page into the cross-backend walk.
func (p *batchesProcessor) buildListWalkResponse(raw []byte) *extprocv3.ProcessingResponse {
	if !p.backendKnown {
		p.logger.Error("backend unknown; cannot re-encode batches list response ids")
		return batchesReEncodeError("backend unknown for upstream batches list response")
	}
	if len(raw) == 0 {
		p.logger.Error("empty body; cannot re-encode batches list response ids")
		return batchesReEncodeError("empty body in upstream batches list response")
	}
	if !gjson.GetBytes(raw, "data").IsArray() {
		p.logger.Error("non-list body; cannot re-encode batches list response ids")
		return batchesReEncodeError("invalid or non-list body in upstream batches list response")
	}

	workingBody := raw
	if p.responseHeaders != nil {
		_, mutatedBody, _, _, err := p.translator.ResponseBody(p.responseHeaders, bytes.NewReader(raw), true, nil)
		if err != nil {
			p.logger.Error("failed to translate response body for batches list endpoint", slog.String("error", err.Error()))
			return batchesReEncodeError("failed to process upstream batches list response")
		}
		if mutatedBody != nil {
			workingBody = mutatedBody
		}
	}

	current := backendKey{namespace: p.backendNamespace, name: p.backendName}
	start := current
	if p.listStartKnown {
		start = p.listStart
	}

	newBody := workingBody
	var err error
	lastNativeID := ""

	for i, item := range gjson.GetBytes(workingBody, "data").Array() {
		// Re-encode the batch id.
		nativeBatchID := item.Get("id").String()
		if nativeBatchID == "" {
			continue
		}
		lastNativeID = nativeBatchID
		gwBatchID, encErr := p.encodeBatchID(nativeBatchID)
		if encErr != nil {
			err = encErr
			break
		}
		if newBody, err = sjson.SetBytes(newBody, fmt.Sprintf("data.%d.id", i), gwBatchID); err != nil {
			break
		}
		// Re-encode embedded file ids within each list item.
		for _, field := range []string{"input_file_id", "output_file_id", "error_file_id"} {
			nativeFileID := item.Get(field).String()
			if nativeFileID == "" {
				continue
			}
			gwFileID, fErr := p.encodeFileID(nativeFileID)
			if fErr != nil {
				err = fErr
				break
			}
			if newBody, err = sjson.SetBytes(newBody, fmt.Sprintf("data.%d.%s", i, field), gwFileID); err != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	if err != nil {
		p.logger.Error("failed to re-encode batches list ids", slog.String("error", err.Error()))
		return batchesReEncodeError("failed to encode id in upstream batches list response")
	}

	// Never leak the backend-native first_id; re-encode it when present.
	if fid := gjson.GetBytes(workingBody, "first_id").String(); fid != "" {
		if gw, e := p.encodeBatchID(fid); e == nil {
			newBody, _ = sjson.SetBytes(newBody, "first_id", gw)
		}
	}

	ordered := orderedRouteBackends(p.config, p.listRouteName)
	upstreamHasMore := gjson.GetBytes(workingBody, "has_more").Bool()
	hasMore, next := nextWalkStep(ordered, start, current, lastNativeID, upstreamHasMore)

	if hasMore {
		token, e := encodeListWalkCursor(p.codec, &next)
		if e != nil {
			p.logger.Warn("failed to encode list cursor, ending pagination", slog.String("error", e.Error()))
			hasMore = false
		} else {
			newBody, _ = sjson.SetBytes(newBody, "last_id", token)
		}
	}
	if !hasMore {
		// Terminal page: never leak the backend-native last_id.
		if lid := gjson.GetBytes(raw, "last_id").String(); lid != "" {
			if gw, e := p.encodeBatchID(lid); e == nil {
				newBody, _ = sjson.SetBytes(newBody, "last_id", gw)
			}
		}
	}
	if newBody, err = sjson.SetBytes(newBody, "has_more", hasMore); err != nil {
		p.logger.Error("failed to set has_more on batches list", slog.String("error", err.Error()))
		return batchesReEncodeError("failed to encode upstream batches list response")
	}
	return bodyMutationResponse(newBody)
}

// SetBackend implements [Processor.SetBackend]. Called on the upstream filter instance with the
// LB-selected backend; for create/list-first-page (no id to decode) it records the backend on
// the router instance so response ids can be encoded. For id-bearing operations the backend is
// already known from decoding and must not be overridden.
func (p *batchesProcessor) SetBackend(_ context.Context, backend *filterapi.RuntimeBackend, routeName string, routeProcessor Processor) error {
	rp, ok := routeProcessor.(*batchesProcessor)
	if !ok {
		panic(fmt.Sprintf("BUG: expected routeProcessor to be *batchesProcessor, got %T", routeProcessor))
	}
	if rn, found := routeNameFromBackendName(backend.Backend.Name); found {
		rp.listRouteName = rn
	} else if routeName != "" {
		rp.listRouteName = routeName
	}
	if rp.backendFromDecode {
		return nil
	}
	ns, name, ok := internalapi.NamespaceAndNameFromBackendName(backend.Backend.Name)
	if !ok {
		rp.logger.Warn("could not parse backend identity for batches response encoding", slog.String("backend", backend.Backend.Name))
		return nil
	}
	rp.backendNamespace = ns
	rp.backendName = name
	rp.backendKnown = true

	if err := rp.resolveTranslator(backend.Backend.Schema); err != nil {
		rp.logger.Info("unsupported schema for batches operation at SetBackend", slog.String("schema", string(backend.Backend.Schema.Name)), slog.String("op", fmt.Sprintf("%d", rp.op)))
		return fmt.Errorf("batch API not supported for schema %s: %w", backend.Backend.Schema.Name, err)
	}
	return nil
}

// rewrittenPath reconstructs the request path with the backend-native id in place of the
// gateway id, preserving any configured prefix before the "/v1/batches" marker and the query.
func (p *batchesProcessor) rewrittenPath(op batchOperation, nativeID string) string {
	pathOnly, query := splitQuery(p.requestHeaders[":path"])
	idx := strings.Index(pathOnly, batchesPathMarker)
	base := pathOnly[:idx+len(batchesPathMarker)]
	suffix := "/" + nativeID
	if op == batchOpCancel {
		suffix += "/cancel"
	}
	return base + suffix + query
}

// classifyBatchesRequest determines the Batch API operation and (where present) the path id from
// the request method and path.
func classifyBatchesRequest(method, rawPath string) (op batchOperation, id string, err error) {
	pathOnly, _ := splitQuery(rawPath)
	idx := strings.Index(pathOnly, batchesPathMarker)
	if idx == -1 {
		return 0, "", fmt.Errorf("not a batches path: %s", rawPath)
	}
	suffix := pathOnly[idx+len(batchesPathMarker):]

	if suffix == "" || suffix == "/" {
		switch method {
		case http.MethodPost:
			return batchOpCreate, "", nil
		case http.MethodGet:
			return batchOpList, "", nil
		}
		return 0, "", fmt.Errorf("unsupported method %s for %s", method, rawPath)
	}

	segs := strings.Split(strings.TrimPrefix(suffix, "/"), "/")
	switch {
	case len(segs) == 1 && segs[0] != "":
		if method == http.MethodGet {
			return batchOpRetrieve, segs[0], nil
		}
	case len(segs) == 2 && segs[0] != "" && segs[1] == "cancel":
		if method == http.MethodPost {
			return batchOpCancel, segs[0], nil
		}
	}
	return 0, "", fmt.Errorf("unsupported method %s for %s", method, rawPath)
}

// encodeBatchID encodes a backend-native batch id into a gateway batch id for the current backend.
func (p *batchesProcessor) encodeBatchID(nativeID string) (string, error) {
	return p.codec.Encode(idcodec.BackendID{
		Namespace: p.backendNamespace,
		Name:      p.backendName,
		Kind:      idcodec.KindBatch,
		NativeID:  nativeID,
	})
}

// encodeFileID encodes a backend-native file id into a gateway file id for the current backend.
func (p *batchesProcessor) encodeFileID(nativeID string) (string, error) {
	return p.codec.Encode(idcodec.BackendID{
		Namespace: p.backendNamespace,
		Name:      p.backendName,
		Kind:      idcodec.KindFile,
		NativeID:  nativeID,
	})
}

// schemaForBackend resolves the VersionedAPISchema for a backend identified by its namespace and name.
func (p *batchesProcessor) schemaForBackend(config *filterapi.RuntimeConfig, ns, name string) (filterapi.VersionedAPISchema, bool) {
	for composite := range config.Backends {
		n, m, ok := internalapi.NamespaceAndNameFromBackendName(composite)
		if !ok || n != ns || m != name {
			continue
		}
		return config.Backends[composite].Backend.Schema, true
	}
	return filterapi.VersionedAPISchema{}, false
}

// resolveTranslator selects and stores the appropriate BatchesTranslator for the given backend
// schema and the receiver's op.
func (p *batchesProcessor) resolveTranslator(schema filterapi.VersionedAPISchema) error {
	var t translator.BatchesTranslator
	var err error
	switch p.op {
	case batchOpCreate:
		t, err = translator.NewBatchCreateTranslator(schema)
	case batchOpRetrieve:
		t, err = translator.NewBatchRetrieveTranslator(schema)
	case batchOpCancel:
		t, err = translator.NewBatchCancelTranslator(schema)
	case batchOpList:
		t, err = translator.NewBatchListTranslator(schema)
	default:
		return fmt.Errorf("resolveTranslator: unsupported batch operation %d", p.op)
	}
	if err != nil {
		return err
	}
	if t == nil {
		return fmt.Errorf("resolveTranslator: nil translator for batch operation %d schema %s", p.op, schema.Name)
	}
	p.translator = t
	return nil
}

// batchesReEncodeError returns an ImmediateResponse (HTTP 502) for cases where a batch or file id
// could not be safely re-encoded. Forwarding the upstream body unchanged in these cases would leak
// backend-native ids to the client, so a controlled error is returned instead. It is returned with
// a nil Go error so the ext_proc stream stays intact.
func batchesReEncodeError(msg string) *extprocv3.ProcessingResponse {
	return createUserFacingErrorResponse(http.StatusBadGateway, "upstream_error", msg)
}
