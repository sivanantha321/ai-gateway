# Sticky Routing Migration Plan: Header Match -> Dynamic Metadata

## Scope and decisions

This plan implements sticky routing using dynamic metadata in Envoy route matching.

Explicit decisions for this branch:

1. Reuse `internalapi.AIGatewayFilterMetadataNamespace` (no new metadata namespace).
2. Do **not** maintain compatibility with header-based sticky routing (dev-only, not in prod).
3. Do **not** generate per-backend sticky HTTPRoutes at all. The controller only manages the main HTTPRoute; sticky Envoy routes are synthesized by the extension server at xDS translation time, with dynamic-metadata matchers for route selection.

---

## Target behavior

For sticky/file-affinity requests, ext_proc sets request dynamic metadata under:

- Namespace: `internalapi.AIGatewayFilterMetadataNamespace`
- Key: `selected_backnd`
- Value: namespace-qualified backend identifier currently used by sticky routes (e.g. `ns1.apple`).

Envoy route matching uses this dynamic metadata key to select per-backend sticky routes.

The per-backend sticky routes exist **only in xDS** (Envoy `RouteConfiguration`): the extension server derives them from the owning `AIGatewayRoute` spec during post-translate, clones the translated rule route per unique backend, and attaches a dynamic-metadata matcher. No per-backend HTTPRoutes are created in Kubernetes, and no header-based sticky route matching is used.

---

## How dynamic metadata sticky routing works (mechanics)

### Why metadata instead of headers

Headers leak implementation details. A header like `x-ai-eg-backend: ns1.apple`:

- may appear in access logs,
- may be forwarded upstream,
- can be **spoofed by clients**.

Dynamic metadata is request-scoped Envoy-internal state: it is never received from or forwarded to the client/upstream, so the routing decision stays internal and unspoofable.

### End-to-end request flow

Example incoming request (encoded file ID carries the sticky backend):

```
GET /v1/files/file-<base64(aigw:v1;id:file-123;model:gpt-4;backend:ns1.apple)>
```

1. **ext_proc (request headers phase)** decodes the ID and resolves `backendName = "ns1.apple"`.
2. ext_proc returns a `ProcessingResponse` whose `dynamic_metadata` field carries:

   ```yaml
   io.envoy.ai_gateway:        # internalapi.AIGatewayFilterMetadataNamespace
     selected_backnd: ns1.apple # internalapi.AIGatewaySelectedBackndMetadataKey
   ```

   together with `ClearRouteCache: true` in the header mutation response.
3. **Envoy** stores the struct in the request's dynamic metadata under the ext_proc filter's *receiving namespace*. For ext_proc-emitted metadata to be written at all, the `ExternalProcessor` filter config **must** list the namespace in `metadata_options.receiving_namespaces.untyped` (see Edit 4.5).
4. Because the route cache is cleared, Envoy **re-runs route matching** with the metadata now present.
5. The synthesized per-backend sticky route matches via a `dynamic_metadata` predicate and selects the backend's cluster directly — no header matching involved.

### Target Envoy route configuration shape

What the extension server must produce in the virtual host (conceptual YAML):

```yaml
routes:
# Synthesized sticky routes — first in the vhost; match ONLY when metadata is set.
- match:
    prefix: "/"
    dynamic_metadata:
    - filter: io.envoy.ai_gateway
      path:
      - key: selected_backnd
      value:
        string_match:
          exact: ns1.apple
  route:
    cluster: <cluster-for-ns1.apple>
- match:
    prefix: "/"
    dynamic_metadata:
    - filter: io.envoy.ai_gateway
      path:
      - key: selected_backnd
      value:
        string_match:
          exact: ns1.orange
  route:
    cluster: <cluster-for-ns1.orange>
# ...one sticky route per unique backend...

# Existing general (model-header) routes — unchanged.
# Existing route-not-found catch-all — unchanged, stays last.
```

Result: ext_proc setting `selected_backnd=ns1.apple` deterministically selects the `ns1.apple` cluster; requests without the metadata never match a sticky route and fall through to the general routes.

Go API mapping (used in Edit 4.2):

- `route.Match.DynamicMetadata` → `[]*matcherv3.MetadataMatcher`
- `MetadataMatcher.Filter` = `internalapi.AIGatewayFilterMetadataNamespace`
- `MetadataMatcher.Path` = single `PathSegment{Key: internalapi.AIGatewaySelectedBackndMetadataKey}`
- `MetadataMatcher.Value` = `ValueMatcher_StringMatch` with `StringMatcher_Exact` = `<ns>.<name>`

---

## Implementation order (file-by-file, exact function-level edits)

## 1) `internal/internalapi/internalapi.go`

### Edit 1.1: add sticky-metadata constants

**Function/section:** top-level constants block (near existing metadata/header constants)

Add:

- `AIGatewaySelectedBackndMetadataKey = "selected_backnd"`

(Optionally add short docs that this key is used by Envoy dynamic metadata route matching for sticky routing.)

### Edit 1.2: remove per-backend sticky HTTPRoute constants

**Function/section:** same constants block

Delete constants that only exist to label/annotate per-backend sticky HTTPRoutes (no longer generated):

- `AIGatewayStickyBackendHTTPRouteAnnotation`
- `AIGatewayStickyRouteOwnerLabel`
- `AIGatewayStickyRouteOwnerNamespaceLabel`
- `AIGatewayStickyRouteBackendLabel`
- `AIGatewayStickyRouteTypeLabel`
- `AIGatewayStickyRouteTypePerBackend`

**Why first:** centralizes key naming before controller/ext_proc/extension changes.

---

## 2) `internal/extproc/processor_impl.go`

### Edit 2.1: add helper to build sticky-backend dynamic metadata

**Function area:** near existing helpers:
- `buildContentLengthDynamicMetadataOnRequest`
- `buildRequestHeaderDynamicMetadata`
- `mergeDynamicMetadata`

Add a helper, e.g.:

- `buildStickyBackendDynamicMetadata(stickyBackend string) *structpb.Struct`
  - Returns `nil` if empty.
  - Produces:
    - namespace: `internalapi.AIGatewayFilterMetadataNamespace`
    - field: `internalapi.AIGatewaySelectedBackndMetadataKey`
  - Resulting struct shape (outer struct keyed by namespace, inner struct keyed by metadata key):

    ```
    {"io.envoy.ai_gateway": {"selected_backnd": "<ns>.<name>"}}
    ```

  - Assign to `ProcessingResponse.DynamicMetadata` (merge with other metadata via `mergeDynamicMetadata` when both present).

### Edit 2.2: set sticky metadata for list-files header-only path

**Function:** `processListFilesRequestHeaders`

Current behavior sets headers:
- model placeholder
- backend header

Change to:
- Keep model header behavior only if still required by other logic.
- Keep in-memory `r.requestHeaders[internalapi.BackendNameHeaderKey] = backendName` if downstream code depends on it.
- **Remove** the `x-ai-eg-backend` header mutation previously added for header-based route matching (no consumer remains).
- Add `DynamicMetadata` in returned `ProcessingResponse` with `selected_backnd=backendName`.
- Keep `ClearRouteCache: true` — required so Envoy re-runs route matching after the metadata is set; without it the sticky route is never re-evaluated.

### Edit 2.3: set sticky metadata for file-id path

**Function:** `processFileIDInPathRequestHeaders`

After backend resolution:
- Build sticky metadata using resolved `backendName`.
- Set `DynamicMetadata` on response.
- **Remove** the sticky-route-matching header mutation (`x-ai-eg-backend`) from the response; metadata fully replaces it.
- Keep `ClearRouteCache: true` (same re-match requirement as Edit 2.2).

### Edit 2.4: include sticky metadata in upstream request-header response path

**Function:** `ProcessRequestHeaders` on `upstreamProcessor`

Current code builds:
- content-length metadata
- request-header metadata

Change:
- Build third metadata block from current sticky backend source (from `u.requestHeaders[internalapi.BackendNameHeaderKey]` or canonical backend value).
- Merge via `mergeDynamicMetadata` chain.
- Return merged metadata.

### Edit 2.5: ensure SetBackend always seeds sticky backend source used above

**Function:** `SetBackend`

Keep/confirm:
- `u.requestHeaders[internalapi.BackendNameHeaderKey] = extractAIServiceBackendName(backend.Backend.Name)`

No compatibility requirement for route-matching headers; this remains internal state for ext_proc logic + metadata generation.

---

## 3) `internal/controller/ai_gateway_route.go`

### Edit 3.1: remove per-backend sticky HTTPRoute reconciliation

**Function:** `syncAIGatewayRoute`

Delete the entire per-backend HTTPRoute block:
- `listExistingPerBackendHTTPRoutes` call + create/update loop over `extractUniqueBackendRefs`
- orphan deletion via `deleteOrphanedPerBackendResources`

The controller manages only the main HTTPRoute.

### Edit 3.2: delete sticky HTTPRoute generation helpers

Delete functions:
- `generateStickyHTTPRouteName`
- `listExistingPerBackendHTTPRoutes`
- `newStickyPerBackendRefHTTPRoute`
- `deleteOrphanedPerBackendResources` (AIGatewayRoute per-backend variant)
- `extractUniqueBackendRefs` / `extractBackendTimeouts` **only if** no remaining callers

### Edit 3.3: add stable rule section names on the main HTTPRoute

**Function:** `newHTTPRoute`

Set `HTTPRouteRule.Name` to a deterministic section name per spec rule (e.g. `rule-<index>` via `generalRuleSectionName(i)`), so the extension server can map translated Envoy routes back to `AIGatewayRoute.Spec.Rules` without relying on per-backend HTTPRoutes.

---

## 4) `internal/extensionserver/post_translate_modify.go`

### Edit 4.1: remove per-backend sticky HTTPRoute handling from cluster modification

**Function:** `maybeModifyCluster`

Delete all logic keyed off per-backend sticky HTTPRoutes:
- `-sticky` route-name suffix detection (`isStickyRoute`)
- owner/backend resolution from `AIGatewayStickyRoute*` labels (k8s `Get` of the per-backend HTTPRoute)
- `stickyBackendRefIndex` narrowing of `BackendRefs`

Resolve the spec rule purely from the main HTTPRoute rule index (e.g. via `resolveAIGatewayRouteRule`); every cluster keeps the full backendRef set of its owning rule. Per-backend endpoint selection remains the responsibility of ext_proc (`SetBackend`).

### Edit 4.2: synthesize per-backend sticky Envoy routes at translation time

**Function:** `enableRouterLevelAIGatewayExtProcOnRoute` (plus new helpers)

For each virtual host containing AI Gateway generated routes:
- Resolve the owning `AIGatewayRoute` (existing route-config/route metadata lookup patterns in this file).
- For each unique backend (`<ns>.<name>`) referenced by `AIGatewayRoute.Spec.Rules`:
  - Clone the translated Envoy route of a spec rule containing that backend (path match, cluster/route action, per-route filter config, timeouts).
  - Strip rule-selection header matchers from the clone (no model/route-selection headers on sticky routes).
  - Inject dynamic metadata predicate via `injectStickyBackendMetadataMatcher(route, stickyBackend)`:
    - namespace/filter: `internalapi.AIGatewayFilterMetadataNamespace`
    - key path: `internalapi.AIGatewaySelectedBackndMetadataKey`
    - exact string match: `<ns>.<name>`
  - Mark the clone with internal route metadata so ext_proc config lookup keeps working.
- Idempotency: skip synthesis if a route with the same sticky metadata matcher already exists.

Delete `stickyBackendValueFromRoute` (annotation-based detection of per-backend HTTPRoutes); the backend value now comes directly from the `AIGatewayRoute` spec.

### Edit 4.3: order sticky routes ahead of catch-all routes

**Function:** `enableRouterLevelAIGatewayExtProcOnRoute`

Keep `sortStickyRoutesFirst(vh.Routes)` (identified via `routeHasStickyMetadataMatcher`): sticky routes only match when the metadata is set, so placing them before general and route-not-found routes is safe and required (they would otherwise sit behind the catch-all).

### Edit 4.4: keep route-not-found and non-sticky behavior unchanged

**Function:** `enableRouterLevelAIGatewayExtProcOnRoute`

Guard so only synthesized sticky routes receive the metadata matcher; general/main routes remain unchanged.

### Edit 4.5: verify ext_proc filter accepts the sticky metadata namespace

**Functions:** `maybeModifyCluster` (upstream `ExternalProcessor` config, ~line 380) and `insertRouterLevelAIGatewayExtProc` (listener-level filter, ~line 830)

Envoy **discards** ext_proc-emitted dynamic metadata unless the namespace is allow-listed on the filter. Both `ExternalProcessor` configs must have:

```go
MetadataOptions: &extprocv3.MetadataOptions{
    ReceivingNamespaces: &extprocv3.MetadataOptions_MetadataNamespaces{
        Untyped: []string{aigv1b1.AIGatewayFilterMetadataNamespace},
    },
},
```

This is already present in both locations on this branch — keep it, and add a test asserting it (Edit 7.3) so the sticky path cannot silently break.

Note on ordering: route re-selection driven by the metadata only works from the **listener-level (router) ext_proc filter** during the request-headers phase combined with `ClearRouteCache`. The upstream-filter-level ext_proc runs after route/cluster selection and cannot influence sticky route matching; its metadata emission (Edit 2.4) is for observability/access-log parity only.

---

## 5) Tests - controller

## `internal/controller/ai_gateway_route_test.go`

### Edit 5.1: remove per-backend sticky HTTPRoute tests

**Tests to edit:**
- `TestAIGatewayRouterController_syncAIGatewayRoute`: remove all assertions that per-backend sticky HTTPRoutes are created/updated/orphan-deleted; assert only the main HTTPRoute is reconciled.
- `Test_newStickyPerBackendRefHTTPRoute_OpenAIFilesAPIStickyRouting`: delete (function under test is removed).

Add/keep assertions:
- Main HTTPRoute rules carry deterministic section names (`rule-<index>`).
- No HTTPRoute carries sticky labels/annotations (constants are removed).

---

## 6) Tests - ext_proc

## `internal/extproc/processor_impl_test.go`

### Edit 6.1: add tests for sticky metadata emission

Add/update tests for:
- `processListFilesRequestHeaders` response contains `DynamicMetadata` with:
  - namespace `internalapi.AIGatewayFilterMetadataNamespace`
  - key `internalapi.AIGatewaySelectedBackndMetadataKey`
  - expected backend value
- `processFileIDInPathRequestHeaders` same assertion for decoded/raw-id flows
- upstream `ProcessRequestHeaders` merges sticky metadata with existing content-length/request-attribute metadata

### Edit 6.2: relax/remove assertions that sticky routing requires backend header mutation

Where tests currently assert sticky header set specifically for route matching, update them to assert metadata-based behavior.

---

## 7) Tests - extension translation

## `internal/extensionserver/extensionserver_test.go` and/or `internal/extensionserver/post_translate_modify_test.go`

### Edit 7.1: add assertions for synthesized sticky routes

Create/update test fixture with an `AIGatewayRoute` (no per-backend HTTPRoutes) and verify the post-translate route config contains one synthesized sticky route per unique backend with:
- dynamic metadata match predicate keyed by:
  - namespace: `internalapi.AIGatewayFilterMetadataNamespace`
  - key: `internalapi.AIGatewaySelectedBackndMetadataKey`
  - value: expected sticky backend (`<ns>.<name>`)
- cluster/route action and per-route config cloned from the owning rule's translated route
- sticky routes ordered before general and route-not-found routes in the virtual host

### Edit 7.2: assert no per-backend HTTPRoute or header matcher dependence

- Ensure translated route matching no longer depends on `internalapi.BackendNameHeaderKey`.
- Ensure `maybeModifyCluster` no longer special-cases `-sticky` HTTPRoute names or sticky labels (remove/replace those test cases).
- Synthesis is idempotent: running post-translate twice does not duplicate sticky routes.

### Edit 7.3: assert ext_proc filter metadata plumbing

- Both generated `ExternalProcessor` configs (upstream cluster filter and listener-level filter) include `MetadataOptions.ReceivingNamespaces.Untyped` containing `aigv1b1.AIGatewayFilterMetadataNamespace`.
- Sticky route `DynamicMetadata` matcher uses `Filter == AIGatewayFilterMetadataNamespace` (same namespace the filter is allowed to write), guarding against namespace drift between emitter and matcher.

---

## 8) Optional cleanup (same PR if small, otherwise follow-up)

## `internal/extproc/processor_impl.go` + related tests

### Edit 8.1: remove comments/docstrings referring to "header-based sticky routing"

Replace with "dynamic metadata sticky routing" wording.

### Edit 8.2: evaluate whether backend header mutation can be reduced

If no longer needed by any downstream translator/protocol path, drop unnecessary header mutations.
(Keep internal request header map entries if still used for encoding/metrics.)

---

## Validation sequence

Run in this order:

1. `internal/controller` tests (per-backend sticky HTTPRoute generation removed; rule section names)
2. `internal/extproc` tests (metadata emission)
3. `internal/extensionserver` tests (xDS sticky route synthesis + matcher injection)
4. focused e2e/data-plane sticky file routing scenario (`/v1/files/...`)

Expected pass criteria:
- no per-backend sticky HTTPRoutes exist in the cluster (and orphans from previous versions are not recreated)
- sticky routes exist only in xDS and are selected only by dynamic metadata
- ext_proc-emitted `selected_backnd` metadata is accepted by Envoy (receiving namespace allow-listed) and triggers route re-match via `ClearRouteCache`
- client spoofed backend headers do not affect sticky backend selection
- no `x-ai-eg-backend` header is required for, or capable of, influencing route selection
- no regressions in file/batch ID decode + backend affinity flows

---

## Suggested commit slices

1. **Commit A:** constants cleanup + controller removal of per-backend sticky HTTPRoutes (+ rule section names) + controller tests
2. **Commit B:** ext_proc metadata emission + ext_proc tests
3. **Commit C:** extension server sticky route synthesis + matcher injection + extension tests
4. **Commit D (optional):** cleanup comments/redundant header mutations

This keeps review scope small and bisect-friendly.

---

## Recommendation: index vs name for encoded IDs

Short answer: **do not use index** as the backend identity in encoded IDs.

### Why index is risky

- Index is positional and can change when rules/backends are reordered.
- Indexes are opaque for debugging and difficult to migrate safely.
- A config-only reorder can silently break old encoded IDs.

### Better alternatives (ranked)

1. **Stable backend token (recommended)**
  - Add an immutable token per backend reference (e.g., generated UUID in `AIGatewayRoute` status/annotation or derived stable hash persisted as metadata).
  - Encode this token in file IDs instead of backend name/index.
  - Resolve token -> current backend at request time.

2. **Kubernetes UID of `AIServiceBackend`**
  - More stable than name across renames? Actually rename in Kubernetes usually means recreate, so UID changes.
  - Still better than index, but requires fallback behavior for recreated objects.

3. **Current qualified name (`namespace.name`)**
  - Human-readable and easy to debug.
  - Breaks on rename (your concern), but deterministic and simple.

### Practical path for this PR set

- Keep current `namespace.name` for now (to avoid scope explosion in sticky-metadata migration).
- Add a follow-up design for **stable backend token** in encoded IDs with:
  - versioned encoding payload (e.g., `aigw:v2;id:...;model:...;backend_token:...`)
  - dual decode support for v1 (name-based) and v2 (token-based)
  - migration period where new IDs are v2 but old v1 IDs keep working.
