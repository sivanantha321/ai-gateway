package backendauth

import (
	"fmt"
	"os"

	corev3 "github.com/envoyproxy/go-control-plane/envoy/config/core/v3"
	extprocv3 "github.com/envoyproxy/go-control-plane/envoy/service/ext_proc/v3"

	"github.com/envoyproxy/ai-gateway/filterconfig"
)

// apiKeyHandler implements [Handler] for api key authz.
type apiKeyHandler struct {
	fileName string
}

func NewAPIKeyHandler(auth *filterconfig.APIKeyAuth) (Handler, error) {
	return &apiKeyHandler{
		fileName: auth.Filename,
	}, nil
}

// Do implements [Handler.Do].
//
// Extracts the api key from the local file and set it as an authorization header.
func (a *apiKeyHandler) Do(requestHeaders map[string]string, headerMut *extprocv3.HeaderMutation, _ *extprocv3.BodyMutation) error {
	// TODO: Stop reading a file on request path.
	secret, err := os.ReadFile(a.fileName)
	if err != nil {
		return err
	}

	requestHeaders["Authorization"] = fmt.Sprintf("Bearer %s", string(secret))
	headerMut.SetHeaders = append(headerMut.SetHeaders, &corev3.HeaderValueOption{
		Header: &corev3.HeaderValue{Key: "Authorization", RawValue: []byte(requestHeaders["Authorization"])},
	})

	return nil
}
