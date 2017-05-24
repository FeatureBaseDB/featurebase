package pilosa_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gorilla/mux"
	"github.com/pilosa/pilosa"
)

type TestHandlerPlugin struct{}

func NewTestHandlerPlugin(handler *pilosa.Handler) pilosa.HandlerPlugin {
	return &TestHandlerPlugin{}
}

func (p *TestHandlerPlugin) RegisterHandlers(router *mux.Router) {
	router.HandleFunc("/hello", p.helloHandler)
}

func (p *TestHandlerPlugin) helloHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "success!")
}

func TestHandlerPlugin_Registered(t *testing.T) {
	pilosa.RegisterHandlerPlugin("/test", NewTestHandlerPlugin)
	h := NewHandler()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, MustNewHTTPRequest("GET", "/test/hello", nil))
	if w.Code != http.StatusOK {
		t.Fatalf("unexpected status code: %d %s", w.Code, w.Body.String())
	} else if body := w.Body.String(); body != "success!" {
		t.Fatalf("unexpected body: %q", body)
	}
}
