package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime/debug"

	"github.com/gorilla/mux"
	"github.com/jaffee/commandeer/pflag"
	pilosaclient "github.com/featurebasedb/featurebase/v3/client"
	"github.com/featurebasedb/featurebase/v3/idk"
	"github.com/featurebasedb/featurebase/v3/idk/api"
	"github.com/pkg/errors"
)

func main() {
	i := &ingester{
		PilosaHosts: []string{"localhost:10101"},
		HttpAddr:    "localhost:8080",
	}

	if err := pflag.LoadEnv(i, "INGESTER_", nil); err != nil {
		log.Fatal(err)
	}
	if i.DryRun {
		log.Printf("%+v\n", i)
		return
	}

	if err := i.Run(); err != nil {
		log.Fatal(err)
	}
}

// ingester is a HTTP handler which lets ingest data to pilosa over "programmatic ingest API".
// https://github.com/molecula/docs/blob/master/docs/internal/proposals/programmatic-ingest-api.md
type ingester struct {
	DryRun          bool     `help:"Dry run - just flag parsing."`
	PilosaHosts     []string `short:"" help:"Comma separated list of host:port pairs for FeatureBase."`
	PilosaGrpcHosts []string `short:"" help:"Comma separated list of host:port pairs for FeatureBase's gRPC endpoint."`
	BatchSize       uint     `short:"" help:"Batch size to use for ingest operations."`
	HttpAddr        string   `short:"" help:"HTTP address for Ingester."`

	client   *pilosaclient.Client
	router   *mux.Router
	tmplMain idk.Main

	AuthToken string `flag:"auth-token" help:"Authentication Token for FeatureBase"`
}

func (i *ingester) Run() error {
	client, err := pilosaclient.NewClient(i.PilosaHosts,
		pilosaclient.OptClientRetries(2),
		pilosaclient.OptClientTotalPoolSize(1000),
		pilosaclient.OptClientPoolSizePerRoute(400),
	)
	if err != nil {
		return errors.Wrap(err, "getting featurebase client")
	}
	defer client.Close()

	i.client = client

	i.router = mux.NewRouter()
	i.router.HandleFunc("/schema", i.postSchema).Methods("POST").Name("PostSchema")
	i.router.HandleFunc("/index/{index}", i.postIndex).Methods("POST").Name("PostIndex")

	m := *idk.NewMain()
	m.PilosaHosts = i.PilosaHosts
	m.PilosaGRPCHosts = i.PilosaGrpcHosts
	m.Pprof = ""
	m.Stats = ""
	m.PackBools = ""
	m.BatchSize = int(i.BatchSize)
	m.AuthToken = i.AuthToken
	i.tmplMain = m

	s := &http.Server{Addr: i.HttpAddr, Handler: i}
	return s.ListenAndServe()
}

// ServeHTTP handles an HTTP request.
func (i *ingester) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		if err := recover(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			stack := debug.Stack()
			log.Printf("PANIC: %s\n%s", err, stack)
		}
	}()

	i.router.ServeHTTP(w, r)
}

// POST /schema
func (i *ingester) postSchema(w http.ResponseWriter, r *http.Request) {
	body := r.Body
	defer body.Close()

	schema, ifNotExists, err := api.DecodeSchema(body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for idxName, idx := range schema.Indexes() {
		if err := i.client.CreateIndex(idx); err != nil {
			if errors.Is(err, pilosaclient.ErrIndexExists) {
				if ifNotExists[idxName] {
					continue
				}

				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		for fldName, fld := range idx.Fields() {
			if fldName == "_exists" {
				continue
			}

			if err := i.client.EnsureField(fld); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}
	}

	// Following code is not really needed,
	// but it's kind of informative if we want to check what's the current pilosa schema.
	// If we decide to keep it, we'll need to improve "pilosa/client",
	// because with current implementation we can serialize just map of indices.
	schema, err = i.client.Schema()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(schema.String())) //nolint: errcheck
}

// POST /index/{index}
func (i *ingester) postIndex(w http.ResponseWriter, r *http.Request) {
	index := mux.Vars(r)["index"]

	schema, err := i.client.Schema()
	if err != nil {
		http.Error(w, fmt.Sprintf("fetching schema: %v", err), http.StatusInternalServerError)
		return
	}

	idx, ok := schema.Indexes()[index]
	if !ok {
		http.Error(w, fmt.Sprintf("index %q not found", index), http.StatusNotFound)
		return
	}

	switch ctype := r.Header.Get("Content-Type"); ctype {
	case "":
		http.Error(w, "missing Content-Type header", http.StatusUnsupportedMediaType)
		return

	case "application/json":
		err = api.IngestJSON(idx, i.tmplMain, r.Body)
		if err != nil {
			errCode := http.StatusInternalServerError
			if errors.As(err, &api.TypeError{}) || errors.As(err, &api.ErrDuplicateElement{}) {
				errCode = http.StatusBadRequest
			}
			http.Error(w, err.Error(), errCode)
			return
		}

	default:
		http.Error(w, fmt.Sprintf("unsupported Content-Type %q", ctype), http.StatusUnsupportedMediaType)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("OK")) //nolint: errcheck
}
