package http

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/featurebasedb/featurebase/v3/dax"
	"github.com/featurebasedb/featurebase/v3/errors"
)

var ErrNotImplemented = errors.New(errors.ErrUncoded, "not implemented")

// Ensure type implements interface.
var _ dax.AddressManager = &AddressManager{}

// AddressManager is an http implementation of the AddressManager interface.
type AddressManager struct {
	mdsAddress dax.Address
}

func NewAddressManager(mdsAddress dax.Address) *AddressManager {
	return &AddressManager{
		mdsAddress: mdsAddress,
	}
}

func (m *AddressManager) AddAddresses(ctx context.Context, addr ...dax.Address) error {
	// Not implemented because it's currently not used
	return ErrNotImplemented
}

func (m *AddressManager) RemoveAddresses(ctx context.Context, addrs ...dax.Address) error {
	if len(addrs) == 0 {
		return nil
	}

	if m.mdsAddress == "" {
		return errors.Errorf("mdsAddress is empty; could not deregister: %s", addrs)
	}
	url := fmt.Sprintf("%s/deregister-nodes", m.mdsAddress.WithScheme("http"))
	log.Printf("SEND deregister-nodes to: %s\n", url)

	req := DeregisterNodesRequest{
		Addresses: addrs,
	}

	// Encode the request.
	postBody, err := json.Marshal(req)
	if err != nil {
		return errors.Wrap(err, "marshalling deregister node request to json")
	}
	requestBody := bytes.NewBuffer(postBody)

	// Post the request.
	request, _ := http.NewRequest(http.MethodPost, url, requestBody)
	request.Header.Add("Content-Type", "application/json")
	request.Header.Add("Accept", "application/json")

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return errors.Wrap(err, "doing deregister node request")
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return errors.Errorf("status code: %d: %s", resp.StatusCode, b)
	}

	return nil
}
