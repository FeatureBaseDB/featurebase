package inspector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/molecula/featurebase/v3/dax"
	mdshttp "github.com/molecula/featurebase/v3/dax/mds/http"
	queryerhttp "github.com/molecula/featurebase/v3/dax/queryer/http"
	"github.com/molecula/featurebase/v3/dax/test/docker"
	"github.com/molecula/featurebase/v3/errors"
)

type Inspector struct {
	cli *client.Client

	// containers is a map of container name to container ID.
	containers map[string]string
}

func NewInspector() (*Inspector, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv)
	if err != nil {
		return nil, errors.Wrap(err, "getting new client with opts")
	}

	containers, err := cli.ContainerList(context.Background(), types.ContainerListOptions{})
	if err != nil {
		return nil, errors.Wrap(err, "getting container list")
	}

	m := make(map[string]string)
	for _, container := range containers {
		if len(container.Names) == 0 {
			continue
		}
		m[container.Names[0]] = container.ID[:10]
	}

	return &Inspector{
		cli:        cli,
		containers: m,
	}, nil
}

type ExecRequest struct {
	address dax.Address
	path    string
	data    interface{}
}

func NewExecRequest(address dax.Address, path string, data interface{}) *ExecRequest {
	return &ExecRequest{
		address: address,
		path:    path,
		data:    data,
	}
}

func (e *ExecRequest) Cmd() []string {
	address := e.address
	if address == "" {
		address = "http://localhost:8080"
	}
	uri := fmt.Sprintf("%s/%s", address, e.path)

	if e.data == nil {
		return []string{
			"curl",
			uri,
		}
	}

	var dArgs string
	switch v := e.data.(type) {
	case string:
		dArgs = v
	case *dax.Table,
		*dax.QualifiedTable,
		mdshttp.RegisterNodeRequest,
		mdshttp.SnapshotFieldKeysRequest,
		mdshttp.SnapshotShardRequest,
		mdshttp.TranslateNodesRequest,
		queryerhttp.QueryRequest,
		queryerhttp.SQLRequest,
		dax.QualifiedTableID:
		b, err := json.Marshal(v)
		if err != nil {
			panic(err) // FIX THIS
		}
		dArgs = string(b)
	default:
		log.Printf("unhandled return type: %T", e.data)
		dArgs = "{}"
	}

	return []string{
		"curl",
		"-d " + dArgs,
		uri,
	}
}

type ExecResponse struct {
	stdOut   string
	stdErr   string
	exitCode int
}

func (e *ExecResponse) Out() string {
	return strings.TrimSpace(e.stdOut)
}

func (e *ExecResponse) Err() string {
	return strings.TrimSpace(e.stdErr)
}

func (i *Inspector) Close() error {
	if i.cli != nil {
		return i.cli.Close()
	}
	return nil
}

// ExecResp is a helper method which runs exec() then resp().
func (i *Inspector) ExecResp(ctx context.Context, container docker.Container, command []string) (ExecResponse, error) {
	var execResp ExecResponse

	exec, err := i.exec(ctx, container, command)
	if err != nil {
		return execResp, err
	}

	return i.resp(context.Background(), exec.ID)
}

func (i *Inspector) exec(ctx context.Context, container docker.Container, command []string) (types.IDResponse, error) {
	hostName := container.Hostname()

	containerID, ok := i.containers[slash(hostName)]
	if !ok {
		return types.IDResponse{}, errors.Errorf("invalid container: %s", hostName)
	}

	config := types.ExecConfig{
		AttachStderr: true,
		AttachStdout: true,
		Cmd:          command,
	}

	return i.cli.ContainerExecCreate(ctx, containerID, config)
}

func (i *Inspector) resp(ctx context.Context, id string) (ExecResponse, error) {
	var execResp ExecResponse

	resp, err := i.cli.ContainerExecAttach(ctx, id, types.ExecStartCheck{})
	if err != nil {
		return execResp, err
	}
	defer resp.Close()

	// read the output
	var outBuf, errBuf bytes.Buffer
	outputDone := make(chan error)

	go func() {
		// StdCopy demultiplexes the stream into two buffers
		_, err = stdcopy.StdCopy(&outBuf, &errBuf, resp.Reader)
		outputDone <- err
	}()

	select {
	case err := <-outputDone:
		if err != nil {
			return execResp, err
		}
		break

	case <-ctx.Done():
		return execResp, ctx.Err()
	}

	stdout, err := io.ReadAll(&outBuf)
	if err != nil {
		return execResp, err
	}
	stderr, err := io.ReadAll(&errBuf)
	if err != nil {
		return execResp, err
	}

	res, err := i.cli.ContainerExecInspect(ctx, id)
	if err != nil {
		return execResp, err
	}

	execResp.exitCode = res.ExitCode
	execResp.stdOut = string(stdout)
	execResp.stdErr = string(stderr)
	return execResp, nil
}

// slash is a helper functions which addresses the fact that the lower level
// docker inspect functions actually store the container name with a leading
// slash. See this for more info: https://github.com/moby/moby/issues/6705
func slash(s string) string {
	return "/" + s
}
