package docker

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os/exec"
	"strings"
	"syscall"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/api/types/strslice"
	"github.com/docker/docker/api/types/volume"
	"github.com/docker/docker/pkg/stdcopy"
	"github.com/docker/go-connections/nat"
	"github.com/molecula/featurebase/v3/errors"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
)

type Composer struct {
	network    string
	containers []Container

	// volumes is a map of container to volume(s).
	volumes map[string][]Volume
}

type Volume struct {
	Type   string
	Source string
	Target string
}

// WithVolume creates a volume and registers it with the provided containers.
// Note that WithVolume must be called before WithService for the applicable
// containers.
func (c *Composer) WithVolume(volume Volume, containers ...Container) *Composer {
	if c.volumes == nil {
		c.volumes = make(map[string][]Volume)
	}

	// delete previous existing networks if any to avoid creation errors
	vol, _ := VolumeByName(volume.Source)
	if vol != nil {
		VolumeRemove(vol.Name)
		VolumePrune(vol.Name)
	}

	// TODO(jaffee) I don't understand what the other volume type is
	// being used for, but if you try to pass a bind mount here it
	// panics, so I put a janky "if" around it.
	if volume.Type != "bind" {
		_, err := VolumeCreate(volume.Source)
		if err != nil {
			panic(err)
		}
	}

	// Register volume with container(s).
	for _, cont := range containers {
		c.volumes[cont.Hostname()] = append(c.volumes[cont.Hostname()], volume)
	}

	return c
}

func (c *Composer) WithNetwork(networkName string) *Composer {
	// delete previous existing networks if any to avoid creation errors
	net, _ := NetworkByName(networkName)
	NetworkRemove(net.Name)
	NetworkPrune(net.Name)

	_, err := NetworkCreate(networkName)
	if err != nil {
		panic(err)
	}

	for _, c := range c.containers {
		if err = NetworkConnect(networkName, c.Hostname()); err != nil {
			panic(err)
		}
	}
	c.network = networkName

	return c
}

func (c *Composer) WithService(imageName string, container ...Container) *Composer {
	for _, cc := range container {
		// remove previous containers with the same name
		pc, _ := ContainerByName(cc.Hostname())
		for _, n := range pc.Names {
			ContainerStop(n)
			ContainerRemove(n)
			ContainerPrune(n)
		}

		_, err := ContainerCreate(imageName, cc, c.volumes[cc.Hostname()])
		if err != nil {
			panic(err)
		}
		c.containers = append(c.containers, cc)
		if c.network != "" {
			if err = NetworkConnect(c.network, cc.Hostname()); err != nil {
				panic(err)
			}
		}
	}

	return c
}

func (c *Composer) Up() error {
	for _, cc := range c.containers {
		if err := ContainerStartWithLogging(cc.Hostname()); err != nil {
			return err
		}
	}

	return nil
}

func (c *Composer) Down() error {
	var errs []error

	for _, cc := range c.containers {
		name := cc.Hostname()

		if err := ContainerStop(name); err != nil {
			log.Printf("Composer.Down error: ContainerStop: %s: %v", name, err)
			errs = append(errs, err)
		}

		if err := ContainerRemove(name); err != nil {
			log.Printf("Composer.Down error: ContainerRemove: %s: %v", name, err)
			errs = append(errs, err)
		}
	}

	if c.network != "" {
		if err := NetworkRemove(c.network); err != nil {
			log.Printf("Composer.Down error: NetworkRemove: %v", err)
			errs = append(errs, err)
		}
	}

	if len(errs) == 0 {
		return nil
	}

	var errString strings.Builder

	for i := range errs {
		errString.WriteString(fmt.Sprintf("(%d) ", i))
		errString.WriteString(errs[i].Error())
		errString.WriteString(" ")
	}

	return errors.New(errors.ErrUncoded, errString.String())
}

type Container interface {
	Hostname() string
	ExposedPorts() []string
	Env() map[string]string
	Cmd() []string
}

// ImagePull pulls the selected image from internet
func ImagePull(imageName string) error {
	// TODO temporal. Problems with authorization
	cmd := exec.Command("docker", "pull", imageName)
	if err := cmd.Run(); err != nil {
		if exiterr, ok := err.(*exec.ExitError); ok {
			if status, ok := exiterr.Sys().(syscall.WaitStatus); ok && status.ExitStatus() > 0 {
				return err
			}
		} else {
			return err
		}
	}
	return nil
}

func ContainerCreate(imageName string, c Container, volumes []Volume) (string, error) {
	exposedPorts := make(nat.PortSet)
	for _, p := range c.ExposedPorts() {
		exposedPorts[nat.Port(p)] = struct{}{}
	}

	var platform *v1.Platform

	mnts := make([]mount.Mount, 0)
	for _, v := range volumes {
		typ := v.Type
		if typ == "" {
			typ = string(mount.TypeVolume)
		}
		mnts = append(mnts, mount.Mount{
			Type:   mount.Type(typ),
			Source: v.Source,
			Target: v.Target,
		})
	}

	resp, err := DefaultClient.ContainerCreate(context.Background(),
		&container.Config{
			Image:        imageName,
			Env:          envToSlice(c.Env()),
			Hostname:     c.Hostname(),
			ExposedPorts: exposedPorts,
			Cmd:          strslice.StrSlice(c.Cmd()),
		},
		&container.HostConfig{
			// AutoRemove:      true,
			NetworkMode:     "bridge",
			PublishAllPorts: true,
			Mounts:          mnts,
		},
		&network.NetworkingConfig{}, platform, c.Hostname())
	if err != nil {
		return "", err
	}
	return resp.ID, err
}

func ContainerByName(containerName string) (types.Container, error) {
	f := filters.NewArgs()
	f.Add("name", containerName)

	l, err := DefaultClient.ContainerList(context.Background(), types.ContainerListOptions{
		Limit:   1,
		Filters: f,
	})
	if err != nil {
		return types.Container{}, err
	}
	if len(l) == 0 {
		return types.Container{}, fmt.Errorf("Container %s not found", containerName)
	}

	return l[0], nil
}

func ContainerPrune(containerName string) error {
	f := filters.NewArgs()
	f.Add("name", containerName)

	_, err := DefaultClient.ContainersPrune(context.Background(), f)
	return err
}

func ContainerStart(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}
	return DefaultClient.ContainerStart(context.Background(), c.ID, types.ContainerStartOptions{})
}

func ContainerStartWithLogging(containerName string) error {
	if err := ContainerStart(containerName); err != nil {
		return err
	}

	go ContainerLogs(containerName)

	return nil
}

func ContainerWait(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	var cond container.WaitCondition = container.WaitConditionNotRunning

	statusCh, errCh := DefaultClient.ContainerWait(context.Background(), c.ID, cond)
	_ = errCh

	status := <-statusCh

	log.Printf("ContainerWait status code: %d", status.StatusCode)
	if status.Error != nil {
		log.Printf("ContainerWait error: %s", status.Error.Message)
	}

	if err != nil {
		return errors.WithMessagef(err, "ContainerWait(%s: %s) error status code: %v", c.ID, containerName, status)
	}
	return nil
}

func ContainerRestart(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	return DefaultClient.ContainerRestart(context.Background(), c.ID, nil)
}

func ContainerPauseAndResume(containerName string, timeout time.Duration) (err error) {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	if err = DefaultClient.ContainerPause(context.Background(), c.ID); err != nil {
		return errors.WithMessagef(err, "ContainerPauseAndResume(%s: %s) error", c.ID, containerName)
	}

	time.AfterFunc(timeout, func() {
		if err = DefaultClient.ContainerUnpause(context.Background(), c.ID); err != nil {
			err = errors.WithMessagef(err, "ContainerUnpause(%s: %s) error", c.ID, containerName)
		}
	})

	return err
}

func ContainerLogs(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	reader, err := DefaultClient.ContainerLogs(context.Background(), c.ID,
		types.ContainerLogsOptions{
			ShowStderr: true,
			ShowStdout: true,
			Follow:     true,
		})
	if err != nil {
		return err
	}
	defer reader.Close()

	r, w := io.Pipe()

	go func() {
		stdcopy.StdCopy(w, w, reader)
	}()

	br := bufio.NewReader(r)
	for {
		line, err := br.ReadString('\n')

		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}

		fmt.Print("LOGS from ", containerName, ": ", line)
	}

	return nil
}

func ContainerStop(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	return DefaultClient.ContainerStop(context.Background(), c.ID, nil)
}

func ContainerRemove(containerName string) error {
	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	return DefaultClient.ContainerRemove(
		context.Background(),
		c.ID,
		types.ContainerRemoveOptions{
			Force:         true,
			RemoveVolumes: true,
		},
	)
}

func VolumeCreate(volumeName string) (string, error) {
	res, err := DefaultClient.
		VolumeCreate(
			context.Background(),
			volume.VolumeCreateBody{
				Name: volumeName,
			},
		)

	return res.Name, err
}

func VolumeByName(volumeName string) (*types.Volume, error) {
	f := filters.NewArgs()
	f.Add("name", volumeName)

	n, err := DefaultClient.VolumeList(context.Background(), f)
	if err != nil {
		return nil, err
	}
	if len(n.Volumes) == 0 {
		return nil, fmt.Errorf("volume %s not found", volumeName)
	}

	return n.Volumes[0], nil
}

func VolumeRemove(volumeName string) error {
	n, err := VolumeByName(volumeName)
	if err != nil {
		return err
	}

	return DefaultClient.VolumeRemove(context.Background(), n.Name, false)
}

func VolumePrune(volumeName string) error {
	f := filters.NewArgs()
	f.Add("name", volumeName)

	_, err := DefaultClient.VolumesPrune(context.Background(), f)
	return err
}

func NetworkCreate(networkName string) (string, error) {
	res, err := DefaultClient.
		NetworkCreate(
			context.Background(),
			networkName,
			types.NetworkCreate{
				CheckDuplicate: true,
			},
		)

	return res.ID, err
}

func NetworkByName(networkName string) (types.NetworkResource, error) {
	f := filters.NewArgs()
	f.Add("name", networkName)

	n, err := DefaultClient.NetworkList(context.Background(), types.NetworkListOptions{
		Filters: f,
	})
	if err != nil {
		return types.NetworkResource{}, err
	}
	if len(n) == 0 {
		return types.NetworkResource{}, fmt.Errorf("network %s not found", networkName)
	}

	return n[0], nil
}

func NetworkRemove(networkName string) error {
	n, err := NetworkByName(networkName)
	if err != nil {
		return err
	}

	return DefaultClient.NetworkRemove(context.Background(), n.ID)
}

func NetworkConnect(networkName, containerName string) error {
	n, err := NetworkByName(networkName)
	if err != nil {
		return err
	}

	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	return DefaultClient.NetworkConnect(context.Background(), n.ID, c.ID, &network.EndpointSettings{})
}

func NetworkDisconnect(networkName, containerName string) error {
	n, err := NetworkByName(networkName)
	if err != nil {
		return err
	}

	c, err := ContainerByName(containerName)
	if err != nil {
		return err
	}

	return DefaultClient.NetworkDisconnect(context.Background(), n.ID, c.ID, true)
}

func NetworkPrune(networkName string) error {
	f := filters.NewArgs()
	f.Add("name", networkName)

	_, err := DefaultClient.NetworksPrune(context.Background(), f)
	return err
}

func envToSlice(env map[string]string) []string {
	var out = make([]string, 0, len(env))
	for k, v := range env {
		out = append(out, fmt.Sprintf("%s=%s", k, v))
	}

	return out
}
