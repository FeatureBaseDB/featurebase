package creator

import (
	"fmt"
	"io"
	"net"
	"path"
	"strconv"
	"sync"

	"time"

	"github.com/BurntSushi/toml"
	"github.com/pilosa/pilosa"
	"github.com/pilosa/pilosa/build"
	pssh "github.com/pilosa/pilosa/ssh"
	"golang.org/x/crypto/ssh"
)

type RemoteCluster struct {
	ClusterHosts []string
	ReplicaN     int
	SSHUser      string
	Keyfile      string
	Key          []byte
	GoMaxProcs   int
	CopyBinary   bool
	GOOS         string
	GOARCH       string
	Stderr       io.Writer
	wg           *sync.WaitGroup
	logs         []io.Reader
	sessions     []*ssh.Session
	pipeRs       []*io.PipeReader
	pipeWs       []*io.PipeWriter
	stdins       []io.WriteCloser
}

// Start creates a configuration for each host in the cluster, copies it to the
// node, and starts the pilosa process on the remote host.
func (c *RemoteCluster) Start() error {
	c.logs = make([]io.Reader, 0)
	if len(c.ClusterHosts) == 0 {
		return fmt.Errorf("no type or hosts specified - cannot continue")
	}

	fleet, err := pssh.NewFleet(c.ClusterHosts, c.SSHUser, c.Keyfile, c.Stderr)
	if err != nil {
		return fmt.Errorf("connecting to cluster hosts: %v", err)
	}
	if c.CopyBinary {
		fmt.Fprintf(c.Stderr, "create: building pilosa binary with GOOS=%v and GOARCH=%v to copy to hosts", c.GOOS, c.GOARCH)

		pkg := "github.com/pilosa/pilosa/cmd/pilosa"
		bin, err := build.Binary(pkg, c.GOOS, c.GOARCH)
		if err != nil {
			return fmt.Errorf("building binary: %v", err)
		}

		err = fleet.WriteFile(path.Base(pkg), "+x", bin)
		if err != nil {
			return fmt.Errorf("writing binary to fleet: %v", err)
		}

	}

	// build config
	conf := pilosa.NewConfigForHosts(c.ClusterHosts)
	conf.Cluster.ReplicaN = c.ReplicaN

	// copy config to remote hosts and start pilosa
	c.wg = &sync.WaitGroup{}
	for _, hostport := range c.ClusterHosts {

		// Set up config for this host
		host, port, err := net.SplitHostPort(hostport)
		if err != nil {
			return fmt.Errorf("splitting hostport: %v", err)
		}
		conf.Host = hostport
		conf.DataDir = "~/.pilosa" + port

		// Get client for host
		client, err := fleet.Get(host)
		if err != nil {
			return fmt.Errorf("connecting to host: %v", err)
		}
		configname := "pilosa" + port + ".conf"
		w, err := client.OpenFile(configname, "")
		if err != nil {
			return fmt.Errorf("opening remote config file: %v", err)
		}
		enc := toml.NewEncoder(w)
		err = enc.Encode(conf)
		if err != nil {
			return fmt.Errorf("encoding config: %v", err)
		}
		err = w.Close()
		if err != nil {
			return err
		}

		// Start pilosa on remote host
		sess, err := client.NewSession()
		if err != nil {
			return err
		}
		// Have to request pty in order to be able to kill remote process
		// reliably.
		modes := ssh.TerminalModes{
			ssh.ISIG: 1,
			ssh.ECHO: 0,
		}
		err = sess.RequestPty("vt100", 40, 80, modes)
		if err != nil {
			return fmt.Errorf("request pty error: %v", err)
		}
		pipeR, pipeW := io.Pipe()
		sess.Stdout = pipeW
		sess.Stderr = pipeW
		inpipe, err := sess.StdinPipe()
		if err != nil {
			return err
		}
		c.logs = append(c.logs, pipeR)
		c.sessions = append(c.sessions, sess)
		c.pipeRs = append(c.pipeRs, pipeR)
		c.pipeWs = append(c.pipeWs, pipeW)
		c.stdins = append(c.stdins, inpipe)

		gomaxprocsString := ""
		if c.GoMaxProcs != 0 {
			gomaxprocsString = "GOMAXPROCS=" + strconv.Itoa(c.GoMaxProcs) + " "
		}

		err = sess.Start("PATH=.:$PATH " + gomaxprocsString + "pilosa -config " + configname)
		if err != nil {
			return err
		}
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			err = sess.Wait()
			if err != nil {
				fmt.Fprintf(c.Stderr, "problem with remote pilosa process: %v\n", err)
			}
		}()
	}
	return nil
}

func (c *RemoteCluster) Hosts() []string   { return c.ClusterHosts }
func (c *RemoteCluster) Logs() []io.Reader { return c.logs }
func (c *RemoteCluster) Shutdown() error {
	for i, sess := range c.sessions {
		var err error
		_, err = c.stdins[i].Write([]byte{3}) // Send Control C
		if err != nil {
			fmt.Fprintf(c.Stderr, "Error write-signaling remote process: %v\n", err)
		}
		// signaling isn't supported by many ssh servers - hence the hack above
		err = sess.Signal(ssh.SIGINT)
		if err != nil {
			fmt.Fprintf(c.Stderr, "Error signaling remote process: %v\n", err)
		}
	}

	done := make(chan struct{}, 1)
	go func() {
		c.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-time.After(time.Second * 5):
		for _, sess := range c.sessions {
			err := sess.Close()
			if err != nil {
				fmt.Fprintf(c.Stderr, "Error closing remote session: %v\n", err)
			}
		}
		return fmt.Errorf("timed out waiting for remote processes to exit")
	}

}
