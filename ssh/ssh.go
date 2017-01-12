package ssh

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"strings"

	"errors"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type Client struct {
	client *ssh.Client
	Stderr io.Writer
}

// NewClient wraps up some of the complexity of using the crypto/ssh pacakge
// directly assuming you want to connect using public key auth and you can pass
// a keyfile or your key is accessible through ssh agent.
func NewClient(host, username, keyfile string, stderr io.Writer) (*Client, error) {
	if username == "" {
		user, err := user.Current()
		if err != nil {
			return nil, err
		}
		username = user.Username
	}

	var auth ssh.AuthMethod
	if keyfile == "" {
		sshAgent, err := net.Dial("unix", os.Getenv("SSH_AUTH_SOCK"))
		if err != nil {
			return nil, err
		}
		auth = ssh.PublicKeysCallback(agent.NewClient(sshAgent).Signers)
	} else {
		return nil, fmt.Errorf("using a keyfile is unimplemented")
	}

	config := &ssh.ClientConfig{
		User: username,
		Auth: []ssh.AuthMethod{auth},
	}

	if strings.Index(host, ":") == -1 {
		host = host + ":22"
	}

	client, err := ssh.Dial("tcp", host, config)
	if err != nil {
		return nil, fmt.Errorf("NewSSH failed Dial - host: %v, config: %v, err: %v ", host, config, err)
	}

	return &Client{client: client, Stderr: stderr}, nil
}

type Fleet []*Client

func NewFleet(hosts []string, username, keyfile string, stderr io.Writer) (Fleet, error) {
	hosts = DedupHosts(hosts)
	clients := make([]*Client, len(hosts))
	for i, host := range hosts {
		client, err := NewClient(host, username, keyfile, stderr)
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}
	return clients, nil
}

// DedupHosts takes a slice of hosts, strips off any specified ports, and
// returns a de-duplicated slice of hosts.
func DedupHosts(hosts []string) []string {
	seenHosts := make(map[string]bool)
	ret := []string{}
	for _, h := range hosts {
		colonIdx := strings.Index(h, ":")
		if colonIdx != -1 {
			h = h[:colonIdx]
		}
		if !seenHosts[h] {
			ret = append(ret, h)
		}
		seenHosts[h] = true
	}
	return ret
}

func (s *Client) NewSession() (*ssh.Session, error) {
	return s.client.NewSession()
}

type remoteFile struct {
	w    io.WriteCloser
	sess *ssh.Session
}

func (r *remoteFile) Write(p []byte) (n int, err error) {
	return r.w.Write(p)
}

func (r *remoteFile) Close() error {
	errc := r.w.Close()
	errw := r.sess.Wait()
	if errc != nil || errw != nil {
		return fmt.Errorf("error closing remote file - close: '%v', wait: '%v'", errc, errw)
	}
	return nil
}

// OpenFile creates or truncates an existing file of the given name on the
// remote host, and returns a WriteCloser which will write to that file. perm
// will be passed directly to chmod to set the file permissions. rm, touch,
// chmod, cat and support for semicolons, double ampersand, and output
// redirection (>>) must be available in the remote shell.
func (s *Client) OpenFile(name, perm string) (io.WriteCloser, error) {
	sess, err := s.NewSession()
	if err != nil {
		return nil, err
	}
	w, err := sess.StdinPipe()
	if err != nil {
		return nil, err
	}
	if perm == "" {
		perm = "0664"
	}
	err = sess.Start(fmt.Sprintf("rm %v; touch %v && chmod %v %v && cat >> %v", name, name, perm, name, name))
	if err != nil {
		return nil, err
	}

	return &remoteFile{w: w, sess: sess}, nil
}

type multiWriteCloser struct {
	wcs []io.WriteCloser
	ws  []io.Writer
}

func newMultiWriteCloser() *multiWriteCloser {
	return &multiWriteCloser{
		wcs: make([]io.WriteCloser, 0),
		ws:  make([]io.Writer, 0),
	}
}

func (mwc *multiWriteCloser) add(wc io.WriteCloser) {
	mwc.wcs = append(mwc.wcs, wc)
	mwc.ws = append(mwc.ws, wc)
}

func (mwc *multiWriteCloser) Write(p []byte) (n int, err error) {
	mw := io.MultiWriter(mwc.ws...)
	return mw.Write(p)
}

func (mwc *multiWriteCloser) Close() error {
	errStr := ""
	for _, wc := range mwc.wcs {
		err := wc.Close()
		if err != nil {
			errStr = errStr + "; " + err.Error()
		}
	}
	if errStr != "" {
		return errors.New(errStr)
	}
	return nil
}

func (sf Fleet) OpenFile(name, perm string) (io.WriteCloser, error) {
	writers := newMultiWriteCloser()
	for _, cli := range sf {
		wc, err := cli.OpenFile(name, "+x")
		if err != nil {
			return nil, err
		}
		writers.add(wc)
	}
	return writers, nil
}

// WriteFile writes all of data (until EOF) into a file with the given name on
// each of the hosts in the fleet. It sets the permissions on the file to <perm>
// which is any valid input to chmod. If <perm> is the empty string, it defaults
// to 0664
func (sf Fleet) WriteFile(name, perm string, data io.Reader) error {
	wc, err := sf.OpenFile(name, perm)
	if err != nil {
		return err
	}

	_, err = io.Copy(wc, data)
	if err != nil {
		return err
	}
	return wc.Close()
}
