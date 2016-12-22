package pilosactl

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"strings"

	"golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/agent"
)

type SSH struct {
	client *ssh.Client
	Stderr io.Writer
}

// NewSSH wraps up some of the complexity of using the crypto/ssh pacakge
// directly assuming you want to connect using public key auth and you can pass
// a keyfile or your key is accessible through ssh agent.
func NewSSH(host, username, keyfile string, stderr io.Writer) (*SSH, error) {
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
		return nil, fmt.Errorf("NewSHH failed Dial: %v ", err)
	}

	return &SSH{client: client, Stderr: stderr}, nil
}

func SSHClients(hosts []string, username, keyfile string, stderr io.Writer) ([]*SSH, error) {
	clients := make([]*SSH, len(hosts))
	for i, host := range hosts {
		client, err := NewSSH(host, username, keyfile, stderr)
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}
	return clients, nil
}

func (s *SSH) NewSession() (*ssh.Session, error) {
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
func (s *SSH) OpenFile(name string, perm string) (io.WriteCloser, error) {
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
